package main

import (
	"encoding/binary"
	"io"
	"log"
	"os/exec"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
	opus "gopkg.in/hraban/opus.v2"
)

// JitterBuffer manages packet buffering for smooth playback
type JitterBuffer struct {
	buffer       [10][]byte // Ring buffer: 10 packets = 200ms at 20ms/packet
	writeIdx     int        // Write position
	readIdx      int        // Read position
	count        int        // Current buffered packets
	prebuffer    int        // Prebuffer count (3 packets = 60ms for low latency)
	started      bool       // Playback started flag
	underrunCount int       // Track drops/underruns for diagnostics
	mu           sync.Mutex
	cond         *sync.Cond // Signal for buffer state changes
}

// NewJitterBuffer creates jitter buffer with prebuffering
func NewJitterBuffer(prebufferPackets int) *JitterBuffer {
	jb := &JitterBuffer{
		prebuffer: prebufferPackets,
		started:   false,
	}
	jb.cond = sync.NewCond(&jb.mu)
	return jb
}

// Enqueue adds decoded PCM packet to buffer
func (jb *JitterBuffer) Enqueue(pcmBytes []byte) bool {
	jb.mu.Lock()
	defer jb.mu.Unlock()

	// Smart overflow handling: drop oldest packet if buffer near full
	if jb.count >= len(jb.buffer)-1 {
		// Buffer at 9/10 or full - drop OLDEST packet (maintain recency)
		if jb.count > 0 {
			jb.readIdx = (jb.readIdx + 1) % len(jb.buffer)
			jb.count--
			jb.underrunCount++ // Count as underrun for stats
			log.Printf("Jitter buffer near full (%d) - dropped oldest packet to prevent overflow", jb.count+1)
		}
	}

	// Add to ring buffer
	jb.buffer[jb.writeIdx] = pcmBytes
	jb.writeIdx = (jb.writeIdx + 1) % len(jb.buffer)
	jb.count++

	// Start playback after prebuffer reached
	if !jb.started && jb.count >= jb.prebuffer {
		jb.started = true
		log.Printf("Jitter buffer prebuffered (%d packets, %dms) - starting playback", 
			jb.prebuffer, jb.prebuffer*20)
	}

	// Signal waiting playback thread
	jb.cond.Signal()
	return true
}

// Dequeue retrieves next packet or generates PLC
func (jb *JitterBuffer) Dequeue(decoder *opus.Decoder) []byte {
	jb.mu.Lock()
	defer jb.mu.Unlock()

	// Wait for prebuffer on first playback
	for !jb.started {
		jb.cond.Wait()
	}

	// Return buffered packet if available
	if jb.count > 0 {
		pcmBytes := jb.buffer[jb.readIdx]
		jb.readIdx = (jb.readIdx + 1) % len(jb.buffer)
		jb.count--
		return pcmBytes
	}

	// Buffer empty: generate PLC audio (underrun or intentional drop)
	jb.underrunCount++
	if jb.underrunCount <= 5 || jb.underrunCount%50 == 0 {
		log.Printf("Jitter buffer empty #%d - using PLC", jb.underrunCount)
	}

	// Generate PLC audio (20ms silence/comfort noise)
	pcmData := make([]int16, 1920) // 960 samples/channel x 2 channels (48kHz)
	n, err := decoder.Decode(nil, pcmData) // nil = PLC mode
	if err != nil || n == 0 {
		// Return silence if PLC fails
		return make([]byte, 1920*2)
	}

	// Opus returns samples per-channel, need to account for stereo interleaved
	channels := 2
	totalSamples := n * channels // 960 × 2 = 1920 for stereo (48kHz)
	
	// Convert PLC to bytes
	pcmBytes := make([]byte, totalSamples*2)
	for i := 0; i < totalSamples; i++ {
		binary.LittleEndian.PutUint16(pcmBytes[i*2:], uint16(pcmData[i]))
	}
	return pcmBytes
}

// GetStats returns buffer statistics
func (jb *JitterBuffer) GetStats() (buffered int, underruns int) {
	jb.mu.Lock()
	defer jb.mu.Unlock()
	return jb.count, jb.underrunCount
}

// AudioPlayback receives audio from WebRTC and plays to speaker
type AudioPlayback struct {
	peerConnection *webrtc.PeerConnection
	cmd            *exec.Cmd
	stdin          io.WriteCloser
	decoder        *opus.Decoder
	jitterBuffer   *JitterBuffer
	running        bool
	deviceInfo     *AudioDeviceInfo
	mu             sync.Mutex
	wg             sync.WaitGroup // Track goroutines
}

// NewAudioPlayback creates audio playback instance
func NewAudioPlayback() *AudioPlayback {
	deviceInfo := DetectAudioDevices()
	return &AudioPlayback{
		running:      false,
		deviceInfo:   deviceInfo,
		jitterBuffer: NewJitterBuffer(5), // 5 packets = 100ms prebuffer (stability vs latency)
	}
}

// StartDecoder initializes the Opus decoder and FFmpeg for audio playback
func (a *AudioPlayback) StartDecoder() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.running {
		return nil
	}

	// Initialize Opus decoder (48kHz stereo)
	// NOTE: Opus 48kHz -> FFmpeg outputs 48kHz -> AEC receives 44.1kHz (FFmpeg resamples)
	var err error
	a.decoder, err = opus.NewDecoder(48000, 2)
	if err != nil {
		log.Printf("ERROR: Failed to create Opus decoder: %v", err)
		return err
	}
	log.Println("Opus decoder initialized (48kHz stereo)")

	// Set speaker volume for clear, loud playback
	log.Println("Audio: Preparing speaker for playback...")
	if err := a.deviceInfo.SetSpeakerVolume(); err != nil {
		log.Printf("Warning: Speaker volume setting failed, continuing anyway: %v", err)
	}

	// Build FFmpeg command with device detection
	// Now using PCM input (s16le) instead of raw Opus
	var args []string
	
	if a.deviceInfo.UsePulseAudio {
		// PulseAudio output - Let PulseAudio handle format conversion (mono/stereo, sample rate)
		// FFmpeg sends stereo 48kHz, PulseAudio converts to device native format automatically
		args = []string{
			"-f", "s16le",        // PCM 16-bit signed little-endian
			"-ar", "48000",       // Input rate: 48kHz (Opus decoded)
			"-ac", "2",           // Input channels: Stereo (Opus decoded)
			"-i", "pipe:0",       // Read from stdin
			"-af", "aformat=sample_fmts=s16:channel_layouts=stereo,highpass=f=80,lowpass=f=8000,volume=6.0,acompressor=threshold=-10dB:ratio=4:attack=5:release=50",
			"-f", "pulse",        // Output to PulseAudio (handles device conversion)
			a.deviceInfo.OutputDevice,
		}
	} else {
		// ALSA fallback with enhanced audio processing
		args = []string{
			"-f", "s16le",        // PCM 16-bit signed little-endian
			"-ar", "48000",       // 48kHz (Opus requirement, no AEC in ALSA)
			"-ac", "2",           // Stereo
			"-i", "pipe:0",       // Read from stdin
			"-af", "highpass=f=80,lowpass=f=8000,volume=6.0,acompressor=threshold=-10dB:ratio=4:attack=5:release=50",
			"-f", "alsa",         // Output to ALSA
			a.deviceInfo.OutputDevice,
		}
	}
	
	a.cmd = exec.Command("ffmpeg", args...)

	a.stdin, err = a.cmd.StdinPipe()
	if err != nil {
		return err
	}

	if err := a.cmd.Start(); err != nil {
		log.Printf("ERROR: Failed to start audio playback: %v", err) // vnextthongnv
		return err
	}

	a.running = true
	
	audioSystem := "PulseAudio"
	aecInfo := ""
	if a.deviceInfo.UsePulseAudio && a.deviceInfo.OutputDevice == "echocancel_sink" {
		aecInfo = " [48kHz -> PulseAudio resample AEC 44.1kHz]"
	}
	if !a.deviceInfo.UsePulseAudio {
		audioSystem = "ALSA"
	}
	log.Printf("Audio playback started (%s%s)", audioSystem, aecInfo)

	return nil
}

// DecodeLoop reads RTP packets and decodes to jitter buffer
func (a *AudioPlayback) DecodeLoop(track *webrtc.TrackRemote) {
	defer a.wg.Done()
	
	packetCount := 0
	emptyPayloadCount := 0
	
	log.Println("Decoder thread started")
	
	for a.running {
		// Read RTP packet with Opus payload
		rtp, _, err := track.ReadRTP()
		if err != nil {
			if err != io.EOF && a.running {
				log.Printf("Audio decode read error: %v", err)
			}
			break
		}

		packetCount++
		
		// Empty RTP payloads are normal: browsers send them during DTX
		// (silence) or as keepalive. Skip silently.
		if len(rtp.Payload) == 0 {
			emptyPayloadCount++
			continue
		}
		
		// Log first few packets for debugging
		if packetCount <= 3 {
			log.Printf("RTP packet %d: PayloadType=%d, Payload=%d bytes, Timestamp=%d", 
				packetCount, rtp.PayloadType, len(rtp.Payload), rtp.Timestamp)
		}

		// Decode Opus payload to PCM
		pcmData := make([]int16, 1920) // 960 samples/channel × 2 channels = 1920 total (48kHz)
		n, err := a.decoder.Decode(rtp.Payload, pcmData)
		if err != nil {
			if a.running {
				log.Printf("Opus decode error (payload %d bytes, PT=%d): %v", 
					len(rtp.Payload), rtp.PayloadType, err)
			}
			continue
		}
		
		// Opus returns samples per-channel (n=960), but pcmData is stereo interleaved (1920)
		channels := 2
		totalSamples := n * channels // 960 × 2 = 1920 for stereo (48kHz)
		
		// Log successful decode on first packet
		if packetCount == 1 {
			log.Printf("First Opus decode successful: %d bytes -> %d samples/ch × %d ch = %d total PCM samples (48kHz)", 
				len(rtp.Payload), n, channels, totalSamples)
		}

		// Trim to actual decoded samples (interleaved stereo)
		pcmData = pcmData[:totalSamples]

		// Convert int16 PCM to bytes
		pcmBytes := make([]byte, len(pcmData)*2) // Each int16 sample = 2 bytes
		for i, sample := range pcmData {
			binary.LittleEndian.PutUint16(pcmBytes[i*2:], uint16(sample))
		}

		// Enqueue to jitter buffer
		a.jitterBuffer.Enqueue(pcmBytes)
		
		// Periodic stats logging
		if packetCount%100 == 0 {
			buffered, drops := a.jitterBuffer.GetStats()
			log.Printf("Jitter buffer stats @ packet %d: buffered=%d, drops=%d", 
				packetCount, buffered, drops)
		}
	}

	log.Println("Decoder thread stopped")
}

// TimedPlaybackLoop dequeues from jitter buffer with drift compensation
func (a *AudioPlayback) TimedPlaybackLoop() {
	defer a.wg.Done()
	
	firstWrite := true
	useDummyPlayback := false
	writeCount := 0
	
	// Precise timing with simple ticker
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	
	startTime := time.Now()
	
	log.Println("Playback thread started (20ms precise ticker)")
	
	for a.running {
		<-ticker.C  // Wait for next tick
		
		// Non-blocking dequeue: get packet immediately (no mutex wait)
		pcmBytes := a.jitterBuffer.Dequeue(a.decoder)
		
		if !useDummyPlayback {
			// Write PCM data to FFmpeg stdin
			if _, err := a.stdin.Write(pcmBytes); err != nil {
				if a.running {
					log.Printf("Audio playback write error: %v", err)
					if firstWrite {
						log.Println("Audio playback device not available, discarding audio for testing")
						useDummyPlayback = true
					} else {
						log.Println("Audio playback failed (FFmpeg/ALSA died) - disabling audio, video continues")
						a.mu.Lock()
						a.running = false
						a.mu.Unlock()
						if a.jitterBuffer != nil {
							a.jitterBuffer.cond.Broadcast()
						}
						break
					}
				}
			}
			firstWrite = false
		}
		
		writeCount++
		
		// Log playback start
		if writeCount == 1 {
			log.Printf("First audio frame written to FFmpeg (playback started)")
		}
		
		// Periodic stats
		if writeCount%100 == 0 {
			buf, drops := a.jitterBuffer.GetStats()
			elapsed := time.Since(startTime)
			avgRate := float64(writeCount) / elapsed.Seconds()
			log.Printf("Playback stats @ write %d: buffered=%d, drops=%d, rate=%.1f pkt/s", 
				writeCount, buf, drops, avgRate)
		}
	}

	log.Println("Playback thread stopped")
}

// PlaybackLoop starts both decode and playback threads
// Exported so it can be called from webrtc.go OnTrack handler
func (a *AudioPlayback) PlaybackLoop(track *webrtc.TrackRemote) {
	log.Println("=== Starting jitter-buffered audio playback ===")
	
	// Start decoder thread (reads RTP -> decodes -> enqueues)
	a.wg.Add(1)
	go a.DecodeLoop(track)
	
	// Start playback thread (dequeues -> writes FFmpeg at 20ms rate)
	a.wg.Add(1)
	go a.TimedPlaybackLoop()
	
	// Wait for both threads to complete
	a.wg.Wait()
	
	log.Println("=== Audio playback threads stopped ===")
}

// Stop ends audio playback
func (a *AudioPlayback) Stop() {
	a.mu.Lock()
	
	if !a.running {
		a.mu.Unlock()
		return
	}

	a.running = false
	a.mu.Unlock()
	
	// Wake up playback thread if waiting on prebuffer
	if a.jitterBuffer != nil {
		a.jitterBuffer.cond.Broadcast()
	}

	// Wait for both goroutines to finish
	a.wg.Wait()

	// Clean up FFmpeg process
	if a.stdin != nil {
		a.stdin.Close()
	}

	if a.cmd != nil && a.cmd.Process != nil {
		a.cmd.Process.Kill()
		a.cmd.Wait()
	}

	log.Println("Audio playback stopped") // vnextthongnv
}

