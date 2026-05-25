package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os/exec"
	"sync"
	"time"

	"github.com/bluenviron/goroslib/v2"
	"github.com/bluenviron/goroslib/v2/pkg/msgs/sensor_msgs"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

// RPi 4 v4l2h264enc is currently broken on the bcm2835_codec driver shipped with
// this Pi OS / kernel: even a trivial videotestsrc -> v4l2h264enc pipeline errors
// with "Failed to process frame" on the first buffer. Skipping detection until
// a kernel/firmware update fixes it. Flip this to true to re-enable.
const enableRPiV4L2Encoder = false

// Global cache for encoder detection to avoid slow gst-inspect calls
var (
	encoderDetectionDone  bool
	hasNVIDIAEncoder      bool
	hasRPiV4L2Encoder     bool
	encoderDetectionMutex sync.Mutex
)

type ROSSubscriber struct {
	track        *webrtc.TrackLocalStaticSample
	node         *goroslib.Node
	sub          *goroslib.Subscriber
	cmd          *exec.Cmd
	isRunning    bool
	stopChan     chan bool
	mu           sync.Mutex
	trackID      string // Unique identifier for this subscriber
	topicName    string
	rosMasterURI string

	// GStreamer stdin pipe for writing BGR images
	gstStdin  io.WriteCloser
	gstStdout io.ReadCloser

	// Cached NAL units
	sps     []byte
	pps     []byte
	lastIDR []byte

	// Timing
	fps              uint32
	sampleDurationUs uint64

	// Image dimensions
	width  uint32
	height uint32

	// Message counter for logging
	messageCount int

	// First frame flag to detect dimensions
	firstFrameReceived   bool
	dimensionInitialized bool

	// Timeout detection
	lastMessageTime time.Time
	timeoutDuration time.Duration

	// Benchmark tracking for encoding latency
	framesWritten     int
	framesRead        int
	lastBenchmarkTime time.Time

	// Frame rate limiting to prevent bursts
	lastFrameAcceptTime time.Time
	minFrameInterval    time.Duration
	framesDropped       int
	encoderStartTime    time.Time // Track when encoder started for warm-up period
}

func NewROSSubscriber(track *webrtc.TrackLocalStaticSample, cameraIndex int, rosMasterURI string, trackID string) *ROSSubscriber {
	fps := uint32(30)

	// Map camera index to ROS topic name
	topicName := getTopicName(cameraIndex)

	return &ROSSubscriber{
		track:            track,
		trackID:          trackID,
		topicName:        topicName,
		rosMasterURI:     rosMasterURI,
		stopChan:         make(chan bool),
		fps:              fps,
		sampleDurationUs: 1000000 / uint64(fps),
		// Half the perfect-period gives jitter slack: at 30fps target, allow up to
		// 60fps input through. Real ROS @30fps with timing jitter passes cleanly.
		// If rosbag overruns (e.g. --rate 3.0 -> ~90fps), x264's stdin pipe blocks
		// naturally and absorbs the overflow.
		minFrameInterval: time.Duration(1000000/uint64(fps)/2) * time.Microsecond, // ~16.67ms at 30fps
		// Don't initialize dimensions - detect from first frame
		width:                0,
		height:               0,
		firstFrameReceived:   false,
		dimensionInitialized: false,
		timeoutDuration:      10 * time.Second, // 10 second timeout for no messages
		lastMessageTime:      time.Now(),
		lastBenchmarkTime:    time.Now(),
		lastFrameAcceptTime:  time.Time{},
	}
}

// CheckTopicExists verifies if a ROS topic is available
func CheckROSTopicExists(topicName string, rosMasterURI string) error {
	// Create temporary ROS node to check topic availability
	node, err := goroslib.NewNode(goroslib.NodeConf{
		Name:          "rmcs_topic_checker",
		MasterAddress: rosMasterURI,
	})
	if err != nil {
		return fmt.Errorf("failed to connect to ROS master: %v", err)
	}
	defer node.Close()

	// Try to create a subscriber to verify topic exists
	// We don't need to actually receive messages, just verify it can subscribe
	sub, err := goroslib.NewSubscriber(goroslib.SubscriberConf{
		Node:  node,
		Topic: topicName,
		Callback: func(msg *sensor_msgs.Image) {
			// Empty callback - we just want to verify subscription works
		},
	})
	if err != nil {
		return fmt.Errorf("topic '%s' not available: %v", topicName, err)
	}
	defer sub.Close()

	// Give it a brief moment to establish subscription
	time.Sleep(100 * time.Millisecond)

	log.Printf("ROS topic '%s' is available", topicName)
	return nil
}

func getTopicName(cameraIndex int) string {
	switch cameraIndex {
	case 1:
		return "/leopard/id1/image_resized"
	case 2:
		return "/leopard/id2/image_resized"
	case 3:
		return "/leopard/id3/image_resized"
	case 4:
		return "/leopard/id4/image_resized"
	case 5:
		return "/leopard/id5/image_resized"
	case 6:
		return "/leopard/id6/image_resized"
	case 7:
		return "/leopard/id7/image_resized"
	case 8:
		return "/flir/id8/image_resized"
	default:
		return "/leopard/id1/image_resized"
	}
}

func (r *ROSSubscriber) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.isRunning {
		return nil
	}

	// Create ROS node with unique name per track to avoid conflicts
	nodeName := fmt.Sprintf("rmcs_subscriber_%s", r.trackID)
	node, err := goroslib.NewNode(goroslib.NodeConf{
		Name:          nodeName,
		MasterAddress: r.rosMasterURI,
	})
	if err != nil {
		return fmt.Errorf("failed to create ROS node: %v", err)
	}
	r.node = node

	// Create subscriber for ROS image topic FIRST to detect dimensions
	sub, err := goroslib.NewSubscriber(goroslib.SubscriberConf{
		Node:  r.node,
		Topic: r.topicName,
		Callback: func(msg *sensor_msgs.Image) {
			r.handleImageMessage(msg)
		},
	})
	if err != nil {
		r.node.Close()
		return fmt.Errorf("failed to create subscriber: %v", err)
	}
	r.sub = sub

	r.isRunning = true
	r.lastMessageTime = time.Now()

	// Start timeout monitor goroutine
	go r.monitorTimeout()

	log.Printf("ROS subscriber started on topic: %s (waiting for first frame to detect dimensions)", r.topicName)
	return nil
}

// monitorTimeout checks if ROS messages have stopped arriving
func (r *ROSSubscriber) monitorTimeout() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopChan:
			return
		case <-ticker.C:
			r.mu.Lock()
			isRunning := r.isRunning
			lastTime := r.lastMessageTime
			timeout := r.timeoutDuration
			r.mu.Unlock()

			if !isRunning {
				return
			}

			timeSinceLastMsg := time.Since(lastTime)
			if timeSinceLastMsg > timeout {
				log.Printf("ROS_TIMEOUT_WARNING: No messages received on topic '%s' for %.1f seconds (last message: %v)",
					r.topicName, timeSinceLastMsg.Seconds(), lastTime.Format("15:04:05"))
			}
		}
	}
}

func (r *ROSSubscriber) initGStreamer() error {
	if r.width == 0 || r.height == 0 {
		return fmt.Errorf("cannot start GStreamer with zero dimensions")
	}

	// Try NVIDIA hardware encoder first (for Jetson)
	log.Printf("Attempting to start GStreamer for dimensions: %dx%d", r.width, r.height)

	// Optimized NVIDIA pipeline for dual-stream performance
	// - Lower bitrate (1.5Mbps instead of 2Mbps) to reduce encoder load
	// - IDR interval set to FPS for fewer keyframes (improves encoding performance)
	// - maxperf-enable=1 for maximum performance
	nvidiaPipeline := fmt.Sprintf(
		"gst-launch-1.0 -q fdsrc ! rawvideoparse width=%d height=%d format=bgr framerate=%d/1 ! "+
			"videoconvert ! nvvidconv ! "+
			"'video/x-raw(memory:NVMM),format=NV12' ! "+
			"nvv4l2h264enc maxperf-enable=1 bitrate=1500000 preset-level=1 idrinterval=%d control-rate=1 ! "+
			"h264parse config-interval=-1 ! fdsink",
		r.width, r.height, r.fps, r.fps,
	)

	// Check available hardware encoders (cached, gst-inspect is slow)
	// Preference: NVIDIA (Jetson) > V4L2 (Raspberry Pi 4) > x264 (CPU)
	var pipeline string
	encoderDetectionMutex.Lock()
	if !encoderDetectionDone {
		log.Printf("First-time encoder detection (this may take a few seconds)...")
		hasNVIDIAEncoder = exec.Command("gst-inspect-1.0", "nvv4l2h264enc").Run() == nil
		if !hasNVIDIAEncoder && enableRPiV4L2Encoder {
			hasRPiV4L2Encoder = exec.Command("gst-inspect-1.0", "v4l2h264enc").Run() == nil
		}
		encoderDetectionDone = true
		switch {
		case hasNVIDIAEncoder:
			log.Printf("NVIDIA nvv4l2h264enc hardware encoder detected (will be used for all streams)")
		case hasRPiV4L2Encoder:
			log.Printf("RPi v4l2h264enc hardware encoder detected (will be used for all streams)")
		default:
			log.Printf("No hardware encoder available, falling back to x264enc software (will be used for all streams)")
		}
	}
	useNVIDIA := hasNVIDIAEncoder
	useRPi := hasRPiV4L2Encoder
	encoderDetectionMutex.Unlock()

	switch {
	case useNVIDIA:
		log.Printf("Using NVIDIA nvv4l2h264enc hardware encoder")
		pipeline = nvidiaPipeline
		r.cmd = exec.Command("/bin/sh", "-c", nvidiaPipeline)

	case useRPi:
		// Raspberry Pi 4 hardware encoder (VideoCore VI via V4L2).
		// h264_profile=1 = Constrained Baseline, matches SDP profile-level-id=42001f.
		// h264_i_frame_period = keyframes per N frames.
		// video_bitrate matches the NVIDIA pipeline (1.5 Mbps).
		// Hardware encoder needs even dimensions; pad if necessary.
		evenWidth := r.width
		if evenWidth%2 != 0 {
			evenWidth++
		}
		evenHeight := r.height
		if evenHeight%2 != 0 {
			evenHeight++
		}
		log.Printf("Padding dimensions from %dx%d to %dx%d for v4l2h264enc", r.width, r.height, evenWidth, evenHeight)

		// Minimal pipeline. Don't force NV12 input -- some RPi firmwares reject it
		// at preroll. Don't pass extra-controls -- driver-specific control IDs cause
		// "Failed to process frame" on the first buffer. Let videoconvert and the
		// encoder negotiate the format; assert baseline profile downstream.
		rpiPipeline := fmt.Sprintf(
			"gst-launch-1.0 -q fdsrc ! rawvideoparse width=%d height=%d format=bgr framerate=%d/1 ! "+
				"videoconvert ! videoscale ! video/x-raw,width=%d,height=%d ! "+
				"v4l2h264enc ! "+
				"h264parse config-interval=-1 ! "+
				"video/x-h264,profile=constrained-baseline,stream-format=byte-stream,alignment=au ! "+
				"fdsink",
			r.width, r.height, r.fps, evenWidth, evenHeight,
		)
		log.Printf("Using RPi v4l2h264enc hardware encoder")
		pipeline = rpiPipeline
		r.cmd = exec.Command("/bin/sh", "-c", rpiPipeline)

	default:
		// CPU fallback. profile=constrained-baseline matches SDP profile-level-id=42001f.
		// rc-lookahead=0 + sync-lookahead=0 kill x264's ~1.3s rate-control buffering.
		// bframes=0 required: WebRTC H.264 cannot carry B-frames.
		log.Printf("No hardware encoder, falling back to x264enc software encoder")
		evenWidth := r.width
		if evenWidth%2 != 0 {
			evenWidth++
		}
		evenHeight := r.height
		if evenHeight%2 != 0 {
			evenHeight++
		}
		log.Printf("Padding dimensions from %dx%d to %dx%d for x264enc", r.width, r.height, evenWidth, evenHeight)

		// Match the original working pipeline (which streamed cleanly but with lag).
		// The lag came from x264's default rc-lookahead=40 frames (~1.3s buffering).
		// Setting rc-lookahead=0 + sync-lookahead=0 removes that.
		// Do NOT add sliced-threads=true: it produces multi-slice frames whose RTP
		// packets corrupt visibly (purple/magenta blocks) on any packet reorder/loss.
		// Default frame-parallel threading produces single-slice output that decodes cleanly.
		softwarePipeline := fmt.Sprintf(
			"gst-launch-1.0 -q fdsrc ! rawvideoparse width=%d height=%d format=bgr framerate=%d/1 ! "+
				"videoconvert ! videoscale ! video/x-raw,width=%d,height=%d,format=I420 ! "+
				"x264enc bitrate=3000 speed-preset=ultrafast key-int-max=%d "+
				"bframes=0 rc-lookahead=0 sync-lookahead=0 threads=2 ! "+
				"video/x-h264,profile=constrained-baseline,stream-format=byte-stream,alignment=au ! "+
				"h264parse config-interval=-1 ! fdsink sync=false",
			r.width, r.height, r.fps, evenWidth, evenHeight, r.fps,
		)
		pipeline = softwarePipeline
		r.cmd = exec.Command("/bin/sh", "-c", softwarePipeline)
	}

	// Log the exact command being executed for debugging
	log.Printf("GStreamer command: %s", pipeline)

	// Get stdin pipe for writing raw BGR frames
	gstStdin, err := r.cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to get GStreamer stdin: %v", err)
	}
	r.gstStdin = gstStdin

	// Get stdout pipe for reading H.264 stream
	stdout, err := r.cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to get GStreamer stdout: %v", err)
	}
	r.gstStdout = stdout

	// Get stderr pipe for logging
	stderr, err := r.cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to get GStreamer stderr: %v", err)
	}

	// Start GStreamer
	if err := r.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start GStreamer: %v", err)
	}

	// Log ALL GStreamer stderr output to help debug crashes
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			if len(line) > 0 {
				log.Printf("[GStreamer stderr track=%s] %s", r.trackID, line)
			}
		}
	}()

	// Monitor GStreamer process - detect crashes
	go func() {
		err := r.cmd.Wait()
		if err != nil {
			// Check if this was an intentional kill (e.g., during camera switch)
			exitErr, isExitError := err.(*exec.ExitError)
			if isExitError && (exitErr.String() == "signal: killed" ||
				exitErr.String() == "waitid: no child processes" ||
				exitErr.String() == "wait: no child processes") {
				log.Printf("GStreamer stopped for track %s (camera switch or cleanup)", r.trackID)
			} else {
				log.Printf("GStreamer process CRASHED for track %s: %v", r.trackID, err)
				log.Printf("HINT: If error mentions 'nvv4l2h264enc' or 'nvvidconv', you MUST run on Jetson device with NVIDIA GPU")
			}
		} else {
			log.Printf("GStreamer process exited normally for track %s", r.trackID)
		}

		// Mark GStreamer as stopped
		r.mu.Lock()
		r.gstStdin = nil
		r.gstStdout = nil
		r.cmd = nil
		r.mu.Unlock()
	}()

	// Reset benchmark counters and frame limiting for new encoder instance
	// NOTE: Caller already holds r.mu lock, so don't lock again
	r.framesWritten = 0
	r.framesRead = 0
	r.framesDropped = 0
	r.lastBenchmarkTime = time.Now()
	r.lastFrameAcceptTime = time.Time{}

	// Start reading H.264 stream from GStreamer
	go r.readH264Stream(stdout)

	r.dimensionInitialized = true
	log.Printf("GStreamer H.264 encoder started successfully for %dx%d @ %d fps", r.width, r.height, r.fps)
	return nil
}

func (r *ROSSubscriber) stopGStreamer() {
	if r.gstStdin != nil {
		r.gstStdin.Close()
		r.gstStdin = nil
	}

	if r.gstStdout != nil {
		r.gstStdout.Close()
		r.gstStdout = nil
	}

	if r.cmd != nil && r.cmd.Process != nil {
		r.cmd.Process.Kill()
		r.cmd.Wait() // Wait for process to exit
		r.cmd = nil
	}

	// Clear cached NAL units
	r.sps = nil
	r.pps = nil
	r.lastIDR = nil

	log.Println("GStreamer stopped")
}

func (r *ROSSubscriber) Stop() {
	// Check if already stopped and mark as stopping
	r.mu.Lock()
	if !r.isRunning {
		r.mu.Unlock()
		return
	}
	r.isRunning = false
	r.dimensionInitialized = false

	// Get references to things we need to clean up
	sub := r.sub
	node := r.node
	topicName := r.topicName
	r.sub = nil
	r.node = nil
	r.mu.Unlock()

	// Signal stop to readH264Stream goroutine
	select {
	case r.stopChan <- true:
	default:
	}

	// Close subscriber (might call callbacks, so done without holding lock)
	if sub != nil {
		sub.Close()
	}

	// Stop GStreamer
	r.stopGStreamer()

	// Close ROS node
	if node != nil {
		node.Close()
	}

	log.Printf("ROS subscriber stopped on topic: %s", topicName)
}

func (r *ROSSubscriber) handleImageMessage(msg *sensor_msgs.Image) {
	// Check if subscriber is still running
	r.mu.Lock()
	if !r.isRunning {
		r.mu.Unlock()
		return
	}

	// Update last message time for timeout monitoring
	r.lastMessageTime = time.Now()

	// Verify encoding is bgr8
	if msg.Encoding != "bgr8" {
		r.mu.Unlock()
		if r.messageCount == 0 {
			log.Printf("WARNING: unexpected encoding %s (expected bgr8)", msg.Encoding)
		}
		return
	}

	// First frame: detect dimensions and start GStreamer
	if !r.firstFrameReceived {
		r.firstFrameReceived = true
		r.width = msg.Width
		r.height = msg.Height
		log.Printf("Detected image dimensions from first frame: %dx%d", r.width, r.height)

		// Start GStreamer with detected dimensions
		if err := r.initGStreamer(); err != nil {
			r.mu.Unlock()
			log.Printf("ERROR: Failed to start GStreamer: %v", err)
			return
		}

		// Mark encoder start time for warm-up period
		r.encoderStartTime = time.Now()
		r.mu.Unlock()
		log.Printf("Skipping first frame to allow encoder initialization")
		return
	}

	// Warm-up period: skip frames for first 500ms to let NVIDIA encoder stabilize
	encoderStart := r.encoderStartTime
	r.mu.Unlock()

	if !encoderStart.IsZero() && time.Since(encoderStart) < 500*time.Millisecond {
		// Still in warm-up period - drop frame
		return
	}

	r.mu.Lock()

	// Handle dimension changes (restart GStreamer)
	if msg.Width != r.width || msg.Height != r.height {
		log.Printf("Image dimensions changed: %dx%d -> %dx%d. Restarting GStreamer...",
			r.width, r.height, msg.Width, msg.Height)

		// Stop old GStreamer
		r.stopGStreamer()

		// Update dimensions
		r.width = msg.Width
		r.height = msg.Height

		// Restart GStreamer with new dimensions
		if err := r.initGStreamer(); err != nil {
			r.mu.Unlock()
			log.Printf("ERROR: Failed to restart GStreamer: %v", err)
			return
		}
	}

	// Frame rate limiting to prevent bursts and encoder overload
	lastAccept := r.lastFrameAcceptTime
	minInterval := r.minFrameInterval
	r.mu.Unlock()

	// Check if enough time has passed since last frame
	if !lastAccept.IsZero() {
		elapsed := time.Since(lastAccept)
		if elapsed < minInterval {
			// Drop frame - too soon since last frame
			r.mu.Lock()
			r.framesDropped++
			r.mu.Unlock()
			return
		}
	}

	// Update last frame accept time
	r.mu.Lock()
	r.lastFrameAcceptTime = time.Now()
	r.mu.Unlock()

	// Add logging to verify messages are being received
	r.messageCount++

	// Validate data size matches expected dimensions
	expectedSize := int(r.width * r.height * 3) // BGR8 = 3 bytes per pixel
	actualSize := len(msg.Data)
	if actualSize != expectedSize {
		log.Printf("ERROR: Data size mismatch. Expected %d bytes (%dx%dx3), got %d bytes",
			expectedSize, r.width, r.height, actualSize)
		log.Printf("ERROR: This will cause severe corruption. Skipping frame.")
		return
	}

	// Get GStreamer stdin and check if process is still running
	r.mu.Lock()
	gstStdin := r.gstStdin
	cmd := r.cmd
	stillRunning := r.isRunning
	r.mu.Unlock()

	// Check if GStreamer process has crashed
	if cmd != nil && cmd.ProcessState != nil && cmd.ProcessState.Exited() {
		// Process has exited - stop trying to write
		return
	}

	if gstStdin != nil && stillRunning {
		n, err := gstStdin.Write(msg.Data)
		if err != nil {
			// Log error only once by checking if process is still alive
			if cmd != nil && cmd.Process != nil {
				log.Printf("ERROR: Failed writing to GStreamer stdin for track %s: %v (GStreamer may have crashed)", r.trackID, err)
			}
			return
		}
		if n != len(msg.Data) {
			log.Printf("ERROR: Incomplete write to GStreamer. Expected %d bytes, wrote %d bytes", len(msg.Data), n)
			return
		}

		// Track frame written for benchmark
		r.mu.Lock()
		r.framesWritten++
		r.mu.Unlock()
	}
}

func (r *ROSSubscriber) readH264Stream(reader io.Reader) {
	buffer := make([]byte, 0, 100000)
	readBuf := make([]byte, 8192)
	framesSent := 0
	waitingForConfig := true

	for {
		select {
		case <-r.stopChan:
			log.Printf("Stopping ROS stream.")
			return
		default:
			// Read data from GStreamer stdout
			n, err := reader.Read(readBuf)
			if err != nil {
				// EOF or pipe closed errors are expected during shutdown/switching
				return
			}

			buffer = append(buffer, readBuf[:n]...)

			// Process NAL units from buffer
			for {
				nalUnit, remaining, found := r.extractNextNALUnit(buffer)
				if !found {
					buffer = remaining
					break
				}

				buffer = remaining

				if len(nalUnit) == 0 {
					continue
				}

				// Get NAL type
				nalType := nalUnit[0] & 0x1F

				// Cache configuration NAL units
				switch nalType {
				case 7: // SPS
					r.mu.Lock()
					r.sps = make([]byte, len(nalUnit))
					copy(r.sps, nalUnit)
					r.mu.Unlock()

				case 8: // PPS
					r.mu.Lock()
					r.pps = make([]byte, len(nalUnit))
					copy(r.pps, nalUnit)
					r.mu.Unlock()

					// Send initial config when we have both SPS and PPS
					if waitingForConfig && r.sps != nil && r.pps != nil {
						waitingForConfig = false
						r.sendNALUnitNoSEI(r.sps)
						r.sendNALUnitNoSEI(r.pps)
					}

				case 5: // IDR
					r.mu.Lock()
					r.lastIDR = make([]byte, len(nalUnit))
					copy(r.lastIDR, nalUnit)
					r.mu.Unlock()
				}

				// Skip frames until we have configuration
				if waitingForConfig {
					continue
				}

				// For IDR frames, prepend SPS+PPS (without SEI)
				if nalType == 5 {
					r.mu.Lock()
					if r.sps != nil {
						r.sendNALUnitNoSEI(r.sps)
					}
					if r.pps != nil {
						r.sendNALUnitNoSEI(r.pps)
					}
					r.mu.Unlock()
				}

				// Send the frame WITH SEI only for video slices (types 1, 5)
				if nalType == 1 || nalType == 5 {
					r.sendNALUnitWithSEI(nalUnit)
					framesSent++

					// Track frame read for benchmark
					r.mu.Lock()
					r.framesRead++
					r.mu.Unlock()

					// Report benchmark every 150 frames (5 seconds at 30fps)
					if framesSent%150 == 0 {
						r.reportBenchmark()
					}
				} else {
					r.sendNALUnitNoSEI(nalUnit)
				}
			}
		}
	}
}

func (r *ROSSubscriber) reportBenchmark() {
	r.mu.Lock()
	written := r.framesWritten
	read := r.framesRead
	dropped := r.framesDropped
	fps := r.fps
	now := time.Now()
	elapsed := now.Sub(r.lastBenchmarkTime).Seconds()
	r.lastBenchmarkTime = now
	r.framesDropped = 0 // Reset counter after reporting
	r.mu.Unlock()

	bufferedFrames := written - read
	latencyMs := (float64(bufferedFrames) / float64(fps)) * 1000.0

	var status string
	if latencyMs < 200 {
		status = "EXCELLENT"
	} else if latencyMs < 500 {
		status = "GOOD"
	} else if latencyMs < 800 {
		status = "OK"
	} else {
		status = "SLOW"
	}

	log.Printf("BENCHMARK [%s]: Latency: %.0fms | Buffer: %d frames | Rate: %.1f fps | Dropped: %d frames",
		status, latencyMs, bufferedFrames, 150.0/elapsed, dropped)
}

func (r *ROSSubscriber) extractNextNALUnit(buffer []byte) (nalUnit []byte, remaining []byte, found bool) {
	// Need at least 4 bytes to check for start code
	if len(buffer) < 4 {
		return nil, buffer, false
	}

	// Find first start code
	startIdx := -1
	startCodeLen := 0

	for i := 0; i <= len(buffer)-3; i++ {
		if buffer[i] == 0 && buffer[i+1] == 0 {
			if buffer[i+2] == 1 {
				startIdx = i
				startCodeLen = 3
				break
			}
			if i <= len(buffer)-4 && buffer[i+2] == 0 && buffer[i+3] == 1 {
				startIdx = i
				startCodeLen = 4
				break
			}
		}
	}

	if startIdx == -1 {
		// No start code found, keep last 3 bytes for next read
		if len(buffer) > 3 {
			return nil, buffer[len(buffer)-3:], false
		}
		return nil, buffer, false
	}

	// Find next start code
	nextIdx := -1
	for i := startIdx + startCodeLen; i <= len(buffer)-3; i++ {
		if buffer[i] == 0 && buffer[i+1] == 0 {
			if buffer[i+2] == 1 {
				nextIdx = i
				break
			}
			if i <= len(buffer)-4 && buffer[i+2] == 0 && buffer[i+3] == 1 {
				nextIdx = i
				break
			}
		}
	}

	if nextIdx == -1 {
		// No complete NAL unit yet, need more data
		return nil, buffer, false
	}

	// Extract NAL unit (without start code)
	nalUnit = buffer[startIdx+startCodeLen : nextIdx]
	remaining = buffer[nextIdx:]

	return nalUnit, remaining, true
}

func (r *ROSSubscriber) sendNALUnitWithSEI(nalUnit []byte) {
	startCode := []byte{0x00, 0x00, 0x00, 0x01}

	// Get current timestamp in microseconds
	timestampUs := uint64(time.Now().UnixNano() / 1000)

	// Create SEI with timestamp
	seiNAL := createSimpleTimestampSEI(timestampUs)

	// Build frame: SEI + NAL unit
	var data []byte
	data = append(data, startCode...)
	data = append(data, seiNAL...)
	data = append(data, startCode...)
	data = append(data, nalUnit...)

	err := r.track.WriteSample(media.Sample{
		Data:     data,
		Duration: time.Duration(r.sampleDurationUs) * time.Microsecond,
	})

	if err != nil && err != io.ErrClosedPipe {
		log.Printf("Error writing sample: %v", err)
	}
}

func (r *ROSSubscriber) sendNALUnitNoSEI(nalUnit []byte) {
	startCode := []byte{0x00, 0x00, 0x00, 0x01}

	// Build frame: just NAL unit without SEI
	var data []byte
	data = append(data, startCode...)
	data = append(data, nalUnit...)

	err := r.track.WriteSample(media.Sample{
		Data:     data,
		Duration: time.Duration(r.sampleDurationUs) * time.Microsecond,
	})

	if err != nil && err != io.ErrClosedPipe {
		log.Printf("Error writing sample: %v", err)
	}
}

func (r *ROSSubscriber) GetInitialNALUnits() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()

	var result []byte
	startCode := []byte{0x00, 0x00, 0x00, 0x01}

	if r.sps != nil {
		result = append(result, startCode...)
		result = append(result, r.sps...)
	}
	if r.pps != nil {
		result = append(result, startCode...)
		result = append(result, r.pps...)
	}
	if r.lastIDR != nil {
		result = append(result, startCode...)
		result = append(result, r.lastIDR...)
	}

	return result
}

// Note: SEI timestamp functions are already defined in camera_capture.go
// We can reuse those functions since they're in the same package
