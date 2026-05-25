# Setup the environment on Raspberry Pi 4

## Overview
- Install GStreamer package

⚠️ **Important:** On Jetson, GStreamer is pre-installed in the system core by default by the vendor (NVIDIA). However, on Raspberry Pi 4, GStreamer is not available out of the box. Therefore, installing GStreamer is mandatory on Raspberry Pi 4 for RMCS to function properly.

---

## Installation

### Install Dependencies

```bash
sudo apt update
```

```bash
sudo apt install -y \
    gstreamer1.0-tools \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    gstreamer1.0-libav
```
---

### 🎯 If you got here, let's start RMCS. No need to repeat the Installation steps next time.
---

## Check if GStreamer is installed

```bash
# Restart PulseAudio
which gst-launch-1.0
  gst-inspect-1.0 x264enc | head -3
  gst-inspect-1.0 rawvideoparse | head -3
