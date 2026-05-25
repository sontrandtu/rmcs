#!/bin/bash

# Run RMCS on Jetson connecting to ROS Master
# Usage: ./run-jetson.sh <ros-master-ip> <topic1> <topic2> <topic3> <topic4> <topic5> <topic6> <topic7>
#
# Examples:
#   ./run-jetson.sh 192.168.1.100 /cam1/image /cam2/image /cam3/image /cam4/image /cam5/image /cam6/image /cam7/image

if [ "$#" -lt 8 ]; then
    echo "Usage: $0 <ros-master-ip> <topic1> <topic2> <topic3> <topic4> <topic5> <topic6> <topic7>"
    echo ""
    echo "Examples:"
    echo "  $0 192.168.1.100 /leopard/id1/image_resized /leopard/id3/image_resized /leopard/id4/image_resized /leopard/id5/image_resized /leopard/id6/image_resized /leopard/id7/image_resized /flir/id8/image_resized"
    echo ""
    echo "All 7 topics must be provided. Client can switch between them using camera buttons 1-7."
    exit 1
fi

ROS_MASTER_IP="$1"
TOPIC1="$2"
TOPIC2="$3"
TOPIC3="$4"
TOPIC4="$5"
TOPIC5="$6"
TOPIC6="$7"
TOPIC7="$8"

echo "Starting RMCS on Jetson..."
echo "Connecting to ROS Master at: $ROS_MASTER_IP:11311"
echo ""

# Get Jetson's local IP address (needed for ROS networking)
if [ "$ROS_MASTER_IP" == "localhost" ] || [ "$ROS_MASTER_IP" == "127.0.0.1" ]; then
    # Local ROS Master - use localhost
    JETSON_IP="127.0.0.1"
else
    # Remote ROS Master - detect Jetson's IP on same network
    JETSON_IP=$(ip route get $ROS_MASTER_IP | awk '{print $7; exit}')
    if [ -z "$JETSON_IP" ]; then
        # Fallback: get first non-loopback IP
        JETSON_IP=$(hostname -I | awk '{print $1}')
    fi
fi

echo "Jetson IP: $JETSON_IP"
echo ""
echo "Camera topic mapping:"
echo "  Camera 1: $TOPIC1"
echo "  Camera 2: $TOPIC2"
echo "  Camera 3: $TOPIC3"
echo "  Camera 4: $TOPIC4"
echo "  Camera 5: $TOPIC5"
echo "  Camera 6: $TOPIC6"
echo "  Camera 7: $TOPIC7"
echo ""

# Set ROS Master URI and Jetson's IP for ROS networking
export ROS_MASTER_URI="http://$ROS_MASTER_IP:11311"
export ROS_IP="$JETSON_IP"

# Export all 7 topics
export ROS_TOPIC_1="$TOPIC1"
export ROS_TOPIC_2="$TOPIC2"
export ROS_TOPIC_3="$TOPIC3"
export ROS_TOPIC_4="$TOPIC4"
export ROS_TOPIC_5="$TOPIC5"
export ROS_TOPIC_6="$TOPIC6"
export ROS_TOPIC_7="$TOPIC7"

# Set library path and run
export LD_LIBRARY_PATH=$(pwd):$LD_LIBRARY_PATH
./streaming
