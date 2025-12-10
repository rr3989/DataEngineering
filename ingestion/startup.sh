#!/bin/bash
# Install Python, Git, and Pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip git

# Set up environment variables (Replace with your actual ID)
PROJECT_ID="vibrant-mantis-289406"
TOPIC_ID="Trade-Events"

# Install Python dependencies
pip3 install google-cloud-pubsub

# Run the publisher script in the background
# We assume the script is in the home directory for this example
nohup python3 trade_publisher.py > publisher.log 2>&1 &