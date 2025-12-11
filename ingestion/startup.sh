#!/bin/bash
sudo apt-get update
sudo apt-get install -y python3 python3-pip git

PROJECT_ID="vibrant-mantis-289406"
TOPIC_ID="Trade-Events"
pip3 install google-cloud-pubsub
