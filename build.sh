#!/bin/bash
set -e

IMAGE_NAME="bridge-stackchan"

echo "Building Docker image: $IMAGE_NAME"
docker build -t "$IMAGE_NAME" .
echo "Done: $IMAGE_NAME"
