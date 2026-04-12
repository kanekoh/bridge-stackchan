#!/bin/bash
set -e

CONTAINER_NAME="bridge-stackchan"
IMAGE_NAME="bridge-stackchan"
PORT="${PORT:-8000}"

echo "Building Docker image: $IMAGE_NAME"
docker build -t "$IMAGE_NAME" .

if docker ps -q -f name="$CONTAINER_NAME" | grep -q .; then
  echo "Container '$CONTAINER_NAME' is already running. Stopping it first."
  docker stop "$CONTAINER_NAME"
fi

if docker ps -aq -f name="$CONTAINER_NAME" | grep -q .; then
  docker rm "$CONTAINER_NAME"
fi

echo "Starting container: $CONTAINER_NAME"
docker run -d \
  --name "$CONTAINER_NAME" \
  --network slirp4netns:allow_host_loopback=true \
  --env-file .env \
  -p "${PORT}:8000" \
  "$IMAGE_NAME"

echo "Started. Logs: docker logs -f $CONTAINER_NAME"
echo "Health: curl http://localhost:${PORT}/healthz"
