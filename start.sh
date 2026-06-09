#!/bin/bash
set -e

CONTAINER_NAME="bridge-stackchan"
IMAGE_NAME="bridge-stackchan"
PORT="${PORT:-8000}"

echo "Building image: $IMAGE_NAME"
podman build -t "$IMAGE_NAME" .

if podman ps -q -f name="$CONTAINER_NAME" | grep -q .; then
  echo "Container '$CONTAINER_NAME' is already running. Stopping it first."
  podman stop "$CONTAINER_NAME"
fi

if podman ps -aq -f name="$CONTAINER_NAME" | grep -q .; then
  podman rm "$CONTAINER_NAME"
fi

mkdir -p data secrets config

echo "Starting container: $CONTAINER_NAME"
podman run -d \
  --name "$CONTAINER_NAME" \
  --network slirp4netns:allow_host_loopback=true \
  --env-file .env \
  -p "${PORT}:8000" \
  -v "$(pwd)/data:/app/data:Z" \
  -v "$(pwd)/secrets:/app/secrets:Z" \
  -v "$(pwd)/config:/app/config:Z" \
  "$IMAGE_NAME"

echo "Started. Logs: podman logs -f $CONTAINER_NAME"
echo "Health: curl http://localhost:${PORT}/healthz"
