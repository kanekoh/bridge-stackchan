#!/bin/bash
set -e

CONTAINER_NAME="bridge-stackchan"
PORT="${PORT:-8000}"

if docker ps -q -f name="$CONTAINER_NAME" | grep -q .; then
  echo "Stopping container: $CONTAINER_NAME"
  docker stop "$CONTAINER_NAME"
  docker rm "$CONTAINER_NAME"
  echo "Stopped."
else
  echo "Container '$CONTAINER_NAME' is not running."
fi

# ポートがまだ使われていれば強制終了
PID=$(lsof -ti tcp:"$PORT" 2>/dev/null || true)
if [ -n "$PID" ]; then
  echo "Port $PORT is still in use by PID $PID. Killing..."
  kill -9 $PID
  echo "Killed PID $PID."
else
  echo "Port $PORT is free."
fi
