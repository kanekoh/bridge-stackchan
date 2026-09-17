FROM python:3.12-slim

WORKDIR /app

# 歌の WAV → MP3 変換に使う（bridge/features/song/cache.py）
RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py calendar_sync.py ./
COPY bridge/ ./bridge/
COPY config/ ./config/
COPY templates/ ./templates/

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
