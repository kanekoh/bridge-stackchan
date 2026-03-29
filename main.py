import io
import os
import uuid
import json
import logging
import threading
from pathlib import Path

import openai
import requests
import paho.mqtt.client as mqtt
from fastapi import FastAPI, Form, HTTPException, UploadFile, File
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

VOICEVOX_URL = os.getenv("VOICEVOX_URL", "http://localhost:50021")
VOICEVOX_SPEAKER = int(os.getenv("VOICEVOX_SPEAKER", "1"))
VOICEVOX_API_KEY = os.getenv("VOICEVOX_API_KEY", "")

MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_TLS = os.getenv("MQTT_TLS", "false").lower() == "true"
MQTT_DEVICE_ID = os.getenv("MQTT_DEVICE_ID", "default")

AUDIO_DIR = Path(os.getenv("AUDIO_DIR", "/tmp/bridge-audio"))
AUDIO_BASE_URL = os.getenv("AUDIO_BASE_URL", "http://localhost:8000")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENCLAW_BASE_URL = os.getenv("OPENCLAW_BASE_URL", "https://api.openai.com/v1")
OPENCLAW_MODEL = os.getenv("OPENCLAW_MODEL", "gpt-4o")
OPENCLAW_SYSTEM_PROMPT = os.getenv(
    "OPENCLAW_SYSTEM_PROMPT",
    (
        "あなたはスタックちゃんというかわいいアシスタントロボットです。"
        "家族みんなと会話します。"
        "返事は短く、かわいく、話しやすい言葉で答えてください。"
        "日本語で答えてください。"
        "英語で話しかけられたときは、やさしい日本語でカタカナ英語を交えて返してください。"
    ),
)

AUDIO_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Bridge API", version="0.1.0")
app.mount("/audio", StaticFiles(directory=str(AUDIO_DIR)), name="audio")


def _build_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    if MQTT_TLS:
        client.tls_set()  # uses system CA bundle; works with HiveMQ Cloud
    return client


class SpeakRequest(BaseModel):
    text: str
    source: str = "unknown"
    priority: str = "normal"
    request_id: str | None = None


def get_audio_url_web(text: str) -> tuple[str, str | None]:
    """Get MP3 URLs from VOICEVOX Web高速版 (api.tts.quest) without downloading.
    Returns (mp3DownloadUrl, mp3StreamingUrl).
    """
    resp = requests.get(
        f"{VOICEVOX_URL}/synthesis",
        params={"speaker": VOICEVOX_SPEAKER, "text": text, "key": VOICEVOX_API_KEY},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()

    if not data.get("success"):
        raise RuntimeError(f"VOICEVOX Web API error: {data}")

    mp3_url = data.get("mp3DownloadUrl")
    if not mp3_url:
        raise RuntimeError(f"No mp3DownloadUrl in response: {data}")

    return mp3_url, data.get("mp3StreamingUrl")


def generate_audio_local(text: str, output_path: Path) -> None:
    """Generate MP3 using local VOICEVOX (audio_query → synthesis → ffmpeg)."""
    import subprocess
    import tempfile

    # Step 1: audio_query
    resp = requests.post(
        f"{VOICEVOX_URL}/audio_query",
        params={"text": text, "speaker": VOICEVOX_SPEAKER},
        timeout=30,
    )
    resp.raise_for_status()
    query = resp.json()

    # Step 2: synthesis → WAV
    resp = requests.post(
        f"{VOICEVOX_URL}/synthesis",
        params={"speaker": VOICEVOX_SPEAKER},
        json=query,
        timeout=60,
    )
    resp.raise_for_status()
    wav_bytes = resp.content

    # Step 3: WAV → MP3
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
        tmp.write(wav_bytes)
        tmp_path = tmp.name

    try:
        subprocess.run(
            ["ffmpeg", "-y", "-i", tmp_path, str(output_path)],
            check=True,
            capture_output=True,
        )
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def resolve_audio_url(text: str) -> tuple[str, str | None]:
    """Return (audioUrl, audioStreamingUrl) for the given text."""
    if VOICEVOX_API_KEY:
        return get_audio_url_web(text)
    else:
        audio_id = str(uuid.uuid4())
        mp3_path = AUDIO_DIR / f"{audio_id}.mp3"
        generate_audio_local(text, mp3_path)
        return f"{AUDIO_BASE_URL}/audio/{audio_id}.mp3", None


def publish_speak(audio_url: str, audio_streaming_url: str | None, text: str, source: str, priority: str, request_id: str) -> None:
    """Publish MQTT speak event to Stack-chan."""
    topic = f"stackchan/{MQTT_DEVICE_ID}/speak"
    msg: dict = {
        "type": "speak",
        "audioUrl": audio_url,
        "text": text,
        "source": source,
        "priority": priority,
        "requestId": request_id,
    }
    if audio_streaming_url:
        msg["audioStreamingUrl"] = audio_streaming_url
    payload = json.dumps(msg, ensure_ascii=False)
    logger.info("MQTT connecting: broker=%s port=%d tls=%s", MQTT_BROKER, MQTT_PORT, MQTT_TLS)
    client = _build_mqtt_client()

    connected = threading.Event()

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            connected.set()
        else:
            logger.error("MQTT connect failed: reason_code=%s", reason_code)

    client.on_connect = on_connect
    client.loop_start()
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=30)

    if not connected.wait(timeout=10):
        client.loop_stop()
        raise RuntimeError("MQTT connection timeout (no CONNACK within 10s)")

    logger.info("MQTT connected")
    msg_info = client.publish(topic, payload, qos=1)
    logger.info("MQTT publish queued: mid=%d", msg_info.mid)
    try:
        msg_info.wait_for_publish(timeout=10)
    except Exception as e:
        client.loop_stop()
        client.disconnect()
        raise RuntimeError(f"MQTT publish not acknowledged: {e}")
    client.loop_stop()
    client.disconnect()
    logger.info("MQTT publish confirmed: topic=%s mid=%d payload=%s", topic, msg_info.mid, payload)


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.post("/speak")
def speak(req: SpeakRequest):
    request_id = req.request_id or str(uuid.uuid4())

    try:
        audio_url, audio_streaming_url = resolve_audio_url(req.text)
    except Exception as e:
        logger.error("VOICEVOX error: %s", e)
        raise HTTPException(status_code=502, detail=f"VOICEVOX error: {e}")

    try:
        publish_speak(audio_url, audio_streaming_url, req.text, req.source, req.priority, request_id)
    except Exception as e:
        logger.error("MQTT error: %s", e)
        raise HTTPException(status_code=502, detail=f"MQTT error: {e}")

    logger.info("Spoke: request_id=%s text=%s", request_id, req.text[:40])
    resp: dict = {"requestId": request_id, "audioUrl": audio_url}
    if audio_streaming_url:
        resp["audioStreamingUrl"] = audio_streaming_url
    return resp


def transcribe_audio(audio_bytes: bytes, filename: str) -> str:
    """Transcribe audio bytes using OpenAI Whisper API."""
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    buf = io.BytesIO(audio_bytes)
    buf.name = filename or "audio.mp3"
    result = client.audio.transcriptions.create(
        model="whisper-1",
        file=buf,
        language="ja",
    )
    return result.text


def chat_with_openclaw(text: str, system_prompt_append: str = "") -> str:
    """Send transcribed text to OpenClaw and return the reply."""
    client = openai.OpenAI(api_key=OPENAI_API_KEY, base_url=OPENCLAW_BASE_URL)
    system_prompt = OPENCLAW_SYSTEM_PROMPT
    if system_prompt_append:
        system_prompt = system_prompt + "\n\n" + system_prompt_append
    response = client.chat.completions.create(
        model=OPENCLAW_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": text},
        ],
    )
    return response.choices[0].message.content


@app.post("/ingest-audio")
async def ingest_audio(
    file: UploadFile = File(...),
    system_prompt_append: str = Form(""),
    source: str = Form("stackchan"),
    priority: str = Form("normal"),
    request_id: str = Form(""),
):
    """
    Receive audio from Stack-chan, run STT, call OpenClaw, speak the reply via VOICEVOX + MQTT.

    Form fields (all optional):
    - system_prompt_append: extra instructions appended to the base system prompt
    - source: label stored in the MQTT message (default: "stackchan")
    - priority: MQTT message priority (default: "normal")
    - request_id: caller-supplied idempotency key (auto-generated if omitted)
    """
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=503, detail="OPENAI_API_KEY is not configured")

    req_id = request_id or str(uuid.uuid4())
    audio_bytes = await file.read()
    logger.info("Received audio: filename=%s size=%d request_id=%s", file.filename, len(audio_bytes), req_id)

    try:
        transcript = transcribe_audio(audio_bytes, file.filename or "audio.mp3")
    except Exception as e:
        logger.error("STT error: %s", e)
        raise HTTPException(status_code=502, detail=f"STT error: {e}")
    logger.info("Transcript: request_id=%s text=%s", req_id, transcript[:80])

    try:
        reply = chat_with_openclaw(transcript, system_prompt_append)
    except Exception as e:
        logger.error("OpenClaw error: %s", e)
        raise HTTPException(status_code=502, detail=f"OpenClaw error: {e}")
    logger.info("OpenClaw reply: request_id=%s text=%s", req_id, reply[:80])

    try:
        audio_url, audio_streaming_url = resolve_audio_url(reply)
    except Exception as e:
        logger.error("VOICEVOX error: %s", e)
        raise HTTPException(status_code=502, detail=f"VOICEVOX error: {e}")

    try:
        publish_speak(audio_url, audio_streaming_url, reply, source, priority, req_id)
    except Exception as e:
        logger.error("MQTT error: %s", e)
        raise HTTPException(status_code=502, detail=f"MQTT error: {e}")

    resp: dict = {
        "requestId": req_id,
        "transcript": transcript,
        "reply": reply,
        "audioUrl": audio_url,
    }
    if audio_streaming_url:
        resp["audioStreamingUrl"] = audio_streaming_url
    return resp
