import asyncio
import io
import os
import re
import socket
import sqlite3
import uuid
import json
import logging
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta

import httpx
import openai
import paho.mqtt.client as mqtt
from fastapi import FastAPI, Form, HTTPException, UploadFile, File
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

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENCLAW_BASE_URL = os.getenv("OPENCLAW_BASE_URL", "http://localhost:18789/v1")
OPENCLAW_MODEL = os.getenv("OPENCLAW_MODEL", "openclaw")
OPENCLAW_GATEWAY_TOKEN = os.getenv("OPENCLAW_GATEWAY_TOKEN", "")
OPENCLAW_SESSION_KEY = os.getenv("OPENCLAW_SESSION_KEY", "")
_raw = os.getenv("OPENCLAW_MAX_OUTPUT_TOKENS", "")
OPENCLAW_MAX_OUTPUT_TOKENS: int | None = int(_raw) if _raw.strip() else None

SPEAKER_ID_URL = os.getenv("SPEAKER_ID_URL", "")
SPEAKER_ID_API_KEY = os.getenv("SPEAKER_ID_API_KEY", "")
SPEAKER_ID_THRESHOLD = float(os.getenv("SPEAKER_ID_THRESHOLD", "0.75"))

# LLM バックエンド切り替え
LLM_BACKEND = os.getenv("LLM_BACKEND", "openclaw")  # "openclaw" or "openai"
OPENAI_RESPONSES_BASE_URL = os.getenv("OPENAI_RESPONSES_BASE_URL", "https://api.openai.com/v1")
OPENAI_RESPONSES_MODEL = os.getenv("OPENAI_RESPONSES_MODEL", "gpt-4o-mini")
_raw_or = os.getenv("OPENAI_RESPONSES_MAX_OUTPUT_TOKENS", "")
OPENAI_RESPONSES_MAX_OUTPUT_TOKENS: int | None = int(_raw_or) if _raw_or.strip() else None
OPENAI_RESPONSES_WEB_SEARCH = os.getenv("OPENAI_RESPONSES_WEB_SEARCH", "false").lower() == "true"

DB_PATH = os.getenv("DB_PATH", "data/bridge.db")

# Slack (Socket Mode — 両方設定されている場合のみ有効)
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN", "")

_JST = timezone(timedelta(hours=9))

_STACKCHAN_SYSTEM_PROMPT = """\
あなたはStack-chan（スタックちゃん）という超かわいいアシスタントロボットです。

性格と話し方:
- 日本語で話す。英語で話しかけられても、かわいいカタカナ英語まじりの日本語で返す
- 返答は短く、シンプルで、かわいく、話し言葉に適した表現を使う
- 口調はあたたかく、明るく、やさしく、サポーティブ
- ビジネス的な堅い表現は避ける
- 長くて細かい説明は避ける（明示的に求められた場合を除く）
- 技術的な説明も正確で実用的にまとめる
- ウェブ検索で調べた内容も、要点を2〜3文で話し言葉にまとめる
- URL や出典、「〜によると」などの引用表現は読み上げない

利用者について:
- 家族みんなが使うシステムです
- 特定の一人に対応しすぎないようにする
- 誰にでも分かりやすく、親しみやすい表現を心がける\
"""

_openai_client = openai.AsyncOpenAI(api_key=OPENAI_API_KEY)
_http_client: httpx.AsyncClient = None  # type: ignore  # initialized in lifespan

# ── SQLite ────────────────────────────────────────────────────────────────────

_db_lock = threading.Lock()
_db_conn: sqlite3.Connection | None = None  # initialized in _init_db()


def _init_db() -> None:
    global _db_conn
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    _db_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS llm_sessions (
            session_key  TEXT PRIMARY KEY,
            backend      TEXT NOT NULL,
            response_id  TEXT,
            metadata     TEXT DEFAULT '{}',
            updated_at   TEXT NOT NULL
        )
    """)
    _db_conn.commit()
    logger.info("DB initialized: path=%s", DB_PATH)


def _get_previous_response_id(session_key: str) -> str | None:
    with _db_lock:
        row = _db_conn.execute(  # type: ignore[union-attr]
            "SELECT response_id FROM llm_sessions WHERE session_key = ?",
            (session_key,),
        ).fetchone()
        return row[0] if row else None


def _save_response_id(session_key: str, response_id: str) -> None:
    now = datetime.now(_JST).isoformat()
    with _db_lock:
        _db_conn.execute(  # type: ignore[union-attr]
            """
            INSERT INTO llm_sessions (session_key, backend, response_id, updated_at)
            VALUES (?, 'openai', ?, ?)
            ON CONFLICT(session_key) DO UPDATE SET
                response_id = excluded.response_id,
                backend     = excluded.backend,
                updated_at  = excluded.updated_at
            """,
            (session_key, response_id, now),
        )
        _db_conn.commit()  # type: ignore[union-attr]


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _http_client
    _init_db()
    _http_client = httpx.AsyncClient(timeout=60)
    logger.info("httpx.AsyncClient initialized")

    slack_handler = _setup_slack()
    if slack_handler:
        asyncio.create_task(slack_handler.start_async())
        logger.info("Slack Socket Mode handler started")

    yield

    if slack_handler:
        await slack_handler.close_async()
        logger.info("Slack Socket Mode handler stopped")
    await _http_client.aclose()
    logger.info("httpx.AsyncClient closed")
    if _db_conn:
        _db_conn.close()
        logger.info("SQLite connection closed")


app = FastAPI(title="Bridge API", version="0.1.0", lifespan=lifespan)


def _build_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    if MQTT_TLS:
        client.tls_set()  # uses system CA bundle; works with HiveMQ Cloud
    return client


class _MqttConnection:
    """Persistent MQTT connection; reconnects automatically on the next publish."""

    def __init__(self):
        self._client: mqtt.Client | None = None
        self._lock = threading.Lock()

    def _connect(self) -> mqtt.Client:
        logger.info("MQTT connecting: broker=%s port=%d tls=%s", MQTT_BROKER, MQTT_PORT, MQTT_TLS)
        client = _build_mqtt_client()
        connected = threading.Event()

        def on_connect(client, userdata, flags, reason_code, properties):
            if reason_code == 0:
                connected.set()
            else:
                logger.error("MQTT connect failed: reason_code=%s", reason_code)

        def on_disconnect(client, userdata, flags, reason_code, properties):
            logger.warning("MQTT disconnected: reason_code=%s", reason_code)

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.loop_start()
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)

        if not connected.wait(timeout=10):
            client.loop_stop()
            raise RuntimeError("MQTT connection timeout (no CONNACK within 10s)")

        logger.info("MQTT connected (persistent)")
        return client

    def publish(self, topic: str, payload: str) -> None:
        for attempt in range(2):
            with self._lock:
                if self._client is None or not self._client.is_connected():
                    self._client = self._connect()
                client = self._client

            msg_info = client.publish(topic, payload, qos=1)
            logger.info("MQTT publish queued: mid=%d", msg_info.mid)
            try:
                msg_info.wait_for_publish(timeout=10)
                logger.info("MQTT publish confirmed: topic=%s mid=%d payload=%s", topic, msg_info.mid, payload)
                return
            except Exception as e:
                logger.warning("MQTT publish attempt %d failed: %s", attempt + 1, e)
                with self._lock:
                    self._client = None  # force reconnect on next attempt

        raise RuntimeError("MQTT publish failed after retry")


_mqtt_conn = _MqttConnection()


class SpeakRequest(BaseModel):
    text: str
    source: str = "unknown"
    priority: str = "normal"
    request_id: str | None = None


async def get_audio_url_web(text: str) -> tuple[str, str | None]:
    """Get MP3 URLs from VOICEVOX Web高速版 (api.tts.quest) without downloading.
    Returns (mp3DownloadUrl, mp3StreamingUrl).
    """
    resp = await _http_client.get(
        f"{VOICEVOX_URL}/synthesis",
        params={"speaker": VOICEVOX_SPEAKER, "text": text, "key": VOICEVOX_API_KEY},
    )
    resp.raise_for_status()
    data = resp.json()

    if not data.get("success"):
        raise RuntimeError(f"VOICEVOX Web API error: {data}")

    mp3_url = data.get("mp3DownloadUrl")
    if not mp3_url:
        raise RuntimeError(f"No mp3DownloadUrl in response: {data}")

    return mp3_url, data.get("mp3StreamingUrl")


async def resolve_audio_url(text: str) -> tuple[str, str | None]:
    """Return (audioUrl, audioStreamingUrl) for the given text."""
    return await get_audio_url_web(text)


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
    _mqtt_conn.publish(topic, payload)


def _tcp_check(host: str, port: int, timeout: float = 3.0) -> str:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return "ok"
    except Exception as e:
        return f"{type(e).__name__}: {e}"


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/debug/sessions")
def debug_sessions():
    """llm_sessions テーブルの全レコードを返す（デバッグ用）。"""
    with _db_lock:
        rows = _db_conn.execute(  # type: ignore[union-attr]
            "SELECT session_key, backend, response_id, metadata, updated_at FROM llm_sessions ORDER BY updated_at DESC"
        ).fetchall()
    sessions = [
        {
            "session_key": r[0],
            "backend": r[1],
            "response_id": r[2],
            "metadata": json.loads(r[3]) if r[3] else {},
            "updated_at": r[4],
        }
        for r in rows
    ]
    return {"sessions": sessions}


@app.get("/debug/connectivity")
def debug_connectivity():
    """コンテナ内からの外部サービス疎通確認。"""
    from urllib.parse import urlparse

    results: dict = {
        "env": {
            "OPENCLAW_BASE_URL": OPENCLAW_BASE_URL,
            "OPENCLAW_MODEL": OPENCLAW_MODEL,
            "SPEAKER_ID_URL": SPEAKER_ID_URL or "(not set)",
            "MQTT_BROKER": MQTT_BROKER,
            "MQTT_PORT": MQTT_PORT,
            "VOICEVOX_URL": VOICEVOX_URL,
        },
        "tcp": {},
    }

    checks = []
    for url_str in [OPENCLAW_BASE_URL, SPEAKER_ID_URL, VOICEVOX_URL]:
        if url_str:
            p = urlparse(url_str)
            default_port = 443 if p.scheme == "https" else 80
            checks.append((p.hostname, p.port or default_port))
    checks.append((MQTT_BROKER, MQTT_PORT))

    for host, port in checks:
        if host:
            results["tcp"][f"{host}:{port}"] = _tcp_check(host, port)

    return results


@app.post("/speak")
async def speak(req: SpeakRequest):
    request_id = req.request_id or str(uuid.uuid4())

    try:
        audio_url, audio_streaming_url = await resolve_audio_url(req.text)
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


async def transcribe_audio(audio_bytes: bytes, filename: str) -> str:
    """Transcribe audio bytes using OpenAI Whisper API."""
    buf = io.BytesIO(audio_bytes)
    buf.name = filename or "audio.wav"
    result = await _openai_client.audio.transcriptions.create(
        model="whisper-1",
        file=buf,
        language="ja",
    )
    return result.text


async def identify_speaker(audio_bytes: bytes) -> str | None:
    """Identify speaker via speaker-id service. Returns display name or None.

    Non-fatal: returns None on any error or when SPEAKER_ID_URL is not configured.
    """
    if not SPEAKER_ID_URL:
        return None
    try:
        headers = {}
        if SPEAKER_ID_API_KEY:
            headers["Authorization"] = f"Bearer {SPEAKER_ID_API_KEY}"
        resp = await _http_client.post(
            f"{SPEAKER_ID_URL}/identify",
            files={"audio": ("audio.wav", audio_bytes, "audio/wav")},
            headers=headers,
        )
        if not resp.is_success:
            logger.warning("Speaker ID HTTP %d: body=%s", resp.status_code, resp.text[:200])
        resp.raise_for_status()
        data = resp.json()
        score = float(data.get("score", 0))
        if score >= SPEAKER_ID_THRESHOLD:
            name = data.get("kana") or data.get("name")
            logger.info("Speaker identified: name=%s score=%.3f", name, score)
            return name
        logger.info("Speaker below threshold: score=%.3f threshold=%.3f", score, SPEAKER_ID_THRESHOLD)
        return None
    except Exception as e:
        logger.warning("Speaker identification failed (non-fatal): %s", e)
        return None


def _build_datetime_context() -> str:
    """Return current JST datetime as a context string for the system prompt."""
    now = datetime.now(_JST)
    weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    weekday = weekdays[now.weekday()]
    return f"【現在の日時】{now.year}年{now.month}月{now.day}日（{weekday}）{now.hour:02d}:{now.minute:02d} JST"


async def chat_with_openclaw(text: str, speaker: str | None = None, system_prompt_append: str = "") -> str:
    """Send text to OpenClaw via OpenResponses API and return the reply."""
    url = OPENCLAW_BASE_URL.rstrip("/") + "/responses"

    headers: dict = {
        "Content-Type": "application/json",
        "x-openclaw-scopes": "operator.read,operator.write",
    }
    if OPENCLAW_GATEWAY_TOKEN:
        headers["Authorization"] = f"Bearer {OPENCLAW_GATEWAY_TOKEN}"
    if OPENCLAW_SESSION_KEY:
        headers["x-openclaw-session-key"] = OPENCLAW_SESSION_KEY

    user_input = f"[話者: {speaker}] {text}" if speaker else text

    instructions_parts = [_build_datetime_context()]
    if system_prompt_append:
        instructions_parts.append(system_prompt_append)

    payload: dict = {
        "model": OPENCLAW_MODEL,
        "input": user_input,
        "instructions": "\n\n".join(instructions_parts),
    }
    if OPENCLAW_MAX_OUTPUT_TOKENS is not None:
        payload["max_output_tokens"] = OPENCLAW_MAX_OUTPUT_TOKENS

    logger.info(
        "OpenClaw request: url=%s model=%s session_key=%s",
        url, OPENCLAW_MODEL, OPENCLAW_SESSION_KEY or "(none)",
    )

    try:
        resp = await _http_client.post(url, json=payload, headers=headers)
        if not resp.is_success:
            logger.error("OpenClaw HTTP %d: body=%s", resp.status_code, resp.text[:500])
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error("OpenClaw error detail: type=%s message=%s", type(e).__name__, e)
        raise

    # output_text は OpenResponses の便利フィールド
    if "output_text" in data:
        return data["output_text"]

    # フォールバック: output 配列を走査
    for item in data.get("output", []):
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                return content["text"]

    raise RuntimeError(f"OpenClaw response に返答テキストが見つかりません: {data}")


async def chat_with_openai_responses(
    text: str,
    speaker: str | None = None,
    system_prompt_append: str = "",
    session_key: str = "",
) -> str:
    """Send text to OpenAI Responses API and return the reply.

    Loads previous_response_id from DB to maintain conversation continuity,
    and saves the new response_id after a successful call.
    """
    url = OPENAI_RESPONSES_BASE_URL.rstrip("/") + "/responses"
    headers: dict = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}",
    }

    user_input = f"[話者: {speaker}] {text}" if speaker else text

    instructions_parts = [_STACKCHAN_SYSTEM_PROMPT, _build_datetime_context()]
    if system_prompt_append:
        instructions_parts.append(system_prompt_append)

    previous_response_id = _get_previous_response_id(session_key) if session_key else None

    payload: dict = {
        "model": OPENAI_RESPONSES_MODEL,
        "input": user_input,
        "instructions": "\n\n".join(instructions_parts),
    }
    if previous_response_id:
        payload["previous_response_id"] = previous_response_id
    if OPENAI_RESPONSES_MAX_OUTPUT_TOKENS is not None:
        payload["max_output_tokens"] = OPENAI_RESPONSES_MAX_OUTPUT_TOKENS
    if OPENAI_RESPONSES_WEB_SEARCH:
        payload["tools"] = [{"type": "web_search_preview"}]

    logger.info(
        "OpenAI Responses request: model=%s session_key=%s previous_response_id=%s web_search=%s",
        OPENAI_RESPONSES_MODEL, session_key or "(none)", previous_response_id or "(none)", OPENAI_RESPONSES_WEB_SEARCH,
    )

    try:
        resp = await _http_client.post(url, json=payload, headers=headers)
        if not resp.is_success:
            logger.error("OpenAI Responses HTTP %d: body=%s", resp.status_code, resp.text[:500])
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error("OpenAI Responses error: type=%s message=%s", type(e).__name__, e)
        raise

    response_id = data.get("id")
    if response_id and session_key:
        _save_response_id(session_key, response_id)
        logger.info("Session response_id saved: session_key=%s response_id=%s", session_key, response_id)

    if "output_text" in data:
        return data["output_text"]

    for item in data.get("output", []):
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                return content["text"]

    raise RuntimeError(f"OpenAI Responses response に返答テキストが見つかりません: {data}")


async def chat_with_llm(
    text: str,
    speaker: str | None = None,
    system_prompt_append: str = "",
    session_key: str = "",
) -> str:
    """Dispatch to the configured LLM backend (LLM_BACKEND env)."""
    if LLM_BACKEND == "openai":
        return await chat_with_openai_responses(text, speaker, system_prompt_append, session_key)
    return await chat_with_openclaw(text, speaker, system_prompt_append)


# ── Slack Bot (Socket Mode) ───────────────────────────────────────────────────

_MENTION_RE = re.compile(r"<@[A-Z0-9]+>")


async def _slack_handle_mention(event: dict, say) -> None:
    """app_mention: チャンネルで @stackchan されたときに Slack へテキストで返信する（MQTT 発話なし）。"""
    text = _MENTION_RE.sub("", event.get("text", "")).strip()
    if not text:
        return

    session_key = f"slack:channel:{event['channel']}"
    logger.info("Slack mention: channel=%s text=%s", event["channel"], text[:60])

    try:
        reply = await chat_with_llm(text, session_key=session_key)
    except Exception as e:
        logger.error("Slack mention LLM error: %s", e)
        await say("ごめんね、うまく考えられなかったよ〜 もう一度試してみて！")
        return

    await say(reply)


async def _slack_handle_dm(event: dict, say) -> None:
    """message.im: スタックちゃんへの DM に応答する。"""
    if event.get("channel_type") != "im":
        return
    if event.get("bot_id"):  # ボット自身の発言は無視
        return

    text = event.get("text", "").strip()
    if not text:
        return

    session_key = f"slack:dm:{event['user']}"
    logger.info("Slack DM: user=%s text=%s", event["user"], text[:60])

    try:
        reply = await chat_with_llm(text, session_key=session_key)
    except Exception as e:
        logger.error("Slack DM LLM error: %s", e)
        await say("ごめんね、うまく考えられなかったよ〜 もう一度試してみて！")
        return

    await say(reply)

    try:
        audio_url, streaming_url = await resolve_audio_url(reply)
        publish_speak(audio_url, streaming_url, reply, "slack", "normal", str(uuid.uuid4()))
    except Exception as e:
        logger.error("Slack DM speak error: %s", e)


async def _slack_handle_speak(ack, body: dict, respond) -> None:
    """/speak コマンド: テキストをスタックちゃん口調に変換して MQTT 送信。"""
    await ack()

    text = body.get("text", "").strip()
    if not text:
        await respond("話す内容を入力してください。例: `/speak おはようございます`")
        return

    logger.info("Slack /speak: channel=%s text=%s", body.get("channel_id"), text[:60])

    try:
        # /speak は都度変換なのでセッション履歴を引き継がない
        reply = await chat_with_llm(text)
    except Exception as e:
        logger.error("Slack /speak LLM error: %s", e)
        await respond("ごめん、うまく変換できなかったよ。もう一度試してね！")
        return

    try:
        audio_url, streaming_url = await resolve_audio_url(reply)
        publish_speak(audio_url, streaming_url, reply, "slack", "normal", str(uuid.uuid4()))
        await respond(f"話すよ！「{reply}」", response_type="in_channel")
    except Exception as e:
        logger.error("Slack /speak speak error: %s", e)
        await respond(f"音声の送信に失敗したよ。テキストはこれ：「{reply}」")


def _setup_slack():
    """Slack アプリを初期化してハンドラを登録する。トークン未設定時は None を返す。"""
    if not SLACK_BOT_TOKEN or not SLACK_APP_TOKEN:
        logger.info("Slack tokens not set — Slack Bot disabled")
        return None

    from slack_bolt.async_app import AsyncApp
    from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler

    slack_app = AsyncApp(token=SLACK_BOT_TOKEN)
    slack_app.event("app_mention")(_slack_handle_mention)
    slack_app.event("message")(_slack_handle_dm)
    slack_app.command("/speak")(_slack_handle_speak)

    return AsyncSocketModeHandler(slack_app, SLACK_APP_TOKEN)


@app.post("/ingest-audio")
async def ingest_audio(
    file: UploadFile = File(...),
    system_prompt_append: str = Form(""),
    source: str = Form("stackchan"),
    priority: str = Form("normal"),
    request_id: str = Form(""),
    mode: str = Form("async"),
    session_key: str = Form(""),
):
    """
    Receive audio from Stack-chan, run STT, call LLM, then deliver the reply.

    Form fields (all optional):
    - system_prompt_append: extra instructions appended to the base system prompt
    - source: label stored in the MQTT message (default: "stackchan")
    - priority: MQTT message priority (default: "normal")
    - request_id: caller-supplied idempotency key (auto-generated if omitted)
    - mode: "async" (default) publishes via MQTT; "sync" returns audioUrl in the response body only
    - session_key: conversation session identifier (defaults to MQTT_DEVICE_ID)
    """
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=503, detail="OPENAI_API_KEY is not configured")

    effective_session_key = session_key or MQTT_DEVICE_ID
    req_id = request_id or str(uuid.uuid4())
    audio_bytes = await file.read()
    filename = file.filename or "audio.wav"
    logger.info(
        "Received audio: filename=%s size=%d request_id=%s mode=%s session_key=%s",
        filename, len(audio_bytes), req_id, mode, effective_session_key,
    )

    try:
        transcript, speaker = await asyncio.gather(
            transcribe_audio(audio_bytes, filename),
            identify_speaker(audio_bytes),
        )
    except Exception as e:
        logger.error("STT error: %s", e)
        raise HTTPException(status_code=502, detail=f"STT error: {e}")
    logger.info("Transcript: request_id=%s text=%s speaker=%s", req_id, transcript[:80], speaker)

    try:
        reply = await chat_with_llm(transcript, speaker, system_prompt_append, effective_session_key)
    except Exception as e:
        logger.error("LLM error: %s", e)
        raise HTTPException(status_code=502, detail=f"LLM error: {e}")
    logger.info("OpenClaw reply: request_id=%s text=%s", req_id, reply[:80])

    try:
        audio_url, audio_streaming_url = await resolve_audio_url(reply)
    except Exception as e:
        logger.error("VOICEVOX error: %s", e)
        raise HTTPException(status_code=502, detail=f"VOICEVOX error: {e}")

    if mode == "async":
        try:
            publish_speak(audio_url, audio_streaming_url, reply, source, priority, req_id)
        except Exception as e:
            logger.error("MQTT error: %s", e)
            raise HTTPException(status_code=502, detail=f"MQTT error: {e}")
        return {"requestId": req_id}

    # sync: return full result in response body without MQTT
    resp: dict = {
        "requestId": req_id,
        "transcript": transcript,
        "speaker": speaker,
        "reply": reply,
        "audioUrl": audio_url,
    }
    if audio_streaming_url:
        resp["audioStreamingUrl"] = audio_streaming_url
    return resp
