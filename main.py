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
from dataclasses import dataclass
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
MQTT_QOS = int(os.getenv("MQTT_QOS", "1"))

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
OPENAI_RESPONSES_WEB_SEARCH_TOOL = os.getenv("OPENAI_RESPONSES_WEB_SEARCH_TOOL", "web_search_preview")

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

# MQTT ACK 待機: requestId → asyncio.Event のマップ
_pending_acks: dict[str, asyncio.Event] = {}
# MQTT スレッドから asyncio へ通知するためのイベントループ参照（lifespan で設定）
_main_loop: asyncio.AbstractEventLoop | None = None
# タイマー管理: timer_id → asyncio.Task / _TimerInfo
_active_timers: dict[str, asyncio.Task] = {}
_active_timer_infos: dict[str, "_TimerInfo"] = {}  # list_timers で参照
# Slack アプリ参照（_setup_slack で設定、タイマー発火時の通知に使用）
_slack_app = None  # type: ignore

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


# ── Timer ─────────────────────────────────────────────────────────────────────


@dataclass
class _TimerInfo:
    timer_id: str
    label: str
    fire_at: datetime
    session_key: str
    slack_channel: str | None  # 設定元が Slack の場合に発火後通知するチャンネル
    snooze_seconds: int | None  # スヌーズ秒数（None = スヌーズなし）


_TIMER_TOOLS = [
    {
        "type": "function",
        "name": "set_timer",
        "description": (
            "タイマーを設定する。指定した秒数後にStack-chanが声で知らせる。"
            "スヌーズ秒数を指定すると、発火後に一度だけ再通知できる。"
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "label": {
                    "type": "string",
                    "description": "タイマーのラベル（例：宿題確認、おやつの時間）",
                },
                "seconds": {
                    "type": "integer",
                    "description": "何秒後に発火するか",
                },
                "snooze_seconds": {
                    "type": "integer",
                    "description": "スヌーズの秒数（省略可）。指定すると発火後にもう一度声かけする",
                },
            },
            "required": ["label", "seconds"],
        },
    },
    {
        "type": "function",
        "name": "list_timers",
        "description": "現在セットされているタイマーの一覧と残り時間を返す。",
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
]


async def _fire_timer(info: _TimerInfo) -> None:
    """タイマー発火処理: LLMで声かけ文を生成 → VOICEVOX → MQTT、Slack 経由なら Slack にも通知。"""
    prompt = f"タイマー「{info.label}」の時間になりました。短く明るく声かけしてください。"
    timer_instruction = (
        "これはタイマーの発火通知です。"
        "スタックちゃんとして、その場にいる家族に向けて声かけしてください。"
        "依頼者への返答にはしないでください。"
    )
    try:
        message = await chat_with_llm(
            prompt,
            system_prompt_append=timer_instruction,
            session_key=info.session_key,
            use_functions=False,  # タイマー発火中は新たなタイマーを受け付けない
        )
    except Exception as e:
        logger.error("Timer LLM error: timer_id=%s error=%s", info.timer_id, e)
        message = f"{info.label}の時間だよ！"

    # MQTT で発話（常に実行）
    try:
        audio_url, streaming_url = await resolve_audio_url(message)
        req_id = str(uuid.uuid4())
        publish_speak(audio_url, streaming_url, message, "timer", "normal", req_id)
        logger.info("Timer fired: timer_id=%s label=%s message=%s", info.timer_id, info.label, message[:60])
    except Exception as e:
        logger.error("Timer speak error: timer_id=%s error=%s", info.timer_id, e)
        return

    # Slack 経由で設定された場合は Slack にも完了通知
    if info.slack_channel and _slack_app:
        try:
            await _slack_app.client.chat_postMessage(
                channel=info.slack_channel,
                text=f"⏰ タイマー「{info.label}」が発火しました：「{message}」",
            )
            logger.info("Timer Slack notified: channel=%s", info.slack_channel)
        except Exception as e:
            logger.warning("Timer Slack notify error: %s", e)

    # スヌーズが設定されている場合は次のタイマーを登録（一回のみ）
    if info.snooze_seconds:
        snooze_id = _register_timer(
            label=f"{info.label}（スヌーズ）",
            seconds=info.snooze_seconds,
            session_key=info.session_key,
            slack_channel=info.slack_channel,
            snooze_seconds=None,
        )
        logger.info("Timer snooze registered: original=%s snooze=%s", info.timer_id, snooze_id)


async def _run_timer(info: _TimerInfo) -> None:
    """asyncio.Task として動作するタイマー。delay 後に _fire_timer を呼ぶ。"""
    delay = (info.fire_at - datetime.now(_JST)).total_seconds()
    if delay > 0:
        await asyncio.sleep(delay)
    _active_timers.pop(info.timer_id, None)
    _active_timer_infos.pop(info.timer_id, None)
    try:
        await _fire_timer(info)
    except Exception as e:
        logger.error("Timer _run_timer error: timer_id=%s error=%s", info.timer_id, e)


def _register_timer(
    label: str,
    seconds: int,
    session_key: str = "",
    slack_channel: str | None = None,
    snooze_seconds: int | None = None,
) -> str:
    """タイマーを登録して asyncio.Task を起動し、timer_id を返す。"""
    timer_id = str(uuid.uuid4())
    fire_at = datetime.now(_JST) + timedelta(seconds=seconds)
    info = _TimerInfo(
        timer_id=timer_id,
        label=label,
        fire_at=fire_at,
        session_key=session_key,
        slack_channel=slack_channel,
        snooze_seconds=snooze_seconds,
    )
    task = asyncio.create_task(_run_timer(info))
    _active_timers[timer_id] = task
    _active_timer_infos[timer_id] = info
    logger.info(
        "Timer registered: timer_id=%s label=%s fire_at=%s slack_channel=%s snooze=%s",
        timer_id, label, fire_at.isoformat(), slack_channel, snooze_seconds,
    )
    return timer_id


async def _handle_function_calls(
    output: list,
    notify_context: dict,
) -> list | None:
    """output 配列に function_call があれば実行して function_call_output リストを返す。なければ None。"""
    function_calls = [item for item in output if item.get("type") == "function_call"]
    if not function_calls:
        return None

    results = []
    for fc in function_calls:
        name = fc.get("name", "")
        # OpenAI Responses API: function_call には id（アイテムID: fc_xxx）と
        # call_id（参照用: call_xxx）の 2 フィールドがある。function_call_output には
        # call_id を使う必要がある。id と call_id が同じ場合のフォールバックも持つ。
        call_id = fc.get("call_id") or fc.get("id", "")
        try:
            args = json.loads(fc.get("arguments", "{}"))
        except json.JSONDecodeError:
            args = {}

        if name == "set_timer":
            timer_id = _register_timer(
                label=args.get("label", "タイマー"),
                seconds=int(args.get("seconds", 60)),
                session_key=notify_context.get("session_key", ""),
                slack_channel=notify_context.get("slack_channel"),
                snooze_seconds=args.get("snooze_seconds"),
            )
            result: dict = {
                "status": "ok",
                "timer_id": timer_id,
                "label": args.get("label"),
                "seconds": args.get("seconds"),
            }
            logger.info(
                "Function call set_timer: label=%s seconds=%s timer_id=%s",
                args.get("label"), args.get("seconds"), timer_id,
            )
        elif name == "list_timers":
            now = datetime.now(_JST)
            timers = []
            for info in _active_timer_infos.values():
                remaining = max(0, int((info.fire_at - now).total_seconds()))
                timers.append({
                    "timer_id": info.timer_id,
                    "label": info.label,
                    "fire_at": info.fire_at.isoformat(),
                    "remaining_seconds": remaining,
                })
            result = {"status": "ok", "timers": timers, "count": len(timers)}
            logger.info("Function call list_timers: count=%d", len(timers))
        else:
            result = {"status": "error", "message": f"Unknown function: {name}"}
            logger.warning("Unknown function call: name=%s", name)

        results.append({
            "type": "function_call_output",
            "call_id": call_id,
            "output": json.dumps(result, ensure_ascii=False),
        })

    return results


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _http_client, _main_loop
    _main_loop = asyncio.get_running_loop()
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

        def on_message(client, userdata, message):
            try:
                data = json.loads(message.payload)
                req_id = data.get("id")
                if req_id and _main_loop:
                    event = _pending_acks.get(req_id)
                    if event:
                        _main_loop.call_soon_threadsafe(event.set)
                        logger.info("MQTT ACK received: requestId=%s", req_id)
            except Exception as e:
                logger.warning("MQTT ACK parse error: %s", e)

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = on_message
        client.loop_start()
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)

        if not connected.wait(timeout=10):
            client.loop_stop()
            raise RuntimeError("MQTT connection timeout (no CONNACK within 10s)")

        client.subscribe("stackchan/ack", qos=MQTT_QOS)
        logger.info("MQTT connected (persistent), subscribed to stackchan/ack qos=%d", MQTT_QOS)
        return client

    def publish(self, topic: str, payload: str) -> None:
        for attempt in range(2):
            with self._lock:
                if self._client is None or not self._client.is_connected():
                    self._client = self._connect()
                client = self._client

            msg_info = client.publish(topic, payload, qos=MQTT_QOS)
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


async def wait_for_ack(request_id: str, timeout: float = 5.0) -> bool:
    """stackchan/ack トピックで requestId に対応する ACK を待つ。

    publish_speak より前に _pending_acks に event を登録しておくと、
    ACK が先に届いた場合も取りこぼさない。
    Returns True if ACK received within timeout, False otherwise.
    """
    event = _pending_acks.get(request_id)
    if event is None:
        event = asyncio.Event()
        _pending_acks[request_id] = event
    try:
        await asyncio.wait_for(event.wait(), timeout=timeout)
        return True
    except asyncio.TimeoutError:
        logger.warning("MQTT ACK timeout: requestId=%s", request_id)
        return False
    finally:
        _pending_acks.pop(request_id, None)


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


@app.get("/debug/timers")
def debug_timers():
    """アクティブなタイマー一覧を返す（デバッグ用）。"""
    now = datetime.now(_JST)
    timers = []
    for info in _active_timer_infos.values():
        remaining = max(0, int((info.fire_at - now).total_seconds()))
        timers.append({
            "timer_id": info.timer_id,
            "label": info.label,
            "fire_at": info.fire_at.isoformat(),
            "remaining_seconds": remaining,
            "slack_channel": info.slack_channel,
            "snooze_seconds": info.snooze_seconds,
        })
    return {"active_count": len(timers), "timers": timers}


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


async def chat_with_openclaw(
    text: str,
    speaker: str | None = None,
    system_prompt_append: str = "",
    notify_context: dict | None = None,
    use_functions: bool = True,
) -> str:
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

    user_input: str | list = f"[話者: {speaker}] {text}" if speaker else text

    instructions_parts = [_build_datetime_context()]
    if system_prompt_append:
        instructions_parts.append(system_prompt_append)

    tools = list(_TIMER_TOOLS) if use_functions else []

    logger.info(
        "OpenClaw request: url=%s model=%s session_key=%s",
        url, OPENCLAW_MODEL, OPENCLAW_SESSION_KEY or "(none)",
    )

    for _ in range(5):  # Function calling ループ（最大 5 回）
        payload: dict = {
            "model": OPENCLAW_MODEL,
            "input": user_input,
            "instructions": "\n\n".join(instructions_parts),
        }
        if OPENCLAW_MAX_OUTPUT_TOKENS is not None:
            payload["max_output_tokens"] = OPENCLAW_MAX_OUTPUT_TOKENS
        if tools:
            payload["tools"] = tools

        try:
            resp = await _http_client.post(url, json=payload, headers=headers)
            if not resp.is_success:
                logger.error("OpenClaw HTTP %d: body=%s", resp.status_code, resp.text[:500])
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error("OpenClaw error detail: type=%s message=%s", type(e).__name__, e)
            raise

        output = data.get("output", [])
        function_outputs = await _handle_function_calls(output, notify_context or {})

        if function_outputs is None:
            # Function call なし → テキストを返す
            if "output_text" in data:
                return data["output_text"]
            for item in output:
                for content in item.get("content", []):
                    if content.get("type") == "output_text":
                        return content["text"]
            raise RuntimeError(f"OpenClaw response に返答テキストが見つかりません: {data}")

        # Function call あり → 結果を渡して継続
        user_input = function_outputs

    raise RuntimeError("OpenClaw function calling loop exceeded max iterations")


async def chat_with_openai_responses(
    text: str,
    speaker: str | None = None,
    system_prompt_append: str = "",
    session_key: str = "",
    notify_context: dict | None = None,
    use_functions: bool = True,
) -> str:
    """Send text to OpenAI Responses API and return the reply.

    Loads previous_response_id from DB to maintain conversation continuity,
    and saves the new response_id after a successful call.
    Supports function calling loop for set_timer and other tools.
    """
    url = OPENAI_RESPONSES_BASE_URL.rstrip("/") + "/responses"
    headers: dict = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}",
    }

    user_input: str | list = f"[話者: {speaker}] {text}" if speaker else text

    instructions_parts = [_STACKCHAN_SYSTEM_PROMPT, _build_datetime_context()]
    if system_prompt_append:
        instructions_parts.append(system_prompt_append)

    previous_response_id = _get_previous_response_id(session_key) if session_key else None

    tools = list(_TIMER_TOOLS) if use_functions else []
    if OPENAI_RESPONSES_WEB_SEARCH:
        tools.append({"type": OPENAI_RESPONSES_WEB_SEARCH_TOOL})

    logger.info(
        "OpenAI Responses request: model=%s session_key=%s previous_response_id=%s web_search=%s",
        OPENAI_RESPONSES_MODEL, session_key or "(none)", previous_response_id or "(none)", OPENAI_RESPONSES_WEB_SEARCH,
    )

    for _ in range(5):  # Function calling ループ（最大 5 回）
        payload: dict = {
            "model": OPENAI_RESPONSES_MODEL,
            "input": user_input,
            "instructions": "\n\n".join(instructions_parts),
        }
        if previous_response_id:
            payload["previous_response_id"] = previous_response_id
        if OPENAI_RESPONSES_MAX_OUTPUT_TOKENS is not None:
            payload["max_output_tokens"] = OPENAI_RESPONSES_MAX_OUTPUT_TOKENS
        if tools:
            payload["tools"] = tools

        try:
            resp = await _http_client.post(url, json=payload, headers=headers)

            # previous_response_id が壊れた状態（未解決の function_call が残っている）の場合、
            # リセットして同じ入力で再試行する。会話の連続性は失われるが処理は継続できる。
            # 注意: function_call_output 送信中（user_input がリスト）はリセットしない。
            #       そこで 400 が出るのは call_id の不一致など別の問題であり、
            #       リセットすると function_call_output だけが残って状況が悪化する。
            if (
                resp.status_code == 400
                and previous_response_id
                and isinstance(user_input, str)
                and "tool" in resp.text.lower()
            ):
                logger.warning(
                    "previous_response_id has unresolved function call, resetting and retrying: "
                    "session_key=%s previous_response_id=%s",
                    session_key, previous_response_id,
                )
                previous_response_id = None
                payload.pop("previous_response_id", None)
                resp = await _http_client.post(url, json=payload, headers=headers)

            if not resp.is_success:
                logger.error("OpenAI Responses HTTP %d: body=%s", resp.status_code, resp.text[:500])
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error("OpenAI Responses error: type=%s message=%s", type(e).__name__, e)
            raise

        response_id = data.get("id")
        output = data.get("output", [])
        function_outputs = await _handle_function_calls(output, notify_context or {})

        if function_outputs is None:
            # 最終テキストレスポンス → ここでのみ DB に保存する
            # （function_call の中間レスポンス ID を保存すると次回の会話で 400 エラーになるため）
            if response_id and session_key:
                _save_response_id(session_key, response_id)
                logger.info("Session response_id saved: session_key=%s response_id=%s", session_key, response_id)
            if "output_text" in data:
                return data["output_text"]
            for item in output:
                for content in item.get("content", []):
                    if content.get("type") == "output_text":
                        return content["text"]
            raise RuntimeError(f"OpenAI Responses response に返答テキストが見つかりません: {data}")

        # Function call あり → ループ内での previous_response_id を更新して継続
        # （DB には保存しない。function_call の未解決状態を DB に残さないため）
        if response_id:
            previous_response_id = response_id
        user_input = function_outputs

    raise RuntimeError("OpenAI Responses function calling loop exceeded max iterations")


async def chat_with_llm(
    text: str,
    speaker: str | None = None,
    system_prompt_append: str = "",
    session_key: str = "",
    notify_context: dict | None = None,
    use_functions: bool = True,
) -> str:
    """Dispatch to the configured LLM backend (LLM_BACKEND env).

    notify_context: {"session_key": str, "slack_channel": str | None}
        - session_key: タイマー発火時に LLM が使うセッションキー
        - slack_channel: タイマー発火時に Slack 通知するチャンネル（None = MQTT のみ）
    use_functions: False にすると Function Calling ツールを含めない（タイマー発火時など）
    """
    if LLM_BACKEND == "openai":
        return await chat_with_openai_responses(
            text, speaker, system_prompt_append, session_key, notify_context, use_functions
        )
    return await chat_with_openclaw(text, speaker, system_prompt_append, notify_context, use_functions)


# ── Slack Bot (Socket Mode) ───────────────────────────────────────────────────

_MENTION_RE = re.compile(r"<@[A-Z0-9]+>")


async def _slack_handle_mention(event: dict, say) -> None:
    """app_mention: チャンネルで @stackchan されたときに Slack へテキストで返信する（MQTT 発話なし）。"""
    text = _MENTION_RE.sub("", event.get("text", "")).strip()
    if not text:
        return

    channel = event["channel"]
    session_key = f"slack:channel:{channel}"
    logger.info("Slack mention: channel=%s text=%s", channel, text[:60])

    try:
        reply = await chat_with_llm(
            text,
            session_key=session_key,
            notify_context={"session_key": session_key, "slack_channel": channel},
        )
    except Exception as e:
        logger.error("Slack mention LLM error: %s", e)
        await say("ごめんね、うまく考えられなかったよ〜 もう一度試してみて！")
        return

    await say(reply)


async def _slack_handle_dm(event: dict, say) -> None:
    """message.im: スタックちゃんへの DM に Slack テキストで返信する（MQTT 発話なし）。"""
    if event.get("channel_type") != "im":
        return
    if event.get("bot_id"):  # ボット自身の発言は無視
        return

    text = event.get("text", "").strip()
    if not text:
        return

    channel = event.get("channel", "")
    user = event["user"]
    session_key = f"slack:dm:{user}"
    logger.info("Slack DM: user=%s text=%s", user, text[:60])

    try:
        reply = await chat_with_llm(
            text,
            session_key=session_key,
            notify_context={"session_key": session_key, "slack_channel": channel},
        )
    except Exception as e:
        logger.error("Slack DM LLM error: %s", e)
        await say("ごめんね、うまく考えられなかったよ〜 もう一度試してみて！")
        return

    await say(reply)


async def _slack_handle_speak(ack, body: dict, respond) -> None:
    """/speak コマンド: テキストをスタックちゃん口調に変換して MQTT 送信。"""
    await ack()

    text = body.get("text", "").strip()
    if not text:
        await respond("話す内容を入力してください。例: `/speak おはようございます`")
        return

    logger.info("Slack /speak: channel=%s text=%s", body.get("channel_id"), text[:60])

    try:
        # /speak は「みんなへの発信」なので、依頼者への返答にならないよう指示を加える
        speak_instruction = (
            "以下はスタックちゃんがその場にいる人に向けて話す内容の原文です。"
            "この内容をスタックちゃんらしい口調に変換してください。"
            "依頼した人への返答や呼びかけにはしないでください。"
        )
        reply = await chat_with_llm(text, system_prompt_append=speak_instruction, use_functions=False)
    except Exception as e:
        logger.error("Slack /speak LLM error: %s", e)
        await respond("ごめん、うまく変換できなかったよ。もう一度試してね！")
        return

    try:
        audio_url, streaming_url = await resolve_audio_url(reply)
        req_id = str(uuid.uuid4())
        # ACK が publish_speak より先に届いても取りこぼさないよう、先に event を登録する
        _pending_acks[req_id] = asyncio.Event()
        publish_speak(audio_url, streaming_url, reply, "slack", "normal", req_id)
    except Exception as e:
        _pending_acks.pop(req_id, None)
        logger.error("Slack /speak speak error: %s", e)
        await respond(f"音声の送信に失敗したよ。テキストはこれ：「{reply}」")
        return

    ack_ok = await wait_for_ack(req_id)
    if ack_ok:
        await respond(f"話すよ！「{reply}」", response_type="in_channel")
    else:
        await respond(f"⚠️ スタックちゃんから応答がなかったよ。届いてないかも。「{reply}」", response_type="in_channel")


_DURATION_RE = re.compile(
    r"^(?:(\d{1,2}):(\d{2}))"       # HH:MM
    r"|(?:(\d+)\s*(h|m|s|時間|分|秒))"  # 数値 + 単位
    r"|(\d+)$",                       # 数値のみ（分とみなす）
    re.IGNORECASE,
)


def _parse_duration(token: str) -> int | None:
    """時間指定トークンを秒数に変換する。解析不能の場合は None を返す。
    例: '3m' → 180, '1h' → 3600, '10s' → 10, '14:30' → 今日の 14:30 JST まで, '30' → 1800
    """
    m = _DURATION_RE.match(token.strip())
    if not m:
        return None

    hh, mm, num, unit, bare = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5)

    if hh is not None:
        # 絶対時刻 HH:MM
        now = datetime.now(_JST)
        target = now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        return max(1, int((target - now).total_seconds()))

    if num is not None:
        n = int(num)
        u = unit.lower()
        if u in ("h", "時間"):
            return n * 3600
        if u in ("m", "分"):
            return n * 60
        if u in ("s", "秒"):
            return n
        return None

    if bare is not None:
        return int(bare) * 60  # 数値のみ → 分

    return None


async def _slack_handle_timer(ack, body: dict, respond) -> None:
    """/timer コマンド: 構造化フォーマットでタイマーを設定する。

    書式: /timer <時間> <ラベル>
      時間例: 3m, 1h, 30s, 14:30, 90（分）
      ラベル例: 宿題確認, おやつの時間
    """
    await ack()

    raw = body.get("text", "").strip()
    if not raw:
        await respond(
            "使い方: `/timer <時間> <ラベル>`\n"
            "時間の例: `3m`（3分）, `1h`（1時間）, `30s`（30秒）, `14:30`（14時30分）\n"
            "例: `/timer 30m 宿題確認`"
        )
        return

    parts = raw.split(None, 1)
    duration_token = parts[0]
    label = parts[1].strip() if len(parts) > 1 else duration_token

    seconds = _parse_duration(duration_token)
    if seconds is None:
        await respond(
            f"⚠️ 時間の指定が解析できなかったよ：`{duration_token}`\n"
            "例: `3m`, `1h`, `30s`, `14:30`, `90`（分）"
        )
        return

    channel_id = body.get("channel_id", "")
    timer_id = _register_timer(
        label=label,
        seconds=seconds,
        session_key="",
        slack_channel=channel_id,
    )

    minutes, secs = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        human = f"{hours}時間{minutes}分後" if minutes else f"{hours}時間後"
    elif minutes:
        human = f"{minutes}分{secs}秒後" if secs else f"{minutes}分後"
    else:
        human = f"{secs}秒後"

    await respond(
        f"⏰ タイマーをセットしたよ！\n"
        f"・ラベル: {label}\n"
        f"・時間: {human}\n"
        f"・ID: `{timer_id}`",
        response_type="in_channel",
    )
    logger.info("Slack /timer: channel=%s label=%s seconds=%d timer_id=%s", channel_id, label, seconds, timer_id)


def _setup_slack():
    """Slack アプリを初期化してハンドラを登録する。トークン未設定時は None を返す。"""
    global _slack_app
    if not SLACK_BOT_TOKEN or not SLACK_APP_TOKEN:
        logger.info("Slack tokens not set — Slack Bot disabled")
        return None

    from slack_bolt.async_app import AsyncApp
    from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler

    _slack_app = AsyncApp(token=SLACK_BOT_TOKEN)
    _slack_app.event("app_mention")(_slack_handle_mention)
    _slack_app.event("message")(_slack_handle_dm)
    _slack_app.command("/speak")(_slack_handle_speak)
    _slack_app.command("/timer")(_slack_handle_timer)

    return AsyncSocketModeHandler(_slack_app, SLACK_APP_TOKEN)


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
        reply = await chat_with_llm(
            transcript,
            speaker,
            system_prompt_append,
            effective_session_key,
            notify_context={"session_key": effective_session_key, "slack_channel": None},
        )
    except Exception as e:
        logger.error("LLM error: %s", e)
        raise HTTPException(status_code=502, detail=f"LLM error: {e}")
    logger.info("LLM reply: backend=%s request_id=%s text=%s", LLM_BACKEND, req_id, reply[:80])

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
