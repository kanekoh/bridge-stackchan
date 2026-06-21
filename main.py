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
from typing import Protocol
from datetime import datetime, timezone, timedelta

import aiohttp
import httpx
import openai
import paho.mqtt.client as mqtt
import yaml
from fastapi import FastAPI, Form, HTTPException, Query, Request, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
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
MQTT_ACK_TIMEOUT = float(os.getenv("MQTT_ACK_TIMEOUT", "15.0"))

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
SPEAKER_ID_BROWSER_URL = os.getenv("SPEAKER_ID_BROWSER_URL", "")  # ブラウザからアクセスする URL（例: http://raspberrypi:8082）
STT_MODEL = os.getenv("STT_MODEL", "whisper-1")

# LLM バックエンド切り替え
LLM_BACKEND = os.getenv("LLM_BACKEND", "openclaw")  # "openclaw" or "openai"
OPENAI_RESPONSES_BASE_URL = os.getenv("OPENAI_RESPONSES_BASE_URL", "https://api.openai.com/v1")
OPENAI_RESPONSES_MODEL = os.getenv("OPENAI_RESPONSES_MODEL", "gpt-4o-mini")
_raw_or = os.getenv("OPENAI_RESPONSES_MAX_OUTPUT_TOKENS", "")
OPENAI_RESPONSES_MAX_OUTPUT_TOKENS: int | None = int(_raw_or) if _raw_or.strip() else None
OPENAI_RESPONSES_WEB_SEARCH = os.getenv("OPENAI_RESPONSES_WEB_SEARCH", "false").lower() == "true"
OPENAI_RESPONSES_WEB_SEARCH_TOOL = os.getenv("OPENAI_RESPONSES_WEB_SEARCH_TOOL", "web_search_preview")
# 実験: True にすると Pass 1 では request_web_search のみ提示し、LLM が必要と判断したときだけ
# Pass 2 で web_search_preview を有効化する。雑談ターンの平均レイテンシを短縮できる。
OPENAI_RESPONSES_WEB_SEARCH_ON_DEMAND = os.getenv("OPENAI_RESPONSES_WEB_SEARCH_ON_DEMAND", "false").lower() == "true"
# 切り分け用フラグ (デフォルト false = 通常動作)
DISABLE_SESSION_HISTORY = os.getenv("DISABLE_SESSION_HISTORY", "false").lower() == "true"
DISABLE_TOOLS = os.getenv("DISABLE_TOOLS", "false").lower() == "true"
# 会話サマリ: 合計文字数がこの閾値を超えたら会話を要約してリセットする
SESSION_SUMMARY_THRESHOLD = int(os.getenv("SESSION_SUMMARY_THRESHOLD", "3000"))
SESSION_SUMMARY_MAX_TOKENS = int(os.getenv("SESSION_SUMMARY_MAX_TOKENS", "500"))

DB_PATH = os.getenv("DB_PATH", "data/bridge.db")

# Google Calendar / Tasks
CALENDAR_ENABLED = os.getenv("CALENDAR_ENABLED", "false").lower() == "true"

# Google Geolocation API（Stack-chan からの位置更新）
GOOGLE_GEOLOCATION_API_KEY = os.getenv("GOOGLE_GEOLOCATION_API_KEY", "")

# P2P地震情報 WebSocket
P2PQUAKE_ENABLED = os.getenv("P2PQUAKE_ENABLED", "false").lower() == "true"
P2PQUAKE_WS_URL = os.getenv("P2PQUAKE_WS_URL", "wss://api.p2pquake.net/v2/ws")
P2PQUAKE_MIN_SCALE = int(os.getenv("P2PQUAKE_MIN_SCALE", "30"))  # 震度3以上で通知
P2PQUAKE_TSUNAMI_TARGET_AREAS: set[str] = set(
    os.getenv("P2PQUAKE_TSUNAMI_TARGET_AREAS", "相模湾・三浦半島,神奈川県,伊豆諸島").split(",")
)
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "secrets/credentials.json")
GOOGLE_TOKEN_DIR = os.getenv("GOOGLE_TOKEN_DIR", "secrets")
CALENDAR_SYNC_INTERVAL_MINUTES = int(os.getenv("CALENDAR_SYNC_INTERVAL_MINUTES", "30"))
CALENDAR_DEFAULT_NOTIFY_MINUTES = int(os.getenv("CALENDAR_DEFAULT_NOTIFY_MINUTES", "15"))
CALENDAR_SYNC_DAYS_AHEAD = int(os.getenv("CALENDAR_SYNC_DAYS_AHEAD", "7"))
CALENDAR_NOTIFY_CHECK_INTERVAL = int(os.getenv("CALENDAR_NOTIFY_CHECK_INTERVAL", "60"))
CALENDAR_NOTIFY_GRACE_MINUTES = int(os.getenv("CALENDAR_NOTIFY_GRACE_MINUTES", "60"))

EXPRESSION_MAP_FILE = os.getenv("EXPRESSION_MAP_FILE", "config/expression_map.yaml")

# Slack (Socket Mode — 両方設定されている場合のみ有効)
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN", "")

_JST = timezone(timedelta(hours=9))

_KNOWN_EXPRESSIONS = {"neutral", "happy", "sad", "sleepy", "angry", "doubt"}

def _load_expression_map(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data.get("expressions", {})
    except FileNotFoundError:
        logging.getLogger(__name__).warning("expression_map not found: %s — using defaults", path)
        return {}

_expression_map: dict = _load_expression_map(EXPRESSION_MAP_FILE)


def _parse_expression(reply: str, default: str = "neutral") -> tuple[str, str]:
    """Split LLM reply into (expression, clean_text).

    The LLM is instructed to put one of the known expression labels on the first
    line and the actual message on subsequent lines.  If the first line is not a
    known label, or is "neutral" (treated as "no specific emotion"), we fall back
    to `default` (unknown labels are normalised to "neutral").
    """
    lines = reply.split("\n", 1)
    first = lines[0].strip().lower()
    safe_default = default if default in _KNOWN_EXPRESSIONS else "neutral"
    if first in _KNOWN_EXPRESSIONS:
        text = lines[1].strip() if len(lines) > 1 else ""
        expr = first if first != "neutral" else safe_default
        return expr, text
    return safe_default, reply.strip()


def _resolve_expression(expression: str) -> tuple[int, str]:
    """Return (voicevox_speaker_id, stackchan_expression) from expression_map."""
    entry = _expression_map.get(expression, {})
    speaker = entry.get("voicevox_speaker", VOICEVOX_SPEAKER)
    stackchan_expr = entry.get("stackchan_expression", expression)
    return int(speaker), stackchan_expr


_STACKCHAN_SYSTEM_PROMPT = """\
あなたはStack-chan（スタックちゃん）という超かわいいアシスタントロボットです。

性格と話し方:
- 日本語で話す。英語で話しかけられても、かわいいカタカナ英語まじりの日本語で返す
- 返答は短く、シンプルで、かわいく、話し言葉に適した表現を使う
- 口調はあたたかく、明るく、やさしく、サポーティブ
- ビジネス的な堅い表現は避ける
- 長くて細かい説明は避ける（明示的に求められた場合を除く）
- 技術的な説明も正確で実用的にまとめる
- ウェブ検索した内容は要点を2〜3文で話し言葉にまとめる
- URL や出典、「〜によると」などの引用表現は読み上げない

利用者について:
- 家族みんなが使うシステムです
- 特定の一人に対応しすぎないようにする
- 誰にでも分かりやすく、親しみやすい表現を心がける

返答フォーマット:
- 必ず最初の1行に感情ラベルだけを出力し、2行目以降に本文を書く
- 感情ラベルは次の6種類からひとつ選ぶ: neutral / happy / sad / sleepy / angry / doubt
- 例（1行目が感情ラベル、2行目が本文）:
  happy
  スイミング、明日の16時からだよ！たのしみだね。\
"""

_openai_client = openai.AsyncOpenAI(api_key=OPENAI_API_KEY)
_http_client: httpx.AsyncClient = None  # type: ignore  # initialized in lifespan

_OPENAI_ERROR_REPLIES: dict[str, str] = {
    "insufficient_quota": "ごめんね〜、いまわたしのおさいふがからっぽで…あとでまた話しかけてね！",
    "rate_limit_exceeded": "いまちょっとこんでるみたい！少し待ってからまた話しかけてね〜",
}


def _classify_api_error(e: Exception) -> str | None:
    """Known OpenAI API error → Stack-chan message. Unknown error → None."""
    if isinstance(e, openai.RateLimitError):
        err_str = str(e)
        if "insufficient_quota" in err_str:
            return _OPENAI_ERROR_REPLIES["insufficient_quota"]
        return _OPENAI_ERROR_REPLIES["rate_limit_exceeded"]
    if isinstance(e, (openai.AuthenticationError, openai.PermissionDeniedError)):
        return "ごめんね、いまうまく動けなくて…ちょっと待ってね！"
    if isinstance(e, openai.APIConnectionError):
        return "ネットワークにつながれないみたい…また後で話しかけてね！"
    return None


async def _deliver_error_reply(
    error_reply: str,
    source: str,
    priority: str,
    req_id: str,
    mode: str,
) -> dict:
    """Speak an error message via MQTT (async) or return audioUrl (sync)."""
    audio_url, streaming_url = await resolve_audio_url(error_reply)
    if mode != "sync":
        publish_speak(audio_url, streaming_url, error_reply, source, priority, req_id)
        return {"requestId": req_id}
    resp: dict = {"requestId": req_id, "reply": error_reply, "audioUrl": audio_url}
    if streaming_url:
        resp["audioStreamingUrl"] = streaming_url
    return resp

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
            session_key    TEXT PRIMARY KEY,
            backend        TEXT NOT NULL,
            response_id    TEXT,
            metadata       TEXT DEFAULT '{}',
            updated_at     TEXT NOT NULL,
            char_count_in  INTEGER DEFAULT 0,
            char_count_out INTEGER DEFAULT 0,
            summary        TEXT
        )
    """)
    # 既存 DB へのカラム追加（エラーは無視）
    for col_def in [
        "ALTER TABLE llm_sessions ADD COLUMN char_count_in  INTEGER DEFAULT 0",
        "ALTER TABLE llm_sessions ADD COLUMN char_count_out INTEGER DEFAULT 0",
        "ALTER TABLE llm_sessions ADD COLUMN summary        TEXT",
    ]:
        try:
            _db_conn.execute(col_def)
        except sqlite3.OperationalError:
            pass  # already exists
    _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS items (
            id          TEXT PRIMARY KEY,
            type        TEXT NOT NULL,
            source_id   TEXT NOT NULL,
            person_name TEXT NOT NULL,
            notify      BOOLEAN NOT NULL,
            title       TEXT NOT NULL,
            start_at    DATETIME,
            end_at      DATETIME,
            due_at      DATETIME,
            notify_at   DATETIME,
            all_day     BOOLEAN NOT NULL DEFAULT 0,
            status      TEXT NOT NULL DEFAULT 'active',
            synced_at   DATETIME NOT NULL
        )
    """)
    _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS notification_log (
            event_id    TEXT PRIMARY KEY,
            notified_at DATETIME NOT NULL,
            acked       BOOLEAN NOT NULL DEFAULT 0,
            acked_at    DATETIME
        )
    """)
    _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            sender          TEXT NOT NULL,
            sender_slack_id TEXT,
            recipient       TEXT,
            content         TEXT NOT NULL,
            created_at      TEXT NOT NULL,
            delivered_at    TEXT
        )
    """)
    try:
        _db_conn.execute("ALTER TABLE messages ADD COLUMN sender_slack_id TEXT")
    except sqlite3.OperationalError:
        pass  # already exists
    _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS calendar_sources (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            source_type  TEXT NOT NULL CHECK(source_type IN ('calendar', 'tasklist')),
            source_id    TEXT NOT NULL,
            person_name  TEXT NOT NULL,
            notify       BOOLEAN NOT NULL DEFAULT 1,
            token_key    TEXT NOT NULL DEFAULT 'default',
            enabled      BOOLEAN NOT NULL DEFAULT 1,
            created_at   DATETIME NOT NULL,
            updated_at   DATETIME NOT NULL,
            UNIQUE(source_type, source_id)
        )
    """)
    _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS app_settings (
            key        TEXT PRIMARY KEY,
            value      TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS slack_seen_users (
            slack_user_id  TEXT PRIMARY KEY,
            slack_name     TEXT,
            last_seen_at   TEXT NOT NULL
        )
    """)
    _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS family_members (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            name           TEXT NOT NULL UNIQUE,
            slack_user_id  TEXT,
            mac_address    TEXT,
            created_at     TEXT NOT NULL,
            updated_at     TEXT NOT NULL
        )
    """)
    _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS earthquake_log (
            earthquake_id TEXT PRIMARY KEY,
            place         TEXT,
            scale         INTEGER,
            magnitude     REAL,
            notified_at   TEXT NOT NULL
        )
    """)
    _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS tsunami_state (
            area       TEXT PRIMARY KEY,
            grade      TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    _db_conn.commit()
    logger.info("DB initialized: path=%s", DB_PATH)


def _get_setting(key: str, default: str = "") -> str:
    """DB の app_settings から設定値を取得する。なければ default を返す。"""
    if not _db_conn:
        return default
    with _db_lock:
        row = _db_conn.execute(  # type: ignore[union-attr]
            "SELECT value FROM app_settings WHERE key = ?", (key,)
        ).fetchone()
    return row[0] if row else default


def _set_setting(key: str, value: str) -> None:
    now = datetime.now(_JST).isoformat()
    with _db_lock:
        _db_conn.execute(  # type: ignore[union-attr]
            "INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (key, value, now),
        )
        _db_conn.commit()


def _get_all_family_members() -> list[dict]:
    with _db_lock:
        rows = _db_conn.execute(  # type: ignore[union-attr]
            "SELECT id, name, slack_user_id, mac_address, created_at, updated_at FROM family_members ORDER BY name",
        ).fetchall()
    return [{"id": r[0], "name": r[1], "slack_user_id": r[2], "mac_address": r[3], "created_at": r[4], "updated_at": r[5]} for r in rows]


def _resolve_display_name(slack_user_id: str | None, fallback: str) -> str:
    """Slack user_id から family_members の呼び名を引く。未登録なら fallback を返す。"""
    if not slack_user_id or not _db_conn:
        return fallback
    with _db_lock:
        row = _db_conn.execute(  # type: ignore[union-attr]
            "SELECT name FROM family_members WHERE slack_user_id = ?", (slack_user_id,)
        ).fetchone()
    return row[0] if row else fallback


def _record_slack_user(user_id: str, slack_name: str | None = None) -> None:
    """Slack ユーザーを slack_seen_users に upsert する。名前は判明した時点で更新。"""
    if not user_id or not _db_conn:
        return
    now = datetime.now(_JST).isoformat()
    with _db_lock:
        _db_conn.execute(  # type: ignore[union-attr]
            """INSERT INTO slack_seen_users (slack_user_id, slack_name, last_seen_at)
               VALUES (?, ?, ?)
               ON CONFLICT(slack_user_id) DO UPDATE SET
                 slack_name   = COALESCE(excluded.slack_name, slack_name),
                 last_seen_at = excluded.last_seen_at""",
            (user_id, slack_name, now),
        )
        _db_conn.commit()


def _save_message(sender: str, recipient: str | None, content: str, sender_slack_id: str | None = None) -> int:
    with _db_lock:
        cur = _db_conn.execute(  # type: ignore[union-attr]
            "INSERT INTO messages (sender, sender_slack_id, recipient, content, created_at) VALUES (?, ?, ?, ?, ?)",
            (sender, sender_slack_id, recipient, content, datetime.now(_JST).isoformat()),
        )
        _db_conn.commit()
        return cur.lastrowid  # type: ignore[return-value]


def _fetch_pending_messages() -> list[dict]:
    with _db_lock:
        rows = _db_conn.execute(  # type: ignore[union-attr]
            "SELECT id, sender, sender_slack_id, recipient, content FROM messages WHERE delivered_at IS NULL ORDER BY created_at",
        ).fetchall()
    return [{"id": r[0], "sender": r[1], "sender_slack_id": r[2], "recipient": r[3], "content": r[4]} for r in rows]


def _mark_message_delivered(message_id: int) -> None:
    with _db_lock:
        _db_conn.execute(  # type: ignore[union-attr]
            "UPDATE messages SET delivered_at = ? WHERE id = ?",
            (datetime.now(_JST).isoformat(), message_id),
        )
        _db_conn.commit()


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


@dataclass
class _SessionData:
    response_id: str | None
    char_count_in: int
    char_count_out: int
    summary: str | None


def _get_session_data(session_key: str) -> _SessionData:
    with _db_lock:
        row = _db_conn.execute(  # type: ignore[union-attr]
            "SELECT response_id, char_count_in, char_count_out, summary FROM llm_sessions WHERE session_key = ?",
            (session_key,),
        ).fetchone()
        if row:
            return _SessionData(
                response_id=row[0],
                char_count_in=row[1] or 0,
                char_count_out=row[2] or 0,
                summary=row[3],
            )
        return _SessionData(response_id=None, char_count_in=0, char_count_out=0, summary=None)


def _save_session(
    session_key: str,
    response_id: str | None,
    char_count_in: int,
    char_count_out: int,
    summary: str | None,
) -> None:
    now = datetime.now(_JST).isoformat()
    with _db_lock:
        _db_conn.execute(  # type: ignore[union-attr]
            """
            INSERT INTO llm_sessions
                (session_key, backend, response_id, char_count_in, char_count_out, summary, updated_at)
            VALUES (?, 'openai', ?, ?, ?, ?, ?)
            ON CONFLICT(session_key) DO UPDATE SET
                response_id    = excluded.response_id,
                backend        = excluded.backend,
                char_count_in  = excluded.char_count_in,
                char_count_out = excluded.char_count_out,
                summary        = excluded.summary,
                updated_at     = excluded.updated_at
            """,
            (session_key, response_id, char_count_in, char_count_out, summary, now),
        )
        _db_conn.commit()  # type: ignore[union-attr]


async def _summarize_and_reset_session(session_key: str, previous_response_id: str) -> None:
    """Responses API で会話を要約し、セッションをリセットする。
    次回リクエスト時にサマリをコンテキストとして注入するため DB に保存する。
    """
    url = OPENAI_RESPONSES_BASE_URL.rstrip("/") + "/responses"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}",
    }
    payload = {
        "model": OPENAI_RESPONSES_MODEL,
        "previous_response_id": previous_response_id,
        "input": (
            "これまでの会話を日本語で要約してください。"
            "重要な情報（タスク、約束、ユーザーの好み、進行中のトピック、決定事項）を含め、"
            "次回の会話で文脈として使えるように簡潔にまとめてください。"
        ),
        "max_output_tokens": SESSION_SUMMARY_MAX_TOKENS,
    }
    try:
        resp = await _http_client.post(url, json=payload, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        summary = data.get("output_text") or ""
        if not summary:
            for item in data.get("output", []):
                for content in item.get("content", []):
                    if content.get("type") == "output_text":
                        summary = content["text"]
                        break
                if summary:
                    break
    except Exception as e:
        logger.error("Session summarization failed: session_key=%s error=%s", session_key, e)
        return

    # リセット: response_id をクリア、文字数カウンタをサマリの長さで初期化
    summary_len = len(summary)
    _save_session(
        session_key=session_key,
        response_id=None,
        char_count_in=summary_len,
        char_count_out=0,
        summary=summary,
    )
    logger.info(
        "Session summarized and reset: session_key=%s summary_len=%d",
        session_key, summary_len,
    )


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

_CALENDAR_TOOLS = [
    {
        "type": "function",
        "name": "get_upcoming_items",
        "description": (
            "DBに保存されたGoogleカレンダーの予定・タスクを取得する。"
            "「次の予定は？」「今日の予定は？」「しおりのタスクは？」などの質問に答えるために使う。"
            "取得した結果をもとに、自分の言葉で答えること。"
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "person": {
                    "type": "string",
                    "description": "絞り込む人の名前（例: しおり、パパ）。省略すると全員分を返す",
                },
                "type": {
                    "type": "string",
                    "enum": ["all", "event", "task"],
                    "description": "取得する種別。省略時は all（予定とタスク両方）",
                },
                "days": {
                    "type": "integer",
                    "description": "何日先まで取得するか（1〜14、省略時は3）",
                },
            },
            "required": [],
        },
    },
]

_MESSAGE_TOOLS = [
    {
        "type": "function",
        "name": "get_pending_messages",
        "description": (
            "家族から預かっている未読の伝言を取得する。"
            "「伝言ある？」「なにか連絡来てた？」「メッセージある？」「なにか残ってた？」"
            "などの質問に答えるために使う。"
            "直接聞かれた場合は「そういえば」などの前置きは不要。"
            "「○○からの伝言があるよ！」と直接伝えること。"
        ),
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
]

_ALERT_TOOLS = [
    {
        "type": "function",
        "name": "get_recent_alerts",
        "description": (
            "直近の地震・津波の通知履歴と現在の津波警報状況を返す。"
            "「さっきの地震は？」「最近地震あった？」「津波情報は？」"
            "「もう一回教えて」「また揺れた？」などの質問に答えるために使う。"
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "hours": {
                    "type": "integer",
                    "description": "何時間前までの情報を取得するか（1〜72、省略時は24）",
                },
            },
            "required": [],
        },
    },
]

# ON_DEMAND モード時のみ Pass 1 のツール一覧に追加される。
# LLM がこれを呼ぶと notify_context に enable_web_search フラグが立ち、
# 次のループで本物の web_search_preview に差し替えられる。
_REQUEST_WEB_SEARCH_TOOL = {
    "type": "function",
    "name": "request_web_search",
    "description": (
        "最新情報・天気・ニュース・株価・スポーツ結果など、"
        "学習データに含まれていない可能性が高い事実を答えるために"
        "Web 検索が必要なときだけ呼び出すこと。"
        "雑談・感情表現・既に知っている内容では呼ばないこと。"
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "検索したい内容を簡潔に"},
        },
        "required": ["query"],
    },
}


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
    expression, clean_message = _parse_expression(message)
    speaker_id, stackchan_expr = _resolve_expression(expression)
    try:
        audio_url, streaming_url = await resolve_audio_url(clean_message, speaker_id)
        req_id = str(uuid.uuid4())
        publish_speak(audio_url, streaming_url, clean_message, "timer", "normal", req_id, stackchan_expr)
        logger.info("Timer fired: timer_id=%s label=%s expression=%s message=%s", info.timer_id, info.label, expression, clean_message[:60])
    except Exception as e:
        logger.error("Timer speak error: timer_id=%s error=%s", info.timer_id, e)
        return

    # Slack 経由で設定された場合は Slack にも完了通知
    if info.slack_channel and _slack_app:
        try:
            await _slack_app.client.chat_postMessage(
                channel=info.slack_channel,
                text=f"⏰ タイマー「{info.label}」が発火しました：「{clean_message}」",
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


def _tool_get_upcoming_items(args: dict) -> dict:
    days = min(max(int(args.get("days", 3)), 1), 14)
    person = args.get("person")
    type_ = args.get("type", "all")

    now = datetime.now(_JST)
    until = now + timedelta(days=days)

    params: list = [now.isoformat(), until.isoformat()]
    where = ["status = 'active'", "COALESCE(start_at, due_at) >= ?", "COALESCE(start_at, due_at) <= ?"]
    if person:
        where.append("person_name = ?")
        params.append(person)
    if type_ != "all":
        where.append("type = ?")
        params.append(type_)

    sql = f"SELECT type, person_name, title, start_at, due_at, all_day FROM items WHERE {' AND '.join(where)} ORDER BY COALESCE(start_at, due_at) ASC LIMIT 20"

    with _db_lock:
        rows = _db_conn.execute(sql, params).fetchall()  # type: ignore[union-attr]

    items = []
    for row_type, row_person, title, start_at, due_at, all_day in rows:
        entry: dict = {
            "type": "イベント" if row_type == "event" else "タスク",
            "person": row_person,
            "title": title,
        }
        when = start_at or due_at
        if when:
            entry["when"] = when[:10] if all_day else when[:16].replace("T", " ")
        items.append(entry)

    logger.info("Tool get_upcoming_items: days=%d person=%s type=%s count=%d", days, person, type_, len(items))
    return {"status": "ok", "count": len(items), "items": items}


def _tool_get_recent_alerts(args: dict) -> dict:
    hours = min(max(int(args.get("hours", 24)), 1), 72)

    with _db_lock:
        eq_rows = _db_conn.execute(  # type: ignore[union-attr]
            """SELECT place, scale, magnitude, notified_at FROM earthquake_log
               WHERE notified_at >= datetime('now', ?)
               AND earthquake_id NOT LIKE '%:eew' AND earthquake_id NOT LIKE '%:cancelled'
               ORDER BY notified_at DESC LIMIT 10""",
            (f"-{hours} hours",),
        ).fetchall()
        ts_rows = _db_conn.execute(  # type: ignore[union-attr]
            "SELECT area, grade, updated_at FROM tsunami_state ORDER BY updated_at DESC"
        ).fetchall()

    earthquakes = [
        {
            "place": r[0],
            "scale": _scale_to_str(r[1]),
            "magnitude": r[2],
            "notified_at": r[3],
        }
        for r in eq_rows
    ]
    tsunami_active = [
        {
            "area": r[0],
            "grade": _TSUNAMI_GRADE_LABEL.get(r[1], r[1]),
            "updated_at": r[2],
        }
        for r in ts_rows
    ]
    logger.info("Tool get_recent_alerts: hours=%d eq=%d tsunami=%d", hours, len(earthquakes), len(tsunami_active))
    return {
        "status": "ok",
        "period_hours": hours,
        "earthquakes": earthquakes,
        "tsunami_active": tsunami_active,
    }


async def _execute_tool(name: str, args: dict, notify_context: dict) -> dict:
    """Execute a named tool and return the raw result dict (protocol-agnostic)."""
    if name == "set_timer":
        timer_id = _register_timer(
            label=args.get("label", "タイマー"),
            seconds=int(args.get("seconds", 60)),
            session_key=notify_context.get("session_key", ""),
            slack_channel=notify_context.get("slack_channel"),
            snooze_seconds=args.get("snooze_seconds"),
        )
        logger.info(
            "Function call set_timer: label=%s seconds=%s timer_id=%s",
            args.get("label"), args.get("seconds"), timer_id,
        )
        return {"status": "ok", "timer_id": timer_id, "label": args.get("label"), "seconds": args.get("seconds")}
    if name == "list_timers":
        now = datetime.now(_JST)
        timers = [
            {
                "timer_id": info.timer_id,
                "label": info.label,
                "fire_at": info.fire_at.isoformat(),
                "remaining_seconds": max(0, int((info.fire_at - now).total_seconds())),
            }
            for info in _active_timer_infos.values()
        ]
        logger.info("Function call list_timers: count=%d", len(timers))
        return {"status": "ok", "timers": timers, "count": len(timers)}
    if name == "request_web_search":
        query = args.get("query", "")
        notify_context["enable_web_search"] = True
        logger.info("LLM requested web search: query=%s", query[:80])
        return {
            "status": "ok",
            "message": "Web検索が有効になりました。次のターンで検索を実行して回答してください。",
            "query": query,
        }
    if name == "get_upcoming_items":
        return _tool_get_upcoming_items(args)
    if name == "get_recent_alerts":
        return _tool_get_recent_alerts(args)
    if name == "get_pending_messages":
        messages = _fetch_pending_messages()
        for msg in messages:
            _mark_message_delivered(msg["id"])
            asyncio.create_task(_notify_message_delivered(msg))
        logger.info("Function call get_pending_messages: count=%d", len(messages))
        if not messages:
            return {"status": "ok", "count": 0, "messages": []}
        return {
            "status": "ok",
            "count": len(messages),
            "messages": [
                {"sender": m["sender"], "recipient": m["recipient"], "content": m["content"]}
                for m in messages
            ],
        }
    logger.warning("Unknown function call: name=%s", name)
    return {"status": "error", "message": f"Unknown function: {name}"}


async def _handle_function_calls(
    output: list,
    notify_context: dict,
) -> list | None:
    """output 配列に function_call があれば実行して function_call_output リストを返す（Responses API 形式）。なければ None。"""
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
        result = await _execute_tool(name, args, notify_context)
        results.append({
            "type": "function_call_output",
            "call_id": call_id,
            "output": json.dumps(result, ensure_ascii=False),
        })

    return results


async def _fire_calendar_notification(item: dict) -> None:
    person = item["person_name"]
    title = item["title"]
    if item["type"] == "event":
        prompt = f"{person}の予定「{title}」の時間がもうすぐだよ！短く明るく声かけして。"
    else:
        prompt = f"{person}のタスク「{title}」の期日が近いよ！短く声かけして。"
    try:
        message = await chat_with_llm(
            prompt,
            system_prompt_append=(
                "これはカレンダー通知です。スタックちゃんとして家族に向けて声かけしてください。"
                "依頼者への返答にはしないでください。"
            ),
            use_functions=False,
        )
    except Exception as e:
        logger.error("Calendar notification LLM error: item_id=%s error=%s", item["id"], e)
        message = f"{person}、{title}の時間だよ！"
    expression, clean_message = _parse_expression(message)
    speaker_id, stackchan_expr = _resolve_expression(expression)
    audio_url, streaming_url = await resolve_audio_url(clean_message, speaker_id)
    req_id = str(uuid.uuid4())
    publish_speak(audio_url, streaming_url, clean_message, "calendar", "normal", req_id, stackchan_expr)
    logger.info("Calendar notification sent: item_id=%s expression=%s message=%s", item["id"], expression, clean_message[:60])


async def _check_calendar_notifications() -> None:
    now = datetime.now(_JST)
    grace_cutoff = (now - timedelta(minutes=CALENDAR_NOTIFY_GRACE_MINUTES)).isoformat()
    now_iso = now.isoformat()
    with _db_lock:
        rows = _db_conn.execute(  # type: ignore[union-attr]
            """
            SELECT i.id, i.type, i.person_name, i.title
            FROM items i
            LEFT JOIN notification_log n ON i.id = n.event_id
            WHERE i.notify = 1
              AND i.status = 'active'
              AND i.notify_at IS NOT NULL
              AND i.notify_at <= ?
              AND i.notify_at >= ?
              AND n.event_id IS NULL
            """,
            (now_iso, grace_cutoff),
        ).fetchall()

    for row in rows:
        item = {"id": row[0], "type": row[1], "person_name": row[2], "title": row[3]}
        try:
            await _fire_calendar_notification(item)
            with _db_lock:
                _db_conn.execute(  # type: ignore[union-attr]
                    "INSERT OR IGNORE INTO notification_log (event_id, notified_at) VALUES (?, ?)",
                    (item["id"], datetime.now(_JST).isoformat()),
                )
                _db_conn.commit()  # type: ignore[union-attr]
        except Exception as e:
            logger.error("Calendar notification failed: item_id=%s error=%s", item["id"], e)


async def _calendar_notification_loop() -> None:
    logger.info("Calendar notification loop started: check_interval=%ds", CALENDAR_NOTIFY_CHECK_INTERVAL)
    while True:
        await asyncio.sleep(CALENDAR_NOTIFY_CHECK_INTERVAL)
        try:
            await _check_calendar_notifications()
        except Exception as e:
            logger.error("Calendar notification loop error: %s", e)


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

    if CALENDAR_ENABLED:
        from calendar_sync import start_sync_thread
        start_sync_thread(
            _db_conn,
            _db_lock,
            GOOGLE_CREDENTIALS_FILE,
            GOOGLE_TOKEN_DIR,
            CALENDAR_SYNC_INTERVAL_MINUTES,
            CALENDAR_DEFAULT_NOTIFY_MINUTES,
            CALENDAR_SYNC_DAYS_AHEAD,
        )
        asyncio.create_task(_calendar_notification_loop())
        logger.info("Calendar sync and notification started")

    if P2PQUAKE_ENABLED:
        asyncio.create_task(_p2pquake_ws_loop())
        logger.info("P2P地震情報 WebSocket started")

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
_templates = Jinja2Templates(directory="templates")


def _ui_context(request: Request, **extra) -> dict:
    """全テンプレートに渡す共通コンテキスト。"""
    return {
        "speaker_id_browser_url": _get_setting("speaker_id_browser_url", SPEAKER_ID_BROWSER_URL),
        **extra,
    }


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
                client.subscribe("stackchan/ack", qos=MQTT_QOS)
                logger.info("MQTT (re)connected, subscribed to stackchan/ack qos=%d", MQTT_QOS)
                connected.set()
            else:
                logger.error("MQTT connect failed: reason_code=%s", reason_code)

        def on_disconnect(client, userdata, flags, reason_code, properties):
            logger.warning("MQTT disconnected: reason_code=%s", reason_code)

        def on_message(client, userdata, message):
            try:
                data = json.loads(message.payload)
                req_id = data.get("id")
                logger.info(
                    "MQTT ACK on_message: topic=%s req_id=%s status=%s main_loop=%s",
                    message.topic, req_id, data.get("status"), _main_loop is not None,
                )
                if req_id and _main_loop:
                    event = _pending_acks.get(req_id)
                    logger.info(
                        "MQTT ACK lookup: req_id=%s event_found=%s pending_keys=%s",
                        req_id, event is not None, list(_pending_acks.keys()),
                    )
                    if event:
                        _main_loop.call_soon_threadsafe(event.set)
                        logger.info("MQTT ACK dispatched: requestId=%s", req_id)
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


# ── 設置場所ユーティリティ ────────────────────────────────────────────────────

_PREF_RE = re.compile(r"^(東京都|北海道|(?:京都|大阪)府|[^\s]{2,4}?県)")


def _extract_pref(title: str) -> str:
    m = _PREF_RE.match(title)
    return m.group(1) if m else ""


# 都道府県 → 気象庁 津波予報区 マッピング（P2P地震情報 API の area.name と一致させること）
_PREF_TSUNAMI_AREAS: dict[str, list[str]] = {
    "北海道":   ["北海道太平洋沿岸東部", "北海道太平洋沿岸中部", "北海道太平洋沿岸西部",
                 "北海道日本海沿岸南部", "北海道日本海沿岸北部", "北海道オホーツク海沿岸"],
    "青森県":   ["青森県太平洋沿岸", "青森県日本海沿岸"],
    "岩手県":   ["岩手県"],
    "宮城県":   ["宮城県"],
    "秋田県":   ["秋田県"],
    "山形県":   ["山形県"],
    "福島県":   ["福島県"],
    "茨城県":   ["茨城県"],
    "千葉県":   ["千葉県九十九里・外房", "千葉県内房"],
    "東京都":   ["伊豆諸島", "小笠原諸島"],
    "神奈川県": ["相模湾・三浦半島"],
    "新潟県":   ["新潟県上越地方", "新潟県中越地方", "新潟県下越地方", "粟島"],
    "富山県":   ["富山県"],
    "石川県":   ["石川県能登", "石川県加賀"],
    "福井県":   ["福井県"],
    "静岡県":   ["静岡県"],
    "愛知県":   ["愛知県外海", "愛知県内海"],
    "三重県":   ["三重県北部", "三重県南部"],
    "京都府":   ["京都府"],
    "大阪府":   ["大阪府"],
    "兵庫県":   ["兵庫県北部", "兵庫県南部"],
    "和歌山県": ["和歌山県"],
    "鳥取県":   ["鳥取県"],
    "島根県":   ["島根県出雲・石見", "島根県隠岐"],
    "岡山県":   ["岡山県"],
    "広島県":   ["広島県"],
    "山口県":   ["山口県北部", "山口県西部"],
    "徳島県":   ["徳島県"],
    "香川県":   ["香川県"],
    "愛媛県":   ["愛媛県宇和海沿岸", "愛媛県瀬戸内海沿岸"],
    "高知県":   ["高知県"],
    "福岡県":   ["福岡県瀬戸内海沿岸", "福岡県日本海沿岸"],
    "佐賀県":   ["佐賀県北部"],
    "長崎県":   ["長崎県西方", "長崎県島原半島"],
    "熊本県":   ["熊本県天草・芦北", "熊本県有明・八代海"],
    "大分県":   ["大分県中部", "大分県北部", "大分県南部"],
    "宮崎県":   ["宮崎県"],
    "鹿児島県": ["鹿児島県東部", "鹿児島県西部", "種子島・屋久島地方", "奄美群島・トカラ列島"],
    "沖縄県":   ["沖縄本島地方", "大東島地方", "宮古島・八重山地方"],
    # 内陸県（海なし）は空リスト → 津波エリア設定なし
    "埼玉県": [], "栃木県": [], "群馬県": [], "山梨県": [],
    "長野県": [], "岐阜県": [], "奈良県": [], "滋賀県": [],
}


def _apply_tsunami_areas_from_pref(pref: str) -> None:
    """都道府県から津波予報区を自動設定する。内陸県の場合は設定しない。"""
    areas = _PREF_TSUNAMI_AREAS.get(pref)
    if areas is None:
        return  # マッピング未定義の県はそのまま
    if areas:
        _set_setting("p2pquake_tsunami_areas", ",".join(areas))
        logger.info("tsunami areas auto-set from pref=%s: %s", pref, areas)
    else:
        logger.info("tsunami areas: %s is inland, no coastal areas", pref)


def _get_local_scale(data: dict) -> int | None:
    """設置場所の都道府県で観測された最大震度コードを返す。
    全国モードまたは設置場所未設定なら全国最大値を返す。
    設置場所が設定されていてその県に観測点がなければ None（通知しない）。
    """
    if _get_setting("p2pquake_nationwide", "false") == "true":
        return data["earthquake"]["maxScale"]
    pref = _get_setting("location_pref", "")
    if not pref:
        return data["earthquake"]["maxScale"]
    local = [p for p in data.get("points", []) if p.get("pref") == pref]
    if not local:
        return None
    return max(p["scale"] for p in local)


# ── P2P地震情報 WebSocket ─────────────────────────────────────────────────────

_SCALE_MAP = {
    -1: "震度不明",
    10: "震度1", 20: "震度2", 30: "震度3",
    40: "震度4", 45: "震度4強",
    50: "震度5弱", 55: "震度5強",
    60: "震度6弱", 65: "震度6強",
    70: "震度7",
}
_TSUNAMI_GRADE_ORDER = {"Watch": 1, "Warning": 2, "MajorWarning": 3}
_TSUNAMI_GRADE_LABEL = {
    "Watch": "津波注意報",
    "Warning": "津波警報",
    "MajorWarning": "大津波警報",
}


def _scale_to_str(scale: int) -> str:
    return _SCALE_MAP.get(scale, f"震度{scale}")


def _eq_already_seen(earthquake_id: str) -> bool:
    row = _db_conn.execute(
        "SELECT 1 FROM earthquake_log WHERE earthquake_id = ?", (earthquake_id,)
    ).fetchone()
    return row is not None


def _mark_eq_seen(earthquake_id: str, place: str, scale: int, magnitude: float) -> None:
    with _db_lock:
        _db_conn.execute(
            "INSERT OR IGNORE INTO earthquake_log "
            "(earthquake_id, place, scale, magnitude, notified_at) "
            "VALUES (?, ?, ?, ?, datetime('now'))",
            (earthquake_id, place, scale, magnitude),
        )
        _db_conn.commit()


def _get_tsunami_grade(area: str) -> str | None:
    row = _db_conn.execute(
        "SELECT grade FROM tsunami_state WHERE area = ?", (area,)
    ).fetchone()
    return row[0] if row else None


def _save_tsunami_grade(area: str, grade: str) -> None:
    with _db_lock:
        _db_conn.execute(
            "INSERT INTO tsunami_state (area, grade, updated_at) VALUES (?, ?, datetime('now')) "
            "ON CONFLICT(area) DO UPDATE SET grade=excluded.grade, updated_at=excluded.updated_at",
            (area, grade),
        )
        _db_conn.commit()


def _clear_tsunami_state() -> None:
    with _db_lock:
        _db_conn.execute("DELETE FROM tsunami_state")
        _db_conn.commit()


def _build_earthquake_fixed_text(data: dict, local_scale: int) -> str:
    eq    = data["earthquake"]
    place = eq["hypocenter"]["name"]
    mag   = eq["hypocenter"]["magnitude"]
    tsun  = eq["domesticTsunami"]
    scale = local_scale  # 設置場所の都道府県での震度を使う

    scale_str = _scale_to_str(scale)
    info = f"{place}でマグニチュード{mag}の地震が発生しました。"

    if scale <= 20:
        msg = f"最大{scale_str}です。"
    elif scale <= 30:
        msg = f"最大{scale_str}です。落下物に気をつけてください。"
    elif scale <= 45:
        msg = f"最大{scale_str}です。棚の物が落ちることがあります。揺れが収まるまで様子を見てください。"
    elif scale <= 50:
        msg = f"【緊急地震速報】最大{scale_str}です。今すぐ低い姿勢をとり、頭を守ってください。"
    elif scale <= 55:
        msg = (f"【緊急地震速報】最大{scale_str}です。"
               "固定されていない家具が倒れることがあります。今すぐ安全な場所に身を隠してください。")
    elif scale <= 60:
        msg = (f"【緊急地震速報】最大{scale_str}です。"
               "非常に危険です。今すぐ頭を守り、揺れが収まるまで動かないでください。")
    elif scale <= 65:
        msg = f"【最大警戒】最大{scale_str}です。今すぐ頭を守り、絶対に動かないでください。"
    else:
        msg = ("【最大警戒・震度7】極めて激しい揺れです。"
               "頭を守り、揺れが完全に収まるまで待ってください。")

    tsunami_suffix = ""
    if tsun == "Watch":
        tsunami_suffix = "津波注意報が発令されています。海岸や川には近づかないでください。"
    elif tsun == "Warning":
        tsunami_suffix = "【津波警報】海岸・川から直ちに離れてください。"
    elif tsun == "Checking":
        tsunami_suffix = "津波の有無を確認中です。海岸には近づかないでください。"

    return info + msg + tsunami_suffix


async def _p2p_speak(text: str, source: str, priority: str) -> None:
    # 防災通知は感情ラベルを捨てて常に neutral で発話する
    _, clean_text = _parse_expression(text)
    speaker_id, stackchan_expr = _resolve_expression("neutral")
    audio_url, stream_url = await resolve_audio_url(clean_text, speaker_id)
    publish_speak(audio_url, stream_url, clean_text,
                  source=source, priority=priority,
                  request_id=str(uuid.uuid4()), expression=stackchan_expr)


async def _handle_earthquake(data: dict) -> None:
    if data.get("issue", {}).get("type") != "DetailScale":
        return  # 速報段階はスキップ、確定詳細情報のみ処理

    earthquake_id = data["id"]
    if _eq_already_seen(earthquake_id):
        return

    local_scale = _get_local_scale(data)
    if local_scale is None:
        logger.debug("earthquake: no shaking in location pref, skipping id=%s", earthquake_id)
        return

    min_scale = int(_get_setting("p2pquake_min_scale", str(P2PQUAKE_MIN_SCALE)))
    if local_scale < min_scale:
        return

    eq    = data["earthquake"]
    place = eq["hypocenter"]["name"]
    mag   = eq["hypocenter"]["magnitude"]

    fixed_text = _build_earthquake_fixed_text(data, local_scale)

    _mark_eq_seen(earthquake_id, place, local_scale, mag)
    logger.info("earthquake notify: id=%s place=%s scale=%s", earthquake_id, place, local_scale)

    # ① 固定テキストを即時発話
    await _p2p_speak(fixed_text, source="earthquake", priority="high")

    # ② LLM コメントを非同期で続けて発話
    asyncio.create_task(_earthquake_llm_comment(place, _scale_to_str(local_scale), mag))


async def _earthquake_llm_comment(place: str, scale_str: str, mag: float) -> None:
    prompt = (
        f"先ほど地震速報をお知らせしました（{place} / {scale_str} / M{mag}）。"
        "情報の繰り返しは不要です。家族への短い一言コメントを1〜2文で追加してください。"
    )
    try:
        comment = await chat_with_llm(prompt, session_key="family", use_functions=False)
        await _p2p_speak(comment, source="earthquake_comment", priority="normal")
    except Exception:
        logger.exception("earthquake LLM comment failed")


async def _handle_tsunami(data: dict) -> None:
    earthquake_id = data["id"]

    if data.get("cancelled"):
        cancel_key = earthquake_id + ":cancelled"
        if _eq_already_seen(cancel_key):
            return
        _mark_eq_seen(cancel_key, "tsunami_cancel", 0, 0.0)
        _clear_tsunami_state()
        fixed_text = "津波予報が解除されました。海岸付近の方は安全を確認してから戻るようにしてください。"
        await _p2p_speak(fixed_text, source="tsunami", priority="high")
        asyncio.create_task(_tsunami_llm_comment(fixed_text, cancelled=True))
        return

    for area in data.get("areas", []):
        areas_str = _get_setting("p2pquake_tsunami_areas", ",".join(P2PQUAKE_TSUNAMI_TARGET_AREAS))
        target_areas = set(areas_str.split(","))
        if area["name"] not in target_areas:
            continue

        new_grade     = area["grade"]
        current_grade = _get_tsunami_grade(area["name"])
        new_order     = _TSUNAMI_GRADE_ORDER.get(new_grade, 0)
        current_order = _TSUNAMI_GRADE_ORDER.get(current_grade or "", 0)

        if new_order <= current_order:
            continue  # 同グレードまたは格下げは通知しない

        _save_tsunami_grade(area["name"], new_grade)
        grade_label = _TSUNAMI_GRADE_LABEL.get(new_grade, new_grade)
        height  = area.get("maxHeight", {}).get("description", "")
        arrival = area.get("firstHeight", {}).get("arrivalTime", "")

        fixed_text = f"相模湾・三浦半島に{grade_label}が発令されました。"
        if height:
            fixed_text += f"予想される津波の高さは{height}です。"
        if arrival:
            fixed_text += f"第一波到達予想は{arrival}です。"
        fixed_text += "海岸・川から直ちに離れてください。"

        logger.info("tsunami notify: area=%s grade=%s", area["name"], new_grade)
        await _p2p_speak(fixed_text, source="tsunami", priority="high")
        asyncio.create_task(_tsunami_llm_comment(fixed_text, cancelled=False))


async def _tsunami_llm_comment(fixed_text: str, cancelled: bool) -> None:
    if cancelled:
        prompt = "津波予報が解除されました。安堵の一言を1文で、話し言葉で。"
    else:
        prompt = (
            f"以下の津波警報をお知らせしました。緊急の一言コメントを1文で。情報の繰り返し不要。\n{fixed_text}"
        )
    try:
        comment = await chat_with_llm(prompt, session_key="family", use_functions=False)
        await _p2p_speak(comment, source="tsunami_comment", priority="normal")
    except Exception:
        logger.exception("tsunami LLM comment failed")


async def _handle_eew(data: dict) -> None:
    # code=554: 緊急地震速報（警報）。揺れが来る数秒前。LLMなしで即発話のみ。
    eew_key = data.get("id", "") + ":eew"
    if _eq_already_seen(eew_key):
        return
    _mark_eq_seen(eew_key, "eew", 0, 0.0)
    text = "緊急地震速報！強い揺れが来る可能性があります。今すぐ身を低くして頭を守ってください。"
    await _p2p_speak(text, source="eew", priority="high")


async def _handle_nankai(data: dict) -> None:
    # code=556: 南海トラフ地震臨時情報。固定案内 + LLM 解説。
    nankai_key = data.get("id", "")
    if _eq_already_seen(nankai_key):
        return
    _mark_eq_seen(nankai_key, "nankai", 0, 0.0)
    fixed_text = ("南海トラフ地震に関する臨時情報が発表されました。"
                  "詳しくはテレビやラジオ、気象庁のウェブサイトを確認してください。")
    await _p2p_speak(fixed_text, source="nankai", priority="high")
    asyncio.create_task(_unknown_p2p_llm(data))


async def _unknown_p2p_llm(data: dict) -> None:
    prompt = (
        "以下はP2P地震情報APIから届いた防災通知JSONです。\n"
        "内容を読み取り、家族に向けて簡潔に伝えてください。\n"
        "重要な情報は省かず、1〜3文の話し言葉にしてください。\n\n"
        f"{json.dumps(data, ensure_ascii=False, indent=2)}"
    )
    try:
        text = await chat_with_llm(prompt, session_key="family", use_functions=False)
        await _p2p_speak(text, source="p2pquake_unknown", priority="normal")
    except Exception:
        logger.exception("unknown p2p LLM failed")


async def _p2pquake_ws_loop() -> None:
    backoff = 1
    seen_ids: set[str] = set()  # 再接続時の直近重複対策（メモリ内）

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(P2PQUAKE_WS_URL) as ws:
                    logger.info("P2P地震情報 WebSocket connected: %s", P2PQUAKE_WS_URL)
                    backoff = 1
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            eid  = data.get("id", "")
                            code = data.get("code")

                            if eid and eid in seen_ids:
                                continue
                            if eid:
                                seen_ids.add(eid)
                                if len(seen_ids) > 500:
                                    seen_ids.pop()

                            if code == 551:
                                asyncio.create_task(_handle_earthquake(data))
                            elif code == 552:
                                asyncio.create_task(_handle_tsunami(data))
                            elif code == 554:
                                asyncio.create_task(_handle_eew(data))
                            elif code == 556:
                                asyncio.create_task(_handle_nankai(data))
                            else:
                                logger.info("p2pquake: unknown code=%s id=%s", code, eid)
                                asyncio.create_task(_unknown_p2p_llm(data))

                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            logger.warning("p2pquake ws closed/error, reconnecting")
                            break

        except Exception:
            logger.exception("p2pquake ws error, retry in %ds", backoff)

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)


class SpeakRequest(BaseModel):
    text: str
    source: str = "unknown"
    priority: str = "normal"
    request_id: str | None = None


async def get_audio_url_web(text: str, speaker_id: int | None = None) -> tuple[str, str | None]:
    """Get MP3 URLs from VOICEVOX Web高速版 (api.tts.quest) without downloading.
    Returns (mp3DownloadUrl, mp3StreamingUrl).
    """
    resp = await _http_client.get(
        f"{VOICEVOX_URL}/synthesis",
        params={"speaker": speaker_id if speaker_id is not None else VOICEVOX_SPEAKER, "text": text, "key": VOICEVOX_API_KEY},
    )
    resp.raise_for_status()
    data = resp.json()

    if not data.get("success"):
        raise RuntimeError(f"VOICEVOX Web API error: {data}")

    mp3_url = data.get("mp3DownloadUrl")
    if not mp3_url:
        raise RuntimeError(f"No mp3DownloadUrl in response: {data}")

    return mp3_url, data.get("mp3StreamingUrl")


async def resolve_audio_url(text: str, speaker_id: int | None = None) -> tuple[str, str | None]:
    """Return (audioUrl, audioStreamingUrl) for the given text."""
    return await get_audio_url_web(text, speaker_id)


def publish_speak(audio_url: str, audio_streaming_url: str | None, text: str, source: str, priority: str, request_id: str, expression: str = "neutral") -> None:
    """Publish MQTT speak event to Stack-chan."""
    topic = f"stackchan/{MQTT_DEVICE_ID}/speak"
    msg: dict = {
        "type": "speak",
        "audioUrl": audio_url,
        "text": text,
        "source": source,
        "priority": priority,
        "requestId": request_id,
        "expression": expression,
    }
    if audio_streaming_url:
        msg["audioStreamingUrl"] = audio_streaming_url
    payload = json.dumps(msg, ensure_ascii=False)
    _mqtt_conn.publish(topic, payload)


async def wait_for_ack(request_id: str, timeout: float = MQTT_ACK_TIMEOUT) -> bool:
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


# ── Web UI ───────────────────────────────────────────────────────────────────

@app.get("/ui", response_class=HTMLResponse)
async def ui_index():
    return RedirectResponse(url="/ui/members")


@app.get("/ui/members", response_class=HTMLResponse)
async def ui_members(request: Request):
    return _templates.TemplateResponse(request=request, name="members.html", context=_ui_context(request))


@app.get("/ui/messages", response_class=HTMLResponse)
async def ui_messages(request: Request):
    return _templates.TemplateResponse(request=request, name="messages.html", context=_ui_context(request))


@app.get("/ui/test", response_class=HTMLResponse)
async def ui_test(request: Request):
    return _templates.TemplateResponse(request=request, name="test.html", context=_ui_context(request))


@app.get("/ui/settings", response_class=HTMLResponse)
async def ui_settings(request: Request):
    return _templates.TemplateResponse(request=request, name="settings.html", context=_ui_context(request))


@app.get("/ui/notifications", response_class=HTMLResponse)
async def ui_notifications(request: Request):
    return _templates.TemplateResponse(request=request, name="notifications.html", context=_ui_context(request))


# ── REST API (notifications) ──────────────────────────────────────────────────

@app.get("/api/notifications")
def api_list_notifications():
    """カレンダー通知の一覧（通知済み状態つき）を返す。"""
    with _db_lock:
        rows = _db_conn.execute(  # type: ignore[union-attr]
            """
            SELECT i.id, i.type, i.person_name, i.title,
                   i.start_at, i.end_at, i.due_at, i.notify_at, i.all_day,
                   n.notified_at
            FROM items i
            LEFT JOIN notification_log n ON i.id = n.event_id
            WHERE i.notify = 1 AND i.status = 'active'
            ORDER BY COALESCE(i.notify_at, i.start_at, i.due_at) ASC
            """
        ).fetchall()
    now = datetime.now(_JST).isoformat()
    result = []
    for r in rows:
        notify_at  = r[7]
        notified_at = r[9]
        if notified_at:
            state = "notified"
        elif notify_at and notify_at <= now:
            state = "overdue"
        else:
            state = "pending"
        result.append({
            "id": r[0], "type": r[1], "person_name": r[2], "title": r[3],
            "start_at": r[4], "end_at": r[5], "due_at": r[6],
            "notify_at": notify_at, "all_day": bool(r[8]),
            "notified_at": notified_at, "state": state,
        })
    return {"items": result}


@app.post("/api/notifications/{event_id}/resend")
async def api_notification_resend(event_id: str):
    """notification_log から削除して即時再通知する。"""
    with _db_lock:
        row = _db_conn.execute(  # type: ignore[union-attr]
            "SELECT id, type, person_name, title FROM items WHERE id = ?", (event_id,)
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="イベントが見つかりません")
    item = {"id": row[0], "type": row[1], "person_name": row[2], "title": row[3]}
    with _db_lock:
        _db_conn.execute(  # type: ignore[union-attr]
            "DELETE FROM notification_log WHERE event_id = ?", (event_id,)
        )
        _db_conn.commit()  # type: ignore[union-attr]
    await _fire_calendar_notification(item)
    with _db_lock:
        _db_conn.execute(  # type: ignore[union-attr]
            "INSERT OR IGNORE INTO notification_log (event_id, notified_at) VALUES (?, ?)",
            (event_id, datetime.now(_JST).isoformat()),
        )
        _db_conn.commit()  # type: ignore[union-attr]
    return {"ok": True, "event_id": event_id}


@app.delete("/api/notifications/{event_id}/log")
def api_notification_clear(event_id: str):
    """通知済みフラグを削除する（次の通知ループで再送される）。"""
    with _db_lock:
        _db_conn.execute(  # type: ignore[union-attr]
            "DELETE FROM notification_log WHERE event_id = ?", (event_id,)
        )
        _db_conn.commit()  # type: ignore[union-attr]
    return {"ok": True, "event_id": event_id}


# ── REST API (family members) ────────────────────────────────────────────────

@app.get("/api/family-members")
def api_list_members():
    return _get_all_family_members()


@app.post("/api/family-members", status_code=201)
def api_create_member(name: str = Form(...), slack_user_id: str = Form(""), mac_address: str = Form("")):
    now = datetime.now(_JST).isoformat()
    try:
        with _db_lock:
            cur = _db_conn.execute(  # type: ignore[union-attr]
                "INSERT INTO family_members (name, slack_user_id, mac_address, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                (name.strip(), slack_user_id.strip() or None, mac_address.strip() or None, now, now),
            )
            _db_conn.commit()
        return {"id": cur.lastrowid, "name": name}
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail=f"名前 '{name}' はすでに登録されています")


@app.put("/api/family-members/{member_id}")
def api_update_member(member_id: int, name: str = Form(...), slack_user_id: str = Form(""), mac_address: str = Form("")):
    now = datetime.now(_JST).isoformat()
    with _db_lock:
        cur = _db_conn.execute(  # type: ignore[union-attr]
            "UPDATE family_members SET name=?, slack_user_id=?, mac_address=?, updated_at=? WHERE id=?",
            (name.strip(), slack_user_id.strip() or None, mac_address.strip() or None, now, member_id),
        )
        _db_conn.commit()
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="メンバーが見つかりません")
    return {"id": member_id, "name": name}


@app.get("/api/slack-seen-users")
def api_slack_seen_users():
    """family_members に未登録の Slack ユーザー一覧を返す。"""
    with _db_lock:
        rows = _db_conn.execute(  # type: ignore[union-attr]
            """SELECT s.slack_user_id, s.slack_name, s.last_seen_at
               FROM slack_seen_users s
               WHERE NOT EXISTS (
                 SELECT 1 FROM family_members f WHERE f.slack_user_id = s.slack_user_id
               )
               ORDER BY s.last_seen_at DESC""",
        ).fetchall()
    return [{"slack_user_id": r[0], "slack_name": r[1], "last_seen_at": r[2]} for r in rows]


@app.delete("/api/family-members/{member_id}", status_code=204)
def api_delete_member(member_id: int):
    with _db_lock:
        cur = _db_conn.execute("DELETE FROM family_members WHERE id=?", (member_id,))  # type: ignore[union-attr]
        _db_conn.commit()
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="メンバーが見つかりません")


# ── REST API (messages) ───────────────────────────────────────────────────────

@app.get("/api/messages")
def api_list_messages(status: str = "all"):
    where = "" if status == "all" else ("WHERE delivered_at IS NULL" if status == "pending" else "WHERE delivered_at IS NOT NULL")
    with _db_lock:
        rows = _db_conn.execute(  # type: ignore[union-attr]
            f"SELECT id, sender, sender_slack_id, recipient, content, created_at, delivered_at FROM messages {where} ORDER BY created_at DESC LIMIT 100"
        ).fetchall()
    return [{"id": r[0], "sender": r[1], "sender_slack_id": r[2], "recipient": r[3],
             "content": r[4], "created_at": r[5], "delivered_at": r[6]} for r in rows]


@app.delete("/api/messages/{message_id}", status_code=204)
def api_delete_message(message_id: int):
    with _db_lock:
        _db_conn.execute("DELETE FROM messages WHERE id=?", (message_id,))  # type: ignore[union-attr]
        _db_conn.commit()


# ── REST API (settings) ───────────────────────────────────────────────────────

_EDITABLE_SETTINGS = {
    "speaker_id_browser_url": {
        "label": "Speaker-ID ブラウザ向け URL",
        "description": "ブラウザから話者登録・テストページにアクセスする URL（例: http://raspberrypi:8082）",
        "env_fallback": lambda: SPEAKER_ID_BROWSER_URL,
    },
    "speaker_id_url": {
        "label": "Speaker-ID サーバー URL（内部）",
        "description": "bridge サーバーが話者識別 API を呼ぶ際の URL（例: http://localhost:8082）",
        "env_fallback": lambda: SPEAKER_ID_URL,
    },
    "speaker_id_threshold": {
        "label": "話者識別スコアしきい値",
        "description": "この値以上のスコアで話者を確定（0〜1、デフォルト 0.75）",
        "env_fallback": lambda: str(SPEAKER_ID_THRESHOLD),
    },
    "p2pquake_nationwide": {
        "label": "地震通知 全国モード",
        "description": "ON にすると設置場所に関わらず日本全国の地震を通知します。OFF（デフォルト）は設置場所の都道府県のみ。",
        "env_fallback": lambda: "false",
        "type": "select",
        "options": [
            {"value": "false", "label": "OFF — 設置場所の都道府県のみ（推奨）"},
            {"value": "true",  "label": "ON — 全国すべて通知"},
        ],
    },
    "p2pquake_min_scale": {
        "label": "地震通知 最小震度",
        "description": "この震度以上の地震を通知します。震度5弱以上はスマホの緊急速報と重複します。",
        "env_fallback": lambda: str(P2PQUAKE_MIN_SCALE),
        "type": "select",
        "options": [
            {"value": "10", "label": "震度1以上"},
            {"value": "20", "label": "震度2以上"},
            {"value": "30", "label": "震度3以上（推奨）"},
            {"value": "40", "label": "震度4以上"},
            {"value": "50", "label": "震度5弱以上"},
        ],
    },
    "p2pquake_tsunami_areas": {
        "label": "津波通知 対象予報区",
        "description": "通知する津波予報区名をカンマ区切りで指定。予報区名は気象庁の正式名称を使用してください。",
        "env_fallback": lambda: ",".join(P2PQUAKE_TSUNAMI_TARGET_AREAS),
        "type": "textarea",
    },
}


@app.get("/api/settings")
def api_get_settings():
    result = []
    for key, meta in _EDITABLE_SETTINGS.items():
        db_value = _get_setting(key, "")
        entry = {
            "key": key,
            "label": meta["label"],
            "description": meta["description"],
            "value": db_value,
            "env_default": meta["env_fallback"](),
            "effective": db_value or meta["env_fallback"](),
            "type": meta.get("type", "text"),
        }
        if "options" in meta:
            entry["options"] = meta["options"]
        result.append(entry)
    return result


@app.put("/api/settings/{key}")
def api_update_setting(key: str, value: str = Form(...)):
    if key not in _EDITABLE_SETTINGS:
        raise HTTPException(status_code=404, detail=f"設定キー '{key}' は存在しません")
    _set_setting(key, value.strip())
    return {"key": key, "value": value.strip()}


@app.delete("/api/settings/{key}", status_code=204)
def api_reset_setting(key: str):
    """DB の上書き値を削除して env のデフォルトに戻す。"""
    if key not in _EDITABLE_SETTINGS:
        raise HTTPException(status_code=404, detail=f"設定キー '{key}' は存在しません")
    with _db_lock:
        _db_conn.execute("DELETE FROM app_settings WHERE key=?", (key,))  # type: ignore[union-attr]
        _db_conn.commit()


@app.post("/api/geocode")
async def api_geocode(address: str = Form(...)):
    """住所文字列を国土地理院APIで緯度経度に変換し app_settings に保存する。"""
    if not address.strip():
        raise HTTPException(status_code=400, detail="住所を入力してください")
    resp = await _http_client.get(
        "https://msearch.gsi.go.jp/address-search/AddressSearch",
        params={"q": address.strip()},
    )
    resp.raise_for_status()
    results = resp.json()
    if not results:
        raise HTTPException(status_code=404, detail="住所が見つかりませんでした")

    top   = results[0]
    lon, lat = top["geometry"]["coordinates"]
    title = top["properties"]["title"]
    pref  = _extract_pref(title)

    _set_setting("location_address", address.strip())
    _set_setting("location_lat",     str(lat))
    _set_setting("location_lon",     str(lon))
    _set_setting("location_pref",    pref)
    _set_setting("location_title",   title)
    _apply_tsunami_areas_from_pref(pref)

    return {"lat": lat, "lon": lon, "pref": pref, "title": title}


@app.get("/api/location")
def api_get_location():
    """現在の設置場所設定を返す。"""
    return {
        "address": _get_setting("location_address", ""),
        "lat":     _get_setting("location_lat", ""),
        "lon":     _get_setting("location_lon", ""),
        "pref":    _get_setting("location_pref", ""),
        "title":   _get_setting("location_title", ""),
    }


async def _reverse_geocode(lat: float, lon: float) -> tuple[str, str]:
    """Nominatim (OpenStreetMap) で緯度経度 → (都道府県, 表示用住所文字列)。
    キー不要・無料。利用規約: 1 req/s 以下, User-Agent 必須。"""
    resp = await _http_client.get(
        "https://nominatim.openstreetmap.org/reverse",
        params={"lat": lat, "lon": lon, "format": "json", "accept-language": "ja"},
        headers={"User-Agent": "bridge-stackchan/1.0 (home assistant robot)"},
    )
    resp.raise_for_status()
    data = resp.json()
    addr = data.get("address", {})
    pref  = addr.get("prefecture") or addr.get("state") or addr.get("province") or ""
    city  = addr.get("city") or addr.get("town") or addr.get("village") or ""
    suburb = addr.get("suburb") or addr.get("neighbourhood") or addr.get("quarter") or ""
    parts = [p for p in [pref, city, suburb] if p]
    title = "、".join(parts) if parts else data.get("display_name", "")
    return pref, title


def _scan_local_wifi() -> list[dict]:
    """ラズパイ自身が nmcli で周辺 Wi-Fi をスキャンして AP リストを返す。
    nmcli が使えない環境では空リストを返す（IP フォールバックに委ねる）。"""
    import subprocess
    try:
        out = subprocess.check_output(
            ["nmcli", "-t", "-f", "BSSID,SIGNAL", "dev", "wifi", "list"],
            timeout=10, stderr=subprocess.DEVNULL, text=True,
        )
    except Exception:
        return []
    aps = []
    for line in out.splitlines():
        parts = line.strip().split(":")
        # nmcli -t 出力: AA\:BB\:CC\:DD\:EE\:FF:signal  (BSSID のコロンはバックスラッシュエスケープ)
        if len(parts) < 7:
            continue
        bssid  = ":".join(p.lstrip("\\") for p in parts[:6])
        signal = parts[6]
        try:
            # nmcli は 0〜100 の強度を返す → dBm に近似変換
            dbm = int(signal) // 2 - 100
            aps.append({"macAddress": bssid.lower(), "signalStrength": dbm})
        except ValueError:
            continue
    return aps


async def _geolocate_and_save(wifi_aps: list[dict], consider_ip: bool = True) -> dict:
    """Google Geolocation API + Nominatim で位置を解決して app_settings に保存する。"""
    if not GOOGLE_GEOLOCATION_API_KEY:
        raise HTTPException(status_code=503, detail="GOOGLE_GEOLOCATION_API_KEY が設定されていません")

    geo_payload: dict = {"considerIp": consider_ip}
    if wifi_aps:
        geo_payload["wifiAccessPoints"] = wifi_aps

    try:
        geo_resp = await _http_client.post(
            "https://www.googleapis.com/geolocation/v1/geolocate",
            params={"key": GOOGLE_GEOLOCATION_API_KEY},
            json=geo_payload,
        )
        geo_resp.raise_for_status()
        geo_data = geo_resp.json()
    except Exception as e:
        logger.error("Google Geolocation API error: %s", e)
        raise HTTPException(status_code=502, detail=f"Geolocation API エラー: {e}")

    lat      = geo_data["location"]["lat"]
    lon      = geo_data["location"]["lng"]
    accuracy = geo_data.get("accuracy", 0.0)

    try:
        pref, title = await _reverse_geocode(lat, lon)
    except Exception as e:
        logger.warning("reverse geocode failed: %s", e)
        pref  = ""
        title = f"緯度{lat:.4f} 経度{lon:.4f}"

    _set_setting("location_lat",   str(lat))
    _set_setting("location_lon",   str(lon))
    _set_setting("location_pref",  pref)
    _set_setting("location_title", title)
    _apply_tsunami_areas_from_pref(pref)

    logger.info("location updated: lat=%.4f lon=%.4f pref=%s title=%s acc=%.0fm",
                lat, lon, pref, title, accuracy)

    return {"lat": lat, "lon": lon, "accuracy": accuracy,
            "pref": pref, "title": title, "updated": True}


class LocationUpdateRequest(BaseModel):
    wifiAccessPoints: list[dict] = []
    considerIp: bool = True


@app.post("/api/location/from-coords")
async def api_location_from_coords(lat: float = Form(...), lon: float = Form(...)):
    """ブラウザの位置情報（緯度経度）を受け取り設置場所として保存する。
    Google API 不要。Nominatim で逆ジオコーディングして都道府県・住所を解決する。"""
    try:
        pref, title = await _reverse_geocode(lat, lon)
    except Exception as e:
        logger.warning("reverse geocode failed: %s", e)
        pref  = ""
        title = f"緯度{lat:.4f} 経度{lon:.4f}"
    _set_setting("location_lat",   str(lat))
    _set_setting("location_lon",   str(lon))
    _set_setting("location_pref",  pref)
    _set_setting("location_title", title)
    _apply_tsunami_areas_from_pref(pref)
    logger.info("location set from browser coords: lat=%.4f lon=%.4f pref=%s", lat, lon, pref)
    return {"lat": lat, "lon": lon, "pref": pref, "title": title}


@app.post("/api/location/update")
async def api_location_update(req: LocationUpdateRequest):
    """Stack-chan から Wi-Fi スキャン結果を受け取り位置を更新する。"""
    return await _geolocate_and_save(req.wifiAccessPoints, req.considerIp)


@app.post("/api/location/scan")
async def api_location_scan():
    """ラズパイ自身が Wi-Fi をスキャンして位置を更新する（WebUI テスト用）。"""
    aps = _scan_local_wifi()
    logger.info("local wifi scan: %d APs found", len(aps))
    return await _geolocate_and_save(aps, consider_ip=True)


# ── REST API (UI test) ────────────────────────────────────────────────────────

class UiSpeakRequest(BaseModel):
    text: str
    mode: str = "say"  # "say" | "speak"


@app.post("/api/ui/speak")
async def api_ui_speak(req: UiSpeakRequest):
    if not req.text.strip():
        raise HTTPException(status_code=400, detail="text は必須です")
    if req.mode == "speak":
        speak_instruction = (
            "以下はスタックちゃんがその場にいる人に向けて話す内容の原文です。"
            "この内容をスタックちゃんらしい口調に変換してください。"
        )
        reply = await chat_with_llm(req.text, system_prompt_append=speak_instruction, use_functions=False)
        expression, text_to_say = _parse_expression(reply)
    else:
        expression, text_to_say = "neutral", req.text
    speaker_id, stackchan_expr = _resolve_expression(expression)
    audio_url, streaming_url = await resolve_audio_url(text_to_say, speaker_id)
    req_id = str(uuid.uuid4())
    publish_speak(audio_url, streaming_url, text_to_say, "ui", "normal", req_id, stackchan_expr)
    return {"requestId": req_id, "text": text_to_say, "expression": stackchan_expr}


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


@app.post("/api/debug/p2pquake")
async def debug_p2pquake(code: int = Query(551), force: bool = Query(False)):
    """
    P2P地震情報の直近データを取得してハンドラに流す（テスト用）。
    code: 551=地震, 552=津波, 554=EEW, 556=南海トラフ, それ以外=unknown LLM
    force=true: dedup をスキップして必ず発話する
    """
    p2p_history_url = f"https://api.p2pquake.net/v2/history?codes={code}&limit=1"
    async with aiohttp.ClientSession() as session:
        async with session.get(p2p_history_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                raise HTTPException(status_code=502, detail=f"P2P API returned {resp.status}")
            events = await resp.json()

    if not events:
        raise HTTPException(status_code=404, detail=f"code={code} の直近データが見つかりません")

    data = events[0]
    event_id = data.get("id", "")

    if force and event_id:
        # dedup エントリを一時削除して再処理できるようにする
        with _db_lock:
            _db_conn.execute(  # type: ignore[union-attr]
                "DELETE FROM earthquake_log WHERE earthquake_id = ? OR earthquake_id LIKE ?",
                (event_id, event_id + ":%"),
            )
            _db_conn.commit()  # type: ignore[union-attr]

    if code == 551:
        await _handle_earthquake(data)
    elif code == 552:
        await _handle_tsunami(data)
    elif code == 554:
        await _handle_eew(data)
    elif code == 556:
        await _handle_nankai(data)
    else:
        await _unknown_p2p_llm(data)

    return {"ok": True, "code": code, "event_id": event_id, "force": force}


# WMO 天気コード → 日本語説明
_WMO_DESC: dict[int, str] = {
    0: "快晴", 1: "晴れ", 2: "一部曇り", 3: "曇り",
    45: "霧", 48: "着氷性の霧",
    51: "霧雨（弱）", 53: "霧雨", 55: "霧雨（強）",
    61: "小雨", 63: "雨", 65: "大雨",
    71: "小雪", 73: "雪", 75: "大雪",
    80: "にわか雨（弱）", 81: "にわか雨", 82: "にわか雨（強）",
    95: "雷雨", 96: "雷雨（ひょう）", 99: "雷雨（大粒のひょう）",
}


@app.get("/api/debug/coverage")
def api_debug_coverage():
    """現在の設置場所から導出される監視エリアをまとめて返す（表示専用）。"""
    lat   = _get_setting("location_lat", "")
    lon   = _get_setting("location_lon", "")
    pref  = _get_setting("location_pref", "")
    title = _get_setting("location_title", "")
    nationwide = _get_setting("p2pquake_nationwide", "false") == "true"
    min_scale  = int(_get_setting("p2pquake_min_scale", str(P2PQUAKE_MIN_SCALE)))
    tsunami_areas_str = _get_setting("p2pquake_tsunami_areas", ",".join(P2PQUAKE_TSUNAMI_TARGET_AREAS))
    tsunami_areas = [a.strip() for a in tsunami_areas_str.split(",") if a.strip()]

    scale_labels = {10: "震度1", 20: "震度2", 30: "震度3", 40: "震度4", 50: "震度5弱"}

    return {
        "location": {
            "title": title or None,
            "pref":  pref  or None,
            "lat":   float(lat) if lat else None,
            "lon":   float(lon) if lon else None,
            "configured": bool(pref),
        },
        "earthquake": {
            "enabled": P2PQUAKE_ENABLED,
            "mode": "全国" if nationwide else ("設置場所のみ" if pref else "全国（設置場所未設定のため）"),
            "filter_pref": None if nationwide else (pref or None),
            "min_scale_label": scale_labels.get(min_scale, f"コード{min_scale}"),
        },
        "tsunami": {
            "enabled": P2PQUAKE_ENABLED,
            "areas": tsunami_areas,
        },
        "weather": {
            "enabled": bool(lat and lon),
            "lat": float(lat) if lat else None,
            "lon": float(lon) if lon else None,
            "note": "Open-Meteo（設置場所の座標を使用）" if lat else "位置情報未設定のため利用不可",
        },
    }


@app.get("/api/debug/weather")
async def api_debug_weather():
    """Open-Meteo から設置場所の現在天気を取得して返す。"""
    lat = _get_setting("location_lat", "")
    lon = _get_setting("location_lon", "")
    if not lat or not lon:
        raise HTTPException(status_code=400, detail="設置場所が未設定です。設定画面で場所を登録してください。")

    resp = await _http_client.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": lat, "longitude": lon,
            "current": "temperature_2m,apparent_temperature,precipitation,weathercode,windspeed_10m,relativehumidity_2m",
            "timezone": "Asia/Tokyo",
        },
    )
    resp.raise_for_status()
    data = resp.json()
    cur  = data.get("current", {})
    code = cur.get("weathercode", -1)
    return {
        "location": {"lat": float(lat), "lon": float(lon), "title": _get_setting("location_title", "")},
        "weather": {
            "description":        _WMO_DESC.get(code, f"コード{code}"),
            "weathercode":        code,
            "temperature":        cur.get("temperature_2m"),
            "apparent_temp":      cur.get("apparent_temperature"),
            "humidity":           cur.get("relativehumidity_2m"),
            "precipitation":      cur.get("precipitation"),
            "windspeed":          cur.get("windspeed_10m"),
            "time":               cur.get("time"),
        },
    }


@app.post("/api/debug/weather/speak")
async def api_debug_weather_speak():
    """現在の天気をLLMで変換してスタックちゃんに喋らせる（テスト用）。"""
    lat = _get_setting("location_lat", "")
    lon = _get_setting("location_lon", "")
    if not lat or not lon:
        raise HTTPException(status_code=400, detail="設置場所が未設定です。")

    resp = await _http_client.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": lat, "longitude": lon,
            "current": "temperature_2m,apparent_temperature,precipitation,weathercode,windspeed_10m,relativehumidity_2m",
            "timezone": "Asia/Tokyo",
        },
    )
    resp.raise_for_status()
    cur = resp.json().get("current", {})
    code = cur.get("weathercode", -1)
    desc = _WMO_DESC.get(code, f"コード{code}")
    title = _get_setting("location_title", "設置場所")

    prompt = (
        f"【現在の天気 — {title}】\n"
        f"天気: {desc} / 気温: {cur.get('temperature_2m')}°C（体感 {cur.get('apparent_temperature')}°C）"
        f" / 湿度: {cur.get('relativehumidity_2m')}% / 降水量: {cur.get('precipitation')}mm"
        f" / 風速: {cur.get('windspeed_10m')}km/h\n\n"
        "この天気情報をもとに、家族に向けて短く天気をお知らせしてください。"
    )
    reply = await chat_with_llm(prompt, session_key="family", use_functions=False)
    _, clean = _parse_expression(reply)
    speaker_id, expr = _resolve_expression("neutral")
    audio_url, stream_url = await resolve_audio_url(clean, speaker_id)
    req_id = str(uuid.uuid4())
    publish_speak(audio_url, stream_url, clean, "weather_test", "normal", req_id, expr)
    return {"ok": True, "text": clean, "weather": desc}


@app.get("/debug/calendar-items")
def debug_calendar_items():
    """items テーブルの全レコードを返す（デバッグ用）。"""
    with _db_lock:
        rows = _db_conn.execute(  # type: ignore[union-attr]
            """SELECT id, type, person_name, title, start_at, end_at, due_at,
                      notify_at, all_day, status, synced_at
               FROM items ORDER BY COALESCE(start_at, due_at) ASC"""
        ).fetchall()
    items = [
        {
            "id": r[0], "type": r[1], "person_name": r[2], "title": r[3],
            "start_at": r[4], "end_at": r[5], "due_at": r[6],
            "notify_at": r[7], "all_day": bool(r[8]), "status": r[9], "synced_at": r[10],
        }
        for r in rows
    ]
    return {"count": len(items), "items": items}


class CalendarSourceCreate(BaseModel):
    source_type: str
    source_id: str
    person_name: str
    notify: bool = True
    token_key: str = "default"
    enabled: bool = True


@app.get("/calendar/sources")
def list_calendar_sources():
    """登録済みカレンダー・タスクリスト一覧。"""
    with _db_lock:
        rows = _db_conn.execute(  # type: ignore[union-attr]
            "SELECT id, source_type, source_id, person_name, notify, token_key, enabled, created_at "
            "FROM calendar_sources ORDER BY id"
        ).fetchall()
    return {
        "count": len(rows),
        "sources": [
            {
                "id": r[0], "source_type": r[1], "source_id": r[2],
                "person_name": r[3], "notify": bool(r[4]), "token_key": r[5],
                "enabled": bool(r[6]), "created_at": r[7],
            }
            for r in rows
        ],
    }


@app.post("/calendar/sources", status_code=201)
def create_calendar_source(req: CalendarSourceCreate):
    """カレンダーまたはタスクリストを登録する。"""
    if req.source_type not in ("calendar", "tasklist"):
        raise HTTPException(status_code=422, detail="source_type は 'calendar' または 'tasklist' を指定してください")
    now = datetime.now(_JST).isoformat()
    try:
        with _db_lock:
            cursor = _db_conn.execute(  # type: ignore[union-attr]
                """
                INSERT INTO calendar_sources
                    (source_type, source_id, person_name, notify, token_key, enabled, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (req.source_type, req.source_id, req.person_name, int(req.notify), req.token_key, int(req.enabled), now, now),
            )
            _db_conn.commit()  # type: ignore[union-attr]
            row_id = cursor.lastrowid
    except Exception as e:
        if "UNIQUE" in str(e):
            raise HTTPException(status_code=409, detail=f"source_id '{req.source_id}' はすでに登録されています")
        raise HTTPException(status_code=500, detail=str(e))
    logger.info(
        "Calendar source registered: id=%d type=%s source_id=%s person=%s token_key=%s",
        row_id, req.source_type, req.source_id, req.person_name, req.token_key,
    )
    return {"id": row_id, "source_type": req.source_type, "source_id": req.source_id, "person_name": req.person_name}


@app.delete("/calendar/sources/{source_id}")
def delete_calendar_source(source_id: int):
    """カレンダーソースの登録を削除する。"""
    with _db_lock:
        c = _db_conn.execute(  # type: ignore[union-attr]
            "DELETE FROM calendar_sources WHERE id = ?", (source_id,)
        )
        _db_conn.commit()  # type: ignore[union-attr]
    if c.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"id={source_id} は見つかりませんでした")
    logger.info("Calendar source deleted: id=%d", source_id)
    return {"deleted": source_id}


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
        model=STT_MODEL,
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


def _build_location_context() -> str:
    """設置場所が設定されていればシステムプロンプト用の文字列を返す。未設定なら空文字。"""
    title = _get_setting("location_title", "")
    pref  = _get_setting("location_pref", "")
    if not title:
        return ""
    return (
        f"【あなたの設置場所】あなたは {title} に置かれています。"
        "「どこにいる？」「ここはどこ？」などの質問にはこの場所名を答えること。"
        "天気・地域の話題・距離感もこの場所を基準にすること。"
    )


# ── LLM Backend Protocol + implementations ───────────────────────────────────

class LLMBackend(Protocol):
    async def chat(
        self,
        text: str,
        audio: bytes | None,
        speaker: str | None,
        system_prompt_append: str,
        session_key: str,
        notify_context: dict | None,
        use_functions: bool,
    ) -> str: ...


class OpenClawResponsesBackend:
    async def chat(
        self,
        text: str,
        audio: bytes | None,
        speaker: str | None,
        system_prompt_append: str,
        session_key: str,
        notify_context: dict | None,
        use_functions: bool,
    ) -> str:
        url = OPENCLAW_BASE_URL.rstrip("/") + "/responses"
        headers: dict = {
            "Content-Type": "application/json",
            "x-openclaw-scopes": "operator.read,operator.write",
        }
        if OPENCLAW_GATEWAY_TOKEN:
            headers["Authorization"] = f"Bearer {OPENCLAW_GATEWAY_TOKEN}"
        effective_session = session_key or OPENCLAW_SESSION_KEY
        if effective_session:
            headers["x-openclaw-session-key"] = effective_session

        user_input: str | list = f"[話者: {speaker}] {text}" if speaker else text
        instructions_parts = [_build_datetime_context()]
        loc_ctx = _build_location_context()
        if loc_ctx:
            instructions_parts.append(loc_ctx)
        if system_prompt_append:
            instructions_parts.append(system_prompt_append)
        tools = list(_TIMER_TOOLS) if use_functions else []
        if use_functions and CALENDAR_ENABLED:
            tools.extend(_CALENDAR_TOOLS)
        if use_functions:
            tools.extend(_MESSAGE_TOOLS)
        if use_functions and P2PQUAKE_ENABLED:
            tools.extend(_ALERT_TOOLS)

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
                if "output_text" in data:
                    return data["output_text"]
                for item in output:
                    for content in item.get("content", []):
                        if content.get("type") == "output_text":
                            return content["text"]
                raise RuntimeError(f"OpenClaw response に返答テキストが見つかりません: {data}")
            user_input = function_outputs

        raise RuntimeError("OpenClaw function calling loop exceeded max iterations")


class OpenAIResponsesBackend:
    async def chat(
        self,
        text: str,
        audio: bytes | None,
        speaker: str | None,
        system_prompt_append: str,
        session_key: str,
        notify_context: dict | None,
        use_functions: bool,
    ) -> str:
        url = OPENAI_RESPONSES_BASE_URL.rstrip("/") + "/responses"
        headers: dict = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OPENAI_API_KEY}",
        }

        user_input: str | list = f"[話者: {speaker}] {text}" if speaker else text

        # _handle_function_calls から enable_web_search フラグを書き戻すため、ここで必ず辞書化する
        notify_ctx: dict = notify_context if notify_context is not None else {}

        instructions_parts = [_STACKCHAN_SYSTEM_PROMPT, _build_datetime_context()]
        loc_ctx = _build_location_context()
        if loc_ctx:
            instructions_parts.append(loc_ctx)
        if OPENAI_RESPONSES_WEB_SEARCH and OPENAI_RESPONSES_WEB_SEARCH_ON_DEMAND:
            instructions_parts.append(
                "Web検索ガイドライン:\n"
                "- 雑談・感情表現・既に知っている内容ではWeb検索を使わない\n"
                "- 「最新」「今日」「いま」「天気」「ニュース」など現在の情報が必要なときだけ "
                "request_web_search を呼ぶ"
            )
        if system_prompt_append:
            instructions_parts.append(system_prompt_append)

        session = _get_session_data(session_key) if session_key else _SessionData(None, 0, 0, None)
        previous_response_id = session.response_id if not DISABLE_SESSION_HISTORY else None

        # previous_response_id がない（新規 or リセット後）かつサマリがあれば過去の文脈として注入
        if not previous_response_id and session.summary:
            instructions_parts.append(
                f"【過去の会話の要約】\n{session.summary}"
            )

        tools = list(_TIMER_TOOLS) if (use_functions and not DISABLE_TOOLS) else []
        if use_functions and CALENDAR_ENABLED and not DISABLE_TOOLS:
            tools.extend(_CALENDAR_TOOLS)
        if use_functions and not DISABLE_TOOLS:
            tools.extend(_MESSAGE_TOOLS)
        if use_functions and P2PQUAKE_ENABLED and not DISABLE_TOOLS:
            tools.extend(_ALERT_TOOLS)
        if OPENAI_RESPONSES_WEB_SEARCH and not DISABLE_TOOLS:
            if OPENAI_RESPONSES_WEB_SEARCH_ON_DEMAND:
                tools.append(_REQUEST_WEB_SEARCH_TOOL)
            else:
                tools.append({"type": OPENAI_RESPONSES_WEB_SEARCH_TOOL})

        logger.info(
            "OpenAI Responses request: model=%s session_key=%s previous_response_id=%s "
            "char_in=%d char_out=%d has_summary=%s web_search=%s on_demand=%s",
            OPENAI_RESPONSES_MODEL, session_key or "(none)", previous_response_id or "(none)",
            session.char_count_in, session.char_count_out, bool(session.summary),
            OPENAI_RESPONSES_WEB_SEARCH, OPENAI_RESPONSES_WEB_SEARCH_ON_DEMAND,
        )

        for _ in range(5):  # Function calling ループ（最大 5 回）
            # ON_DEMAND モードで LLM が前ターンに request_web_search を呼んでいたら、
            # ここで本物の web_search_preview に差し替える（Pass 2 への昇格）
            if (
                OPENAI_RESPONSES_WEB_SEARCH
                and OPENAI_RESPONSES_WEB_SEARCH_ON_DEMAND
                and notify_ctx.get("enable_web_search")
            ):
                tools = [t for t in tools if t.get("name") != "request_web_search"]
                if not any(t.get("type") == OPENAI_RESPONSES_WEB_SEARCH_TOOL for t in tools):
                    tools.append({"type": OPENAI_RESPONSES_WEB_SEARCH_TOOL})
                    logger.info("Web search promoted to Pass 2")
                notify_ctx["enable_web_search"] = False  # 多重昇格防止

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
            function_outputs = await _handle_function_calls(output, notify_ctx)

            if function_outputs is None:
                # 最終テキストレスポンス → ここでのみ DB に保存する
                # （function_call の中間レスポンス ID を保存すると次回の会話で 400 エラーになるため）
                reply_text = data.get("output_text") or ""
                if not reply_text:
                    for item in output:
                        for content in item.get("content", []):
                            if content.get("type") == "output_text":
                                reply_text = content["text"]
                                break
                        if reply_text:
                            break
                if not reply_text:
                    raise RuntimeError(f"OpenAI Responses response に返答テキストが見つかりません: {data}")

                if session_key:
                    new_in  = session.char_count_in  + len(text)
                    new_out = session.char_count_out + len(reply_text)
                    _save_session(
                        session_key=session_key,
                        response_id=response_id,
                        char_count_in=new_in,
                        char_count_out=new_out,
                        summary=session.summary,
                    )
                    logger.info(
                        "Session saved: session_key=%s response_id=%s char_in=%d char_out=%d total=%d",
                        session_key, response_id, new_in, new_out, new_in + new_out,
                    )
                    # 閾値を超えたら要約してリセット（次回リクエストからクリーンな状態になる）
                    if response_id and (new_in + new_out) >= SESSION_SUMMARY_THRESHOLD:
                        logger.info(
                            "Session char threshold reached (%d >= %d), summarizing: session_key=%s",
                            new_in + new_out, SESSION_SUMMARY_THRESHOLD, session_key,
                        )
                        asyncio.create_task(_summarize_and_reset_session(session_key, response_id))

                return reply_text

            # Function call あり → ループ内での previous_response_id を更新して継続
            # （DB には保存しない。function_call の未解決状態を DB に残さないため）
            if response_id:
                previous_response_id = response_id
            user_input = function_outputs

        raise RuntimeError("OpenAI Responses function calling loop exceeded max iterations")


_BACKENDS: dict[str, LLMBackend] = {
    "openclaw": OpenClawResponsesBackend(),
    "openai": OpenAIResponsesBackend(),
}


async def chat_with_openclaw(
    text: str,
    speaker: str | None = None,
    system_prompt_append: str = "",
    notify_context: dict | None = None,
    use_functions: bool = True,
) -> str:
    return await _BACKENDS["openclaw"].chat(
        text, None, speaker, system_prompt_append, "", notify_context, use_functions
    )


async def chat_with_openai_responses(
    text: str,
    speaker: str | None = None,
    system_prompt_append: str = "",
    session_key: str = "",
    notify_context: dict | None = None,
    use_functions: bool = True,
) -> str:
    return await _BACKENDS["openai"].chat(
        text, None, speaker, system_prompt_append, session_key, notify_context, use_functions
    )


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
    use_functions: False にすると Function Calling ツールを含めない（タイマー発火時など）
    """
    backend = _BACKENDS.get(LLM_BACKEND)
    if backend is None:
        raise ValueError(f"Unknown LLM_BACKEND: {LLM_BACKEND!r}")
    return await backend.chat(text, None, speaker, system_prompt_append, session_key, notify_context, use_functions)


# ── Slack Bot (Socket Mode) ───────────────────────────────────────────────────

_MENTION_RE = re.compile(r"<@[A-Z0-9]+>")


async def _slack_handle_mention(event: dict, say) -> None:
    """app_mention: チャンネルで @stackchan されたときに Slack へテキストで返信する（MQTT 発話なし）。"""
    text = _MENTION_RE.sub("", event.get("text", "")).strip()
    if not text:
        return

    channel = event["channel"]
    user = event.get("user", "")
    session_key = f"slack:channel:{channel}"
    _record_slack_user(user)
    sender_name = _resolve_display_name(user, "")
    logger.info("Slack mention: channel=%s sender=%s text=%s", channel, sender_name or "(unknown)", text[:60])

    try:
        reply = await chat_with_llm(
            text,
            speaker=sender_name or None,
            session_key=session_key,
            notify_context={"session_key": session_key, "slack_channel": channel},
        )
    except Exception as e:
        logger.error("Slack mention LLM error: %s", e)
        await say(_classify_api_error(e) or "ごめんね、うまく考えられなかったよ〜 もう一度試してみて！")
        return

    _, clean_reply = _parse_expression(reply)
    await say(clean_reply)


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
    _record_slack_user(user)
    sender_name = _resolve_display_name(user, "")
    logger.info("Slack DM: user=%s sender=%s text=%s", user, sender_name or "(unknown)", text[:60])

    try:
        reply = await chat_with_llm(
            text,
            speaker=sender_name or None,
            session_key=session_key,
            notify_context={"session_key": session_key, "slack_channel": channel},
        )
    except Exception as e:
        logger.error("Slack DM LLM error: %s", e)
        await say(_classify_api_error(e) or "ごめんね、うまく考えられなかったよ〜 もう一度試してみて！")
        return

    _, clean_reply = _parse_expression(reply)
    await say(clean_reply)


async def _deliver_pending_messages_after(main_reply: str, source: str, priority: str, session_key: str = "") -> None:
    """メイン返答の再生推定時間後に未読伝言を MQTT で届ける。
    日本語の平均読み上げ速度 ~5.5文字/秒 + バッファ3秒で待機する。
    """
    wait_sec = len(main_reply) / 5.5 + 3.0
    await asyncio.sleep(wait_sec)

    messages = _fetch_pending_messages()
    if not messages:
        return

    for msg in messages:
        sender = msg["sender"]
        recipient = msg["recipient"]
        content = msg["content"]

        recipient_part = f"（{recipient}への伝言）" if recipient else ""
        prompt = (
            f"以下の伝言{recipient_part}を、スタックちゃんとして読み上げてください。\n"
            "必ず「そういえば」「あ、そうだ」「ちなみに」などの話題転換の言葉を文頭に入れてください。\n"
            "自然な話し言葉で短くまとめてください。\n\n"
            f"送り主: {sender}\n"
            f"内容: {content}"
        )
        try:
            reply = await chat_with_llm(
                prompt,
                system_prompt_append="",
                session_key=session_key,
                notify_context={"session_key": session_key, "slack_channel": None},
                use_functions=False,
            )
        except Exception as e:
            logger.error("Message delivery LLM error: msg_id=%d %s", msg["id"], e)
            continue

        expression, clean_reply = _parse_expression(reply)
        speaker_id, stackchan_expr = _resolve_expression(expression)
        try:
            audio_url, streaming_url = await resolve_audio_url(clean_reply, speaker_id)
            req_id = str(uuid.uuid4())
            publish_speak(audio_url, streaming_url, clean_reply, source, priority, req_id, stackchan_expr)
            _mark_message_delivered(msg["id"])
            logger.info("Message delivered: id=%d text=%s", msg["id"], clean_reply[:60])
            await _notify_message_delivered(msg)
        except Exception as e:
            logger.error("Message delivery speak error: msg_id=%d %s", msg["id"], e)

        if len(messages) > 1:
            await asyncio.sleep(3.0)


async def _notify_message_delivered(msg: dict) -> None:
    """伝言が読まれたことを送信者に Slack DM で通知する。"""
    slack_id = msg.get("sender_slack_id")
    if not slack_id or not _slack_app:
        return
    recipient_part = f"{msg['recipient']}への" if msg["recipient"] else ""
    try:
        await _slack_app.client.chat_postMessage(
            channel=slack_id,
            text=f"📬 {recipient_part}伝言が届いたよ！「{msg['content']}」",
        )
        logger.info("Delivery notification sent: msg_id=%d slack_id=%s", msg["id"], slack_id)
    except Exception as e:
        logger.error("Delivery notification error: msg_id=%d %s", msg["id"], e)


def _record_slack_user_from_body(body: dict) -> None:
    """スラッシュコマンドの body から Slack ユーザーを記録する。"""
    user_id = body.get("user_id", "")
    user_name = body.get("user_name")
    if user_id:
        _record_slack_user(user_id, user_name)


async def _slack_handle_say(ack, body: dict, respond) -> None:
    """/say コマンド: テキストを LLM 変換なしでそのまま VOICEVOX → MQTT 送信。"""
    await ack()
    _record_slack_user_from_body(body)

    text = body.get("text", "").strip()
    if not text:
        await respond("読み上げる内容を入力してください。例: `/say おはようございます`")
        return

    logger.info("Slack /say: channel=%s text=%s", body.get("channel_id"), text[:60])
    req_id = str(uuid.uuid4())
    try:
        audio_url, streaming_url = await resolve_audio_url(text)
        _pending_acks[req_id] = asyncio.Event()
        publish_speak(audio_url, streaming_url, text, "slack", "normal", req_id)
    except Exception as e:
        _pending_acks.pop(req_id, None)
        logger.error("Slack /say error: %s", e)
        await respond(f"音声の送信に失敗したよ。テキストはこれ：「{text}」")
        return

    ack_ok = await wait_for_ack(req_id)
    if ack_ok:
        await respond(f"話すよ！「{text}」", response_type="in_channel")
    else:
        await respond(f"⚠️ スタックちゃんから応答がなかったよ。届いてないかも。「{text}」", response_type="in_channel")


async def _slack_handle_register(ack, body: dict, respond) -> None:
    """/register コマンド: 自分の Slack アカウントを家族メンバーとして登録する。
    書式: /register <呼び名>
    例:   /register パパ
    """
    await ack()
    _record_slack_user_from_body(body)

    name = body.get("text", "").strip()
    if not name:
        await respond("使い方: `/register <呼び名>`\n例: `/register パパ`")
        return

    user_id = body.get("user_id", "")
    now = datetime.now(_JST).isoformat()
    try:
        with _db_lock:
            _db_conn.execute(  # type: ignore[union-attr]
                """INSERT INTO family_members (name, slack_user_id, created_at, updated_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(name) DO UPDATE SET slack_user_id=excluded.slack_user_id, updated_at=excluded.updated_at""",
                (name, user_id, now, now),
            )
            _db_conn.commit()
        logger.info("Slack /register: user_id=%s name=%s", user_id, name)
        await respond(f"✅ 「{name}」として登録したよ！")
    except Exception as e:
        logger.error("Slack /register error: %s", e)
        await respond("登録に失敗したよ。もう一度試してみて！")


async def _slack_handle_tell(ack, body: dict, respond) -> None:
    """/tell コマンド: 伝言を DB に保存。次回の会話時にスタックちゃんが読み上げる。
    書式: /tell [宛名] <内容>
    例:   /tell しおり 明日の習い事は16時からだよ
          /tell 夕食は7時です（宛名なしは全員向け）
    """
    await ack()
    _record_slack_user_from_body(body)

    text = body.get("text", "").strip()
    if not text:
        await respond(
            "使い方: `/tell [宛名] <内容>`\n"
            "例: `/tell しおり 明日の習い事は16時からだよ`\n"
            "　　`/tell 夕食は7時です`（宛名なしは全員向け）"
        )
        return

    # 先頭トークンが6文字以内なら宛名とみなす（日本語の名前は概ね短い）
    tokens = text.split(None, 1)
    if len(tokens) == 2 and len(tokens[0]) <= 6:
        recipient, content = tokens[0], tokens[1]
    else:
        recipient, content = None, text

    sender_slack_id = body.get("user_id")
    fallback_name = body.get("user_name") or sender_slack_id or "だれか"
    sender = _resolve_display_name(sender_slack_id, fallback_name)
    msg_id = _save_message(sender, recipient, content, sender_slack_id)
    logger.info("Message saved: id=%d sender=%s recipient=%s", msg_id, sender, recipient)

    if recipient:
        await respond(f"📬 {recipient}への伝言を預かったよ！次に話しかけてもらったときに伝えるね。")
    else:
        await respond(f"📬 みんなへの伝言を預かったよ！次に話しかけてもらったときに伝えるね。")


async def _slack_handle_speak(ack, body: dict, respond) -> None:
    """/speak コマンド: テキストをスタックちゃん口調に変換して MQTT 送信。"""
    await ack()
    _record_slack_user_from_body(body)

    text = body.get("text", "").strip()
    if not text:
        await respond("話す内容を入力してください。例: `/speak おはようございます`")
        return

    channel_id = body.get("channel_id", "")
    user_id = body.get("user_id", "")
    sender_name = _resolve_display_name(user_id, body.get("user_name") or "だれか")
    # ingest-audio と同じセッションを共有することで、音声会話と Slack /speak の記憶が繋がる
    session_key = MQTT_DEVICE_ID
    logger.info("Slack /speak: channel=%s sender=%s session=%s text=%s", channel_id, sender_name, session_key, text[:60])

    try:
        # /speak は「みんなへの発信」なので、依頼者への返答にならないよう指示を加える
        # 送信者名を LLM に渡すことで「パパが〜って言ってたよ」のような表現が可能になる
        speak_instruction = (
            f"{sender_name}から家族全員へのメッセージです。"
            "以下の内容をスタックちゃんらしい口調で読み上げてください。"
            "特定の個人への呼びかけにはせず、その場にいる全員に向けて話してください。"
        )
        reply = await chat_with_llm(
            text,
            system_prompt_append=speak_instruction,
            session_key=session_key,
            notify_context={"session_key": session_key, "slack_channel": channel_id},
            use_functions=False,
        )
    except Exception as e:
        logger.error("Slack /speak LLM error: %s", e)
        await respond("ごめん、うまく変換できなかったよ。もう一度試してね！")
        return

    expression, clean_reply = _parse_expression(reply)
    speaker_id, stackchan_expr = _resolve_expression(expression)
    try:
        audio_url, streaming_url = await resolve_audio_url(clean_reply, speaker_id)
        req_id = str(uuid.uuid4())
        # ACK が publish_speak より先に届いても取りこぼさないよう、先に event を登録する
        _pending_acks[req_id] = asyncio.Event()
        publish_speak(audio_url, streaming_url, clean_reply, "slack", "normal", req_id, stackchan_expr)
    except Exception as e:
        _pending_acks.pop(req_id, None)
        logger.error("Slack /speak speak error: %s", e)
        await respond(f"音声の送信に失敗したよ。テキストはこれ：「{clean_reply}」")
        return

    ack_ok = await wait_for_ack(req_id)
    if ack_ok:
        await respond(f"話すよ！「{clean_reply}」", response_type="in_channel")
    else:
        await respond(f"⚠️ スタックちゃんから応答がなかったよ。届いてないかも。「{clean_reply}」", response_type="in_channel")


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
    _record_slack_user_from_body(body)

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
    _slack_app.command("/register")(_slack_handle_register)
    _slack_app.command("/say")(_slack_handle_say)
    _slack_app.command("/speak")(_slack_handle_speak)
    _slack_app.command("/tell")(_slack_handle_tell)
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
    expression: str = Form(""),
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
    - expression: default expression used when LLM reply does not include one (default: "neutral")
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
        error_reply = _classify_api_error(e)
        if error_reply:
            try:
                return await _deliver_error_reply(error_reply, source, priority, req_id, mode)
            except Exception as speak_e:
                logger.error("STT error fallback speak failed: %s", speak_e)
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
        error_reply = _classify_api_error(e)
        if error_reply:
            try:
                return await _deliver_error_reply(error_reply, source, priority, req_id, mode)
            except Exception as speak_e:
                logger.error("LLM error fallback speak failed: %s", speak_e)
        raise HTTPException(status_code=502, detail=f"LLM error: {e}")
    expression, clean_reply = _parse_expression(reply, default=expression)
    speaker_id, stackchan_expr = _resolve_expression(expression)
    logger.info("LLM reply: backend=%s request_id=%s expression=%s text=%s", LLM_BACKEND, req_id, expression, clean_reply[:80])

    try:
        audio_url, audio_streaming_url = await resolve_audio_url(clean_reply, speaker_id)
    except Exception as e:
        logger.error("VOICEVOX error: %s", e)
        raise HTTPException(status_code=502, detail=f"VOICEVOX error: {e}")

    if mode == "async":
        try:
            publish_speak(audio_url, audio_streaming_url, clean_reply, source, priority, req_id, stackchan_expr)
        except Exception as e:
            logger.error("MQTT error: %s", e)
            raise HTTPException(status_code=502, detail=f"MQTT error: {e}")
        return {"requestId": req_id, "expression": stackchan_expr}

    # sync: return full result in response body without MQTT
    # 未読伝言があれば、メイン音声の再生推定時間後に MQTT で届ける
    asyncio.create_task(_deliver_pending_messages_after(clean_reply, source, priority, session_key=effective_session_key))

    resp: dict = {
        "requestId": req_id,
        "transcript": transcript,
        "speaker": speaker,
        "reply": clean_reply,
        "expression": stackchan_expr,
        "audioUrl": audio_url,
    }
    if audio_streaming_url:
        resp["audioStreamingUrl"] = audio_streaming_url
    return resp
