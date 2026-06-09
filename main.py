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
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "secrets/credentials.json")
GOOGLE_TOKEN_DIR = os.getenv("GOOGLE_TOKEN_DIR", "secrets")
CALENDAR_SYNC_INTERVAL_MINUTES = int(os.getenv("CALENDAR_SYNC_INTERVAL_MINUTES", "30"))
CALENDAR_DEFAULT_NOTIFY_MINUTES = int(os.getenv("CALENDAR_DEFAULT_NOTIFY_MINUTES", "15"))
CALENDAR_SYNC_DAYS_AHEAD = int(os.getenv("CALENDAR_SYNC_DAYS_AHEAD", "7"))
CALENDAR_NOTIFY_CHECK_INTERVAL = int(os.getenv("CALENDAR_NOTIFY_CHECK_INTERVAL", "60"))
CALENDAR_NOTIFY_GRACE_MINUTES = int(os.getenv("CALENDAR_NOTIFY_GRACE_MINUTES", "60"))

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
- ウェブ検索した内容は要点を2〜3文で話し言葉にまとめる
- URL や出典、「〜によると」などの引用表現は読み上げない

利用者について:
- 家族みんなが使うシステムです
- 特定の一人に対応しすぎないようにする
- 誰にでも分かりやすく、親しみやすい表現を心がける\
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
    audio_url, streaming_url = await resolve_audio_url(message)
    req_id = str(uuid.uuid4())
    publish_speak(audio_url, streaming_url, message, "calendar", "normal", req_id)
    logger.info("Calendar notification sent: item_id=%s message=%s", item["id"], message[:60])


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
        await say(_classify_api_error(e) or "ごめんね、うまく考えられなかったよ〜 もう一度試してみて！")
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
        await say(_classify_api_error(e) or "ごめんね、うまく考えられなかったよ〜 もう一度試してみて！")
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
