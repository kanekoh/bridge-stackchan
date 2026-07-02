import logging
import sqlite3
import sys
import threading
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from bridge.config import (
    DB_PATH, _JST, SESSION_SUMMARY_THRESHOLD, SESSION_SUMMARY_MAX_TOKENS,
    OPENAI_RESPONSES_BASE_URL, OPENAI_API_KEY, OPENAI_RESPONSES_MODEL,
)

logger = logging.getLogger(__name__)

_db_lock = threading.Lock()
_db_conn: sqlite3.Connection | None = None  # initialized in _init_db()


def _init_db() -> None:
    global _db_conn
    import os
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
    _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS device_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id   TEXT NOT NULL,
            level       TEXT,
            ts_ms       INTEGER,
            msg         TEXT,
            raw_json    TEXT NOT NULL,
            received_at TEXT NOT NULL
        )
    """)
    _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS web_checks (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            name               TEXT NOT NULL UNIQUE,
            url                TEXT NOT NULL DEFAULT '',
            check_prompt       TEXT NOT NULL DEFAULT '',
            enabled            BOOLEAN NOT NULL DEFAULT 0,
            notify_time        TEXT NOT NULL DEFAULT '07:55',
            notify_expression  TEXT NOT NULL DEFAULT 'happy',
            mode               TEXT NOT NULL DEFAULT 'check',
            last_checked_at    TEXT,
            last_status        TEXT,
            last_notified_date TEXT,
            created_at         TEXT NOT NULL,
            updated_at         TEXT NOT NULL
        )
    """)
    try:
        _db_conn.execute("ALTER TABLE web_checks ADD COLUMN mode TEXT NOT NULL DEFAULT 'check'")
    except sqlite3.OperationalError:
        pass  # already exists
    _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS device_metrics (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id       TEXT NOT NULL,
            ts_ms           INTEGER NOT NULL,
            heap_free       INTEGER,
            heap_min        INTEGER,
            psram_free      INTEGER,
            stack_speech    INTEGER,
            stack_playback  INTEGER,
            stack_netmon    INTEGER,
            stack_mqtttask  INTEGER,
            received_at     TEXT NOT NULL
        )
    """)
    _db_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_device_metrics_device_ts"
        " ON device_metrics(device_id, ts_ms)"
    )
    _now_iso = datetime.now(_JST).isoformat()
    for _seed in [
        ("リニア体験乗車",
         "https://travel.jr-central.co.jp/plan/linear/",
         "超電導リニア体験乗車の申し込みが現在受付中かどうかを判定してください。"),
        ("関東ITS健保 健保大会",
         "",
         "健保大会の参加申し込みが現在受付中かどうかを判定してください。"),
    ]:
        _db_conn.execute(
            "INSERT OR IGNORE INTO web_checks "
            "(name, url, check_prompt, enabled, notify_time, notify_expression, created_at, updated_at) "
            "VALUES (?, ?, ?, 0, '07:55', 'happy', ?, ?)",
            (_seed[0], _seed[1], _seed[2], _now_iso, _now_iso),
        )
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


def _get_display_tz() -> timezone | ZoneInfo:
    """設置場所から保存されたタイムゾーンを返す。未設定なら JST。"""
    tz_name = _get_setting("location_timezone", "Asia/Tokyo")
    try:
        return ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, Exception):
        return _JST


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


def _get_session_data(session_key: str) -> "_SessionData":
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
    # Lazy lookup of _http_client from main to avoid circular import
    main_mod = sys.modules.get("main")
    _http_client = getattr(main_mod, "_http_client", None) if main_mod else None
    if _http_client is None:
        logger.warning("_summarize_and_reset_session: _http_client not available")
        return

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
