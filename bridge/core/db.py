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
    for col_def in [
        "ALTER TABLE earthquake_log ADD COLUMN lat   REAL",
        "ALTER TABLE earthquake_log ADD COLUMN lon   REAL",
        "ALTER TABLE earthquake_log ADD COLUMN depth REAL",
    ]:
        try:
            _db_conn.execute(col_def)
        except sqlite3.OperationalError:
            pass  # already exists
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
    _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS ingest_audio_metrics (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id        TEXT,
            ts_ms             INTEGER NOT NULL,
            mode              TEXT,
            transcript_chars  INTEGER,
            reply_chars       INTEGER,
            stt_ms            INTEGER,
            llm_ms            INTEGER,
            voicevox_ms       INTEGER,
            mqtt_ms           INTEGER,
            total_ms          INTEGER,
            created_at        TEXT NOT NULL
        )
    """)
    _db_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ingest_audio_metrics_ts ON ingest_audio_metrics(ts_ms)"
    )
    # stt_ms は STT と話者認証の並列区間 max(STT, 話者認証) を記録しているため、
    # どちらが律速かを切り分けられるよう内訳を別カラムで持つ。
    for col_def in [
        "ALTER TABLE ingest_audio_metrics ADD COLUMN stt_only_ms    INTEGER",
        "ALTER TABLE ingest_audio_metrics ADD COLUMN speaker_id_ms  INTEGER",
        "ALTER TABLE ingest_audio_metrics ADD COLUMN upload_ms      INTEGER",
        "ALTER TABLE ingest_audio_metrics ADD COLUMN audio_bytes    INTEGER",
        "ALTER TABLE ingest_audio_metrics ADD COLUMN transport      TEXT",
    ]:
        try:
            _db_conn.execute(col_def)
        except sqlite3.OperationalError:
            pass  # already exists
    # 会話の生ログ。llm_sessions.summary は要約の要約で細部が失われるため、
    # 後から記憶を組み立て直せるよう発話そのものを残す。
    _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS conversations (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            session_key TEXT,
            speaker     TEXT,
            user_text   TEXT NOT NULL,
            reply_text  TEXT,
            source      TEXT,
            created_at  TEXT NOT NULL
        )
    """)
    _db_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_conversations_created ON conversations(created_at)"
    )
    _db_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_conversations_speaker ON conversations(speaker, created_at)"
    )
    _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS location_history (
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            lat                    REAL NOT NULL,
            lon                    REAL NOT NULL,
            title                  TEXT,
            pref                   TEXT,
            source                 TEXT,
            distance_from_home_km  REAL,
            is_away                INTEGER,
            created_at             TEXT NOT NULL
        )
    """)
    _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS trips (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            title           TEXT,
            started_at      TEXT NOT NULL,
            ended_at        TEXT,
            max_distance_km REAL,
            updated_at      TEXT NOT NULL
        )
    """)
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


_CONVERSATION_MAX_ROWS = 50000


def _save_conversation(
    *,
    session_key: str,
    speaker: str | None,
    user_text: str,
    reply_text: str,
    source: str,
) -> None:
    """会話 1 往復を生のまま保存する（要約前の記録を残すのが目的）。

    保存に失敗しても会話自体は成立させたいので、例外は握りつぶして warning に留める。
    """
    if not user_text:
        return
    try:
        with _db_lock:
            _db_conn.execute(  # type: ignore[union-attr]
                "INSERT INTO conversations"
                " (session_key, speaker, user_text, reply_text, source, created_at)"
                " VALUES (?,?,?,?,?,?)",
                (session_key, speaker, user_text, reply_text, source,
                 datetime.now(_JST).isoformat()),
            )
            _db_conn.execute(  # type: ignore[union-attr]
                "DELETE FROM conversations WHERE id NOT IN"
                " (SELECT id FROM conversations ORDER BY id DESC LIMIT ?)",
                (_CONVERSATION_MAX_ROWS,),
            )
            _db_conn.commit()  # type: ignore[union-attr]
    except Exception as e:
        logger.warning("conversation save failed (non-fatal): %s", e)


def _fetch_conversations(
    *, since: str | None = None, speaker: str | None = None, limit: int = 500,
) -> list[dict]:
    """会話ログを新しい順に取得する。夜間バッチや UI からの参照用。"""
    sql = ("SELECT id, session_key, speaker, user_text, reply_text, source, created_at"
           " FROM conversations WHERE 1=1")
    params: list = []
    if since:
        sql += " AND created_at >= ?"
        params.append(since)
    if speaker:
        sql += " AND speaker = ?"
        params.append(speaker)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    with _db_lock:
        rows = _db_conn.execute(sql, params).fetchall()  # type: ignore[union-attr]
    return [
        {"id": r[0], "session_key": r[1], "speaker": r[2], "user_text": r[3],
         "reply_text": r[4], "source": r[5], "created_at": r[6]}
        for r in rows
    ]


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


def _filter_messages_for_speaker(messages: list[dict], speaker: str | None) -> list[dict]:
    """宛先未設定（全員向け）の伝言と、話者名が recipient と一致する伝言だけを残す。

    話者が特定できていない場合（speaker=None）は、宛先付きの伝言を誤って
    無関係な人に届けないよう除外する。
    """
    def _matches(msg: dict) -> bool:
        recipient = (msg.get("recipient") or "").strip()
        if not recipient:
            return True
        return bool(speaker) and recipient == speaker.strip()
    return [m for m in messages if _matches(m)]


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


def _save_ingest_metrics(
    *,
    request_id: str,
    mode: str,
    transcript_chars: int,
    reply_chars: int,
    stt_ms: int,
    llm_ms: int,
    voicevox_ms: int,
    mqtt_ms: int | None,
    total_ms: int,
    stt_only_ms: int | None = None,
    speaker_id_ms: int | None = None,
    upload_ms: int | None = None,
    audio_bytes: int | None = None,
    transport: str | None = None,
) -> None:
    """/ingest-audio の各ステージ所要時間を記録する（直近 5000 件を保持）。"""
    now = datetime.now(_JST)
    with _db_lock:
        _db_conn.execute(  # type: ignore[union-attr]
            "INSERT INTO ingest_audio_metrics"
            " (request_id, ts_ms, mode, transcript_chars, reply_chars,"
            "  stt_ms, llm_ms, voicevox_ms, mqtt_ms, total_ms, created_at,"
            "  stt_only_ms, speaker_id_ms, upload_ms, audio_bytes, transport)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                request_id, int(now.timestamp() * 1000), mode,
                transcript_chars, reply_chars,
                stt_ms, llm_ms, voicevox_ms, mqtt_ms, total_ms,
                now.isoformat(),
                stt_only_ms, speaker_id_ms, upload_ms, audio_bytes, transport,
            ),
        )
        _db_conn.execute(
            "DELETE FROM ingest_audio_metrics WHERE id NOT IN"
            " (SELECT id FROM ingest_audio_metrics ORDER BY id DESC LIMIT 5000)"
        )
        _db_conn.commit()  # type: ignore[union-attr]


def _fetch_ingest_metrics(hours: int) -> list[dict]:
    cutoff_ms = int((datetime.now(_JST) - timedelta(hours=hours)).timestamp() * 1000)
    with _db_lock:
        rows = _db_conn.execute(  # type: ignore[union-attr]
            "SELECT ts_ms, mode, transcript_chars, reply_chars,"
            "       stt_ms, llm_ms, voicevox_ms, mqtt_ms, total_ms,"
            "       stt_only_ms, speaker_id_ms, upload_ms, audio_bytes, transport"
            " FROM ingest_audio_metrics WHERE ts_ms >= ? ORDER BY ts_ms ASC",
            (cutoff_ms,),
        ).fetchall()
    return [
        {
            "ts_ms": r[0], "mode": r[1],
            "transcript_chars": r[2], "reply_chars": r[3],
            "stt_ms": r[4], "llm_ms": r[5], "voicevox_ms": r[6],
            "mqtt_ms": r[7], "total_ms": r[8],
            "stt_only_ms": r[9], "speaker_id_ms": r[10],
            "upload_ms": r[11], "audio_bytes": r[12], "transport": r[13],
        }
        for r in rows
    ]


def _save_location_history(
    *,
    lat: float,
    lon: float,
    title: str,
    pref: str,
    source: str,
    distance_from_home_km: float | None,
    is_away: bool | None,
) -> int:
    now = datetime.now(_JST).isoformat()
    with _db_lock:
        cur = _db_conn.execute(  # type: ignore[union-attr]
            "INSERT INTO location_history"
            " (lat, lon, title, pref, source, distance_from_home_km, is_away, created_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (
                lat, lon, title, pref, source,
                distance_from_home_km,
                None if is_away is None else int(is_away),
                now,
            ),
        )
        _db_conn.commit()  # type: ignore[union-attr]
        return cur.lastrowid  # type: ignore[return-value]


def _fetch_location_history(limit: int = 200) -> list[dict]:
    with _db_lock:
        rows = _db_conn.execute(  # type: ignore[union-attr]
            "SELECT id, lat, lon, title, pref, source, distance_from_home_km, is_away, created_at"
            " FROM location_history ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [
        {
            "id": r[0], "lat": r[1], "lon": r[2], "title": r[3], "pref": r[4],
            "source": r[5], "distance_from_home_km": r[6],
            "is_away": None if r[7] is None else bool(r[7]),
            "created_at": r[8],
        }
        for r in rows
    ]


def _get_active_trip() -> dict | None:
    """終了していない（ended_at IS NULL）旅行があれば返す。"""
    with _db_lock:
        row = _db_conn.execute(  # type: ignore[union-attr]
            "SELECT id, title, started_at, ended_at, max_distance_km, updated_at"
            " FROM trips WHERE ended_at IS NULL ORDER BY id DESC LIMIT 1",
        ).fetchone()
    if not row:
        return None
    return {
        "id": row[0], "title": row[1], "started_at": row[2],
        "ended_at": row[3], "max_distance_km": row[4], "updated_at": row[5],
    }


def _start_trip(title: str, max_distance_km: float) -> int:
    now = datetime.now(_JST).isoformat()
    with _db_lock:
        cur = _db_conn.execute(  # type: ignore[union-attr]
            "INSERT INTO trips (title, started_at, ended_at, max_distance_km, updated_at)"
            " VALUES (?, ?, NULL, ?, ?)",
            (title, now, max_distance_km, now),
        )
        _db_conn.commit()  # type: ignore[union-attr]
        return cur.lastrowid  # type: ignore[return-value]


def _update_trip_progress(trip_id: int, max_distance_km: float) -> None:
    now = datetime.now(_JST).isoformat()
    with _db_lock:
        _db_conn.execute(  # type: ignore[union-attr]
            "UPDATE trips SET max_distance_km = MAX(max_distance_km, ?), updated_at = ? WHERE id = ?",
            (max_distance_km, now, trip_id),
        )
        _db_conn.commit()  # type: ignore[union-attr]


def _end_trip(trip_id: int) -> None:
    now = datetime.now(_JST).isoformat()
    with _db_lock:
        _db_conn.execute(  # type: ignore[union-attr]
            "UPDATE trips SET ended_at = ?, updated_at = ? WHERE id = ?",
            (now, now, trip_id),
        )
        _db_conn.commit()  # type: ignore[union-attr]


def _fetch_trips(limit: int = 50) -> list[dict]:
    with _db_lock:
        rows = _db_conn.execute(  # type: ignore[union-attr]
            "SELECT id, title, started_at, ended_at, max_distance_km, updated_at"
            " FROM trips ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [
        {
            "id": r[0], "title": r[1], "started_at": r[2],
            "ended_at": r[3], "max_distance_km": r[4], "updated_at": r[5],
        }
        for r in rows
    ]
