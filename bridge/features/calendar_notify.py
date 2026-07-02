"""Calendar notification loop for Stack-chan.

publish_speak, resolve_audio_url, chat_with_llm, and DB functions are
accessed lazily via sys.modules["main"] to avoid circular imports.
"""
import asyncio
import logging
import sys
import uuid
from datetime import datetime, timedelta

from bridge.config import (
    CALENDAR_NOTIFY_CHECK_INTERVAL, CALENDAR_NOTIFY_GRACE_MINUTES, _JST,
)

logger = logging.getLogger(__name__)


async def _fire_calendar_notification(item: dict) -> None:
    _main = sys.modules["main"]
    chat_with_llm = _main.chat_with_llm
    _parse_expression = _main._parse_expression
    _resolve_expression = _main._resolve_expression
    resolve_audio_url = _main.resolve_audio_url
    publish_speak = _main.publish_speak

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
    _main = sys.modules["main"]
    _db_lock = _main._db_lock
    _db_conn = _main._db_conn

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
