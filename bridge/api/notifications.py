"""Notifications and messages endpoints."""
from datetime import datetime

from fastapi import APIRouter, HTTPException

from bridge.config import _JST
from bridge.core.db import _db_lock, _db_conn
from bridge.features.calendar_notify import _fire_calendar_notification

router = APIRouter()


@router.get("/api/notifications")
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


@router.post("/api/notifications/{event_id}/resend")
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


@router.delete("/api/notifications/{event_id}/log")
def api_notification_clear(event_id: str):
    """通知済みフラグを削除する（次の通知ループで再送される）。"""
    with _db_lock:
        _db_conn.execute(  # type: ignore[union-attr]
            "DELETE FROM notification_log WHERE event_id = ?", (event_id,)
        )
        _db_conn.commit()  # type: ignore[union-attr]
    return {"ok": True, "event_id": event_id}


@router.get("/api/messages")
def api_list_messages(status: str = "all"):
    where = "" if status == "all" else ("WHERE delivered_at IS NULL" if status == "pending" else "WHERE delivered_at IS NOT NULL")
    with _db_lock:
        rows = _db_conn.execute(  # type: ignore[union-attr]
            f"SELECT id, sender, sender_slack_id, recipient, content, created_at, delivered_at FROM messages {where} ORDER BY created_at DESC LIMIT 100"
        ).fetchall()
    return [{"id": r[0], "sender": r[1], "sender_slack_id": r[2], "recipient": r[3],
             "content": r[4], "created_at": r[5], "delivered_at": r[6]} for r in rows]


@router.delete("/api/messages/{message_id}", status_code=204)
def api_delete_message(message_id: int):
    with _db_lock:
        _db_conn.execute("DELETE FROM messages WHERE id=?", (message_id,))  # type: ignore[union-attr]
        _db_conn.commit()
