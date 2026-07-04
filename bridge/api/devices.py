"""Device log, metrics, family members, and slack-seen-users endpoints."""
import sqlite3
from datetime import datetime

from fastapi import APIRouter, Form, HTTPException, Query

from bridge.config import MQTT_DEVICE_ID, _JST
from bridge.core.db import _db_lock, _db_conn, _get_setting, _get_display_tz, _get_all_family_members

router = APIRouter()


@router.get("/api/device/log")
def api_device_log(limit: int = Query(default=200, le=500)):
    """スタックちゃんから受信したログを返す。ts_ms を表示用文字列に変換して返す。"""
    tz = _get_display_tz()
    with _db_lock:
        rows = _db_conn.execute(
            "SELECT device_id, level, ts_ms, msg, raw_json, received_at"
            " FROM device_log ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    entries = []
    for device_id, level, ts_ms, msg, raw_json, received_at in rows:
        if ts_ms is not None:
            ts_dt = datetime.fromtimestamp(ts_ms / 1000, tz=tz)
            ts_str = ts_dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            ts_str = received_at[:19].replace("T", " ")
        entries.append({
            "device_id":   device_id,
            "level":       level or "",
            "ts_str":      ts_str,
            "msg":         msg or "",
            "received_at": received_at[:19].replace("T", " "),
        })
    return {"device_id": MQTT_DEVICE_ID, "timezone": _get_setting("location_timezone", "Asia/Tokyo"), "logs": entries}


@router.get("/api/device/metrics")
def api_device_metrics(hours: int = Query(default=2, le=24)):
    """スタックちゃんから受信したメトリクス履歴を返す（最大 hours 時間分）。"""
    limit = hours * 60  # 60秒ごとなので hours*60 件が上限
    with _db_lock:
        rows = _db_conn.execute(
            "SELECT ts_ms, heap_free, heap_min, psram_free,"
            "       stack_speech, stack_playback, stack_netmon, stack_mqtttask"
            " FROM (SELECT * FROM device_metrics WHERE device_id=?"
            "       ORDER BY ts_ms DESC LIMIT ?)"
            " ORDER BY ts_ms ASC",
            (MQTT_DEVICE_ID, limit),
        ).fetchall()
    return {
        "device_id": MQTT_DEVICE_ID,
        "points": [
            {
                "ts_ms":          r[0],
                "heap_free":      r[1],
                "heap_min":       r[2],
                "psram_free":     r[3],
                "stack_speech":   r[4],
                "stack_playback": r[5],
                "stack_netmon":   r[6],
                "stack_mqtttask": r[7],
            }
            for r in rows
        ],
    }


@router.get("/api/family-members")
def api_list_members():
    return _get_all_family_members()


@router.post("/api/family-members", status_code=201)
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


@router.put("/api/family-members/{member_id}")
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


@router.get("/api/slack-seen-users")
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


@router.delete("/api/family-members/{member_id}", status_code=204)
def api_delete_member(member_id: int):
    with _db_lock:
        cur = _db_conn.execute("DELETE FROM family_members WHERE id=?", (member_id,))  # type: ignore[union-attr]
        _db_conn.commit()
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="メンバーが見つかりません")
