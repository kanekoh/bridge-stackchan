"""Device log, metrics, family members, and slack-seen-users endpoints."""
import logging
import re
import sqlite3
from datetime import datetime
from typing import Literal

import httpx
from fastapi import APIRouter, Form, HTTPException, Query
from pydantic import BaseModel, Field

from bridge.config import DEVICE_HTTP_TIMEOUT, DEVICE_HTTP_URL, MQTT_DEVICE_ID, _JST
import bridge.core.db as _db_mod
from bridge.core.db import _db_lock, _get_setting, _get_display_tz, _get_all_family_members
from bridge.devices.mqtt import get_device_state, publish_device_set

logger = logging.getLogger(__name__)

router = APIRouter()

_TIME_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")


class DeviceSettingsUpdate(BaseModel):
    brightness: int | None = Field(default=None, ge=0, le=100)
    volume: int | None = Field(default=None, ge=0, le=100)
    speakerId: int | None = None
    greeting: str | None = None
    sleepStart: str | None = None
    sleepEnd: str | None = None
    restart: bool | None = None
    servoTest: Literal["x", "y", False] | None = None
    logLevel: Literal["error", "warn", "info", "debug", "trace"] | None = None
    wakeWord: bool | None = None
    wakeWordRegister: bool | None = None
    wakeWordThreshold: int | None = Field(default=None, ge=10, le=2000)


@router.get("/api/device/log")
def api_device_log(limit: int = Query(default=200, le=500)):
    """スタックちゃんから受信したログを返す。ts_ms を表示用文字列に変換して返す。"""
    tz = _get_display_tz()
    with _db_lock:
        rows = _db_mod._db_conn.execute(
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


@router.get("/api/device/state")
def api_device_state():
    """デバイスから直近30秒間隔で受信した device/state を返す。"""
    state = get_device_state(MQTT_DEVICE_ID)
    if state is None:
        return {"available": False, "device_id": MQTT_DEVICE_ID}
    return {"available": True, "device_id": MQTT_DEVICE_ID, **state}


@router.post("/api/device/settings")
def api_device_settings(req: DeviceSettingsUpdate):
    """stackchan/{device}/device/set へ設定変更を publish する。未指定フィールドは送らない。"""
    for field_name in ("sleepStart", "sleepEnd"):
        value = getattr(req, field_name)
        if value is not None and not _TIME_RE.match(value):
            raise HTTPException(status_code=422, detail=f"{field_name} は HH:MM 形式で指定してください")

    greeting = req.greeting if req.greeting else None  # 空文字は「変更なし」
    restart = True if req.restart else None  # False/未指定は送らない

    sent = {
        "wakeWord": req.wakeWord,
        "wakeWordRegister": True if req.wakeWordRegister else None,  # False は送らない
        "wakeWordThreshold": req.wakeWordThreshold,
        "brightness": req.brightness,
        "volume": req.volume,
        "speakerId": req.speakerId,
        "greeting": greeting,
        "sleepStart": req.sleepStart,
        "sleepEnd": req.sleepEnd,
        "restart": restart,
        "servoTest": req.servoTest,  # "x"/"y"/False いずれも明示的に送る（restart と違い停止も必要なため）
        "logLevel": req.logLevel,
    }
    sent = {k: v for k, v in sent.items() if v is not None}
    if not sent:
        raise HTTPException(status_code=422, detail="送信する設定がありません")

    publish_device_set(MQTT_DEVICE_ID, **sent)
    return {"ok": True, "sent": sent}


# ── デバイス本体への直接 HTTP ────────────────────────────────────────────────
# MQTT の device/state は30秒間隔なので、ウェイクワードのしきい値調整のように
# 「話すたびに動く値」を見るには遅すぎる。調整中だけデバイスの HTTP を直接読む。
# ブラウザから直に叩くと、UI が HTTPS のときに混在コンテンツで止められるため、
# Bridge が代わりに取りに行く。

def _device_http_base() -> str:
    """デバイスの HTTP アドレス。UI（app_settings）が環境変数より優先。"""
    base = (_get_setting("device_http_url", "") or DEVICE_HTTP_URL).strip().rstrip("/")
    if base and not base.startswith("http"):
        base = "http://" + base
    return base


@router.get("/api/device/live")
async def api_device_live():
    """デバイスの GET /device をそのまま返す（しきい値調整用の短間隔ポーリング）。

    アドレス未設定や届かないときも 200 で available=false を返す。
    ポーリングのたびにエラーダイアログが出ると調整の邪魔になるため。
    """
    base = _device_http_base()
    if not base:
        return {"available": False, "reason": "デバイスの HTTP アドレスが未設定です"}
    try:
        async with httpx.AsyncClient(timeout=DEVICE_HTTP_TIMEOUT) as client:
            res = await client.get(f"{base}/device")
            res.raise_for_status()
            return {"available": True, "url": base, "state": res.json()}
    except Exception as e:
        logger.debug("device live fetch failed: %s", e)
        return {"available": False, "url": base, "reason": f"{type(e).__name__}: {e}"}


@router.post("/api/device/wakeword")
async def api_device_wakeword(req: DeviceSettingsUpdate):
    """ウェイクワードの設定を送る。デバイスの HTTP を優先し、駄目なら MQTT に回す。

    登録ボタンやしきい値は、押してすぐ結果を見たい操作なので、
    30秒間隔の device/set より速い HTTP を先に試す。
    """
    sent = {
        "wakeWord": req.wakeWord,
        "wakeWordRegister": True if req.wakeWordRegister else None,
        "wakeWordThreshold": req.wakeWordThreshold,
    }
    sent = {k: v for k, v in sent.items() if v is not None}
    if not sent:
        raise HTTPException(status_code=422, detail="送信する設定がありません")

    base = _device_http_base()
    if base:
        try:
            async with httpx.AsyncClient(timeout=DEVICE_HTTP_TIMEOUT) as client:
                res = await client.post(f"{base}/device", json=sent)
                res.raise_for_status()
            return {"ok": True, "sent": sent, "transport": "http"}
        except Exception as e:
            logger.info("device HTTP 経由に失敗、MQTT に切り替えます: %s", e)

    publish_device_set(MQTT_DEVICE_ID, **sent)
    return {"ok": True, "sent": sent, "transport": "mqtt"}


@router.get("/api/device/metrics")
def api_device_metrics(hours: int = Query(default=2, le=24)):
    """スタックちゃんから受信したメトリクス履歴を返す（最大 hours 時間分）。"""
    limit = hours * 60  # 60秒ごとなので hours*60 件が上限
    with _db_lock:
        rows = _db_mod._db_conn.execute(
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
            cur = _db_mod._db_conn.execute(  # type: ignore[union-attr]
                "INSERT INTO family_members (name, slack_user_id, mac_address, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                (name.strip(), slack_user_id.strip() or None, mac_address.strip() or None, now, now),
            )
            _db_mod._db_conn.commit()
        return {"id": cur.lastrowid, "name": name}
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail=f"名前 '{name}' はすでに登録されています")


@router.put("/api/family-members/{member_id}")
def api_update_member(member_id: int, name: str = Form(...), slack_user_id: str = Form(""), mac_address: str = Form("")):
    now = datetime.now(_JST).isoformat()
    with _db_lock:
        cur = _db_mod._db_conn.execute(  # type: ignore[union-attr]
            "UPDATE family_members SET name=?, slack_user_id=?, mac_address=?, updated_at=? WHERE id=?",
            (name.strip(), slack_user_id.strip() or None, mac_address.strip() or None, now, member_id),
        )
        _db_mod._db_conn.commit()
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="メンバーが見つかりません")
    return {"id": member_id, "name": name}


@router.get("/api/slack-seen-users")
def api_slack_seen_users():
    """family_members に未登録の Slack ユーザー一覧を返す。"""
    with _db_lock:
        rows = _db_mod._db_conn.execute(  # type: ignore[union-attr]
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
        cur = _db_mod._db_conn.execute("DELETE FROM family_members WHERE id=?", (member_id,))  # type: ignore[union-attr]
        _db_mod._db_conn.commit()
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="メンバーが見つかりません")
