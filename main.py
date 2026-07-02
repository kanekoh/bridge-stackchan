from bridge.config import *  # noqa: F401,F403

import logging
logger = logging.getLogger(__name__)

from bridge.core.expression import (
    _load_expression_map, _expression_map, _parse_expression,
    _resolve_expression, _STACKCHAN_SYSTEM_PROMPT,
)

_openai_client = openai.AsyncOpenAI(api_key=OPENAI_API_KEY)
_http_client: httpx.AsyncClient = None  # type: ignore  # initialized in lifespan

from bridge.core.errors import _OPENAI_ERROR_REPLIES, _classify_api_error, _deliver_error_reply

from bridge.devices.mqtt import (
    _pending_acks, _main_loop, _mqtt_conn, _MqttConnection,
    publish_speak, wait_for_ack, set_main_loop,
)

from bridge.core.db import (
    _db_lock, _db_conn, _init_db,
    _get_setting, _set_setting, _get_display_tz,
    _get_previous_response_id, _save_response_id,
    _SessionData, _get_session_data, _save_session,
    _summarize_and_reset_session,
    _get_all_family_members, _resolve_display_name,
    _record_slack_user, _save_message,
    _fetch_pending_messages, _mark_message_delivered,
)

from bridge.llm.persona import _build_datetime_context, _build_location_context

from bridge.llm.backends import (
    LLMBackend, OpenClawResponsesBackend, OpenAIResponsesBackend,
    _BACKENDS, chat_with_openclaw, chat_with_openai_responses, chat_with_llm,
)

from bridge.llm.tools import (
    _TIMER_TOOLS, _CALENDAR_TOOLS, _MESSAGE_TOOLS, _ALERT_TOOLS, _WEATHER_TOOLS,
    _REQUEST_WEB_SEARCH_TOOL,
    _tool_get_weather, _tool_get_upcoming_items, _tool_get_recent_alerts,
    _execute_tool, _handle_function_calls,
)

from bridge.features.timers import (
    _TimerInfo, _active_timers, _active_timer_infos,
    _fire_timer, _run_timer, _register_timer,
)

from bridge.features.calendar_notify import (
    _fire_calendar_notification, _check_calendar_notifications, _calendar_notification_loop,
)

from bridge.features.weather.notify import (
    _nowcast_tile_coords, _nowcast_pixel_intensity,
    _fetch_nowcast_rain_data, _fetch_openmeteo_rain_data,
    _load_amedas_station_table, _fetch_amedas_snapshots, _estimate_rain_movement,
    _fetch_amedas_openmeteo_rain_data, _check_rain_notification, _rain_llm_comment,
    _fetch_sky_condition, _weather_notify_loop,
    _fetch_iss_tle, _az_to_direction, _get_sunset_utc, _calc_iss_passes,
    _iss_speak, _iss_notify_loop,
    _iss_tle_cache, _iss_notified_passes,
    _HtmlTextExtractor, _expand_url_template, _run_web_check, _web_check_notify_loop,
)

from bridge.features.quake import (
    _PREF_RE, _extract_pref, _PREF_TSUNAMI_AREAS,
    _apply_tsunami_areas_from_pref, _fetch_and_save_timezone,
    _haversine_km, _get_local_scale,
    _SCALE_MAP, _TSUNAMI_GRADE_ORDER, _TSUNAMI_GRADE_LABEL,
    _scale_to_str, _eq_already_seen, _mark_eq_seen,
    _get_tsunami_grade, _save_tsunami_grade, _clear_tsunami_state,
    _build_earthquake_fixed_text, _p2p_speak,
    _handle_earthquake, _earthquake_llm_comment,
    _handle_tsunami, _tsunami_llm_comment,
    _handle_eew, _unknown_p2p_llm,
    _p2pquake_log_event, _p2pquake_ws_loop,
    _p2pquake_ws_status, _p2pquake_recent_events, _P2PQUAKE_EVENT_BUFFER,
)

# Slack アプリ参照（_setup_slack で設定、タイマー発火時の通知に使用）
_slack_app = None  # type: ignore

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _http_client
    import bridge.devices.mqtt as _mqtt_mod
    _mqtt_mod.set_main_loop(asyncio.get_running_loop())
    _init_db()
    _http_client = httpx.AsyncClient(timeout=60)
    _audio_mod._http_client = _http_client  # share client with audio module
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

    asyncio.create_task(_weather_notify_loop())
    logger.info("Weather notify loop started")

    asyncio.create_task(_iss_notify_loop())  # DB設定で後から有効化可能なため常時起動
    logger.info("ISS notify loop started")

    asyncio.create_task(_web_check_notify_loop())
    logger.info("Web check notify loop started")

    _mqtt_conn.start()
    logger.info("MQTT eager connect started")

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


class SpeakRequest(BaseModel):
    text: str
    source: str = "unknown"
    priority: str = "normal"
    request_id: str | None = None


import bridge.core.audio as _audio_mod
from bridge.core.audio import get_audio_url_web, resolve_audio_url


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


@app.get("/ui/logs", response_class=HTMLResponse)
async def ui_logs(request: Request):
    return _templates.TemplateResponse(request=request, name="logs.html", context=_ui_context(request))


@app.get("/ui/web-checks", response_class=HTMLResponse)
async def ui_web_checks(request: Request):
    return _templates.TemplateResponse(request=request, name="web_checks.html", context=_ui_context(request))


@app.get("/ui/metrics", response_class=HTMLResponse)
async def ui_metrics(request: Request):
    return _templates.TemplateResponse(request=request, name="metrics.html", context=_ui_context(request))


@app.get("/api/device/log")
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


@app.get("/api/device/metrics")
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
            {"value": "0",  "label": "すべて（テスト用）"},
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
    "weather_notify_rain": {
        "label": "天気通知 — 雨降り始め",
        "description": "30分以内に雨が降り始めると予測されたときに通知します。設置場所の位置情報が必要です。",
        "env_fallback": lambda: WEATHER_NOTIFY_RAIN,
        "type": "select",
        "options": [
            {"value": "false", "label": "OFF"},
            {"value": "true",  "label": "ON"},
        ],
    },
    "rain_source": {
        "label": "天気通知 — データソース",
        "description": "雨通知に使うデータソース。AMeDAS+Open-Meteoは公式観測と数値予報の組み合わせで推奨。Open-Meteoのみは軽量だが現況精度が劣る。",
        "env_fallback": lambda: "amedas+openmeteo",
        "type": "select",
        "options": [
            {"value": "amedas+openmeteo", "label": "AMeDAS + Open-Meteo（推奨・公式・10分更新）"},
            {"value": "openmeteo",        "label": "Open-Meteo のみ（15分更新）"},
        ],
    },
    "iss_notify_enabled": {
        "label": "ISS 通過通知",
        "description": (
            "朝7:45-7:55に当日夕方〜夜の通過予告、通過5分前に直前通知を行います。"
            "設置場所の位置情報が必要です。仰角30度以上のパスのみ対象。"
        ),
        "env_fallback": lambda: str(ISS_NOTIFY_ENABLED).lower(),
        "type": "select",
        "options": [
            {"value": "false", "label": "OFF"},
            {"value": "true",  "label": "ON"},
        ],
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
    asyncio.create_task(_fetch_and_save_timezone(lat, lon))

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
    asyncio.create_task(_fetch_and_save_timezone(lat, lon))

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
    asyncio.create_task(_fetch_and_save_timezone(lat, lon))
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


@app.get("/api/debug/p2pquake/status")
def api_p2pquake_status():
    """P2P地震情報 WebSocket の接続状態と直近受信イベントを返す。"""
    return {
        "enabled": P2PQUAKE_ENABLED,
        "ws": dict(_p2pquake_ws_status),
        "recent_events": list(reversed(_p2pquake_recent_events)),  # 新しい順
    }


@app.post("/api/debug/p2pquake")
async def debug_p2pquake(code: int = Query(551), force: bool = Query(False)):
    """
    P2P地震情報の直近データを取得してハンドラに流す（テスト用）。
    code: 551=地震, 552=津波, 554=EEW, 556=南海トラフ, それ以外=unknown LLM
    force=true: dedup をスキップして必ず発話する
    """
    p2p_history_url = f"https://api.p2pquake.net/v2/history?codes={code}&limit=1"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(p2p_history_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    raise HTTPException(status_code=502, detail=f"P2P API returned {resp.status}")
                events = await resp.json()
    except TimeoutError:
        raise HTTPException(status_code=504, detail="P2P history API がタイムアウトしました。しばらく待ってから再試行してください。")

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
    elif code in (554, 556):
        await _handle_eew(data)
    elif code in (555, 561, 9611):
        pass  # ログのみ
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


@app.post("/api/debug/weather/rain-check")
async def api_debug_rain_check():
    """雨検知チェックをその場で実行する（クールダウンリセット後）。"""
    _set_setting("weather_rain_notified", "")
    await _check_rain_notification()
    return {"ok": True, "active_source": _get_setting("rain_source", "nowcast")}


@app.get("/api/debug/rain/status")
async def api_debug_rain_status():
    """全ソースのデータを並列取得して比較返却する。"""
    lat_str = _get_setting("location_lat", "")
    lon_str = _get_setting("location_lon", "")
    if not lat_str or not lon_str:
        return {"ok": False, "detail": "位置情報未設定"}
    lat, lon = float(lat_str), float(lon_str)

    results = await asyncio.gather(
        _fetch_openmeteo_rain_data(lat, lon),
        _fetch_nowcast_rain_data(lat, lon),
        _fetch_amedas_openmeteo_rain_data(lat, lon),
        return_exceptions=True,
    )
    def _safe(r):
        return r if not isinstance(r, Exception) else {"error": str(r)}

    ac = _safe(results[2])

    # クールダウン状態
    cooldown_str = _get_setting("weather_rain_notified", "")
    cooldown_remaining_min = None
    if cooldown_str:
        try:
            elapsed = (datetime.now(_JST) - datetime.fromisoformat(cooldown_str)).total_seconds()
            remaining = 3 * 3600 - elapsed
            cooldown_remaining_min = max(0, round(remaining / 60))
        except ValueError:
            pass

    # 現在の状態から次のアクションを予測
    next_action = "不明"
    if not isinstance(ac, dict) or ac.get("error"):
        next_action = "データ取得エラー"
    else:
        now_dry  = ac.get("now_dry", True)
        soon_wet = ac.get("soon_wet", False)
        amedas_approaching = ac.get("amedas", {}).get("approaching", False)
        if not now_dry:
            if not cooldown_str:
                next_action = "⚠ 予告なし雨を検知 → 「気づいたら雨」通知を送信"
            else:
                next_action = "🌧 降雨中・通知済み（クールダウン維持）"
        elif not soon_wet:
            if cooldown_str and amedas_approaching:
                next_action = "AMeDAS 接近中のためクールダウン保持"
            elif cooldown_str:
                next_action = "乾燥・雨予報なし → クールダウンリセット"
            else:
                next_action = "☀ 乾燥・待機中"
        else:
            if cooldown_remaining_min and cooldown_remaining_min > 0:
                next_action = f"クールダウン中（あと {cooldown_remaining_min} 分）"
            else:
                next_action = "✅ 雨接近 → 通知を送信"

    return {
        "ok": True,
        "active_source": _get_setting("rain_source", "amedas+openmeteo"),
        "target_lat": lat,
        "target_lon": lon,
        "cooldown_notified_at": cooldown_str or None,
        "cooldown_remaining_min": cooldown_remaining_min,
        "next_action": next_action,
        "openmeteo":        _safe(results[0]),
        "nowcast":          _safe(results[1]),
        "amedas+openmeteo": ac,
    }


@app.get("/api/debug/iss")
async def api_debug_iss():
    """現在位置をもとに ISS の次の通過情報と日没時刻を返す（テスト用）。"""
    lat = _get_setting("location_lat", "")
    lon = _get_setting("location_lon", "")
    if not lat or not lon:
        return {"ok": False, "detail": "位置情報未設定"}
    tle = await _fetch_iss_tle()
    if not tle:
        return {"ok": False, "detail": "TLE 取得失敗"}
    tle1, tle2 = tle
    passes = _calc_iss_passes(float(lat), float(lon), tle1, tle2, hours=24, min_el=10.0)
    sunset_utc = _get_sunset_utc(float(lat), float(lon))
    sunset_jst = sunset_utc.astimezone(_JST).strftime("%H:%M") if sunset_utc else None
    return {
        "ok": True,
        "sunset_jst": sunset_jst,
        "min_elevation": ISS_MIN_ELEVATION,
        "passes": [
            {
                "rise_jst":   p["rise_jst"].strftime("%Y-%m-%d %H:%M"),
                "max_jst":    p["max_jst"].strftime("%H:%M"),
                "max_el_deg": round(p["max_el_deg"], 1),
                "direction":  p["direction"],
                "visible":    p["max_el_deg"] >= ISS_MIN_ELEVATION,
            }
            for p in passes[:10]
        ],
    }


@app.post("/api/debug/iss/morning-preview")
async def api_debug_iss_morning_preview():
    """朝の予告通知をその場でテスト送信する。"""
    lat = _get_setting("location_lat", "")
    lon = _get_setting("location_lon", "")
    if not lat or not lon:
        raise HTTPException(400, "位置情報未設定")
    tle = await _fetch_iss_tle()
    if not tle:
        raise HTTPException(503, "TLE 取得失敗")
    tle1, tle2 = tle
    sunset_utc = _get_sunset_utc(float(lat), float(lon))
    all_passes = _calc_iss_passes(float(lat), float(lon), tle1, tle2,
                                   hours=24, min_el=ISS_MIN_ELEVATION)
    if sunset_utc:
        sunset_jst = sunset_utc.astimezone(_JST)
        evening_passes = [p for p in all_passes if p["rise_jst"] >= sunset_jst]
    else:
        sunset_jst = None
        evening_passes = [p for p in all_passes
                          if p["rise_jst"].hour >= 16 or p["rise_jst"].hour < 3]
    if not evening_passes:
        return {"ok": False, "detail": "今夜は見える機会がありません"}
    best = max(evening_passes, key=lambda p: p["max_el_deg"])
    sunset_hint = (
        f"（今日の日没は{sunset_jst.strftime('%H時%M分')}ごろです）"
        if sunset_jst else ""
    )
    prompt = (
        f"今夜{best['rise_jst'].strftime('%H時%M分')}ごろに"
        f"ISSが{best['direction']}の空から見えます。"
        f"最高点は{best['max_jst'].strftime('%H時%M分')}ごろで"
        f"かなり高いところまで上がります（最大{best['max_el_deg']:.0f}度）。"
        f"{sunset_hint}"
        "家族に「今夜ISSが見えるよ」と朝のうちに予告してください。"
        "日没時刻も自然に添えてください。「仰角」は使わず、わかりやすく。1〜2文で。"
    )
    await _iss_speak(prompt, source="iss_morning_preview_test")
    return {"ok": True, "pass": {
        "rise_jst": best["rise_jst"].strftime("%H:%M"),
        "max_el_deg": round(best["max_el_deg"], 1),
        "direction": best["direction"],
    }}


@app.post("/api/debug/iss/immediate")
async def api_debug_iss_immediate():
    """次の通過の直前通知をその場でテスト送信する。"""
    lat = _get_setting("location_lat", "")
    lon = _get_setting("location_lon", "")
    if not lat or not lon:
        raise HTTPException(400, "位置情報未設定")
    tle = await _fetch_iss_tle()
    if not tle:
        raise HTTPException(503, "TLE 取得失敗")
    tle1, tle2 = tle
    passes = _calc_iss_passes(float(lat), float(lon), tle1, tle2,
                               hours=24, min_el=ISS_MIN_ELEVATION)
    if not passes:
        return {"ok": False, "detail": "24時間以内に見えるパスがありません"}
    p = passes[0]
    now_utc = datetime.now(timezone.utc)
    secs_until = (p["rise_jst"].astimezone(timezone.utc) - now_utc).total_seconds()
    minutes_until = max(1, round(secs_until / 60))
    prompt = (
        f"ISSが約{minutes_until}分後に{p['direction']}の空から見えはじめます。"
        f"{p['max_jst'].strftime('%H時%M分')}ごろが一番高くなります"
        f"（空の高さ{p['max_el_deg']:.0f}度相当）。"
        "家族に「もうすぐISSが来るよ、空を見てみて！」と短く伝えてください。"
        "「仰角」は使わず、わかりやすく。1〜2文で。"
    )
    await _iss_speak(prompt, source="iss_notify_test")
    return {"ok": True, "pass": {
        "rise_jst": p["rise_jst"].strftime("%H:%M"),
        "minutes_until": minutes_until,
        "max_el_deg": round(p["max_el_deg"], 1),
        "direction": p["direction"],
    }}


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


# ── 申し込み受付確認（Web チェック）──────────────────────────────────────────

class WebCheckCreate(BaseModel):
    name: str
    url: str = ""
    check_prompt: str = "申し込みが現在受付中かどうかを判定してください。"
    mode: str = "check"
    enabled: bool = True
    notify_time: str = "07:55"
    notify_expression: str = "happy"


class WebCheckUpdate(BaseModel):
    name: str | None = None
    url: str | None = None
    check_prompt: str | None = None
    mode: str | None = None
    enabled: bool | None = None
    notify_time: str | None = None
    notify_expression: str | None = None


@app.get("/api/web-checks")
def list_web_checks():
    with _db_lock:
        rows = _db_conn.execute(
            "SELECT id, name, url, check_prompt, enabled, notify_time, notify_expression, mode, "
            "last_checked_at, last_status, last_notified_date, created_at, updated_at "
            "FROM web_checks ORDER BY id"
        ).fetchall()
    return {
        "items": [
            {
                "id": r[0], "name": r[1], "url": r[2], "check_prompt": r[3],
                "enabled": bool(r[4]), "notify_time": r[5], "notify_expression": r[6],
                "mode": r[7] or "check",
                "last_checked_at": r[8], "last_status": r[9],
                "last_notified_date": r[10], "created_at": r[11], "updated_at": r[12],
            }
            for r in rows
        ]
    }


@app.post("/api/web-checks", status_code=201)
def create_web_check(req: WebCheckCreate):
    now = datetime.now(_JST).isoformat()
    try:
        with _db_lock:
            cursor = _db_conn.execute(
                "INSERT INTO web_checks "
                "(name, url, check_prompt, mode, enabled, notify_time, notify_expression, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (req.name, req.url, req.check_prompt, req.mode, int(req.enabled),
                 req.notify_time, req.notify_expression, now, now),
            )
            _db_conn.commit()
            row_id = cursor.lastrowid
    except Exception as e:
        if "UNIQUE" in str(e):
            raise HTTPException(status_code=409, detail=f"「{req.name}」はすでに登録されています")
        raise HTTPException(status_code=500, detail=str(e))
    return {"id": row_id, "name": req.name}


@app.put("/api/web-checks/{wc_id}")
def update_web_check(wc_id: int, req: WebCheckUpdate):
    now = datetime.now(_JST).isoformat()
    fields: list[str] = []
    vals: list = []
    if req.name is not None:
        fields.append("name = ?"); vals.append(req.name)
    if req.url is not None:
        fields.append("url = ?"); vals.append(req.url)
    if req.check_prompt is not None:
        fields.append("check_prompt = ?"); vals.append(req.check_prompt)
    if req.mode is not None:
        fields.append("mode = ?"); vals.append(req.mode)
    if req.enabled is not None:
        fields.append("enabled = ?"); vals.append(int(req.enabled))
    if req.notify_time is not None:
        fields.append("notify_time = ?"); vals.append(req.notify_time)
    if req.notify_expression is not None:
        fields.append("notify_expression = ?"); vals.append(req.notify_expression)
    if not fields:
        raise HTTPException(status_code=422, detail="更新するフィールドがありません")
    fields.append("updated_at = ?"); vals.append(now)
    vals.append(wc_id)
    with _db_lock:
        c = _db_conn.execute(
            f"UPDATE web_checks SET {', '.join(fields)} WHERE id = ?", vals
        )
        _db_conn.commit()
    if c.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"id={wc_id} は見つかりません")
    return {"updated": wc_id}


@app.delete("/api/web-checks/{wc_id}", status_code=204)
def delete_web_check(wc_id: int):
    with _db_lock:
        c = _db_conn.execute("DELETE FROM web_checks WHERE id = ?", (wc_id,))
        _db_conn.commit()
    if c.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"id={wc_id} は見つかりません")


@app.post("/api/web-checks/{wc_id}/run")
async def run_web_check_now(wc_id: int):
    """手動で今すぐチェックを実行する（read モードは実際に読み上げも行う）。"""
    with _db_lock:
        row = _db_conn.execute(
            "SELECT id, name, url, check_prompt, mode, notify_expression FROM web_checks WHERE id = ?", (wc_id,)
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"id={wc_id} は見つかりません")
    _, name, url, check_prompt, mode, notify_expression = row
    if not url:
        raise HTTPException(status_code=422, detail="URL が未設定です")
    now_jst = datetime.now(_JST)
    result = await _run_web_check(
        wc_id, name, url, check_prompt, now_jst, today_str=None,
        mode=mode or "check", notify_expression=notify_expression or "happy",
    )
    return {"name": name, **result}


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


import bridge.integrations.stt as _stt_mod
from bridge.integrations.stt import transcribe_audio, identify_speaker


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
