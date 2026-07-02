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
    import bridge.core.http as _http_mod
    _http_mod._http_client = _http_client  # share client with other modules
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

from bridge.api.ui import router as _ui_router
app.include_router(_ui_router)

from bridge.api.speak import router as _speak_router
app.include_router(_speak_router)

from bridge.api.devices import router as _devices_router
app.include_router(_devices_router)

from bridge.api.notifications import router as _notifications_router
app.include_router(_notifications_router)

from bridge.api.settings import router as _settings_router
app.include_router(_settings_router)

import bridge.core.audio as _audio_mod
from bridge.core.audio import get_audio_url_web, resolve_audio_url


class SpeakRequest(BaseModel):
    text: str
    source: str = "unknown"
    priority: str = "normal"
    request_id: str | None = None


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


import bridge.integrations.stt as _stt_mod
from bridge.integrations.stt import transcribe_audio, identify_speaker

from bridge.integrations.slack import (
    _MENTION_RE, _DURATION_RE,
    _slack_handle_mention, _slack_handle_dm,
    _deliver_pending_messages_after, _notify_message_delivered,
    _record_slack_user_from_body,
    _slack_handle_say, _slack_handle_register, _slack_handle_tell,
    _slack_handle_speak, _parse_duration, _slack_handle_timer, _setup_slack,
)


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
