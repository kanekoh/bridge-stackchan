"""Debug and health endpoints."""
import asyncio
import json
import socket
import statistics
import uuid
from datetime import datetime, timezone

import aiohttp
from fastapi import APIRouter, HTTPException, Query

from bridge.config import (
    _JST,
    OPENCLAW_BASE_URL, OPENCLAW_MODEL, SPEAKER_ID_URL,
    MQTT_BROKER, MQTT_PORT, VOICEVOX_URL,
    P2PQUAKE_ENABLED, P2PQUAKE_MIN_SCALE, P2PQUAKE_TSUNAMI_TARGET_AREAS,
    ISS_MIN_ELEVATION, SESSION_SUMMARY_THRESHOLD, LLM_BACKEND, MQTT_DEVICE_ID,
)
import bridge.core.db as _db_mod
from bridge.core.db import _db_lock, _get_setting, _set_setting, _fetch_ingest_metrics, _fetch_conversations
from bridge.core.expression import _parse_expression, _resolve_expression
from bridge.core.audio import resolve_audio_url
from bridge.devices.mqtt import publish_speak
from bridge.features.timers import _active_timer_infos
from bridge.features.quake import (
    _p2pquake_ws_status, _p2pquake_recent_events,
    _handle_earthquake, _handle_tsunami, _handle_eew, _unknown_p2p_llm,
)
from bridge.features.weather.notify import (
    _fetch_openmeteo_rain_data, _fetch_nowcast_rain_data, _fetch_amedas_openmeteo_rain_data,
    _check_rain_notification,
    _fetch_iss_tle, _get_sunset_utc, _calc_iss_passes, _iss_speak,
)
from bridge.llm.backends import chat_with_llm
import bridge.core.http as _http_mod

router = APIRouter()


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


def _tcp_check(host: str, port: int, timeout: float = 3.0) -> str:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return "ok"
    except Exception as e:
        return f"{type(e).__name__}: {e}"


@router.get("/healthz")
def healthz():
    return {"status": "ok"}


@router.get("/debug/sessions")
def debug_sessions():
    """llm_sessions テーブルの全レコードを返す（デバッグ用）。"""
    with _db_lock:
        rows = _db_mod._db_conn.execute(  # type: ignore[union-attr]
            "SELECT session_key, backend, response_id, metadata, updated_at,"
            "       char_count_in, char_count_out, summary"
            " FROM llm_sessions ORDER BY updated_at DESC"
        ).fetchall()
    sessions = [
        {
            "session_key": r[0],
            "backend": r[1],
            "response_id": r[2],
            "metadata": json.loads(r[3]) if r[3] else {},
            "updated_at": r[4],
            "char_count_in": r[5] or 0,
            "char_count_out": r[6] or 0,
            "summary": r[7],
        }
        for r in rows
    ]
    return {"sessions": sessions, "summary_threshold": SESSION_SUMMARY_THRESHOLD}


# 文字数（transcript + reply）によるカテゴリ分け。実トークン数ではなく文字数を代理指標として使う。
_INGEST_CHAR_BUCKETS = [
    ("short（〜60字）", 0, 60),
    ("medium（60〜200字）", 60, 200),
    ("long（200字〜）", 200, None),
]
_INGEST_STAGES = ["stt_ms", "llm_ms", "voicevox_ms", "mqtt_ms", "total_ms"]


@router.get("/api/debug/ingest-metrics")
def api_debug_ingest_metrics(hours: int = Query(default=2, le=168)):
    """/ingest-audio の各ステージ所要時間を時系列で返す（積み上げグラフ用）。"""
    return {"hours": hours, "points": _fetch_ingest_metrics(hours)}


@router.get("/api/debug/ingest-metrics/stats")
def api_debug_ingest_metrics_stats(hours: int = Query(default=24, le=168)):
    """文字数カテゴリごとに、各ステージの最小・最大・平均・中央値を返す。"""
    points = _fetch_ingest_metrics(hours)

    def _total_chars(p: dict) -> int:
        return (p.get("transcript_chars") or 0) + (p.get("reply_chars") or 0)

    buckets = []
    for label, lo, hi in _INGEST_CHAR_BUCKETS:
        bucket_points = [
            p for p in points
            if _total_chars(p) >= lo and (hi is None or _total_chars(p) < hi)
        ]
        stage_stats = {}
        for stage in _INGEST_STAGES:
            values = [p[stage] for p in bucket_points if p.get(stage) is not None]
            stage_stats[stage] = {
                "min": min(values),
                "max": max(values),
                "mean": round(statistics.mean(values), 1),
                "median": round(statistics.median(values), 1),
            } if values else None
        buckets.append({"label": label, "count": len(bucket_points), "stages": stage_stats})

    return {"hours": hours, "total_count": len(points), "buckets": buckets}


@router.get("/debug/timers")
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


@router.get("/debug/connectivity")
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

    # OpenClaw は LLM_BACKEND=openclaw のときだけ使われるため、未選択時は疎通チェック対象から外す
    _candidates = [
        ("Speaker ID", SPEAKER_ID_URL),
        ("VOICEVOX", VOICEVOX_URL),
    ]
    if LLM_BACKEND == "openclaw":
        _candidates.insert(0, ("OpenClaw Gateway (LLM)", OPENCLAW_BASE_URL))

    checks = []
    for label, url_str in _candidates:
        if url_str:
            p = urlparse(url_str)
            default_port = 443 if p.scheme == "https" else 80
            checks.append((label, p.hostname, p.port or default_port))
    checks.append(("MQTT Broker", MQTT_BROKER, MQTT_PORT))

    for label, host, port in checks:
        if host:
            results["tcp"][label] = {
                "target": f"{host}:{port}",
                "result": _tcp_check(host, port),
            }

    return results


@router.get("/api/debug/p2pquake/status")
def api_p2pquake_status():
    """P2P地震情報 WebSocket の接続状態と直近受信イベントを返す。"""
    return {
        "enabled": P2PQUAKE_ENABLED,
        "ws": dict(_p2pquake_ws_status),
        "recent_events": list(reversed(_p2pquake_recent_events)),  # 新しい順
    }


@router.get("/api/debug/conversations")
def api_debug_conversations(
    limit: int = Query(default=100, le=1000),
    speaker: str = Query(default=""),
):
    """保存済みの会話生ログを新しい順に返す（記憶抽出バッチ・確認用）。"""
    return {"conversations": _fetch_conversations(speaker=speaker or None, limit=limit)}


@router.get("/api/debug/earthquake/map")
def api_debug_earthquake_map(limit: int = Query(default=20, le=200)):
    """地図表示用に、緯度経度のある地震ログを新しい順で返す（表示件数は呼び出し側で選べる）。"""
    with _db_lock:
        rows = _db_mod._db_conn.execute(
            "SELECT earthquake_id, place, scale, magnitude, lat, lon, depth, notified_at "
            "FROM earthquake_log "
            "WHERE lat IS NOT NULL AND lon IS NOT NULL "
            "ORDER BY notified_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return {
        "events": [
            {
                "id": r[0], "place": r[1], "scale": r[2], "magnitude": r[3],
                "lat": r[4], "lon": r[5], "depth": r[6], "notified_at": r[7],
            }
            for r in rows
        ],
    }


@router.post("/api/debug/p2pquake")
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
            _db_mod._db_conn.execute(  # type: ignore[union-attr]
                "DELETE FROM earthquake_log WHERE earthquake_id = ? OR earthquake_id LIKE ?",
                (event_id, event_id + ":%"),
            )
            _db_mod._db_conn.commit()  # type: ignore[union-attr]

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


@router.get("/api/debug/coverage")
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

    with _db_lock:
        active_rows = _db_mod._db_conn.execute(  # type: ignore[union-attr]
            "SELECT area, grade, updated_at FROM tsunami_state ORDER BY updated_at DESC"
        ).fetchall()
    tsunami_active = [{"area": r[0], "grade": r[1], "updated_at": r[2]} for r in active_rows]

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
            "active": tsunami_active,
        },
        "weather": {
            "enabled": bool(lat and lon),
            "lat": float(lat) if lat else None,
            "lon": float(lon) if lon else None,
            "note": "Open-Meteo（設置場所の座標を使用）" if lat else "位置情報未設定のため利用不可",
        },
    }


@router.get("/api/debug/weather")
async def api_debug_weather():
    """Open-Meteo から設置場所の現在天気を取得して返す。"""
    lat = _get_setting("location_lat", "")
    lon = _get_setting("location_lon", "")
    if not lat or not lon:
        raise HTTPException(status_code=400, detail="設置場所が未設定です。設定画面で場所を登録してください。")

    resp = await _http_mod._http_client.get(
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


@router.post("/api/debug/weather/speak")
async def api_debug_weather_speak():
    """現在の天気をLLMで変換してスタックちゃんに喋らせる（テスト用）。"""
    lat = _get_setting("location_lat", "")
    lon = _get_setting("location_lon", "")
    if not lat or not lon:
        raise HTTPException(status_code=400, detail="設置場所が未設定です。")

    resp = await _http_mod._http_client.get(
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
    reply = await chat_with_llm(prompt, session_key=MQTT_DEVICE_ID, use_functions=False)
    _, clean = _parse_expression(reply)
    speaker_id, expr = _resolve_expression("neutral")
    audio_url, stream_url = await resolve_audio_url(clean, speaker_id)
    req_id = str(uuid.uuid4())
    publish_speak(audio_url, stream_url, clean, "weather_test", "normal", req_id, expr)
    return {"ok": True, "text": clean, "weather": desc}


@router.post("/api/debug/weather/rain-check")
async def api_debug_rain_check():
    """雨検知チェックをその場で実行する（クールダウンリセット後）。"""
    _set_setting("weather_rain_notified", "")
    await _check_rain_notification()
    return {"ok": True, "active_source": _get_setting("rain_source", "nowcast")}


@router.get("/api/debug/rain/status")
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
                next_action = "予告なし雨を検知 → 「気づいたら雨」通知を送信"
            else:
                next_action = "降雨中・通知済み（クールダウン維持）"
        elif not soon_wet:
            if cooldown_str and amedas_approaching:
                next_action = "AMeDAS 接近中のためクールダウン保持"
            elif cooldown_str:
                next_action = "乾燥・雨予報なし → クールダウンリセット"
            else:
                next_action = "乾燥・待機中"
        else:
            if cooldown_remaining_min and cooldown_remaining_min > 0:
                next_action = f"クールダウン中（あと {cooldown_remaining_min} 分）"
            else:
                next_action = "雨接近 → 通知を送信"

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


@router.get("/api/debug/iss")
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


@router.post("/api/debug/iss/morning-preview")
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


@router.post("/api/debug/iss/immediate")
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
