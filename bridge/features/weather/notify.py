"""Weather notification functions for Stack-chan.

_check_rain_notification, _rain_llm_comment use publish_speak, chat_with_llm etc.
These are accessed lazily via sys.modules["main"] to avoid circular imports.
"""
import asyncio
import io
import logging
import math
import sys
import uuid
from datetime import datetime, timezone, timedelta
from html.parser import HTMLParser

import ephem
from PIL import Image

from bridge.config import (
    _JST,
    WEATHER_CHECK_INTERVAL, WEATHER_NOTIFY_RAIN,
    WEATHER_RAIN_THRESHOLD, WEATHER_RAIN_SUDDEN_MUL,
    _NOWCAST_ZOOM, _NOWCAST_COLOR_MAP,
    ISS_NOTIFY_ENABLED, ISS_MIN_ELEVATION, ISS_NOTIFY_AHEAD, ISS_TLE_URL,
)

logger = logging.getLogger(__name__)

# ISS state (mutable module-level cache)
_iss_tle_cache: dict = {}
_iss_notified_passes: set = set()


def _nowcast_tile_coords(lat: float, lon: float) -> tuple[int, int, int, int]:
    """緯度経度 → ナウキャストタイル座標 (tx, ty, pixel_x, pixel_y)。"""
    zoom = _NOWCAST_ZOOM
    n = 2 ** zoom
    px_f = (lon + 180) / 360 * n * 256
    lat_r = math.radians(lat)
    py_f = (1 - math.log(math.tan(lat_r) + 1 / math.cos(lat_r)) / math.pi) / 2 * n * 256
    return int(px_f // 256), int(py_f // 256), int(px_f % 256), int(py_f % 256)


async def _nowcast_pixel_intensity(bt: str, vt: str, lat: float, lon: float) -> float:
    """指定 basetime/validtime のタイルから降水強度 (mm/h) を返す。取得失敗時は 0.0。"""
    tx, ty, px, py = _nowcast_tile_coords(lat, lon)
    url = (
        f"https://www.jma.go.jp/bosai/jmatile/data/nowc"
        f"/{bt}/none/{vt}/surface/hrpns/{_NOWCAST_ZOOM}/{tx}/{ty}.png"
    )
    try:
        resp = await sys.modules["main"]._http_client.get(url, headers={"User-Agent": "bridge-stackchan/1.0"}, timeout=8)
        if resp.status_code == 404:
            return 0.0
        resp.raise_for_status()
        img = Image.open(io.BytesIO(resp.content)).convert("RGBA")
        r, g, b, a = img.getpixel((px, py))
        if a == 0:
            return 0.0
        best = min(_NOWCAST_COLOR_MAP, key=lambda c: sum((c[0][i] - (r, g, b)[i]) ** 2 for i in range(3)))
        return best[1]
    except Exception:
        return 0.0


async def _fetch_nowcast_rain_data(lat: float, lon: float) -> dict:
    """N1（観測）・N2（予報）タイムラインを取得して返す。"""
    n1_resp, n2_resp = await asyncio.gather(
        sys.modules["main"]._http_client.get("https://www.jma.go.jp/bosai/jmatile/data/nowc/targetTimes_N1.json", timeout=8),
        sys.modules["main"]._http_client.get("https://www.jma.go.jp/bosai/jmatile/data/nowc/targetTimes_N2.json", timeout=8),
    )
    n1_resp.raise_for_status()
    n2_resp.raise_for_status()
    n1_list = n1_resp.json()
    n2_list = n2_resp.json()

    # N1: 最新 basetime から直近 7 ステップ（35 分分の観測）
    n1_basetimes = sorted(set(t["basetime"] for t in n1_list))[-7:]
    # N2: 最新 basetime の全予報エントリ
    n2_bt = max(t["basetime"] for t in n2_list)
    n2_entries = sorted([t for t in n2_list if t["basetime"] == n2_bt], key=lambda t: t["validtime"])

    obs_vals, fct_vals = await asyncio.gather(
        asyncio.gather(*[_nowcast_pixel_intensity(bt, bt, lat, lon) for bt in n1_basetimes]),
        asyncio.gather(*[_nowcast_pixel_intensity(e["basetime"], e["validtime"], lat, lon) for e in n2_entries]),
    )

    now_utc = datetime.now(timezone.utc)

    obs_timeline = []
    for bt, mm in zip(n1_basetimes, obs_vals):
        dt = datetime.strptime(bt, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        obs_timeline.append({
            "offset_min": int((dt - now_utc).total_seconds() // 60),
            "time_jst": dt.astimezone(_JST).strftime("%H:%M"),
            "mm_h": mm,
            "type": "observed",
        })

    fct_timeline = []
    n2_bt_dt = datetime.strptime(n2_bt, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    for entry, mm in zip(n2_entries, fct_vals):
        dt = datetime.strptime(entry["validtime"], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        fct_timeline.append({
            "offset_min": int((dt - now_utc).total_seconds() // 60),
            "time_jst": dt.astimezone(_JST).strftime("%H:%M"),
            "mm_h": mm,
            "type": "forecast",
        })

    threshold_mmh = WEATHER_RAIN_THRESHOLD * 4  # mm/15min → mm/h
    current_mm = float(obs_vals[-1]) if obs_vals else 0.0
    soon_mm = [e["mm_h"] for e in fct_timeline if 1 <= e["offset_min"] <= 35]
    now_dry = current_mm < threshold_mmh
    soon_wet = any(m >= threshold_mmh for m in soon_mm)
    sudden = any(m >= threshold_mmh * WEATHER_RAIN_SUDDEN_MUL for m in soon_mm)

    return {
        "obs_time_jst": datetime.strptime(n1_basetimes[-1], "%Y%m%d%H%M%S")
                        .replace(tzinfo=timezone.utc).astimezone(_JST).strftime("%H:%M"),
        "fct_bt_jst": n2_bt_dt.astimezone(_JST).strftime("%H:%M"),
        "timeline_obs": obs_timeline,
        "timeline_fct": fct_timeline,
        "current_mm_h": current_mm,
        "now_dry": now_dry,
        "soon_wet": soon_wet,
        "sudden": sudden,
        "threshold_mmh": threshold_mmh,
    }


async def _fetch_openmeteo_rain_data(lat: float, lon: float) -> dict:
    """Open-Meteo minutely_15 データを取得して返す（通知判定付き）。"""
    resp = await sys.modules["main"]._http_client.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": lat, "longitude": lon,
            "minutely_15": "precipitation",
            "timezone": "Asia/Tokyo",
            "past_minutely_15": 6,
            "forecast_minutely_15": 6,
        },
    )
    resp.raise_for_status()
    m15 = resp.json().get("minutely_15", {})
    times  = m15.get("time", [])
    precip = m15.get("precipitation", [])

    # 現在に最も近いインデックスを特定
    now_str = datetime.now(_JST).strftime("%Y-%m-%dT%H:%M")
    cur_idx = 0
    for i, t in enumerate(times):
        if t <= now_str:
            cur_idx = i

    timeline = []
    for i, (t, p) in enumerate(zip(times, precip)):
        timeline.append({
            "offset_min": (i - cur_idx) * 15,
            "time_jst": t[11:16],
            "mm_15min": p,
            "type": "observed" if i <= cur_idx else "forecast",
        })

    threshold = WEATHER_RAIN_THRESHOLD
    current_val = precip[cur_idx] if cur_idx < len(precip) else 0.0
    soon_vals   = [precip[j] for j in range(cur_idx + 1, min(cur_idx + 4, len(precip)))]
    now_dry  = current_val < threshold
    soon_wet = any(p >= threshold for p in soon_vals)
    sudden   = any(p >= threshold * WEATHER_RAIN_SUDDEN_MUL for p in soon_vals)

    return {
        "timeline": timeline,
        "current_mm_15min": current_val,
        "now_dry": now_dry,
        "soon_wet": soon_wet,
        "sudden": sudden,
        "threshold": threshold,
    }


# ── AMeDAS + Open-Meteo 複合雨予報 ──────────────────────────────────────────

_amedas_station_cache: dict | None = None
_AMEDAS_RADIUS_KM = 120.0
_AMEDAS_N_SNAPSHOTS = 7   # 10分刻み × 6間隔 = 60分


async def _load_amedas_station_table() -> dict:
    """AMeDAS 観測点マスタをキャッシュ取得（初回のみ HTTP）。"""
    global _amedas_station_cache
    if _amedas_station_cache is not None:
        return _amedas_station_cache
    resp = await sys.modules["main"]._http_client.get(
        "https://www.jma.go.jp/bosai/amedas/const/amedastable.json"
    )
    resp.raise_for_status()
    _amedas_station_cache = resp.json()
    return _amedas_station_cache


async def _fetch_amedas_snapshots(
    lat: float, lon: float
) -> tuple[list[dict], dict, list[int]]:
    """
    過去 60 分の AMeDAS スナップショット 7 枚を並列取得する。

    Returns:
        snapshots   : list[dict]  各時刻の {station_id: obs_dict}（古い→新しい順）
        station_meta: dict         {station_id: {name, lat, lon, dist_km, direction}}
        t_minutes   : list[int]   各スナップの相対時刻 [-60, -50, ..., 0]
    """
    table = await _load_amedas_station_table()
    cos_lat = math.cos(math.radians(lat))

    # 最新時刻を取得（JST ISO 形式 → UTC タイムスタンプ文字列）
    r = await sys.modules["main"]._http_client.get(
        "https://www.jma.go.jp/bosai/amedas/data/latest_time.txt"
    )
    r.raise_for_status()
    latest_dt = datetime.fromisoformat(r.text.strip()).astimezone(timezone.utc)

    # 7 スナップショットの UTC タイムスタンプを生成（古い→新しい順）
    timestamps = [
        (latest_dt - timedelta(minutes=(_AMEDAS_N_SNAPSHOTS - 1 - i) * 10))
        .strftime("%Y%m%d%H%M%S")
        for i in range(_AMEDAS_N_SNAPSHOTS)
    ]
    t_minutes = [-((_AMEDAS_N_SNAPSHOTS - 1 - i) * 10) for i in range(_AMEDAS_N_SNAPSHOTS)]

    # 並列取得
    urls = [
        f"https://www.jma.go.jp/bosai/amedas/data/map/{ts}.json"
        for ts in timestamps
    ]
    responses = await asyncio.gather(
        *[sys.modules["main"]._http_client.get(url) for url in urls], return_exceptions=True
    )

    snapshots: list[dict] = []
    for resp in responses:
        if isinstance(resp, Exception) or resp.status_code != 200:
            snapshots.append({})
            continue
        try:
            snapshots.append(resp.json())
        except Exception:
            snapshots.append({})

    # 半径フィルタ＋メタデータ構築
    dirs16 = ["北","北北東","北東","東北東","東","東南東","南東","南南東",
               "南","南南西","南西","西南西","西","西北西","北西","北北西"]
    station_meta: dict = {}
    for sid, info in table.items():
        if "lat" not in info or "lon" not in info:
            continue
        s_lat = info["lat"][0] + info["lat"][1] / 60
        s_lon = info["lon"][0] + info["lon"][1] / 60
        dist_km = math.sqrt(
            ((s_lat - lat) * 111) ** 2 + ((s_lon - lon) * 111 * cos_lat) ** 2
        )
        if dist_km > _AMEDAS_RADIUS_KM:
            continue
        ang = math.degrees(math.atan2((s_lon - lon) * cos_lat, s_lat - lat))
        station_meta[sid] = {
            "name": info.get("kjName", sid),
            "lat": s_lat,
            "lon": s_lon,
            "dist_km": round(dist_km, 1),
            "direction": dirs16[int((ang + 360 + 11.25) % 360 / 22.5)],
        }

    return snapshots, station_meta, t_minutes


def _estimate_rain_movement(
    snapshots: list[dict],
    station_meta: dict,
    t_minutes: list[int],
    target_lat: float,
    target_lon: float,
) -> dict:
    """
    AMeDAS 多点・多時刻観測から雨の移動ベクトルと到達時刻を推定する。

    手法: 降水開始時刻 t_i = a*x_i + b*y_i + c の最小二乗回帰
    (x,y) は目標地点を原点とした km 単位の平面座標。
    詳細は docs/rain_prediction_design.md を参照。
    """
    import numpy as np

    cos_lat = math.cos(math.radians(target_lat))
    dirs16 = ["北","北北東","北東","東北東","東","東南東","南東","南南東",
               "南","南南西","南西","西南西","西","西北西","北西","北北西"]

    # 各観測点の降水開始時刻を特定（最初に precipitation10m > 0 となった時刻）
    onset: dict[str, float] = {}
    for i, (snap, t) in enumerate(zip(snapshots, t_minutes)):
        for sid, obs in snap.items():
            if sid not in station_meta or sid in onset:
                continue
            prec = (obs.get("precipitation10m") or [0])[0] or 0
            if prec > 0:
                onset[sid] = float(t)

    # 現在（最新スナップ）の降水観測点
    cur_snap = snapshots[-1]
    wet_now: list[dict] = []
    for sid, obs in cur_snap.items():
        if sid not in station_meta:
            continue
        prec = (obs.get("precipitation10m") or [0])[0] or 0
        if prec > 0:
            wet_now.append({
                "name": station_meta[sid]["name"],
                "dist_km": station_meta[sid]["dist_km"],
                "direction": station_meta[sid]["direction"],
                "prec_mm": prec,
            })
    wet_now.sort(key=lambda s: s["dist_km"])

    # 全観測点の現在降水量・onset時刻（地図表示用）
    all_stations = []
    for sid, meta in station_meta.items():
        prec = 0.0
        if sid in cur_snap:
            obs = cur_snap[sid]
            prec = (obs.get("precipitation10m") or [0])[0] or 0
        all_stations.append({
            "name": meta["name"],
            "lat": meta["lat"],
            "lon": meta["lon"],
            "dist_km": meta["dist_km"],
            "prec_mm": prec,
            "onset_min": onset.get(sid),  # 降水開始時刻（分）、なければ None
        })
    all_stations.sort(key=lambda s: s["dist_km"])

    # 回帰に使えるデータが 3 点未満 → 低確信度で返す
    if len(onset) < 3:
        nearby_wet = [s for s in wet_now if s["dist_km"] <= 30]
        return {
            "method": "insufficient_data",
            "n_stations": len(onset),
            "wet_now": wet_now,
            "all_stations": all_stations,
            "approaching": len(nearby_wet) > 0,
            "arrival_min": None,
            "direction_str": "不明",
            "direction_deg": 0.0,
            "speed_kmh": 0.0,
            "confidence": "low",
        }

    # 回帰行列を構築（距離逆数重み付き最小二乗法）
    xs, ys, ts = [], [], []
    for sid, t in onset.items():
        m = station_meta[sid]
        xs.append((m["lon"] - target_lon) * 111 * cos_lat)
        ys.append((m["lat"] - target_lat) * 111)
        ts.append(t)

    xs_arr = np.array(xs)
    ys_arr = np.array(ys)
    dists  = np.sqrt(xs_arr ** 2 + ys_arr ** 2)
    weights = 1.0 / np.maximum(dists, 5.0)  # 最低 5km でクランプ（ゼロ除算回避）

    A     = np.column_stack([xs_arr, ys_arr, np.ones(len(xs_arr))])
    t_vec = np.array(ts)
    A_w   = A * weights[:, np.newaxis]
    t_w   = t_vec * weights
    try:
        coeffs, residuals, rank, _ = np.linalg.lstsq(A_w, t_w, rcond=None)
        a, b, c = float(coeffs[0]), float(coeffs[1]), float(coeffs[2])
    except Exception:
        return {"method": "regression_failed", "confidence": "low",
                "wet_now": wet_now, "all_stations": all_stations,
                "approaching": False, "arrival_min": None,
                "direction_str": "不明", "direction_deg": 0.0,
                "speed_kmh": 0.0, "n_stations": len(onset)}

    # 移動方向・速度
    grad_mag = math.sqrt(a ** 2 + b ** 2)
    if grad_mag < 1e-9:
        speed_kmh, direction_deg = 0.0, 0.0
    else:
        speed_kmh = (1.0 / grad_mag) * 60   # km/h
        # 勾配 (a,b) = t が増える方向 = 雨が進んでいる方向（移動先）
        direction_deg = (math.degrees(math.atan2(a, b)) + 360) % 360

    direction_str = dirs16[int((direction_deg + 11.25) % 360 / 22.5)]

    # 目標地点への到達予測: t_target = c (minutes, 負 = 過去)
    arrival_min = float(c)   # 正なら未来（あと arrival_min 分）

    # 確信度
    n_pts = len(onset)
    rmse = 0.0
    if len(residuals) > 0 and residuals[0] > 0:
        rmse = math.sqrt(float(residuals[0]) / n_pts)

    # 速度が物理的にあり得ない場合（>250km/h）は「広域降雨で方向不明」
    widespread = speed_kmh > 250
    if widespread:
        confidence = "low"
    elif n_pts >= 6 and rmse < 15:
        confidence = "high"
    elif n_pts >= 3 and rmse < 25:
        confidence = "medium"
    else:
        confidence = "low"

    approaching = 0 < arrival_min <= 60 and not widespread

    return {
        "method": "regression",
        "n_stations": n_pts,
        "direction_deg": round(direction_deg, 1),
        "direction_str": direction_str,
        "speed_kmh": round(speed_kmh, 1),
        "arrival_min": round(arrival_min) if approaching else None,
        "approaching": approaching,
        "wet_now": wet_now,
        "all_stations": all_stations,
        "confidence": confidence,
        "rmse_min": round(rmse, 1),
        "regression_coeffs": [round(a, 6), round(b, 6), round(c, 2)],
        "target_lat": target_lat,
        "target_lon": target_lon,
    }


async def _fetch_amedas_openmeteo_rain_data(lat: float, lon: float) -> dict:
    """
    AMeDAS 多点観測 + Open-Meteo を組み合わせた雨予報。
    docs/rain_prediction_design.md 参照。
    """
    # 並列取得
    amedas_task = asyncio.create_task(_fetch_amedas_snapshots(lat, lon))
    om_task = asyncio.create_task(_fetch_openmeteo_rain_data(lat, lon))
    (snapshots, station_meta, t_minutes), om_data = await asyncio.gather(
        amedas_task, om_task
    )

    movement = _estimate_rain_movement(snapshots, station_meta, t_minutes, lat, lon)

    # 現在乾燥かどうか: AMeDAS 20km 内 AND Open-Meteo 現在値 の両方が乾燥
    nearby_wet = [s for s in movement.get("wet_now", []) if s["dist_km"] <= 20]
    amedas_now_dry = len(nearby_wet) == 0
    now_dry = amedas_now_dry and om_data.get("now_dry", True)

    # 30 分以内に到達予測か（AMeDAS）
    amedas_soon_wet = movement.get("approaching", False) and (
        movement.get("arrival_min") or 999
    ) <= 30

    # Open-Meteo の 30 分先予報
    om_soon_wet = om_data.get("soon_wet", False)
    om_sudden  = om_data.get("sudden", False)

    # どちらかが「もうすぐ雨」なら通知
    soon_wet = amedas_soon_wet or om_soon_wet
    openmeteo_confirms = amedas_soon_wet and om_soon_wet

    # 確信度に応じた sudden 判定
    sudden = om_sudden or (
        amedas_soon_wet
        and (movement.get("arrival_min") or 999) <= 15
        and movement.get("confidence") in ("high", "medium")
    )

    return {
        "amedas": movement,
        "openmeteo": om_data,
        "now_dry": now_dry,
        "amedas_now_dry": amedas_now_dry,
        "om_now_dry": om_data.get("now_dry", True),
        "soon_wet": soon_wet,
        "sudden": sudden,
        "openmeteo_confirms": openmeteo_confirms,
    }


async def _check_rain_notification() -> None:
    """雨降り始めを検知して通知する。rain_source 設定でデータソースを切り替え可能。"""
    lat_str = sys.modules["main"]._get_setting("location_lat", "")
    lon_str = sys.modules["main"]._get_setting("location_lon", "")
    if not lat_str or not lon_str:
        return
    lat, lon = float(lat_str), float(lon_str)

    source = sys.modules["main"]._get_setting("rain_source", "amedas+openmeteo")
    try:
        if source == "amedas+openmeteo":
            data = await _fetch_amedas_openmeteo_rain_data(lat, lon)
        elif source == "nowcast":
            data = await _fetch_nowcast_rain_data(lat, lon)
        else:
            data = await _fetch_openmeteo_rain_data(lat, lon)
        now_dry  = data["now_dry"]
        soon_wet = data["soon_wet"]
        sudden   = data["sudden"]
    except Exception as e:
        logger.warning("rain check error (source=%s): %s", source, e)
        return

    now   = datetime.now(_JST)
    hour  = now.hour
    if 5 <= hour < 10:    time_label = "朝"
    elif 10 <= hour < 14: time_label = "昼"
    elif 14 <= hour < 18: time_label = "夕方"
    elif 18 <= hour < 22: time_label = "夜"
    else:                 time_label = "深夜"

    if not now_dry:
        # 現在降雨中 — 事前通知なしで降り始めた場合のみ「気づいたら雨」通知
        if not sys.modules["main"]._get_setting("weather_rain_notified", ""):
            sys.modules["main"]._set_setting("weather_rain_notified", now.isoformat())
            logger.info("rain notify (unexpected): source=%s time=%s", source, time_label)
            await sys.modules["main"]._p2p_speak("あれ、気づいたら雨が降り始めてる！", source="weather_rain", priority="normal")
            asyncio.create_task(_rain_llm_comment(False, time_label, hour, unexpected=True))
        return

    if not soon_wet:
        # 乾燥かつ雨の予報なし — AMeDAS が接近中のときはクールダウンを保持
        amedas_approaching = data.get("amedas", {}).get("approaching", False)
        if sys.modules["main"]._get_setting("weather_rain_notified", "") and not amedas_approaching:
            sys.modules["main"]._set_setting("weather_rain_notified", "")
            logger.debug("rain cooldown reset: dry, no rain expected (source=%s)", source)
        return

    last_str = sys.modules["main"]._get_setting("weather_rain_notified", "")
    if last_str:
        try:
            elapsed = (now - datetime.fromisoformat(last_str)).total_seconds()
            if elapsed < 3 * 3600:
                return
        except ValueError:
            pass

    fixed_text = (
        "急に雨が降り始めます！洗濯物や開けている窓に注意してください。" if sudden
        else "30分以内に雨が降り始めそうです。"
    )
    sys.modules["main"]._set_setting("weather_rain_notified", now.isoformat())
    logger.info("rain notify: source=%s sudden=%s time=%s", source, sudden, time_label)

    await sys.modules["main"]._p2p_speak(fixed_text, source="weather_rain", priority="normal")
    asyncio.create_task(_rain_llm_comment(sudden, time_label, hour))


async def _rain_llm_comment(sudden: bool, time_label: str, hour: int, unexpected: bool = False) -> None:
    title = sys.modules["main"]._get_setting("location_title", "")

    # 時間帯ごとの文脈ヒント
    if hour < 10:
        hint = "通勤・通学・洗濯物の取り込みなど朝の行動を意識して"
    elif hour < 14:
        hint = "外出中の傘や昼食時の移動を意識して"
    elif hour < 18:
        hint = "帰宅時の傘や洗濯物の取り込みを意識して"
    elif hour < 22:
        hint = "洗濯物・窓の締め忘れ・翌日の準備を意識して"
    else:
        hint = "翌朝の傘や洗濯物の準備を意識して"

    if unexpected:
        kind = "予報になかった雨が気づいたら降り始めていた"
    elif sudden:
        kind = "急な雨"
    else:
        kind = "30分以内に雨が来る"

    prompt = (
        f"{kind}という通知をしました"
        f"（{time_label}・場所: {title}）。"
        f"{hint}、家族への短い一言を1文で。通知文の繰り返し不要。"
    )
    try:
        comment = await sys.modules["main"].chat_with_llm(prompt, session_key="family", use_functions=False)
        await sys.modules["main"]._p2p_speak(comment, source="weather_rain_comment", priority="normal")
    except Exception:
        logger.exception("rain LLM comment failed")


async def _fetch_sky_condition(lat: float, lon: float, at_utc: datetime | None = None) -> dict:
    """Open-Meteo から雲量・降水量を取得して空の見え方を返す。
    at_utc=None なら現在値、指定すればその時刻の時間予報値を返す。
    """
    params: dict = {
        "latitude": lat, "longitude": lon,
        "timezone": "Asia/Tokyo",
        "current": "cloud_cover,precipitation,weather_code",
    }
    if at_utc is not None:
        params["hourly"] = "cloud_cover,precipitation,weather_code"
        params["forecast_days"] = 2

    try:
        resp = await sys.modules["main"]._http_client.get("https://api.open-meteo.com/v1/forecast", params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("_fetch_sky_condition failed: %s", e)
        return {"cloud_cover": 0, "precipitation": 0.0, "summary": "天気不明", "is_visible": True}

    if at_utc is not None:
        times = data.get("hourly", {}).get("time", [])
        target_str = at_utc.astimezone(_JST).strftime("%Y-%m-%dT%H:00")
        if target_str in times:
            idx = times.index(target_str)
        else:
            # 最近傍時刻
            target_ts = at_utc.timestamp()
            idx = min(
                range(len(times)),
                key=lambda i: abs(datetime.fromisoformat(times[i]).replace(tzinfo=_JST).timestamp() - target_ts),
                default=0,
            )
        cloud_cover  = data["hourly"]["cloud_cover"][idx]
        precipitation = data["hourly"]["precipitation"][idx]
    else:
        cur = data.get("current", {})
        cloud_cover  = cur.get("cloud_cover", 0)
        precipitation = cur.get("precipitation", 0.0)

    if precipitation > 0:
        summary    = f"雨（{precipitation}mm）"
        is_visible = False
    elif cloud_cover >= 80:
        summary    = f"曇り（雲量{cloud_cover}%）"
        is_visible = False
    elif cloud_cover >= 50:
        summary    = f"薄曇り（雲量{cloud_cover}%）"
        is_visible = True
    else:
        summary    = f"晴れ（雲量{cloud_cover}%）"
        is_visible = True

    return {"cloud_cover": cloud_cover, "precipitation": precipitation,
            "summary": summary, "is_visible": is_visible}


async def _fetch_iss_tle() -> tuple[str, str] | None:
    """CelesTrak から ISS TLE を取得。当日キャッシュあれば再利用。"""
    today = datetime.now(_JST).strftime("%Y-%m-%d")
    if _iss_tle_cache.get("date") == today:
        return _iss_tle_cache["line1"], _iss_tle_cache["line2"]
    try:
        resp = await sys.modules["main"]._http_client.get(ISS_TLE_URL, timeout=10)
        resp.raise_for_status()
        lines = [l.strip() for l in resp.text.strip().splitlines() if l.strip()]
        if len(lines) < 3:
            raise ValueError("TLE に3行以上必要")
        _iss_tle_cache.update({"date": today, "line1": lines[1], "line2": lines[2]})
        logger.info("ISS TLE 更新: %s", lines[0])
        return lines[1], lines[2]
    except Exception:
        logger.exception("ISS TLE 取得失敗")
        return None


def _az_to_direction(az_rad: float) -> str:
    """方位角（ラジアン）を8方位の日本語に変換。"""
    dirs = ["北", "北東", "東", "南東", "南", "南西", "西", "北西"]
    idx = int((math.degrees(az_rad) % 360 + 22.5) / 45) % 8
    return dirs[idx]


def _get_sunset_utc(lat: float, lon: float) -> datetime | None:
    """当日の日没時刻（UTC）を ephem で計算して返す。計算失敗時は None。"""
    try:
        obs = ephem.Observer()
        obs.lat       = str(lat)
        obs.lon       = str(lon)
        obs.elevation = 10
        obs.pressure  = 0
        obs.horizon   = "-0:34"  # 標準的な日没（大気屈折 34 分補正）
        obs.date      = ephem.now()
        sunset = obs.next_setting(ephem.Sun())
        return ephem.Date(sunset).datetime().replace(tzinfo=timezone.utc)
    except Exception:
        logger.debug("日没時刻計算失敗")
        return None


def _calc_iss_passes(lat: float, lon: float, tle1: str, tle2: str,
                     hours: int = 18, min_el: float = 10.0) -> list[dict]:
    """指定時間内の ISS 通過リストを返す。min_el 以上の最大仰角のパスのみ含む。"""
    obs = ephem.Observer()
    obs.lat       = str(lat)
    obs.lon       = str(lon)
    obs.elevation = 10
    obs.horizon   = "10"
    obs.pressure  = 0
    iss = ephem.readtle("ISS", tle1, tle2)

    now_utc  = datetime.now(timezone.utc)
    end_ephem = ephem.Date(ephem.now() + hours / 24.0)
    obs.date  = ephem.Date(now_utc.replace(tzinfo=None))

    passes = []
    while True:
        try:
            rise_t, rise_az, max_t, max_el, set_t, _ = obs.next_pass(iss)
        except Exception:
            break
        if rise_t > end_ephem:
            break
        max_el_deg = math.degrees(max_el)
        if max_el_deg >= min_el:
            rise_utc = ephem.Date(rise_t).datetime().replace(tzinfo=timezone.utc)
            max_utc  = ephem.Date(max_t).datetime().replace(tzinfo=timezone.utc)
            passes.append({
                "rise_jst":    rise_utc.astimezone(_JST),
                "max_jst":     max_utc.astimezone(_JST),
                "max_el_deg":  max_el_deg,
                "direction":   _az_to_direction(float(rise_az)),
                "pass_key":    rise_utc.astimezone(_JST).strftime("%Y%m%d%H%M"),
            })
        obs.date = ephem.Date(set_t) + ephem.minute
    return passes


async def _iss_speak(prompt: str, source: str) -> None:
    _main = sys.modules["main"]
    text = await _main.chat_with_llm(prompt, session_key="family", use_functions=False)
    expr_label, clean_text = _main._parse_expression(text)
    speaker_id, stackchan_expr = _main._resolve_expression(expr_label or "happy")
    audio_url, stream_url = await _main.resolve_audio_url(clean_text, speaker_id)
    _main.publish_speak(audio_url, stream_url, clean_text,
                  source=source, priority="normal",
                  request_id=str(uuid.uuid4()), expression=stackchan_expr)


async def _iss_notify_loop() -> None:
    """
    2種類の ISS 通知を行うループ（1分ごとにチェック）。
    ① 朝の予告（7:45-7:55）: 当日の夕方〜夜に見えるパスを予告
    ② 直前通知（ISS_NOTIFY_AHEAD 分前）: 「もうすぐ来るよ」
    """
    logger.info("ISS notify loop started: min_elevation=%.0f° ahead=%dmin",
                ISS_MIN_ELEVATION, ISS_NOTIFY_AHEAD)
    morning_announced_date: str = ""  # 朝予告の重複防止

    while True:
        await asyncio.sleep(60)
        try:
            if not sys.modules["main"]._get_setting("iss_notify_enabled", str(ISS_NOTIFY_ENABLED)).lower() == "true":
                continue

            lat = sys.modules["main"]._get_setting("location_lat", "")
            lon = sys.modules["main"]._get_setting("location_lon", "")
            if not lat or not lon:
                continue

            tle = await _fetch_iss_tle()
            if not tle:
                continue
            tle1, tle2 = tle

            now_jst  = datetime.now(_JST)
            today_str = now_jst.strftime("%Y-%m-%d")

            # ── ① 朝の予告（7:45-7:55, 1日1回） ──────────────────────
            if 7 * 60 + 45 <= now_jst.hour * 60 + now_jst.minute <= 7 * 60 + 55:
                if morning_announced_date != today_str:
                    # 日没時刻を取得して日没後のパスに絞る
                    sunset_utc = _get_sunset_utc(float(lat), float(lon))
                    all_passes = _calc_iss_passes(
                        float(lat), float(lon), tle1, tle2,
                        hours=20, min_el=ISS_MIN_ELEVATION
                    )
                    if sunset_utc:
                        sunset_jst = sunset_utc.astimezone(_JST)
                        evening_passes = [p for p in all_passes if p["rise_jst"] >= sunset_jst]
                        logger.info("ISS 朝予告: 日没=%s 候補=%d件",
                                    sunset_jst.strftime("%H:%M"), len(evening_passes))
                    else:
                        sunset_jst = None
                        evening_passes = [
                            p for p in all_passes
                            if p["rise_jst"].hour >= 16 or p["rise_jst"].hour < 3
                        ]
                    if evening_passes:
                        best = max(evening_passes, key=lambda p: p["max_el_deg"])
                        sunset_hint = (
                            f"（今日の日没は{sunset_jst.strftime('%H時%M分')}ごろです）"
                            if sunset_jst else ""
                        )
                        sky = await _fetch_sky_condition(
                            float(lat), float(lon),
                            at_utc=best["rise_jst"].astimezone(timezone.utc),
                        )
                        if not sky["is_visible"]:
                            sky_hint = f"ただし今夜の観測時間帯の天気は{sky['summary']}の予報なので、見えないかもしれません。"
                        elif sky["cloud_cover"] >= 50:
                            sky_hint = f"今夜の天気は{sky['summary']}の予報なので、雲の切れ間から見えるかも。"
                        else:
                            sky_hint = f"今夜の天気は{sky['summary']}の予報で、よく見えそうです。"
                        prompt = (
                            f"今夜{best['rise_jst'].strftime('%H時%M分')}ごろに"
                            f"ISSが{best['direction']}の空から見えます。"
                            f"最高点は{best['max_jst'].strftime('%H時%M分')}ごろで"
                            f"かなり高いところまで上がります（最大{best['max_el_deg']:.0f}度）。"
                            f"{sunset_hint}{sky_hint}"
                            "家族に「今夜ISSが見えるよ」と朝のうちに予告してください。"
                            "天気のことも自然に触れてください。日没時刻も自然に添えてください。「仰角」は使わず、わかりやすく。1〜2文で。"
                        )
                        try:
                            await _iss_speak(prompt, source="iss_morning_preview")
                            morning_announced_date = today_str
                            logger.info("ISS 朝予告送信: %s 方向=%s max_el=%.0f°",
                                        best["rise_jst"].strftime("%H:%M"),
                                        best["direction"], best["max_el_deg"])
                        except Exception:
                            logger.exception("ISS 朝予告失敗")
                    else:
                        morning_announced_date = today_str  # 今日は見えないのでスキップ記録
                        logger.info("ISS 朝予告: 今夜は見える機会なし（日没=%s）",
                                    sunset_jst.strftime("%H:%M") if sunset_jst else "不明")

            # ── ② 直前通知（ISS_NOTIFY_AHEAD 分以内に通過開始） ─────────
            passes = _calc_iss_passes(
                float(lat), float(lon), tle1, tle2,
                hours=1, min_el=ISS_MIN_ELEVATION
            )
            notify_window = ISS_NOTIFY_AHEAD * 60
            now_utc = datetime.now(timezone.utc)

            for p in passes:
                secs_until = (p["rise_jst"].astimezone(timezone.utc) - now_utc).total_seconds()
                if not (0 < secs_until <= notify_window):
                    continue
                if p["pass_key"] in _iss_notified_passes:
                    continue

                _iss_notified_passes.add(p["pass_key"])
                if len(_iss_notified_passes) > 30:
                    _iss_notified_passes.discard(min(_iss_notified_passes))

                minutes_until = max(1, round(secs_until / 60))
                sky = await _fetch_sky_condition(float(lat), float(lon))
                if not sky["is_visible"]:
                    sky_hint = f"残念ながら今は{sky['summary']}なので見えないかもしれませんが、"
                elif sky["cloud_cover"] >= 50:
                    sky_hint = f"今は{sky['summary']}なので雲の切れ間を狙って、"
                else:
                    sky_hint = ""
                prompt = (
                    f"ISSが約{minutes_until}分後に{p['direction']}の空から見えはじめます。"
                    f"{p['max_jst'].strftime('%H時%M分')}ごろが一番高くなります"
                    f"（空の高さ{p['max_el_deg']:.0f}度相当）。"
                    f"{sky_hint}"
                    "家族に「もうすぐISSが来るよ！」と短く伝えてください。"
                    "今の天気も自然に一言添えてください。「仰角」は使わず、わかりやすく。1〜2文で。"
                )
                try:
                    await _iss_speak(prompt, source="iss_notify")
                    logger.info("ISS 直前通知送信: rise=%s 方向=%s max_el=%.0f°",
                                p["rise_jst"].strftime("%H:%M"), p["direction"], p["max_el_deg"])
                except Exception:
                    logger.exception("ISS 直前通知失敗")

        except Exception:
            logger.exception("ISS notify loop error")


class _HtmlTextExtractor(HTMLParser):
    """HTML から本文テキストを抽出する。
    script/style/noscript および nav/header/footer/aside（広告・ナビ領域）をスキップ。
    """

    _SKIP_TAGS = frozenset({"script", "style", "noscript"})
    _BLOCK_TAGS = frozenset({"nav", "header", "footer", "aside"})

    def __init__(self) -> None:
        super().__init__()
        self._texts: list[str] = []
        self._skip: int = 0
        self._block: int = 0

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag in self._SKIP_TAGS:
            self._skip += 1
        elif tag in self._BLOCK_TAGS:
            self._block += 1

    def handle_endtag(self, tag: str) -> None:
        if tag in self._SKIP_TAGS:
            self._skip = max(0, self._skip - 1)
        elif tag in self._BLOCK_TAGS:
            self._block = max(0, self._block - 1)

    def handle_data(self, data: str) -> None:
        if self._skip == 0 and self._block == 0:
            t = data.strip()
            if t:
                self._texts.append(t)

    def get_text(self, max_chars: int = 6000) -> str:
        return " ".join(self._texts)[:max_chars]


def _expand_url_template(url: str, now_jst: datetime) -> str:
    """URL テンプレート変数を展開する。
    {weekly_monday} → 今週月曜日の YYYY-MM-DD
    {today}         → 今日の YYYY-MM-DD
    """
    monday = now_jst - timedelta(days=now_jst.weekday())
    return (
        url.replace("{weekly_monday}", monday.strftime("%Y-%m-%d"))
           .replace("{today}", now_jst.strftime("%Y-%m-%d"))
    )


async def _run_web_check(
    wc_id: int, name: str, url: str, check_prompt: str,
    now_jst: datetime, today_str: str | None = None,
    mode: str = "check", notify_expression: str = "happy",
) -> dict:
    """URL を取得して LLM でステータス判定または内容読み上げを行う。

    mode='check': open/closed を判定し、open のときだけ通知する。
    mode='read' : LLM が抽出したテキストを毎回読み上げる。
    """
    expanded_url = _expand_url_template(url, now_jst)

    try:
        resp = await sys.modules["main"]._http_client.get(expanded_url, follow_redirects=True, timeout=30)
        resp.raise_for_status()
        parser = _HtmlTextExtractor()
        parser.feed(resp.text)
        page_text = parser.get_text(6000)
    except Exception as e:
        logger.warning("Web check fetch failed: name=%s url=%s err=%s", name, expanded_url, e)
        with sys.modules["bridge.core.db"]._db_lock:
            sys.modules["bridge.core.db"]._db_conn.execute(
                "UPDATE web_checks SET last_checked_at=?, last_status='error' WHERE id=?",
                (now_jst.isoformat(), wc_id),
            )
            sys.modules["bridge.core.db"]._db_conn.commit()
        return {"status": "error", "message": str(e)}

    if mode == "read":
        prompt = (
            f"以下はウェブページ「{name}」の本文テキストです。\n\n"
            f"{check_prompt}\n\n"
            "返答フォーマット: 1行目に感情ラベル(neutral/happy/sad/sleepy/angry/doubt)、"
            "2行目以降に読み上げ本文（話し言葉・短め）。\n\n"
            f"ページ本文:\n{page_text}"
        )
        try:
            answer = await sys.modules["main"].chat_with_llm(prompt, session_key="__web_check__", use_functions=False)
            expr_label, clean_text = sys.modules["main"]._parse_expression(answer)
            speaker_id, stackchan_expr = sys.modules["main"]._resolve_expression(expr_label or notify_expression)
            audio_url, stream_url = await sys.modules["main"].resolve_audio_url(clean_text, speaker_id)
            sys.modules["main"].publish_speak(audio_url, stream_url, clean_text,
                          source="web_check", priority="normal",
                          request_id=str(uuid.uuid4()), expression=stackchan_expr)
            if today_str:
                with sys.modules["bridge.core.db"]._db_lock:
                    sys.modules["bridge.core.db"]._db_conn.execute(
                        "UPDATE web_checks SET last_notified_date=?, last_checked_at=?, last_status='read' WHERE id=?",
                        (today_str, now_jst.isoformat(), wc_id),
                    )
                    sys.modules["bridge.core.db"]._db_conn.commit()
            logger.info("Web check read done: name=%s", name)
        except Exception:
            logger.exception("Web check read failed: name=%s", name)
            with sys.modules["bridge.core.db"]._db_lock:
                sys.modules["bridge.core.db"]._db_conn.execute(
                    "UPDATE web_checks SET last_checked_at=?, last_status='error' WHERE id=?",
                    (now_jst.isoformat(), wc_id),
                )
                sys.modules["bridge.core.db"]._db_conn.commit()
            return {"status": "error"}
        return {"status": "read", "text": clean_text}

    # mode == "check"
    prompt = (
        f"以下はウェブページ「{name}」の本文テキストです（URL: {expanded_url}）。\n\n"
        f"{check_prompt}\n\n"
        "受付中・申し込み可能であれば「open」、"
        "受付していない・終了・未開始であれば「closed」と一言だけ答えてください。\n\n"
        f"ページ本文:\n{page_text}"
    )
    try:
        answer = await sys.modules["main"].chat_with_llm(prompt, session_key="__web_check__", use_functions=False)
        status = "open" if "open" in answer.lower() else "closed"
    except Exception as e:
        logger.warning("Web check LLM failed: name=%s err=%s", name, e)
        status = "error"

    logger.info("Web check result: name=%s status=%s url=%s", name, status, expanded_url)

    with sys.modules["bridge.core.db"]._db_lock:
        sys.modules["bridge.core.db"]._db_conn.execute(
            "UPDATE web_checks SET last_checked_at=?, last_status=? WHERE id=?",
            (now_jst.isoformat(), status, wc_id),
        )
        sys.modules["bridge.core.db"]._db_conn.commit()

    if status == "open":
        try:
            speak_prompt = (
                f"「{name}」の申し込みが現在受け付けられています。"
                "家族に申し込みが始まっていることを短く教えてください。"
                "1〜2文で、かわいく・話し言葉で。"
            )
            await _iss_speak(speak_prompt, source="web_check")
            if today_str:
                with sys.modules["bridge.core.db"]._db_lock:
                    sys.modules["bridge.core.db"]._db_conn.execute(
                        "UPDATE web_checks SET last_notified_date=? WHERE id=?",
                        (today_str, wc_id),
                    )
                    sys.modules["bridge.core.db"]._db_conn.commit()
            logger.info("Web check notification sent: name=%s", name)
        except Exception:
            logger.exception("Web check speak failed: name=%s", name)

    return {"status": status}


async def _web_check_notify_loop() -> None:
    """毎朝 notify_time ごろに有効なアイテムをチェックする。"""
    logger.info("Web check notify loop started")
    while True:
        await asyncio.sleep(60)
        try:
            now_jst = datetime.now(_JST)
            now_min = now_jst.hour * 60 + now_jst.minute
            today_str = now_jst.strftime("%Y-%m-%d")

            with sys.modules["bridge.core.db"]._db_lock:
                rows = sys.modules["bridge.core.db"]._db_conn.execute(
                    "SELECT id, name, url, check_prompt, notify_time, last_notified_date, mode, notify_expression "
                    "FROM web_checks WHERE enabled = 1 AND url != ''"
                ).fetchall()

            monday_str = (now_jst - timedelta(days=now_jst.weekday())).strftime("%Y-%m-%d")

            for wc_id, name, url, check_prompt, notify_time, last_notified_date, mode, notify_expression in rows:
                try:
                    nh, nm = map(int, notify_time.split(":"))
                except Exception:
                    nh, nm = 7, 55
                target_min = nh * 60 + nm
                if not (0 <= now_min - target_min <= 10):
                    continue
                # read モードは週1回（今週すでに読んだらスキップ）、check モードは日1回
                if mode == "read":
                    if last_notified_date and last_notified_date >= monday_str:
                        continue
                else:
                    if last_notified_date == today_str:
                        continue
                await _run_web_check(
                    wc_id, name, url, check_prompt, now_jst, today_str,
                    mode=mode or "check", notify_expression=notify_expression or "happy",
                )

        except Exception:
            logger.exception("Web check notify loop error")


async def _weather_notify_loop() -> None:
    logger.info("Weather notify loop started: check_interval=%ds", WEATHER_CHECK_INTERVAL)
    await asyncio.sleep(10)  # 起動直後に一度チェック
    while True:
        try:
            if sys.modules["main"]._get_setting("weather_notify_rain", WEATHER_NOTIFY_RAIN) == "true":
                await _check_rain_notification()
        except Exception:
            logger.exception("Weather notify loop error")
        source = sys.modules["main"]._get_setting("rain_source", "nowcast")
        interval = 300 if source == "nowcast" else WEATHER_CHECK_INTERVAL
        await asyncio.sleep(interval)

