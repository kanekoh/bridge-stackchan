"""Settings, geocode, and location endpoints."""
import asyncio
import logging

from fastapi import APIRouter, Form, HTTPException
from pydantic import BaseModel

from bridge.config import (
    SPEAKER_ID_BROWSER_URL, SPEAKER_ID_URL, SPEAKER_ID_THRESHOLD,
    P2PQUAKE_MIN_SCALE, P2PQUAKE_TSUNAMI_TARGET_AREAS,
    WEATHER_NOTIFY_RAIN, ISS_NOTIFY_ENABLED,
    GOOGLE_GEOLOCATION_API_KEY,
)
import bridge.core.db as _db_mod
from bridge.core.db import (
    _db_lock, _get_setting, _set_setting,
    _fetch_location_history, _fetch_trips,
)
from bridge.features.quake import _extract_pref, _apply_tsunami_areas_from_pref, _fetch_and_save_timezone
from bridge.features.travel import _record_location_and_check_travel
import bridge.core.http as _http_mod

logger = logging.getLogger(__name__)
router = APIRouter()


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
    "stackchan_birthday": {
        "label": "スタックちゃんの誕生日",
        "description": (
            "MM-DD 形式で入力（例: 04-01）。この日だけシステムプロンプトに"
            "「今日は誕生日」と伝わり、それ以外の日は普段どおり会話します。"
        ),
        "env_fallback": lambda: "",
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


@router.get("/api/settings")
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


@router.put("/api/settings/{key}")
def api_update_setting(key: str, value: str = Form(...)):
    if key not in _EDITABLE_SETTINGS:
        raise HTTPException(status_code=404, detail=f"設定キー '{key}' は存在しません")
    _set_setting(key, value.strip())
    return {"key": key, "value": value.strip()}


@router.delete("/api/settings/{key}", status_code=204)
def api_reset_setting(key: str):
    """DB の上書き値を削除して env のデフォルトに戻す。"""
    if key not in _EDITABLE_SETTINGS:
        raise HTTPException(status_code=404, detail=f"設定キー '{key}' は存在しません")
    with _db_lock:
        _db_mod._db_conn.execute("DELETE FROM app_settings WHERE key=?", (key,))  # type: ignore[union-attr]
        _db_mod._db_conn.commit()


@router.post("/api/geocode")
async def api_geocode(address: str = Form(...)):
    """住所文字列を国土地理院APIで緯度経度に変換し app_settings に保存する。"""
    if not address.strip():
        raise HTTPException(status_code=400, detail="住所を入力してください")
    resp = await _http_mod._http_client.get(
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
    asyncio.create_task(_record_location_and_check_travel(lat, lon, title, pref, "manual_geocode"))

    return {"lat": lat, "lon": lon, "pref": pref, "title": title}


@router.get("/api/location")
def api_get_location():
    """現在の設置場所設定と、旅行検出の基準になっている「家」の設定を返す。"""
    return {
        "address": _get_setting("location_address", ""),
        "lat":     _get_setting("location_lat", ""),
        "lon":     _get_setting("location_lon", ""),
        "pref":    _get_setting("location_pref", ""),
        "title":   _get_setting("location_title", ""),
        "home": {
            "lat":   _get_setting("location_home_lat", ""),
            "lon":   _get_setting("location_home_lon", ""),
            "title": _get_setting("location_home_title", ""),
        },
    }


async def _reverse_geocode(lat: float, lon: float) -> tuple[str, str]:
    """Nominatim (OpenStreetMap) で緯度経度 → (都道府県, 表示用住所文字列)。
    キー不要・無料。利用規約: 1 req/s 以下, User-Agent 必須。"""
    resp = await _http_mod._http_client.get(
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


async def _geolocate_and_save(wifi_aps: list[dict], consider_ip: bool = True, source: str = "device_wifi") -> dict:
    """Google Geolocation API + Nominatim で位置を解決して app_settings に保存する。"""
    if not GOOGLE_GEOLOCATION_API_KEY:
        raise HTTPException(status_code=503, detail="GOOGLE_GEOLOCATION_API_KEY が設定されていません")

    geo_payload: dict = {"considerIp": consider_ip}
    if wifi_aps:
        geo_payload["wifiAccessPoints"] = wifi_aps

    try:
        geo_resp = await _http_mod._http_client.post(
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
    asyncio.create_task(_record_location_and_check_travel(lat, lon, title, pref, source))

    logger.info("location updated: lat=%.4f lon=%.4f pref=%s title=%s acc=%.0fm",
                lat, lon, pref, title, accuracy)

    return {"lat": lat, "lon": lon, "accuracy": accuracy,
            "pref": pref, "title": title, "updated": True}


class LocationUpdateRequest(BaseModel):
    wifiAccessPoints: list[dict] = []
    considerIp: bool = True


@router.post("/api/location/from-coords")
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
    asyncio.create_task(_record_location_and_check_travel(lat, lon, title, pref, "browser"))
    logger.info("location set from browser coords: lat=%.4f lon=%.4f pref=%s", lat, lon, pref)
    return {"lat": lat, "lon": lon, "pref": pref, "title": title}


@router.post("/api/location/update")
async def api_location_update(req: LocationUpdateRequest):
    """Stack-chan から Wi-Fi スキャン結果を受け取り位置を更新する。"""
    return await _geolocate_and_save(req.wifiAccessPoints, req.considerIp, source="device_wifi")


@router.post("/api/location/scan")
async def api_location_scan():
    """ラズパイ自身が Wi-Fi をスキャンして位置を更新する（WebUI テスト用）。"""
    aps = _scan_local_wifi()
    logger.info("local wifi scan: %d APs found", len(aps))
    return await _geolocate_and_save(aps, consider_ip=True, source="pi_wifi_scan")


@router.get("/api/location/history")
def api_location_history(limit: int = 200):
    """位置履歴を新しい順に返す（旅行検出のデバッグ・「家」選択用）。"""
    return {"history": _fetch_location_history(limit)}


class SetHomeRequest(BaseModel):
    lat: float
    lon: float
    title: str = ""


@router.post("/api/location/set-home")
def api_location_set_home(req: SetHomeRequest):
    """位置履歴の中から選んだ地点を「家」として設定する。旅行検出の基準点になる。"""
    _set_setting("location_home_lat",   str(req.lat))
    _set_setting("location_home_lon",   str(req.lon))
    _set_setting("location_home_title", req.title)
    logger.info("home location set: lat=%.4f lon=%.4f title=%s", req.lat, req.lon, req.title)
    return {"lat": req.lat, "lon": req.lon, "title": req.title}


@router.get("/api/travel/trips")
def api_travel_trips(limit: int = 50):
    """旅行（家から離れていた期間）の履歴を新しい順に返す。"""
    return {"trips": _fetch_trips(limit)}
