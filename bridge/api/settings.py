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
    OPENAI_RESPONSES_MODEL, STT_MODEL, OPENAI_RESPONSES_REASONING_EFFORT,
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


# 会話生成に使うモデル。低価格帯のみを選択肢にする。
# latency_ms / tool_ms は実測値（Responses API・ツール定義込み・gpt-5 系は effort=none）。
# 価格は 1M トークンあたりの USD。数値は目安で、OpenAI 側の改定で変わりうる。
_LLM_MODEL_OPTIONS = [
    {
        "value": "gpt-4.1-nano", "label": "gpt-4.1-nano",
        "price_in": 0.10, "price_out": 0.40,
        "latency_ms": 1279, "tool_ms": 4307,
        "note": "最安。ただし Web 検索に非対応。通知用に向く",
    },
    {
        "value": "gpt-4o-mini", "label": "gpt-4o-mini",
        "price_in": 0.15, "price_out": 0.60,
        "latency_ms": 1955, "tool_ms": 2110,
        "note": "安価で安定。ツール往復が速い",
    },
    {
        "value": "gpt-5.6-luna", "label": "gpt-5.6-luna",
        "price_in": 0.20, "price_out": 1.20,
        "latency_ms": 951, "tool_ms": 2544,
        "note": "新しい世代。応答が速い。推論の深さは下の設定で調整",
    },
    {
        "value": "gpt-4.1-mini", "label": "gpt-4.1-mini",
        "price_in": 0.40, "price_out": 1.60,
        "latency_ms": 1202, "tool_ms": 2812,
        "note": "この中では高め。バランス型",
    },
]

# 音声認識モデル。価格は音声1分あたりの USD、latency_ms は実測平均。
_STT_MODEL_OPTIONS = [
    {
        "value": "gpt-4o-mini-transcribe", "label": "gpt-4o-mini-transcribe",
        "price_min": 0.003, "latency_ms": 828,
        "note": "最安。ただし実測のばらつきが大きい",
    },
    {
        "value": "gpt-transcribe", "label": "gpt-transcribe",
        "price_min": 0.0045, "latency_ms": 619,
        "note": "最も速く安定。誤り率も低い（2026-07 の新モデル）",
    },
    {
        "value": "gpt-4o-transcribe", "label": "gpt-4o-transcribe",
        "price_min": 0.006, "latency_ms": 748,
        "note": "従来の上位モデル",
    },
]

_EDITABLE_SETTINGS = {
    "openai_responses_model": {
        "label": "会話モデル（LLM）",
        "description": (
            "スタックちゃんの返答を生成するモデル。低価格帯のみを選択肢にしています。"
            "レイテンシは同一条件での実測値で、通信状況により上下します。"
            "gpt-4.1-nano は Web 検索に対応していないため、会話モデルに選ぶと"
            "Web 検索を有効にしていても使われません（通知モデルには問題ありません）。"
        ),
        "env_fallback": lambda: OPENAI_RESPONSES_MODEL,
        "type": "model_select",
        "options": _LLM_MODEL_OPTIONS,
        "price_unit": "1Mトークン",
    },
    "openai_responses_model_notify": {
        "label": "通知モデル（地震・天気・タイマーなど）",
        "description": (
            "地震・天気・タイマー・カレンダーなどの短い一言を作るモデル。"
            "道具も会話履歴も使わない単純な生成なので、会話モデルより安いものを選べます。"
            "会話の要約にも使われます。未設定なら会話モデルと同じものを使います。"
        ),
        "env_fallback": lambda: OPENAI_RESPONSES_MODEL,
        "type": "model_select",
        "options": _LLM_MODEL_OPTIONS,
        "price_unit": "1Mトークン",
    },
    "openai_responses_reasoning_effort": {
        "label": "推論の深さ（gpt-5 系のみ）",
        "description": (
            "gpt-5 系モデルが返答前にどれだけ考えるか。深くするほど遅く、"
            "隠れた推論トークンのぶん課金も増えます。会話用途では none で十分です。"
            "gpt-4 系を選んでいる場合この設定は送信されません。"
        ),
        "env_fallback": lambda: OPENAI_RESPONSES_REASONING_EFFORT,
        "type": "select",
        "options": [
            {"value": "none",   "label": "none — 推論なし（推奨・最速）"},
            {"value": "low",    "label": "low — 少し考える"},
            {"value": "medium", "label": "medium — 標準（API既定）"},
            {"value": "high",   "label": "high — じっくり考える"},
        ],
    },
    "stt_model": {
        "label": "音声認識モデル（STT）",
        "description": "話しかけた音声を文字にするモデル。価格は音声1分あたりです。",
        "env_fallback": lambda: STT_MODEL,
        "type": "model_select",
        "options": _STT_MODEL_OPTIONS,
        "price_unit": "音声1分",
    },
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
        if "price_unit" in meta:
            entry["price_unit"] = meta["price_unit"]
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
