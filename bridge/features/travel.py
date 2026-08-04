"""Location history and travel (trip) detection for Stack-chan.

Hooked into every location-resolution endpoint in bridge/api/settings.py.
Compares each new location against the configured "home" location; when the
distance crosses TRAVEL_DISTANCE_THRESHOLD_KM, starts/ends a trip record and
speaks a short comment so the event is remembered in conversation (session_key
= MQTT_DEVICE_ID, the same thread as voice conversations).

publish_speak, chat_with_llm etc. are accessed lazily via sys.modules["main"]
to avoid circular imports.
"""
import asyncio
import math
import sys
import uuid
import logging

from bridge.config import MQTT_DEVICE_ID, TRAVEL_DISTANCE_THRESHOLD_KM
from bridge.core.db import (
    _save_location_history, _get_active_trip,
    _start_trip, _update_trip_progress, _end_trip,
)

logger = logging.getLogger(__name__)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return r * 2 * math.asin(math.sqrt(a))


def _get_home_location() -> tuple[float, float, str] | None:
    _main = sys.modules.get("main")
    if _main is None:
        return None
    lat = _main._get_setting("location_home_lat", "")
    lon = _main._get_setting("location_home_lon", "")
    title = _main._get_setting("location_home_title", "")
    if not lat or not lon:
        return None
    return float(lat), float(lon), title


async def _speak_travel_comment(prompt: str, source: str) -> None:
    _main = sys.modules["main"]
    try:
        reply = await _main.chat_with_llm(
            prompt,
            system_prompt_append=(
                "これは設置場所の変化を検知した通知です。"
                "スタックちゃんとして家族に向けて短く一言だけ声かけしてください。依頼者への返答にはしないでください。"
            ),
            session_key=MQTT_DEVICE_ID,
            use_functions=False,
            purpose="notify",
        )
    except Exception:
        logger.exception("travel comment LLM failed")
        return
    expression, clean_text = _main._parse_expression(reply)
    speaker_id, stackchan_expr = _main._resolve_expression(expression)
    try:
        audio_url, streaming_url = await _main.resolve_audio_url(clean_text, speaker_id)
        req_id = str(uuid.uuid4())
        _main.publish_speak(audio_url, streaming_url, clean_text, source, "normal", req_id, stackchan_expr)
    except Exception:
        logger.exception("travel comment speak failed")


async def _record_location_and_check_travel(lat: float, lon: float, title: str, pref: str, source: str) -> None:
    """位置解決のたびに呼ぶ。履歴に記録し、「家」からの距離をもとに旅行の開始・終了を検知する。"""
    home = _get_home_location()
    distance_km: float | None = None
    is_away: bool | None = None
    if home is not None:
        home_lat, home_lon, _home_title = home
        distance_km = _haversine_km(home_lat, home_lon, lat, lon)
        is_away = distance_km > TRAVEL_DISTANCE_THRESHOLD_KM

    _save_location_history(
        lat=lat, lon=lon, title=title, pref=pref, source=source,
        distance_from_home_km=distance_km, is_away=is_away,
    )

    if home is None:
        return  # 「家」未設定では旅行判定できない

    active_trip = _get_active_trip()
    if is_away and active_trip is None:
        _start_trip(title=title, max_distance_km=distance_km)
        logger.info("trip started: title=%s distance_km=%.1f", title, distance_km)
        asyncio.create_task(_speak_travel_comment(
            f"いつもと違う場所（{title}）にいることに気づきました。1〜2文で驚きと楽しみを込めて話してください。",
            "travel_start",
        ))
    elif is_away and active_trip is not None:
        _update_trip_progress(active_trip["id"], distance_km)
    elif not is_away and active_trip is not None:
        _end_trip(active_trip["id"])
        logger.info("trip ended: title=%s", active_trip["title"])
        asyncio.create_task(_speak_travel_comment(
            f"「{active_trip['title']}」への外出から、いつもの場所（家）に戻ってきたようです。"
            "おかえりの一言を1文で。",
            "travel_end",
        ))
