"""Earthquake and tsunami notification for Stack-chan (P2P地震情報 integration).

Functions that call publish_speak, chat_with_llm, etc. access them lazily
via sys.modules["main"] to avoid circular imports.
"""
import asyncio
import json
import logging
import math
import re
import sys
from datetime import datetime, timedelta

import aiohttp

from bridge.config import (
    _JST,
    P2PQUAKE_ENABLED, P2PQUAKE_WS_URL, P2PQUAKE_MIN_SCALE,
    P2PQUAKE_TSUNAMI_TARGET_AREAS,
)

logger = logging.getLogger(__name__)

# P2P地震情報 WebSocket 接続状態
_p2pquake_ws_status: dict = {
    "connected": False,
    "connected_at": None,
    "disconnected_at": None,
    "reconnect_count": 0,
    "last_event_at": None,
    "last_event_code": None,
    "last_event_id": None,
}
_p2pquake_recent_events: list[dict] = []  # 直近50件（受信 → フィルタ結果まで）
_P2PQUAKE_EVENT_BUFFER = 50

# ── 設置場所ユーティリティ ────────────────────────────────────────────────────

_PREF_RE = re.compile(r"^(東京都|北海道|(?:京都|大阪)府|[^\s]{2,4}?県)")


def _extract_pref(title: str) -> str:
    m = _PREF_RE.match(title)
    return m.group(1) if m else ""



# 都道府県 → 気象庁 津波予報区 マッピング（P2P地震情報 API の area.name と一致させること）
_PREF_TSUNAMI_AREAS: dict[str, list[str]] = {
    "北海道":   ["北海道太平洋沿岸東部", "北海道太平洋沿岸中部", "北海道太平洋沿岸西部",
                 "北海道日本海沿岸南部", "北海道日本海沿岸北部", "北海道オホーツク海沿岸"],
    "青森県":   ["青森県太平洋沿岸", "青森県日本海沿岸"],
    "岩手県":   ["岩手県"],
    "宮城県":   ["宮城県"],
    "秋田県":   ["秋田県"],
    "山形県":   ["山形県"],
    "福島県":   ["福島県"],
    "茨城県":   ["茨城県"],
    "千葉県":   ["千葉県九十九里・外房", "千葉県内房"],
    "東京都":   ["伊豆諸島", "小笠原諸島"],
    "神奈川県": ["相模湾・三浦半島"],
    "新潟県":   ["新潟県上越地方", "新潟県中越地方", "新潟県下越地方", "粟島"],
    "富山県":   ["富山県"],
    "石川県":   ["石川県能登", "石川県加賀"],
    "福井県":   ["福井県"],
    "静岡県":   ["静岡県"],
    "愛知県":   ["愛知県外海", "愛知県内海"],
    "三重県":   ["三重県北部", "三重県南部"],
    "京都府":   ["京都府"],
    "大阪府":   ["大阪府"],
    "兵庫県":   ["兵庫県北部", "兵庫県南部"],
    "和歌山県": ["和歌山県"],
    "鳥取県":   ["鳥取県"],
    "島根県":   ["島根県出雲・石見", "島根県隠岐"],
    "岡山県":   ["岡山県"],
    "広島県":   ["広島県"],
    "山口県":   ["山口県北部", "山口県西部"],
    "徳島県":   ["徳島県"],
    "香川県":   ["香川県"],
    "愛媛県":   ["愛媛県宇和海沿岸", "愛媛県瀬戸内海沿岸"],
    "高知県":   ["高知県"],
    "福岡県":   ["福岡県瀬戸内海沿岸", "福岡県日本海沿岸"],
    "佐賀県":   ["佐賀県北部"],
    "長崎県":   ["長崎県西方", "長崎県島原半島"],
    "熊本県":   ["熊本県天草・芦北", "熊本県有明・八代海"],
    "大分県":   ["大分県中部", "大分県北部", "大分県南部"],
    "宮崎県":   ["宮崎県"],
    "鹿児島県": ["鹿児島県東部", "鹿児島県西部", "種子島・屋久島地方", "奄美群島・トカラ列島"],
    "沖縄県":   ["沖縄本島地方", "大東島地方", "宮古島・八重山地方"],
    # 内陸県（海なし）は空リスト → 津波エリア設定なし
    "埼玉県": [], "栃木県": [], "群馬県": [], "山梨県": [],
    "長野県": [], "岐阜県": [], "奈良県": [], "滋賀県": [],
}


def _apply_tsunami_areas_from_pref(pref: str) -> None:
    """都道府県から津波予報区を自動設定する。内陸県の場合は設定しない。"""
    areas = _PREF_TSUNAMI_AREAS.get(pref)
    if areas is None:
        return
    if areas:
        sys.modules["main"]._set_setting("p2pquake_tsunami_areas", ",".join(areas))
        logger.info("tsunami areas auto-set from pref=%s: %s", pref, areas)
    else:
        logger.info("tsunami areas: %s is inland, no coastal areas", pref)


async def _fetch_and_save_timezone(lat: float, lon: float) -> None:
    """Open-Meteo からタイムゾーン文字列を取得して location_timezone に保存する。"""
    try:
        resp = await _http_client.get(
            "https://api.open-meteo.com/v1/forecast",
            params={"latitude": lat, "longitude": lon, "timezone": "auto", "forecast_days": 1},
            timeout=8,
        )
        resp.raise_for_status()
        tz_name = resp.json().get("timezone", "Asia/Tokyo")
        sys.modules["main"]._set_setting("location_timezone", tz_name)
        logger.info("location_timezone updated: %s", tz_name)
    except Exception:
        logger.debug("timezone fetch skipped (Open-Meteo unavailable)")


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """2点間の距離を km で返す（Haversine 公式）。"""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def _get_local_scale(data: dict) -> int | None:
    """設置場所の都道府県で観測された最大震度コードを返す。
    全国モードまたは設置場所未設定なら全国最大値を返す。
    設置場所が設定されていてその県に観測点がなければ None（通知しない）。
    """
    if sys.modules["main"]._get_setting("p2pquake_nationwide", "false") == "true":
        return data["earthquake"]["maxScale"]
    pref = sys.modules["main"]._get_setting("location_pref", "")
    if not pref:
        return data["earthquake"]["maxScale"]
    local = [p for p in data.get("points", []) if p.get("pref") == pref]
    if not local:
        return None
    return max(p["scale"] for p in local)


# ── P2P地震情報 WebSocket ─────────────────────────────────────────────────────

_SCALE_MAP = {
    -1: "震度不明",
    10: "震度1", 20: "震度2", 30: "震度3",
    40: "震度4", 45: "震度4強",
    50: "震度5弱", 55: "震度5強",
    60: "震度6弱", 65: "震度6強",
    70: "震度7",
}
_TSUNAMI_GRADE_ORDER = {"Watch": 1, "Warning": 2, "MajorWarning": 3}
_TSUNAMI_GRADE_LABEL = {
    "Watch": "津波注意報",
    "Warning": "津波警報",
    "MajorWarning": "大津波警報",
}


def _scale_to_str(scale: int) -> str:
    return _SCALE_MAP.get(scale, f"震度{scale}")


def _eq_already_seen(earthquake_id: str) -> bool:
    row = sys.modules["bridge.core.db"]._db_conn.execute(
        "SELECT 1 FROM earthquake_log WHERE earthquake_id = ?", (earthquake_id,)
    ).fetchone()
    return row is not None


def _mark_eq_seen(earthquake_id: str, place: str, scale: int, magnitude: float) -> None:
    with sys.modules["bridge.core.db"]._db_lock:
        sys.modules["bridge.core.db"]._db_conn.execute(
            "INSERT OR IGNORE INTO earthquake_log "
            "(earthquake_id, place, scale, magnitude, notified_at) "
            "VALUES (?, ?, ?, ?, datetime('now'))",
            (earthquake_id, place, scale, magnitude),
        )
        sys.modules["bridge.core.db"]._db_conn.commit()


def _get_tsunami_grade(area: str) -> str | None:
    row = sys.modules["bridge.core.db"]._db_conn.execute(
        "SELECT grade FROM tsunami_state WHERE area = ?", (area,)
    ).fetchone()
    return row[0] if row else None


def _save_tsunami_grade(area: str, grade: str) -> None:
    with sys.modules["bridge.core.db"]._db_lock:
        sys.modules["bridge.core.db"]._db_conn.execute(
            "INSERT INTO tsunami_state (area, grade, updated_at) VALUES (?, ?, datetime('now')) "
            "ON CONFLICT(area) DO UPDATE SET grade=excluded.grade, updated_at=excluded.updated_at",
            (area, grade),
        )
        sys.modules["bridge.core.db"]._db_conn.commit()


def _clear_tsunami_state() -> None:
    with sys.modules["bridge.core.db"]._db_lock:
        sys.modules["bridge.core.db"]._db_conn.execute("DELETE FROM tsunami_state")
        sys.modules["bridge.core.db"]._db_conn.commit()


def _build_earthquake_fixed_text(data: dict, local_scale: int) -> str:
    eq    = data["earthquake"]
    place = eq["hypocenter"]["name"]
    mag   = eq["hypocenter"]["magnitude"]
    tsun  = eq["domesticTsunami"]
    scale = local_scale  # 設置場所の都道府県での震度を使う

    scale_str = _scale_to_str(scale)
    info = f"{place}でマグニチュード{mag}の地震が発生しました。"

    if scale <= 20:
        msg = f"最大{scale_str}です。"
    elif scale <= 30:
        msg = f"最大{scale_str}です。落下物に気をつけてください。"
    elif scale <= 45:
        msg = f"最大{scale_str}です。棚の物が落ちることがあります。揺れが収まるまで様子を見てください。"
    elif scale <= 50:
        msg = f"【緊急地震速報】最大{scale_str}です。今すぐ低い姿勢をとり、頭を守ってください。"
    elif scale <= 55:
        msg = (f"【緊急地震速報】最大{scale_str}です。"
               "固定されていない家具が倒れることがあります。今すぐ安全な場所に身を隠してください。")
    elif scale <= 60:
        msg = (f"【緊急地震速報】最大{scale_str}です。"
               "非常に危険です。今すぐ頭を守り、揺れが収まるまで動かないでください。")
    elif scale <= 65:
        msg = f"【最大警戒】最大{scale_str}です。今すぐ頭を守り、絶対に動かないでください。"
    else:
        msg = ("【最大警戒・震度7】極めて激しい揺れです。"
               "頭を守り、揺れが完全に収まるまで待ってください。")

    tsunami_suffix = ""
    if tsun == "Watch":
        tsunami_suffix = "津波注意報が発令されています。海岸や川には近づかないでください。"
    elif tsun == "Warning":
        tsunami_suffix = "【津波警報】海岸・川から直ちに離れてください。"
    elif tsun == "Checking":
        tsunami_suffix = "津波の有無を確認中です。海岸には近づかないでください。"

    return info + msg + tsunami_suffix


async def _p2p_speak(text: str, source: str, priority: str) -> None:
    import uuid
    # 防災通知は感情ラベルを捨てて常に neutral で発話する
    _main = sys.modules["main"]
    _, clean_text = _main._parse_expression(text)
    speaker_id, stackchan_expr = _main._resolve_expression("neutral")
    audio_url, stream_url = await _main.resolve_audio_url(clean_text, speaker_id)
    _main.publish_speak(audio_url, stream_url, clean_text,
                  source=source, priority=priority,
                  request_id=str(uuid.uuid4()), expression=stackchan_expr)


async def _handle_earthquake(data: dict) -> None:
    earthquake_id = data.get("id") or data.get("_id") or ""
    issue_type = data.get("issue", {}).get("type")

    # DetailScale（確定値）または ScalePrompt（速報）を対象にする
    # Destination（震源のみ）は震度情報がないためスキップ
    if issue_type not in ("DetailScale", "ScalePrompt"):
        logger.info("earthquake skip: id=%s reason=unsupported_type type=%s", earthquake_id, issue_type)
        return

    if not earthquake_id:
        logger.warning("earthquake skip: reason=no_id type=%s", issue_type)
        return

    # DetailScale は ScalePrompt より優先。同一地震で ScalePrompt 通知済みでも DetailScale は再通知する
    dedup_id = f"{earthquake_id}:{issue_type}"
    if _eq_already_seen(dedup_id):
        logger.info("earthquake skip: id=%s reason=already_seen type=%s", earthquake_id, issue_type)
        return

    local_scale = _get_local_scale(data)
    if local_scale is None:
        pref = sys.modules["main"]._get_setting("location_pref", "")
        logger.info("earthquake skip: id=%s reason=no_observation_in_pref pref=%s", earthquake_id, pref)
        return

    min_scale = int(sys.modules["main"]._get_setting("p2pquake_min_scale", str(P2PQUAKE_MIN_SCALE)))
    if local_scale < min_scale:
        logger.info("earthquake skip: id=%s reason=below_min_scale local=%s min=%s", earthquake_id, local_scale, min_scale)
        return

    eq    = data["earthquake"]
    place = eq.get("hypocenter", {}).get("name") or "不明"
    mag   = eq.get("hypocenter", {}).get("magnitude") or -1

    # 震源距離を計算
    hypo_lat = eq.get("hypocenter", {}).get("latitude")
    hypo_lon = eq.get("hypocenter", {}).get("longitude")
    dist_km: float | None = None
    try:
        home_lat = float(sys.modules["main"]._get_setting("location_lat", ""))
        home_lon = float(sys.modules["main"]._get_setting("location_lon", ""))
        if hypo_lat and hypo_lon:
            dist_km = _haversine_km(home_lat, home_lon, float(hypo_lat), float(hypo_lon))
    except (ValueError, TypeError):
        pass

    fixed_text = _build_earthquake_fixed_text(data, local_scale)

    _mark_eq_seen(dedup_id, place, local_scale, mag)
    logger.info("earthquake notify: id=%s type=%s place=%s scale=%s dist_km=%s",
                earthquake_id, issue_type, place, local_scale,
                f"{dist_km:.0f}" if dist_km is not None else "unknown")

    # ① 固定テキストを即時発話
    await _p2p_speak(fixed_text, source="earthquake", priority="high")

    # ② LLM コメントを非同期で続けて発話
    asyncio.create_task(_earthquake_llm_comment(place, _scale_to_str(local_scale), mag, dist_km))


async def _earthquake_llm_comment(place: str, scale_str: str, mag: float, dist_km: float | None = None) -> None:
    if dist_km is not None:
        if dist_km < 50:
            dist_context = f"震源はここからわずか約{dist_km:.0f}kmと非常に近い場所です。"
        elif dist_km < 150:
            dist_context = f"震源はここから約{dist_km:.0f}kmと比較的近い場所です。"
        elif dist_km < 400:
            dist_context = f"震源はここから約{dist_km:.0f}kmほど離れた場所です。"
        else:
            dist_context = f"震源はここから約{dist_km:.0f}kmと遠い場所です。"
    else:
        dist_context = ""

    prompt = (
        f"先ほど地震速報をお知らせしました（{place} / {scale_str} / M{mag}）。{dist_context}"
        "情報の繰り返しは不要です。距離感や規模に合わせた、家族への短い一言コメントを1〜2文で追加してください。"
        "遠い地震なら安心させる言葉を、近い地震や大きな地震なら気をつけるよう促してください。"
    )
    try:
        comment = await sys.modules["main"].chat_with_llm(prompt, session_key="family", use_functions=False)
        await _p2p_speak(comment, source="earthquake_comment", priority="normal")
    except Exception:
        logger.exception("earthquake LLM comment failed")


async def _handle_tsunami(data: dict) -> None:
    earthquake_id = data["id"]

    if data.get("cancelled"):
        cancel_key = earthquake_id + ":cancelled"
        if _eq_already_seen(cancel_key):
            return
        _mark_eq_seen(cancel_key, "tsunami_cancel", 0, 0.0)
        _clear_tsunami_state()
        fixed_text = "津波予報が解除されました。海岸付近の方は安全を確認してから戻るようにしてください。"
        await _p2p_speak(fixed_text, source="tsunami", priority="high")
        asyncio.create_task(_tsunami_llm_comment(fixed_text, cancelled=True))
        return

    areas_str    = sys.modules["main"]._get_setting("p2pquake_tsunami_areas", ",".join(P2PQUAKE_TSUNAMI_TARGET_AREAS))
    target_areas = set(a.strip() for a in areas_str.split(",") if a.strip())
    for area in data.get("areas", []):
        if area["name"] not in target_areas:
            logger.info("tsunami skip: id=%s area=%s reason=not_in_target", earthquake_id, area["name"])
            continue

        new_grade     = area["grade"]
        current_grade = _get_tsunami_grade(area["name"])
        new_order     = _TSUNAMI_GRADE_ORDER.get(new_grade, 0)
        current_order = _TSUNAMI_GRADE_ORDER.get(current_grade or "", 0)

        if new_order <= current_order:
            logger.info("tsunami skip: id=%s area=%s reason=not_escalating current=%s new=%s",
                        earthquake_id, area["name"], current_grade, new_grade)
            continue

        _save_tsunami_grade(area["name"], new_grade)
        grade_label = _TSUNAMI_GRADE_LABEL.get(new_grade, new_grade)
        height  = area.get("maxHeight", {}).get("description", "")
        arrival = area.get("firstHeight", {}).get("arrivalTime", "")

        fixed_text = f"相模湾・三浦半島に{grade_label}が発令されました。"
        if height:
            fixed_text += f"予想される津波の高さは{height}です。"
        if arrival:
            fixed_text += f"第一波到達予想は{arrival}です。"
        fixed_text += "海岸・川から直ちに離れてください。"

        logger.info("tsunami notify: area=%s grade=%s", area["name"], new_grade)
        await _p2p_speak(fixed_text, source="tsunami", priority="high")
        asyncio.create_task(_tsunami_llm_comment(fixed_text, cancelled=False))


async def _tsunami_llm_comment(fixed_text: str, cancelled: bool) -> None:
    if cancelled:
        prompt = "津波予報が解除されました。安堵の一言を1文で、話し言葉で。"
    else:
        prompt = (
            f"以下の津波警報をお知らせしました。緊急の一言コメントを1文で。情報の繰り返し不要。\n{fixed_text}"
        )
    try:
        comment = await sys.modules["main"].chat_with_llm(prompt, session_key="family", use_functions=False)
        await _p2p_speak(comment, source="tsunami_comment", priority="normal")
    except Exception:
        logger.exception("tsunami LLM comment failed")


async def _handle_eew(data: dict) -> None:
    # code=554: 緊急地震速報（発表検出）。EEW チャイム検出。LLMなしで即発話のみ。
    # code=556: 緊急地震速報（警報）。予測震度・到達時刻つき。同様に即発話。
    eew_key = data.get("id", str(data.get("code", ""))) + ":eew"
    if _eq_already_seen(eew_key):
        return
    _mark_eq_seen(eew_key, "eew", 0, 0.0)
    text = "緊急地震速報！強い揺れが来る可能性があります。今すぐ身を低くして頭を守ってください。"
    await _p2p_speak(text, source="eew", priority="high")


async def _unknown_p2p_llm(data: dict) -> None:
    prompt = (
        "以下はP2P地震情報APIから届いた防災通知JSONです。\n"
        "内容を読み取り、家族に向けて簡潔に伝えてください。\n"
        "重要な情報は省かず、1〜3文の話し言葉にしてください。\n\n"
        f"{json.dumps(data, ensure_ascii=False, indent=2)}"
    )
    try:
        text = await sys.modules["main"].chat_with_llm(prompt, session_key="family", use_functions=False)
        await _p2p_speak(text, source="p2pquake_unknown", priority="normal")
    except Exception:
        logger.exception("unknown p2p LLM failed")


def _p2pquake_log_event(code: int | None, eid: str, summary: str, action: str) -> None:
    """受信イベントを即時ログ出力 + メモリバッファに積む。"""
    now = datetime.now(_JST).isoformat()
    logger.info("p2pquake recv: code=%s id=%s action=%s summary=%s", code, eid, action, summary)
    entry = {"time": now, "code": code, "id": eid, "summary": summary, "action": action}
    _p2pquake_recent_events.append(entry)
    if len(_p2pquake_recent_events) > _P2PQUAKE_EVENT_BUFFER:
        _p2pquake_recent_events.pop(0)
    _p2pquake_ws_status["last_event_at"]   = now
    _p2pquake_ws_status["last_event_code"] = code
    _p2pquake_ws_status["last_event_id"]   = eid


async def _p2pquake_ws_loop() -> None:
    backoff = 1
    seen_ids: set[str] = set()  # 再接続時の直近重複対策（メモリ内）

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(P2PQUAKE_WS_URL) as ws:
                    now = datetime.now(_JST).isoformat()
                    _p2pquake_ws_status["connected"]      = True
                    _p2pquake_ws_status["connected_at"]   = now
                    _p2pquake_ws_status["disconnected_at"] = None
                    logger.info("P2P地震情報 WebSocket connected: %s", P2PQUAKE_WS_URL)
                    backoff = 1

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            eid  = data.get("id", "")
                            code = data.get("code")

                            # ネットワーク統計・感知情報はログ不要なのでスキップ
                            if code in (555, 561, 9611):
                                continue

                            # 受信直後にサマリを作ってログ
                            if code == 551:
                                eq = data.get("earthquake", {})
                                summary = (
                                    f"{eq.get('hypocenter', {}).get('name', '?')} "
                                    f"M{eq.get('hypocenter', {}).get('magnitude', '?')} "
                                    f"最大{_scale_to_str(eq.get('maxScale', -1))} "
                                    f"type={data.get('issue', {}).get('type', '?')}"
                                )
                            elif code == 552:
                                areas = [a.get("name") for a in data.get("areas", [])]
                                summary = f"cancelled={data.get('cancelled')} areas={areas}"
                            elif code == 554:
                                summary = "EEW発表検出"
                            elif code == 555:
                                # 各地域のP2P接続ピア数（感知情報ではなくネットワーク接続状況）
                                areas = data.get("areas", [])
                                total = sum(a.get("peer", 0) for a in areas)
                                summary = f"接続ピア数: 合計{total}クライアント ({len(areas)}地域)"
                            elif code == 556:
                                eq = data.get("earthquake", {})
                                hypo = eq.get("hypocenter", {})
                                summary = (
                                    f"EEW警報: {hypo.get('name', '?')} "
                                    f"M{hypo.get('magnitude', '?')} "
                                    f"最大予測{_scale_to_str(eq.get('maxScale', -1))}"
                                )
                            elif code == 561:
                                areas = data.get("areas", [])
                                total = sum(a.get("peer", 0) for a in areas)
                                area_parts = [
                                    f"id={a['id']}:{a['peer']}件"
                                    for a in sorted(areas, key=lambda x: x.get("peer", 0), reverse=True)[:5]
                                ]
                                summary = f"感知情報(ユーザー報告): 合計{total}件 [{', '.join(area_parts)}]"
                            elif code == 9611:
                                summary = (
                                    f"感知情報解析結果: "
                                    f"confidence={data.get('confidence', '?')} "
                                    f"count={data.get('count', '?')}"
                                )
                            else:
                                summary = str(data)[:120]

                            # code 551 は issue.type ごとに別キーで管理
                            # （ScalePrompt → DetailScale の順で別メッセージが来るため）
                            issue_type_key = data.get("issue", {}).get("type", "") if code == 551 else ""
                            dedup_key = f"{eid}:{issue_type_key}" if issue_type_key else eid
                            if eid and dedup_key in seen_ids:
                                _p2pquake_log_event(code, eid, summary, "skip:dup_memory")
                                continue
                            if eid:
                                seen_ids.add(dedup_key)
                                if len(seen_ids) > 500:
                                    seen_ids.pop()

                            _p2pquake_log_event(code, eid, summary, "dispatch")

                            if code == 551:
                                asyncio.create_task(_handle_earthquake(data))
                            elif code == 552:
                                asyncio.create_task(_handle_tsunami(data))
                            elif code == 554:
                                asyncio.create_task(_handle_eew(data))  # EEW発表検出
                            elif code == 555:
                                pass  # 接続ピア数: ログのみ
                            elif code == 556:
                                asyncio.create_task(_handle_eew(data))  # EEW警報（予測震度つき）
                            elif code == 561:
                                pass  # 地震感知情報（ユーザー報告）: ログのみ
                            elif code == 9611:
                                pass  # 感知情報解析結果: ログのみ
                            else:
                                asyncio.create_task(_unknown_p2p_llm(data))

                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            logger.warning("p2pquake ws closed/error, reconnecting")
                            break

        except Exception:
            logger.exception("p2pquake ws error, retry in %ds", backoff)

        _p2pquake_ws_status["connected"]       = False
        _p2pquake_ws_status["disconnected_at"] = datetime.now(_JST).isoformat()
        _p2pquake_ws_status["reconnect_count"] += 1
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)


