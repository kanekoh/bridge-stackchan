"""LLM tool schemas and execution logic.

Circular-import note: _execute_tool and _handle_function_calls call functions
defined in main.py (chat_with_llm, publish_speak, _register_timer, etc.).
These are accessed lazily via sys.modules["main"] to avoid import cycles.
"""
import asyncio
import json
import logging
import sys
from datetime import datetime, timedelta

from bridge.config import DISABLE_TOOLS, _JST

logger = logging.getLogger(__name__)

# ── Tool Schemas ───────────────────────────────────────────────────────────────

_TIMER_TOOLS = [
    {
        "type": "function",
        "name": "set_timer",
        "description": (
            "タイマーを設定する。指定した秒数後にStack-chanが声で知らせる。"
            "スヌーズ秒数を指定すると、発火後に一度だけ再通知できる。"
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "label": {
                    "type": "string",
                    "description": "タイマーのラベル（例：宿題確認、おやつの時間）",
                },
                "seconds": {
                    "type": "integer",
                    "description": "何秒後に発火するか",
                },
                "snooze_seconds": {
                    "type": "integer",
                    "description": "スヌーズの秒数（省略可）。指定すると発火後にもう一度声かけする",
                },
            },
            "required": ["label", "seconds"],
        },
    },
    {
        "type": "function",
        "name": "list_timers",
        "description": "現在セットされているタイマーの一覧と残り時間を返す。",
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
]

_CALENDAR_TOOLS = [
    {
        "type": "function",
        "name": "get_upcoming_items",
        "description": (
            "DBに保存されたGoogleカレンダーの予定・タスクを取得する。"
            "「次の予定は？」「今日の予定は？」「しおりのタスクは？」などの質問に答えるために使う。"
            "取得した結果をもとに、自分の言葉で答えること。"
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "person": {
                    "type": "string",
                    "description": "絞り込む人の名前（例: しおり、パパ）。省略すると全員分を返す",
                },
                "type": {
                    "type": "string",
                    "enum": ["all", "event", "task"],
                    "description": "取得する種別。省略時は all（予定とタスク両方）",
                },
                "days": {
                    "type": "integer",
                    "description": "何日先まで取得するか（1〜14、省略時は3）",
                },
            },
            "required": [],
        },
    },
]

_MESSAGE_TOOLS = [
    {
        "type": "function",
        "name": "get_pending_messages",
        "description": (
            "家族から預かっている未読の伝言を取得する。"
            "「伝言ある？」「なにか連絡来てた？」「メッセージある？」「なにか残ってた？」"
            "などの質問に答えるために使う。"
            "直接聞かれた場合は「そういえば」などの前置きは不要。"
            "「○○からの伝言があるよ！」と直接伝えること。"
        ),
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
]

_ALERT_TOOLS = [
    {
        "type": "function",
        "name": "get_recent_alerts",
        "description": (
            "直近の地震・津波の通知履歴と現在の津波警報状況を返す。"
            "「さっきの地震は？」「最近地震あった？」「津波情報は？」"
            "「もう一回教えて」「また揺れた？」などの質問に答えるために使う。"
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "hours": {
                    "type": "integer",
                    "description": "何時間前までの情報を取得するか（1〜72、省略時は24）",
                },
            },
            "required": [],
        },
    },
]

_WEATHER_TOOLS = [
    {
        "type": "function",
        "name": "get_weather",
        "description": (
            "現在の天気・気温・降水量、および今後数日間の天気予報を取得する。"
            "「今日の天気は？」「明日雨降る？」「東京の天気は？」などに答えるために使う。"
            "場所を指定しなければ、スタックちゃんが置かれている場所の天気を返す。"
            "天気をウェブ検索で調べる代わりに必ずこのツールを使うこと。"
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "location": {
                    "type": "string",
                    "description": (
                        "天気を取得したい場所の名前（例：東京、大阪、札幌）。"
                        "省略するとスタックちゃんの設置場所の天気を返す。"
                    ),
                },
                "forecast_days": {
                    "type": "integer",
                    "description": "何日分の予報を取得するか（0=現在のみ、1=今日、3=3日間、最大7）。省略時は3。",
                },
            },
            "required": [],
        },
    },
]

_MEMORY_TOOLS = [
    {
        "type": "function",
        "name": "recall",
        "description": (
            "昔の出来事や、前に聞いた話を思い出す。"
            "「夏休みどこ行ったっけ？」「前に話したあれ何だっけ？」など、"
            "少し前のことを聞かれて手元の記憶では答えられないときに使う。"
            "直近の話題や今わかっていることには使わない。"
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "思い出したい内容を簡潔に（例：夏休みの旅行、しおりの好きな食べ物）",
                },
            },
            "required": ["query"],
        },
    },
]

# ON_DEMAND モード時のみ Pass 1 のツール一覧に追加される。
# LLM がこれを呼ぶと notify_context に enable_web_search フラグが立ち、
# 次のループで本物の web_search_preview に差し替えられる。
_REQUEST_WEB_SEARCH_TOOL = {
    "type": "function",
    "name": "request_web_search",
    "description": (
        "最新ニュース・株価・スポーツ結果など、"
        "学習データに含まれていない可能性が高い事実を答えるために"
        "Web 検索が必要なときだけ呼び出すこと。"
        "天気・気温・降水量は get_weather ツールを使うこと（Web 検索不要）。"
        "雑談・感情表現・既に知っている内容では呼ばないこと。"
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "検索したい内容を簡潔に"},
        },
        "required": ["query"],
    },
}


# ── Tool implementations ───────────────────────────────────────────────────────

async def _tool_get_weather(args: dict) -> dict:
    """天気ツールの実装: Open-Meteo で天気を取得して LLM が読みやすい形式で返す。"""
    _main = sys.modules["main"]
    _http_client = _main._http_client
    _get_setting = _main._get_setting
    _WMO_DESC = _main._WMO_DESC

    location_name: str = args.get("location", "")
    forecast_days: int = min(max(int(args.get("forecast_days", 3)), 0), 7)

    if location_name:
        # 場所名 → Nominatim でジオコーディング
        try:
            resp = await _http_client.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": location_name, "format": "json", "limit": 1, "accept-language": "ja"},
                headers={"User-Agent": "bridge-stackchan/1.0"},
                timeout=8,
            )
            resp.raise_for_status()
            hits = resp.json()
            if not hits:
                return {"status": "error", "message": f"「{location_name}」の場所が見つかりませんでした"}
            lat = float(hits[0]["lat"])
            lon = float(hits[0]["lon"])
            display_name = hits[0].get("display_name", location_name).split(",")[0]
        except Exception as e:
            logger.warning("Nominatim geocoding failed: %s", e)
            return {"status": "error", "message": f"場所の検索に失敗しました: {e}"}
    else:
        lat_str = _get_setting("location_lat", "")
        lon_str = _get_setting("location_lon", "")
        if not lat_str or not lon_str:
            return {"status": "error", "message": "設置場所が未設定です。場所を指定して聞いてください。"}
        lat = float(lat_str)
        lon = float(lon_str)
        display_name = _get_setting("location_title", _get_setting("location_pref", "設置場所"))

    try:
        params: dict = {
            "latitude": lat,
            "longitude": lon,
            "timezone": "Asia/Tokyo",
            "current": "temperature_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m,relative_humidity_2m",
        }
        if forecast_days > 0:
            params["daily"] = "weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum,precipitation_probability_max"
            params["forecast_days"] = forecast_days + 1  # 今日を含む日数

        resp = await _http_client.get("https://api.open-meteo.com/v1/forecast", params=params, timeout=10)
        resp.raise_for_status()
        d = resp.json()
    except Exception as e:
        logger.warning("Open-Meteo fetch failed: %s", e)
        return {"status": "error", "message": f"天気データの取得に失敗しました: {e}"}

    cur = d.get("current", {})
    result: dict = {
        "status": "ok",
        "location": display_name,
        "current": {
            "description":     _WMO_DESC.get(cur.get("weather_code", -1), "不明"),
            "temperature":     cur.get("temperature_2m"),
            "apparent_temp":   cur.get("apparent_temperature"),
            "humidity":        cur.get("relative_humidity_2m"),
            "precipitation":   cur.get("precipitation"),
            "wind_speed":      cur.get("wind_speed_10m"),
            "time":            (cur.get("time") or "")[:16],
        },
    }

    if forecast_days > 0 and "daily" in d:
        daily = d["daily"]
        times  = daily.get("time", [])
        codes  = daily.get("weather_code", [])
        t_max  = daily.get("temperature_2m_max", [])
        t_min  = daily.get("temperature_2m_min", [])
        prec   = daily.get("precipitation_sum", [])
        prob   = daily.get("precipitation_probability_max", [])
        result["forecast"] = [
            {
                "date":              times[i],
                "description":       _WMO_DESC.get(codes[i] if i < len(codes) else -1, "不明"),
                "temp_max":          t_max[i] if i < len(t_max) else None,
                "temp_min":          t_min[i] if i < len(t_min) else None,
                "precipitation_sum": prec[i]  if i < len(prec) else None,
                "rain_probability":  prob[i]  if i < len(prob) else None,
            }
            for i in range(min(forecast_days + 1, len(times)))
        ]

    logger.info("Function call get_weather: location=%s lat=%.4f lon=%.4f days=%d",
                display_name, lat, lon, forecast_days)
    return result


def _tool_get_upcoming_items(args: dict) -> dict:
    _main = sys.modules["main"]
    _db_lock = sys.modules["bridge.core.db"]._db_lock
    _db_conn = sys.modules["bridge.core.db"]._db_conn

    days = min(max(int(args.get("days", 3)), 1), 14)
    person = args.get("person")
    type_ = args.get("type", "all")

    now = datetime.now(_JST)
    until = now + timedelta(days=days)

    params: list = [now.isoformat(), until.isoformat()]
    where = ["status = 'active'", "COALESCE(start_at, due_at) >= ?", "COALESCE(start_at, due_at) <= ?"]
    if person:
        where.append("person_name = ?")
        params.append(person)
    if type_ != "all":
        where.append("type = ?")
        params.append(type_)

    sql = f"SELECT type, person_name, title, start_at, due_at, all_day FROM items WHERE {' AND '.join(where)} ORDER BY COALESCE(start_at, due_at) ASC LIMIT 20"

    with _db_lock:
        rows = _db_conn.execute(sql, params).fetchall()  # type: ignore[union-attr]

    items = []
    for row_type, row_person, title, start_at, due_at, all_day in rows:
        entry: dict = {
            "type": "イベント" if row_type == "event" else "タスク",
            "person": row_person,
            "title": title,
        }
        when = start_at or due_at
        if when:
            entry["when"] = when[:10] if all_day else when[:16].replace("T", " ")
        items.append(entry)

    logger.info("Tool get_upcoming_items: days=%d person=%s type=%s count=%d", days, person, type_, len(items))
    return {"status": "ok", "count": len(items), "items": items}


def _tool_get_recent_alerts(args: dict) -> dict:
    _main = sys.modules["main"]
    _db_lock = sys.modules["bridge.core.db"]._db_lock
    _db_conn = sys.modules["bridge.core.db"]._db_conn
    _scale_to_str = _main._scale_to_str
    _TSUNAMI_GRADE_LABEL = _main._TSUNAMI_GRADE_LABEL

    hours = min(max(int(args.get("hours", 24)), 1), 72)

    with _db_lock:
        eq_rows = _db_conn.execute(  # type: ignore[union-attr]
            """SELECT place, scale, magnitude, notified_at FROM earthquake_log
               WHERE notified_at >= datetime('now', ?)
               AND earthquake_id NOT LIKE '%:eew' AND earthquake_id NOT LIKE '%:cancelled'
               ORDER BY notified_at DESC LIMIT 10""",
            (f"-{hours} hours",),
        ).fetchall()
        ts_rows = _db_conn.execute(  # type: ignore[union-attr]
            "SELECT area, grade, updated_at FROM tsunami_state ORDER BY updated_at DESC"
        ).fetchall()

    earthquakes = [
        {
            "place": r[0],
            "scale": _scale_to_str(r[1]),
            "magnitude": r[2],
            "notified_at": r[3],
        }
        for r in eq_rows
    ]
    tsunami_active = [
        {
            "area": r[0],
            "grade": _TSUNAMI_GRADE_LABEL.get(r[1], r[1]),
            "updated_at": r[2],
        }
        for r in ts_rows
    ]
    logger.info("Tool get_recent_alerts: hours=%d eq=%d tsunami=%d", hours, len(earthquakes), len(tsunami_active))
    return {
        "status": "ok",
        "period_hours": hours,
        "earthquakes": earthquakes,
        "tsunami_active": tsunami_active,
    }


async def _tool_recall(args: dict, notify_context: dict) -> dict:
    """埋め込み検索で長期記憶を引く。話者に見せてよいものだけが対象。"""
    from bridge.core.db import _fetch_memories_with_embeddings
    from bridge.llm.embeddings import embed_one, search

    query = str(args.get("query", "")).strip()
    if not query:
        return {"status": "error", "message": "検索内容が空です"}
    speaker = notify_context.get("speaker")
    rows = _fetch_memories_with_embeddings(speaker)
    if not rows:
        return {"status": "ok", "memories": [], "note": "まだ覚えていることがありません"}
    qvec = await embed_one(query)
    if qvec is None:
        return {"status": "error", "message": "記憶の検索に失敗しました"}
    hits = search(qvec, rows, top_k=5)
    logger.info("Function call recall: query=%s hits=%d speaker=%s", query, len(hits), speaker)
    return {
        "status": "ok",
        "memories": [
            {"content": h["content"], "about": h.get("about"),
             "when": h.get("happened_on") or (h.get("created_at") or "")[:10]}
            for h in hits
        ],
    }


async def _execute_tool(name: str, args: dict, notify_context: dict) -> dict:
    """Execute a named tool and return the raw result dict (protocol-agnostic)."""
    _main = sys.modules["main"]

    if name == "set_timer":
        _register_timer = _main._register_timer
        timer_id = _register_timer(
            label=args.get("label", "タイマー"),
            seconds=int(args.get("seconds", 60)),
            session_key=notify_context.get("session_key", ""),
            slack_channel=notify_context.get("slack_channel"),
            snooze_seconds=args.get("snooze_seconds"),
        )
        logger.info(
            "Function call set_timer: label=%s seconds=%s timer_id=%s",
            args.get("label"), args.get("seconds"), timer_id,
        )
        return {"status": "ok", "timer_id": timer_id, "label": args.get("label"), "seconds": args.get("seconds")}
    if name == "recall":
        return await _tool_recall(args, notify_context)
    if name == "list_timers":
        _active_timer_infos = _main._active_timer_infos
        now = datetime.now(_JST)
        timers = [
            {
                "timer_id": info.timer_id,
                "label": info.label,
                "fire_at": info.fire_at.isoformat(),
                "remaining_seconds": max(0, int((info.fire_at - now).total_seconds())),
            }
            for info in _active_timer_infos.values()
        ]
        logger.info("Function call list_timers: count=%d", len(timers))
        return {"status": "ok", "timers": timers, "count": len(timers)}
    if name == "request_web_search":
        query = args.get("query", "")
        notify_context["enable_web_search"] = True
        logger.info("LLM requested web search: query=%s", query[:80])
        return {
            "status": "ok",
            "message": "Web検索が有効になりました。次のターンで検索を実行して回答してください。",
            "query": query,
        }
    if name == "get_weather":
        return await _tool_get_weather(args)
    if name == "get_upcoming_items":
        return _tool_get_upcoming_items(args)
    if name == "get_recent_alerts":
        return _tool_get_recent_alerts(args)
    if name == "get_pending_messages":
        _fetch_pending_messages = _main._fetch_pending_messages
        _mark_message_delivered = _main._mark_message_delivered
        _notify_message_delivered = _main._notify_message_delivered
        _filter_messages_for_speaker = _main._filter_messages_for_speaker
        messages = _filter_messages_for_speaker(_fetch_pending_messages(), notify_context.get("speaker"))
        for msg in messages:
            _mark_message_delivered(msg["id"])
            asyncio.create_task(_notify_message_delivered(msg))
        logger.info("Function call get_pending_messages: count=%d", len(messages))
        if not messages:
            return {"status": "ok", "count": 0, "messages": []}
        return {
            "status": "ok",
            "count": len(messages),
            "messages": [
                {"sender": m["sender"], "recipient": m["recipient"], "content": m["content"]}
                for m in messages
            ],
        }
    logger.warning("Unknown function call: name=%s", name)
    return {"status": "error", "message": f"Unknown function: {name}"}


async def _handle_function_calls(
    output: list,
    notify_context: dict,
) -> list | None:
    """output 配列に function_call があれば実行して function_call_output リストを返す（Responses API 形式）。なければ None。"""
    function_calls = [item for item in output if item.get("type") == "function_call"]
    if not function_calls:
        return None

    results = []
    for fc in function_calls:
        name = fc.get("name", "")
        # OpenAI Responses API: function_call には id（アイテムID: fc_xxx）と
        # call_id（参照用: call_xxx）の 2 フィールドがある。function_call_output には
        # call_id を使う必要がある。id と call_id が同じ場合のフォールバックも持つ。
        call_id = fc.get("call_id") or fc.get("id", "")
        try:
            args = json.loads(fc.get("arguments", "{}"))
        except json.JSONDecodeError:
            args = {}
        result = await _execute_tool(name, args, notify_context)
        results.append({
            "type": "function_call_output",
            "call_id": call_id,
            "output": json.dumps(result, ensure_ascii=False),
        })

    return results
