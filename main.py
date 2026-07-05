from bridge.config import *  # noqa: F401,F403

import logging
import time
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
    _fetch_pending_messages, _mark_message_delivered, _filter_messages_for_speaker,
    _save_ingest_metrics,
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
    import bridge.core.db as _db_mod
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
            _db_mod._db_conn,
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
    if _db_mod._db_conn:
        _db_mod._db_conn.close()
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

from bridge.api.calendar import router as _calendar_router
app.include_router(_calendar_router)

from bridge.api.web_checks import router as _web_checks_router
app.include_router(_web_checks_router)

from bridge.api.debug import router as _debug_router
app.include_router(_debug_router)

import bridge.core.audio as _audio_mod
from bridge.core.audio import get_audio_url_web, resolve_audio_url

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


class SpeakRequest(BaseModel):
    text: str
    source: str = "unknown"
    priority: str = "normal"
    request_id: str | None = None


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

    _t0 = time.monotonic()

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
    _t_stt = time.monotonic()
    logger.info("Transcript: request_id=%s text=%s speaker=%s", req_id, transcript[:80], speaker)

    try:
        reply = await chat_with_llm(
            transcript,
            speaker,
            system_prompt_append,
            effective_session_key,
            notify_context={"session_key": effective_session_key, "slack_channel": None, "speaker": speaker},
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
    _t_llm = time.monotonic()
    expression, clean_reply = _parse_expression(reply, default=expression)
    speaker_id, stackchan_expr = _resolve_expression(expression)
    logger.info("LLM reply: backend=%s request_id=%s expression=%s text=%s", LLM_BACKEND, req_id, expression, clean_reply[:80])

    try:
        audio_url, audio_streaming_url = await resolve_audio_url(clean_reply, speaker_id)
    except Exception as e:
        logger.error("VOICEVOX error: %s", e)
        raise HTTPException(status_code=502, detail=f"VOICEVOX error: {e}")
    _t_voicevox = time.monotonic()

    def _record_ingest_metrics(mqtt_ms: int | None) -> None:
        try:
            _save_ingest_metrics(
                request_id=req_id,
                mode=mode,
                transcript_chars=len(transcript),
                reply_chars=len(clean_reply),
                stt_ms=round((_t_stt - _t0) * 1000),
                llm_ms=round((_t_llm - _t_stt) * 1000),
                voicevox_ms=round((_t_voicevox - _t_llm) * 1000),
                mqtt_ms=mqtt_ms,
                total_ms=round((time.monotonic() - _t0) * 1000),
            )
        except Exception as e:
            logger.warning("ingest_audio metrics save error: %s", e)

    if mode == "async":
        try:
            publish_speak(audio_url, audio_streaming_url, clean_reply, source, priority, req_id, stackchan_expr)
        except Exception as e:
            logger.error("MQTT error: %s", e)
            raise HTTPException(status_code=502, detail=f"MQTT error: {e}")
        _record_ingest_metrics(mqtt_ms=round((time.monotonic() - _t_voicevox) * 1000))
        return {"requestId": req_id, "expression": stackchan_expr}

    asyncio.create_task(_deliver_pending_messages_after(clean_reply, source, priority, session_key=effective_session_key, speaker=speaker))
    _record_ingest_metrics(mqtt_ms=None)

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
