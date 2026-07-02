"""Timer management for Stack-chan.

_fire_timer calls publish_speak, chat_with_llm, resolve_audio_url, and
accesses _slack_app — all retrieved lazily via sys.modules["main"] to
avoid circular imports.
"""
import asyncio
import logging
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

from bridge.config import _JST

logger = logging.getLogger(__name__)

# ── State ──────────────────────────────────────────────────────────────────────

_active_timers: dict[str, asyncio.Task] = {}
_active_timer_infos: dict = {}  # timer_id → _TimerInfo


# ── Data class ────────────────────────────────────────────────────────────────

@dataclass
class _TimerInfo:
    timer_id: str
    label: str
    fire_at: datetime
    session_key: str
    slack_channel: str | None  # 設定元が Slack の場合に発火後通知するチャンネル
    snooze_seconds: int | None  # スヌーズ秒数（None = スヌーズなし）


# ── Timer functions ────────────────────────────────────────────────────────────

async def _fire_timer(info: _TimerInfo) -> None:
    """タイマー発火処理: LLMで声かけ文を生成 → VOICEVOX → MQTT、Slack 経由なら Slack にも通知。"""
    _main = sys.modules["main"]
    chat_with_llm = _main.chat_with_llm
    _parse_expression = _main._parse_expression
    _resolve_expression = _main._resolve_expression
    resolve_audio_url = _main.resolve_audio_url
    publish_speak = _main.publish_speak
    _slack_app = _main._slack_app

    prompt = f"タイマー「{info.label}」の時間になりました。短く明るく声かけしてください。"
    timer_instruction = (
        "これはタイマーの発火通知です。"
        "スタックちゃんとして、その場にいる家族に向けて声かけしてください。"
        "依頼者への返答にはしないでください。"
    )
    try:
        message = await chat_with_llm(
            prompt,
            system_prompt_append=timer_instruction,
            session_key=info.session_key,
            use_functions=False,  # タイマー発火中は新たなタイマーを受け付けない
        )
    except Exception as e:
        logger.error("Timer LLM error: timer_id=%s error=%s", info.timer_id, e)
        message = f"{info.label}の時間だよ！"

    # MQTT で発話（常に実行）
    expression, clean_message = _parse_expression(message)
    speaker_id, stackchan_expr = _resolve_expression(expression)
    try:
        audio_url, streaming_url = await resolve_audio_url(clean_message, speaker_id)
        req_id = str(uuid.uuid4())
        publish_speak(audio_url, streaming_url, clean_message, "timer", "normal", req_id, stackchan_expr)
        logger.info("Timer fired: timer_id=%s label=%s expression=%s message=%s", info.timer_id, info.label, expression, clean_message[:60])
    except Exception as e:
        logger.error("Timer speak error: timer_id=%s error=%s", info.timer_id, e)
        return

    # Slack 経由で設定された場合は Slack にも完了通知
    if info.slack_channel and _slack_app:
        try:
            await _slack_app.client.chat_postMessage(
                channel=info.slack_channel,
                text=f"⏰ タイマー「{info.label}」が発火しました：「{clean_message}」",
            )
            logger.info("Timer Slack notified: channel=%s", info.slack_channel)
        except Exception as e:
            logger.warning("Timer Slack notify error: %s", e)

    # スヌーズが設定されている場合は次のタイマーを登録（一回のみ）
    if info.snooze_seconds:
        snooze_id = _register_timer(
            label=f"{info.label}（スヌーズ）",
            seconds=info.snooze_seconds,
            session_key=info.session_key,
            slack_channel=info.slack_channel,
            snooze_seconds=None,
        )
        logger.info("Timer snooze registered: original=%s snooze=%s", info.timer_id, snooze_id)


async def _run_timer(info: _TimerInfo) -> None:
    """asyncio.Task として動作するタイマー。delay 後に _fire_timer を呼ぶ。"""
    delay = (info.fire_at - datetime.now(_JST)).total_seconds()
    if delay > 0:
        await asyncio.sleep(delay)
    _active_timers.pop(info.timer_id, None)
    _active_timer_infos.pop(info.timer_id, None)
    try:
        await _fire_timer(info)
    except Exception as e:
        logger.error("Timer _run_timer error: timer_id=%s error=%s", info.timer_id, e)


def _register_timer(
    label: str,
    seconds: int,
    session_key: str = "",
    slack_channel: str | None = None,
    snooze_seconds: int | None = None,
) -> str:
    """タイマーを登録して asyncio.Task を起動し、timer_id を返す。"""
    timer_id = str(uuid.uuid4())
    fire_at = datetime.now(_JST) + timedelta(seconds=seconds)
    info = _TimerInfo(
        timer_id=timer_id,
        label=label,
        fire_at=fire_at,
        session_key=session_key,
        slack_channel=slack_channel,
        snooze_seconds=snooze_seconds,
    )
    task = asyncio.create_task(_run_timer(info))
    _active_timers[timer_id] = task
    _active_timer_infos[timer_id] = info
    logger.info(
        "Timer registered: timer_id=%s label=%s fire_at=%s slack_channel=%s snooze=%s",
        timer_id, label, fire_at.isoformat(), slack_channel, snooze_seconds,
    )
    return timer_id
