"""歌の再生。gatekeeper を通してから MQTT で鳴らす。

再生経路は発話とまったく同じ（stackchan/{device}/speak に audioUrl を載せる）。
違うのは音源が api.tts.quest の MP3 ではなく、Pi 上で事前生成した MP3 だという点だけ
なので、ファーム側の変更は要らない。

publish_speak は bridge.devices.mqtt から直接使う（歌は LLM を経由しないため、
main.py 越しの遅延参照は不要）。
"""
import logging
import uuid

from bridge.config import SONG_PUBLIC_BASE_URL
from bridge.devices.mqtt import publish_speak
from bridge.features import gatekeeper
from bridge.features.song import cache as _cache
from bridge.features.song import library, settings
from bridge.features.song.engine import get_engine_info
from bridge.features.song.score import Score

logger = logging.getLogger(__name__)

# gatekeeper 上の種別。歌はすべて同じスピーカーを取り合うので 1 種別にまとめる。
GATE_KIND = "song"


class SongNotAllowed(RuntimeError):
    """gatekeeper に止められた。理由を message に持つ。"""


def _public_base_url() -> str:
    base = (SONG_PUBLIC_BASE_URL or "").strip().rstrip("/")
    if not base:
        raise RuntimeError(
            "SONG_PUBLIC_BASE_URL が未設定です"
            "（スタックちゃんから見た Bridge の URL、例: http://raspberrypi.local:8000）"
        )
    return base


def song_url(filename: str) -> str:
    return f"{_public_base_url()}/songs/{filename}"


def check_allowed(*, trigger_key: str = "", once: bool = False, respect_quiet: bool = True) -> tuple[bool, str]:
    """今この瞬間に歌ってよいかを返す。再生はしない（UI の状態表示にも使う）。"""
    return gatekeeper.allow(
        GATE_KIND,
        trigger_key,
        cooldown_sec=settings.get_int("song_cooldown_minutes") * 60,
        once=once,
        quiet_start=settings.get_str("song_quiet_start"),
        quiet_end=settings.get_str("song_quiet_end"),
        respect_quiet=respect_quiet,
    )


async def play_song(
    score: Score,
    *,
    source: str,
    trigger_key: str = "",
    once: bool = False,
    respect_quiet: bool = True,
    bypass_gate: bool = False,
    expression: str = "happy",
    priority: str = "normal",
) -> dict:
    """曲を 1 曲鳴らす。

    - bypass_gate: UI の試聴など、人が今まさに押した操作のときだけ True
    - respect_quiet: 本人が声で頼んだ場合は深夜でも応じる（False）
    """
    if not bypass_gate:
        ok, reason = check_allowed(trigger_key=trigger_key, once=once, respect_quiet=respect_quiet)
        if not ok:
            logger.info("歌を見送りました: song=%s source=%s reason=%s", score.id, source, reason)
            raise SongNotAllowed(reason)

    filename, key = await _cache.ensure_song(score)
    url = song_url(filename)
    info = await get_engine_info()
    duration = score.duration_sec(info["frame_rate"])

    req_id = str(uuid.uuid4())
    # audioStreamingUrl は発話と同じく必ず載せる。publish_speak は偽値のとき
    # フィールドごと落とすため、None を渡すと発話とペイロードの形が変わり、
    # デバイスは ACK を返すだけで取りに来ない（歌が無音になる原因だった）。
    # 歌は静的ファイルで Range 取得もできるので、同じ URL をそのまま使う。
    publish_speak(
        url, url,
        f"♪{score.title}",   # 画面表示・ログ用。読み上げはされない（音源は歌そのもの）
        source, priority, req_id, expression,
    )

    if not bypass_gate:
        # 曲が鳴り終わるまで（＋転送と再生開始の余裕 3 秒）は次を重ねない
        gatekeeper.record(GATE_KIND, trigger_key, busy_sec=duration + 3.0)
    library.record_play(score.id, source=source, trigger_key=trigger_key)

    logger.info(
        "歌を再生: song=%s source=%s duration=%.1fs url=%s request_id=%s",
        score.id, source, duration, url, req_id,
    )
    return {
        "requestId": req_id,
        "songId": score.id,
        "title": score.title,
        "url": url,
        "cacheKey": key,
        "durationSec": round(duration, 2),
        "credit": score.credit,
    }


async def play_song_id(song_id: str, **kwargs) -> dict:
    score = library.get_score(song_id)
    if score is None:
        raise KeyError(f"曲が見つかりません: {song_id}")
    return await play_song(score, **kwargs)


def is_at_home() -> bool:
    """スタックちゃんが「いつもの家にいて、生きている」か。

    Wi-Fi 在席検知は未実装なので、いまは
      ・旅行中（trips に未終了のレコード）でない
      ・device/state を直近 5 分以内に受信している
    の 2 つで代用する。留守そのものは判定できないが、
    「旅行に持ち出している」「電源が落ちている」ときに鳴らすことは防げる。
    """
    from bridge.config import MQTT_DEVICE_ID
    from bridge.core.db import _get_active_trip
    from bridge.devices.mqtt import get_device_state

    try:
        if _get_active_trip() is not None:
            return False
    except Exception as e:
        logger.warning("旅行状態の判定に失敗（在宅とみなす）: %s", e)

    state = get_device_state(MQTT_DEVICE_ID)
    if not state:
        return False
    received_at = state.get("received_at")
    if not received_at:
        return False
    from datetime import datetime, timedelta
    try:
        received = datetime.fromisoformat(received_at)
    except ValueError:
        return False
    return datetime.now(received.tzinfo) - received < timedelta(minutes=5)
