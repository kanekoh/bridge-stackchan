"""歌のトリガー（ルールベース。LLM には判定させない）。

カレンダー起点:
    出発リミット = 予定の開始時刻 − 移動時間 − 準備バッファ
  を計算し、そこまで残り N 分を切った時点で、在宅なら 1 回だけ歌う。
  移動時間・準備バッファは全体の既定値（app_settings）を使い、
  予定ごとに song_trigger_overrides で上書きできる。

ふと歌う:
    用事がなくても、日中に在宅していてしばらく歌っていなければ、
  たまに勝手に歌い出す。毎回きっちり同じ間隔で鳴ると機械らしくなるので、
  条件を満たした回ごとに確率で決める。
"""
import asyncio
import logging
import random
from datetime import datetime, timedelta

from bridge.config import (
    SONG_IDLE_CHECK_INTERVAL, SONG_TRIGGER_CHECK_INTERVAL, _JST,
)
import bridge.core.db as _db_mod
from bridge.core.db import _db_lock, _get_display_tz
from bridge.features import gatekeeper
from bridge.features.song import library, play, settings
from bridge.features.song.play import SongNotAllowed

logger = logging.getLogger(__name__)

# 出発リミットを過ぎてから、まだ間に合うとみなして歌う猶予（分）。
# サービス再起動直後などにリミットを跨いでいても取りこぼさないためのもの。
LATE_GRACE_MINUTES = 5

# 何時間先の予定まで見るか。これ以上先は出発リミットにも届かない。
LOOKAHEAD_HOURS = 6


def _overrides() -> dict[str, dict]:
    """予定ごとの上書き設定を item_id をキーにして返す。"""
    try:
        with _db_lock:
            rows = _db_mod._db_conn.execute(  # type: ignore[union-attr]
                "SELECT item_id, travel_minutes, prep_minutes, song_id, enabled FROM song_trigger_overrides"
            ).fetchall()
    except Exception as e:
        logger.warning("song_trigger_overrides の読み出しに失敗: %s", e)
        return {}
    return {
        r[0]: {"travel_minutes": r[1], "prep_minutes": r[2], "song_id": r[3], "enabled": bool(r[4])}
        for r in rows
    }


def _upcoming_events(now: datetime) -> list[dict]:
    """時刻の決まっている直近の予定を返す（終日予定は出発リミットを持たないので除く）。"""
    horizon = (now + timedelta(hours=LOOKAHEAD_HOURS)).isoformat()
    try:
        with _db_lock:
            rows = _db_mod._db_conn.execute(  # type: ignore[union-attr]
                "SELECT id, person_name, title, start_at FROM items"
                " WHERE type = 'event' AND status = 'active' AND all_day = 0"
                "   AND start_at IS NOT NULL AND start_at > ? AND start_at <= ?"
                " ORDER BY start_at ASC",
                (now.isoformat(), horizon),
            ).fetchall()
    except Exception as e:
        logger.warning("予定の読み出しに失敗: %s", e)
        return []
    return [{"id": r[0], "person_name": r[1], "title": r[2], "start_at": r[3]} for r in rows]


def due_departures(now: datetime | None = None) -> list[dict]:
    """いま歌うべき（出発リミットまで残り N 分を切った）予定を返す。

    再生の可否は見ない純粋な時刻計算なので、UI の「次にいつ鳴るか」表示にも使える。
    """
    now = now or datetime.now(_JST)
    lead = settings.get_int("song_trigger_lead_minutes")
    default_travel = settings.get_int("song_trigger_travel_minutes")
    default_prep = settings.get_int("song_trigger_prep_minutes")
    overrides = _overrides()

    due: list[dict] = []
    for item in _upcoming_events(now):
        ov = overrides.get(item["id"], {})
        if ov and not ov.get("enabled", True):
            continue
        travel = ov.get("travel_minutes") if ov.get("travel_minutes") is not None else default_travel
        prep = ov.get("prep_minutes") if ov.get("prep_minutes") is not None else default_prep
        try:
            start = datetime.fromisoformat(item["start_at"])
        except ValueError:
            continue
        depart_limit = start - timedelta(minutes=travel + prep)
        remaining_min = (depart_limit - now).total_seconds() / 60
        if -LATE_GRACE_MINUTES <= remaining_min <= lead:
            due.append({
                **item,
                "travel_minutes": travel,
                "prep_minutes": prep,
                "depart_limit": depart_limit.isoformat(),
                "remaining_minutes": round(remaining_min, 1),
                "song_id": ov.get("song_id") or settings.get_str("song_trigger_song"),
            })
    return due


async def check_departure_songs() -> None:
    """出発リミットが近い予定があれば歌う。予定 1 件につき 1 回まで。"""
    if not settings.get_bool("song_trigger_enabled"):
        return

    due = due_departures()
    if not due:
        return

    if not play.is_at_home():
        logger.info("出発リミットが近い予定があるが在宅でないため見送り: count=%d", len(due))
        return

    for item in due:
        score = library.get_score(item["song_id"])
        if score is None:
            logger.warning("トリガー対象の曲が見つかりません: song_id=%s", item["song_id"])
            continue
        try:
            result = await play.play_song(
                score,
                source="song_calendar",
                trigger_key=item["id"],
                once=True,          # 同じ予定では二度と鳴らさない
                expression="doubt",
            )
        except SongNotAllowed:
            continue  # 理由は play 側でログ済み
        except Exception as e:
            logger.error("出発リミットの歌に失敗: item_id=%s error=%s", item["id"], e)
            continue
        logger.info(
            "出発リミットの歌: %s「%s」残り%.1f分 song=%s",
            item["person_name"], item["title"], item["remaining_minutes"], result["songId"],
        )
        return  # 1 周につき 1 曲。重なった予定で歌が連続しないようにする


async def song_trigger_loop() -> None:
    logger.info("Song trigger loop started: check_interval=%ds", SONG_TRIGGER_CHECK_INTERVAL)
    while True:
        await asyncio.sleep(SONG_TRIGGER_CHECK_INTERVAL)
        try:
            await check_departure_songs()
        except Exception as e:
            logger.error("Song trigger loop error: %s", e)


# ── ふと歌う ─────────────────────────────────────────────────────────────────

def _within_idle_window(now: datetime) -> bool:
    """歌い出してよい時間帯か（設置場所のタイムゾーン基準）。"""
    start, end = settings.get_str("song_idle_start"), settings.get_str("song_idle_end")
    if not start or not end:
        return True
    try:
        sh, sm = (int(x) for x in start.split(":"))
        eh, em = (int(x) for x in end.split(":"))
    except ValueError:
        return True
    cur = now.hour * 60 + now.minute
    return sh * 60 + sm <= cur < eh * 60 + em


def _hours_since_last_song(now: datetime) -> float:
    """最後に歌ってからの時間。一度も歌っていなければ十分に大きい値。"""
    last = gatekeeper.last_fired_at(play.GATE_KIND)
    if not last:
        return float("inf")
    try:
        return (now - datetime.fromisoformat(last)).total_seconds() / 3600
    except ValueError:
        return float("inf")


def idle_song_due(now: datetime | None = None) -> tuple[bool, str]:
    """いま「ふと歌う」条件を満たしているか。確率は見ない（理由を UI に出すため）。"""
    if not settings.get_bool("song_idle_enabled"):
        return False, "無効"
    now = now or datetime.now(_get_display_tz())
    if not _within_idle_window(now):
        return False, f"時間帯外（{settings.get_str('song_idle_start')}〜{settings.get_str('song_idle_end')}）"
    hours = _hours_since_last_song(now)
    min_hours = settings.get_float("song_idle_min_hours")
    if hours < min_hours:
        return False, f"前に歌ってから{hours:.1f}時間（{min_hours:g}時間空ける）"
    if not play.is_at_home():
        return False, "在宅でない"
    return True, "ok"


async def check_idle_song() -> None:
    """条件を満たしていれば、確率でひとり歌を歌う。"""
    ok, reason = idle_song_due()
    if not ok:
        logger.debug("ふと歌う: 見送り（%s）", reason)
        return

    chance = settings.get_float("song_idle_chance")
    if random.random() >= chance:
        logger.debug("ふと歌う: 今回は歌わない（確率 %.2f）", chance)
        return

    mood = settings.get_str("song_idle_mood")
    score = library.pick_by_mood(mood) if mood else library.pick_for_idle()
    if score is None:
        logger.info("ふと歌う: 歌える曲がありません（mood=%s）", mood or "指定なし")
        return

    try:
        result = await play.play_song(score, source="song_idle", expression="happy")
    except SongNotAllowed as e:
        logger.info("ふと歌う: 見送り（%s）", e)
        return
    except Exception as e:
        logger.error("ふと歌うのに失敗: song=%s error=%s", score.id, e)
        return
    logger.info("ふと歌いました: song=%s title=%s", result["songId"], result["title"])


async def song_idle_loop() -> None:
    logger.info("Song idle loop started: check_interval=%ds", SONG_IDLE_CHECK_INTERVAL)
    while True:
        await asyncio.sleep(SONG_IDLE_CHECK_INTERVAL)
        try:
            await check_idle_song()
        except Exception as e:
            logger.error("Song idle loop error: %s", e)
