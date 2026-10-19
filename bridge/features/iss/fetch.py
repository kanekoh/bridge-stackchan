"""OEM の取得とキャッシュ。

取得先が落ちても通知を止めないことが目的なので、方針は3段構え。

  1. 覚えている暦がまだ期間内なら、取りに行かない（15日分あるので普段は毎日取らない）
  2. 取りに行って成功したら、ファイルに保存する（再起動しても残る）
  3. 取りに行けなければ、保存してある暦を使い続ける（古い旨をログと UI に出す）

以前の TLE 版は「当日分をメモリに持つだけ」だったため、取得先（CelesTrak）が
落ちた瞬間に通知が止まり、再起動で記憶も消えていた。
"""
import logging
import os
from datetime import datetime, timedelta, timezone

from bridge.config import ISS_OEM_URL, ISS_OEM_CACHE_FILE, ISS_OEM_REFRESH_HOURS
from bridge.features.iss import oem

logger = logging.getLogger(__name__)

_cache: oem.Ephemeris | None = None
_last_attempt: datetime | None = None
_last_error: str = ""


def _load_from_disk() -> oem.Ephemeris | None:
    if not os.path.exists(ISS_OEM_CACHE_FILE):
        return None
    try:
        with open(ISS_OEM_CACHE_FILE, encoding="utf-8") as f:
            return oem.parse_oem(f.read())
    except Exception as e:
        logger.warning("保存してある暦を読めません: %s", e)
        return None


def _save_to_disk(text: str) -> None:
    try:
        os.makedirs(os.path.dirname(ISS_OEM_CACHE_FILE), exist_ok=True)
        with open(ISS_OEM_CACHE_FILE, "w", encoding="utf-8") as f:
            f.write(text)
    except OSError as e:
        logger.warning("暦を保存できません（動作は続けます）: %s", e)


def _needs_refresh(eph: oem.Ephemeris | None, now: datetime) -> bool:
    """取りに行くべきか。期間に余裕があるうちは行かない。"""
    if eph is None:
        return True
    # 残りが1日を切ったら取りに行く。15日分あるので、普段は数日に1回で足りる。
    return eph.end - now < timedelta(days=1)


async def get_ephemeris(force: bool = False) -> oem.Ephemeris | None:
    """使える暦を返す。取りに行けなければ、古いものをそのまま返す。"""
    global _cache, _last_attempt, _last_error
    now = datetime.now(timezone.utc)

    if _cache is None:
        _cache = _load_from_disk()
        if _cache:
            logger.info("保存してある暦を読み込みました: %s", oem.describe(_cache, now))

    if not force and not _needs_refresh(_cache, now):
        return _cache

    # 失敗が続くときに毎回叩かない（CelesTrak のときは10分ごとに例外を吐き続けていた）
    if (not force and _last_attempt
            and now - _last_attempt < timedelta(hours=ISS_OEM_REFRESH_HOURS)):
        return _cache

    _last_attempt = now
    try:
        from bridge.core.http import get_http_client
        resp = await get_http_client().get(ISS_OEM_URL, timeout=60)
        resp.raise_for_status()
        fresh = oem.parse_oem(resp.text)
    except Exception as e:
        _last_error = f"{type(e).__name__}: {e}"
        if _cache is not None:
            logger.warning(
                "暦を取得できませんでした。保存してあるもの（残り%.1f日）を使います: %s",
                (_cache.end - now).total_seconds() / 86400, _last_error,
            )
        else:
            logger.error("暦を取得できず、保存してあるものもありません: %s", _last_error)
        return _cache

    _last_error = ""
    _cache = fresh
    _save_to_disk(resp.text)
    logger.info("暦を更新しました: %s", oem.describe(fresh, now))
    return fresh


def status() -> dict:
    """UI・デバッグ用の状態。取得できていない間も、何を使っているかが分かるようにする。"""
    now = datetime.now(timezone.utc)
    info: dict = {
        "url": ISS_OEM_URL,
        "cacheFile": ISS_OEM_CACHE_FILE,
        "lastAttempt": _last_attempt.isoformat() if _last_attempt else None,
        "lastError": _last_error,
        "available": _cache is not None,
    }
    if _cache is not None:
        info["ephemeris"] = oem.describe(_cache, now)
    return info


def reset_for_test() -> None:
    """テスト用。プロセス内のキャッシュを捨てる。"""
    global _cache, _last_attempt, _last_error
    _cache, _last_attempt, _last_error = None, None, ""
