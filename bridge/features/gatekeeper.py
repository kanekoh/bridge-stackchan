"""発火の抑制（gatekeeper）。

これまで「1回だけ通知する」「深夜は黙る」は機能ごとに書かれていた
（notification_log、weather_rain_notified、各ループ内の時刻判定）。
歌のように複数のトリガーが同じ出力先（スピーカー）を取り合うものが出てきたので、
判定をここ 1 か所にまとめる。

判定はすべてルールベースで、LLM には一切問い合わせない。

    ok, reason = allow("song", key=item_id, cooldown_sec=1800, once=True)
    if ok:
        ...  # 実際に鳴らす
        record("song", key=item_id, busy_sec=song_duration)
"""
import logging
import time
from datetime import datetime

from bridge.config import _JST
import bridge.core.db as _db_mod
from bridge.core.db import _db_lock, _get_display_tz

logger = logging.getLogger(__name__)

# 種別ごとの「まだ鳴り終わっていない」時刻（monotonic）。
# 再生中の重複を防ぐためのプロセス内状態で、再起動すればリセットされてよい。
_busy_until: dict[str, float] = {}


def _parse_hhmm(text: str, default: tuple[int, int]) -> tuple[int, int]:
    try:
        h, m = text.split(":")
        return int(h), int(m)
    except (ValueError, AttributeError):
        return default


def in_quiet_hours(quiet_start: str, quiet_end: str, now: datetime | None = None) -> bool:
    """設置場所のタイムゾーンで深夜帯かどうか。日をまたぐ指定（22:00〜07:00）に対応する。"""
    if not quiet_start or not quiet_end:
        return False
    now = now or datetime.now(_get_display_tz())
    sh, sm = _parse_hhmm(quiet_start, (22, 0))
    eh, em = _parse_hhmm(quiet_end, (7, 0))
    cur = now.hour * 60 + now.minute
    start, end = sh * 60 + sm, eh * 60 + em
    if start == end:
        return False
    if start < end:
        return start <= cur < end
    return cur >= start or cur < end  # 日をまたぐ


def last_fired_at(kind: str, key: str | None = None) -> str | None:
    """最後に発火した時刻（ISO文字列）。key を指定するとその key に限る。"""
    sql = "SELECT fired_at FROM gate_log WHERE kind = ?"
    params: list = [kind]
    if key is not None:
        sql += " AND key = ?"
        params.append(key)
    sql += " ORDER BY id DESC LIMIT 1"
    try:
        with _db_lock:
            row = _db_mod._db_conn.execute(sql, params).fetchone()  # type: ignore[union-attr]
    except Exception as e:
        logger.warning("gate_log の読み出しに失敗: %s", e)
        return None
    return row[0] if row else None


def allow(
    kind: str,
    key: str = "",
    *,
    cooldown_sec: int = 0,
    once: bool = False,
    quiet_start: str = "",
    quiet_end: str = "",
    respect_quiet: bool = True,
) -> tuple[bool, str]:
    """発火してよいかを返す。(可否, 理由) の組で、理由はログと UI にそのまま出す。

    - once:       同じ (kind, key) では二度と鳴らさない（予定 1 件につき 1 回）
    - cooldown:   同じ kind が直前に鳴ってから cooldown_sec 経つまで鳴らさない
    - quiet:      深夜帯は鳴らさない
    - busy:       前の再生が終わる前には重ねない
    """
    busy = _busy_until.get(kind, 0.0)
    if busy > time.monotonic():
        return False, f"再生中（あと{int(busy - time.monotonic())}秒）"

    if respect_quiet and in_quiet_hours(quiet_start, quiet_end):
        return False, f"深夜帯（{quiet_start}〜{quiet_end}）"

    if once and key:
        if last_fired_at(kind, key) is not None:
            return False, "この対象では発火済み"

    if cooldown_sec > 0:
        last = last_fired_at(kind)
        if last:
            try:
                elapsed = (datetime.now(_JST) - datetime.fromisoformat(last)).total_seconds()
            except ValueError:
                elapsed = cooldown_sec  # 読めない値はクールダウン明けとみなす
            if elapsed < cooldown_sec:
                return False, f"クールダウン中（あと{int(cooldown_sec - elapsed)}秒）"

    return True, "ok"


def record(kind: str, key: str = "", *, busy_sec: float = 0.0) -> None:
    """発火を記録する。busy_sec の間は同じ kind の重複発火を止める。"""
    if busy_sec > 0:
        _busy_until[kind] = time.monotonic() + busy_sec
    now = datetime.now(_JST).isoformat()
    try:
        with _db_lock:
            _db_mod._db_conn.execute(  # type: ignore[union-attr]
                "INSERT INTO gate_log (kind, key, fired_at) VALUES (?,?,?)", (kind, key, now)
            )
            _db_mod._db_conn.execute(  # type: ignore[union-attr]
                "DELETE FROM gate_log WHERE id NOT IN"
                " (SELECT id FROM gate_log ORDER BY id DESC LIMIT 2000)"
            )
            _db_mod._db_conn.commit()  # type: ignore[union-attr]
    except Exception as e:
        logger.warning("gate_log の記録に失敗: %s", e)


def clear(kind: str, key: str = "") -> None:
    """発火済み記録を消す（UI から「もう一度鳴らせるようにする」ため）。"""
    _busy_until.pop(kind, None)
    sql = "DELETE FROM gate_log WHERE kind = ?"
    params: list = [kind]
    if key:
        sql += " AND key = ?"
        params.append(key)
    with _db_lock:
        _db_mod._db_conn.execute(sql, params)  # type: ignore[union-attr]
        _db_mod._db_conn.commit()  # type: ignore[union-attr]
