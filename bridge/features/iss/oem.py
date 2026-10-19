"""OEM（CCSDS Orbit Ephemeris Message）の解析と補間。

NASA が公開するのは「4分ごとの位置と速度の表」で、軌道要素ではない。
欲しいのは任意の時刻の位置なので、表の間を埋める必要がある。

位置だけの補間（直線・スプライン）だと、4分で約1800km進む軌道の曲がりを
再現できない。速度も一緒に与えられているので、**エルミート補間**を使う。
両端の位置と速度に一致する3次式になり、4分間隔でも誤差は数十m程度に収まる。

ここは純関数だけ。取得（HTTP）とキャッシュは fetch.py が持つ。
"""
import logging
import re
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# "2026-10-19T10:01:30.000 2408.58 -3509.47 -5300.90 5.75 4.99 -0.69"
_STATE_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)\s+"
    r"([-\d.eE+]+)\s+([-\d.eE+]+)\s+([-\d.eE+]+)\s+"
    r"([-\d.eE+]+)\s+([-\d.eE+]+)\s+([-\d.eE+]+)\s*$"
)
_META_RE = re.compile(r"^([A-Z_]+)\s*=\s*(.+?)\s*$")


class OemError(Exception):
    """OEM として読めなかった。"""


def _parse_time(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class Ephemeris:
    """ある期間の ISS の位置・速度の表（EME2000 慣性座標系、km と km/s）。"""

    times: list[datetime]
    positions: list[tuple[float, float, float]]
    velocities: list[tuple[float, float, float]]
    created_at: datetime | None = None
    object_name: str = "ISS"

    @property
    def start(self) -> datetime:
        return self.times[0]

    @property
    def end(self) -> datetime:
        return self.times[-1]

    def covers(self, when: datetime) -> bool:
        return self.start <= when <= self.end

    def age_days(self, now: datetime | None = None) -> float:
        """作成からの日数。古いほど誤差が増えるので、鮮度を表示に出す。"""
        if self.created_at is None:
            return 0.0
        now = now or datetime.now(timezone.utc)
        return (now - self.created_at).total_seconds() / 86400

    def position_at(self, when: datetime) -> tuple[float, float, float]:
        """任意時刻の位置（km）。表の間はエルミート補間で埋める。"""
        if not self.covers(when):
            raise OemError(
                f"{when.isoformat()} は暦の範囲外です"
                f"（{self.start.isoformat()}〜{self.end.isoformat()}）"
            )
        i = bisect_right(self.times, when) - 1
        if i >= len(self.times) - 1:          # 終端ちょうど
            return self.positions[-1]

        t0, t1 = self.times[i], self.times[i + 1]
        span = (t1 - t0).total_seconds()
        s = (when - t0).total_seconds() / span

        p0, p1 = self.positions[i], self.positions[i + 1]
        v0, v1 = self.velocities[i], self.velocities[i + 1]

        # 3次エルミート基底。速度は km/s なので、区間長 span を掛けて無次元化する。
        h00 = 2 * s**3 - 3 * s**2 + 1
        h10 = s**3 - 2 * s**2 + s
        h01 = -2 * s**3 + 3 * s**2
        h11 = s**3 - s**2
        return tuple(                                        # type: ignore[return-value]
            h00 * p0[k] + h10 * span * v0[k] + h01 * p1[k] + h11 * span * v1[k]
            for k in range(3)
        )


def parse_oem(text: str) -> Ephemeris:
    """OEM のテキストを読む。数値行以外（メタ・COMMENT・空行）は読み飛ばす。"""
    times: list[datetime] = []
    positions: list[tuple[float, float, float]] = []
    velocities: list[tuple[float, float, float]] = []
    created_at: datetime | None = None
    object_name = "ISS"

    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("COMMENT"):
            continue
        state = _STATE_RE.match(line)
        if state:
            times.append(_parse_time(state.group(1)))
            positions.append(tuple(float(state.group(i)) for i in (2, 3, 4)))    # type: ignore[arg-type]
            velocities.append(tuple(float(state.group(i)) for i in (5, 6, 7)))   # type: ignore[arg-type]
            continue
        meta = _META_RE.match(line)
        if meta:
            key, value = meta.group(1), meta.group(2)
            if key == "CREATION_DATE":
                try:
                    created_at = _parse_time(value)
                except ValueError:
                    logger.warning("OEM の CREATION_DATE を読めません: %s", value)
            elif key == "OBJECT_NAME":
                object_name = value

    if len(times) < 2:
        raise OemError(f"状態ベクトルが足りません（{len(times)}行）")
    if any(b <= a for a, b in zip(times, times[1:])):
        raise OemError("時刻が昇順に並んでいません")

    return Ephemeris(times, positions, velocities, created_at, object_name)


def trim(eph: Ephemeris, start: datetime, end: datetime) -> Ephemeris:
    """期間を切り出す（テストや保存用。両端は補間できるよう1点ずつ余分に残す）。"""
    lo = max(0, bisect_right(eph.times, start) - 1)
    hi = min(len(eph.times), bisect_right(eph.times, end) + 1)
    return Ephemeris(
        eph.times[lo:hi], eph.positions[lo:hi], eph.velocities[lo:hi],
        eph.created_at, eph.object_name,
    )


def describe(eph: Ephemeris, now: datetime | None = None) -> dict:
    """UI・ログ向けの要約。"""
    now = now or datetime.now(timezone.utc)
    return {
        "object": eph.object_name,
        "createdAt": eph.created_at.isoformat() if eph.created_at else None,
        "start": eph.start.isoformat(),
        "end": eph.end.isoformat(),
        "points": len(eph.times),
        "ageDays": round(eph.age_days(now), 2),
        "remainingDays": round((eph.end - now).total_seconds() / 86400, 2),
        "stale": eph.end - now < timedelta(days=1),
    }
