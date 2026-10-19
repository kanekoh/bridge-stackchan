"""慣性座標の位置から、観測地での方位・仰角と通過時刻を出す。

TLE + ephem のときは SGP4 がここを全部やっていた。OEM は位置の表なので、
地球の自転（と歳差）を自分で当てて、地上から見た角度に直す必要がある。

手順:
    EME2000（J2000 慣性）
      → 歳差を当てて「その日の平均赤道・平均分点」へ
      → グリニッジ平均恒星時ぶん回して地球固定（ECEF）へ
      → 観測地を原点とする ENU（東・北・上）へ
      → 方位角と仰角

章動（最大17秒角 ≒ 2km）と極運動は入れていない。仰角の誤差は 0.01 度未満で、
「今夜19時ごろ南西の空」という用途には影響しない。歳差は J2000 から26年ぶんで
0.36度（地上で約40km）になるため、こちらは省かない。
"""
import math
from datetime import datetime, timedelta, timezone

# WGS84
_A_EARTH = 6378.137          # 赤道半径 [km]
_F_EARTH = 1 / 298.257223563  # 扁平率
_E2 = _F_EARTH * (2 - _F_EARTH)

_ARCSEC = math.pi / (180 * 3600)


def julian_centuries(when: datetime) -> float:
    """J2000.0 からのユリウス世紀数。"""
    jd = julian_date(when)
    return (jd - 2451545.0) / 36525.0


def julian_date(when: datetime) -> float:
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    when = when.astimezone(timezone.utc)
    y, m = when.year, when.month
    if m <= 2:
        y, m = y - 1, m + 12
    a = y // 100
    b = 2 - a + a // 4
    day_fraction = (
        when.hour + when.minute / 60 + (when.second + when.microsecond / 1e6) / 3600
    ) / 24
    return (
        math.floor(365.25 * (y + 4716))
        + math.floor(30.6001 * (m + 1))
        + when.day + day_fraction + b - 1524.5
    )


def gmst_rad(when: datetime) -> float:
    """グリニッジ平均恒星時（ラジアン）。IAU 1982 の式。"""
    t = julian_centuries(when)
    seconds = (
        67310.54841
        + (876600 * 3600 + 8640184.812866) * t
        + 0.093104 * t**2
        - 6.2e-6 * t**3
    )
    return math.radians((seconds / 240.0) % 360.0)


def precess_j2000_to_date(vec: tuple[float, float, float], when: datetime) -> tuple[float, float, float]:
    """J2000 平均分点 → その日の平均分点（IAU 1976 の歳差）。"""
    t = julian_centuries(when)
    zeta = (2306.2181 * t + 0.30188 * t**2 + 0.017998 * t**3) * _ARCSEC
    z = (2306.2181 * t + 1.09468 * t**2 + 0.018203 * t**3) * _ARCSEC
    theta = (2004.3109 * t - 0.42665 * t**2 - 0.041833 * t**3) * _ARCSEC

    x, y, zz = vec
    # Rz(-zeta)
    cz, sz = math.cos(zeta), math.sin(zeta)
    x1, y1, z1 = cz * x - sz * y, sz * x + cz * y, zz
    # Ry(theta)
    ct, st = math.cos(theta), math.sin(theta)
    x2, y2, z2 = ct * x1 + st * z1, y1, -st * x1 + ct * z1
    # Rz(-z)
    cZ, sZ = math.cos(z), math.sin(z)
    return (cZ * x2 - sZ * y2, sZ * x2 + cZ * y2, z2)


def precess_date_to_j2000(vec: tuple[float, float, float], when: datetime) -> tuple[float, float, float]:
    """その日の平均分点 → J2000（precess_j2000_to_date の逆）。"""
    t = julian_centuries(when)
    zeta = (2306.2181 * t + 0.30188 * t**2 + 0.017998 * t**3) * _ARCSEC
    z = (2306.2181 * t + 1.09468 * t**2 + 0.018203 * t**3) * _ARCSEC
    theta = (2004.3109 * t - 0.42665 * t**2 - 0.041833 * t**3) * _ARCSEC

    x, y, zz = vec
    cZ, sZ = math.cos(z), math.sin(z)
    x1, y1, z1 = cZ * x + sZ * y, -sZ * x + cZ * y, zz
    ct, st = math.cos(theta), math.sin(theta)
    x2, y2, z2 = ct * x1 - st * z1, y1, st * x1 + ct * z1
    cz, sz = math.cos(zeta), math.sin(zeta)
    return (cz * x2 + sz * y2, -sz * x2 + cz * y2, z2)


def ecef_to_eci(vec: tuple[float, float, float], when: datetime) -> tuple[float, float, float]:
    """地球固定座標 → EME2000（eci_to_ecef の逆）。"""
    g = gmst_rad(when)
    x, y, z = vec
    cg, sg = math.cos(g), math.sin(g)
    mod = (cg * x - sg * y, sg * x + cg * y, z)
    return precess_date_to_j2000(mod, when)


def eci_to_ecef(vec: tuple[float, float, float], when: datetime) -> tuple[float, float, float]:
    """EME2000 の位置を地球固定座標へ。"""
    x, y, z = precess_j2000_to_date(vec, when)
    g = gmst_rad(when)
    cg, sg = math.cos(g), math.sin(g)
    return (cg * x + sg * y, -sg * x + cg * y, z)


def ecef_to_geodetic(vec: tuple[float, float, float]) -> tuple[float, float, float]:
    """地球固定座標 → 緯度・経度（度）と楕円体高（km）。Bowring の方法。"""
    x, y, z = vec
    lon = math.atan2(y, x)
    p = math.hypot(x, y)
    b = _A_EARTH * (1 - _F_EARTH)
    ep2 = (_A_EARTH**2 - b**2) / b**2
    theta = math.atan2(z * _A_EARTH, p * b)
    lat = math.atan2(
        z + ep2 * b * math.sin(theta) ** 3,
        p - _E2 * _A_EARTH * math.cos(theta) ** 3,
    )
    n = _A_EARTH / math.sqrt(1 - _E2 * math.sin(lat) ** 2)
    alt = p / math.cos(lat) - n if abs(math.cos(lat)) > 1e-12 else abs(z) - b
    return math.degrees(lat), math.degrees(lon), alt


def geodetic_to_ecef(lat_deg: float, lon_deg: float, alt_km: float = 0.0) -> tuple[float, float, float]:
    """観測地の緯度経度 → 地球固定座標（km）。"""
    lat, lon = math.radians(lat_deg), math.radians(lon_deg)
    n = _A_EARTH / math.sqrt(1 - _E2 * math.sin(lat) ** 2)
    return (
        (n + alt_km) * math.cos(lat) * math.cos(lon),
        (n + alt_km) * math.cos(lat) * math.sin(lon),
        (n * (1 - _E2) + alt_km) * math.sin(lat),
    )


def look_angles(
    sat_eci: tuple[float, float, float], when: datetime,
    lat_deg: float, lon_deg: float, alt_km: float = 0.0,
) -> tuple[float, float, float]:
    """観測地から見た (方位角°, 仰角°, 距離km)。方位は北から東回り。"""
    sx, sy, sz = eci_to_ecef(sat_eci, when)
    ox, oy, oz = geodetic_to_ecef(lat_deg, lon_deg, alt_km)
    dx, dy, dz = sx - ox, sy - oy, sz - oz

    lat, lon = math.radians(lat_deg), math.radians(lon_deg)
    sin_lat, cos_lat = math.sin(lat), math.cos(lat)
    sin_lon, cos_lon = math.sin(lon), math.cos(lon)

    east = -sin_lon * dx + cos_lon * dy
    north = -sin_lat * cos_lon * dx - sin_lat * sin_lon * dy + cos_lat * dz
    up = cos_lat * cos_lon * dx + cos_lat * sin_lon * dy + sin_lat * dz

    rng = math.sqrt(dx * dx + dy * dy + dz * dz)
    az = math.degrees(math.atan2(east, north)) % 360.0
    el = math.degrees(math.asin(up / rng)) if rng else 90.0
    return az, el, rng


def subpoint(sat_eci: tuple[float, float, float], when: datetime) -> tuple[float, float, float]:
    """直下点の緯度・経度（度）と高度（km）。実測値との突き合わせに使う。"""
    return ecef_to_geodetic(eci_to_ecef(sat_eci, when))


def _elevation_at(eph, when: datetime, lat: float, lon: float, alt_km: float) -> float:
    return look_angles(eph.position_at(when), when, lat, lon, alt_km)[1]


def find_passes(
    eph, lat: float, lon: float, *,
    start: datetime | None = None,
    hours: float = 18.0,
    min_elevation: float = 10.0,
    horizon: float = 0.0,
    step_sec: int = 30,
    alt_km: float = 0.01,
) -> list[dict]:
    """期間内の通過を返す。最大仰角が min_elevation 未満のものは捨てる。

    30秒刻みで仰角を見て、地平線（horizon）をまたいだ瞬間を二分法で詰める。
    ISS の通過は10分前後あるので、30秒刻みで取りこぼすことはない。
    """
    start = start or datetime.now(timezone.utc)
    if start < eph.start:
        start = eph.start
    end = min(start + timedelta(hours=hours), eph.end)

    passes: list[dict] = []
    step = timedelta(seconds=step_sec)
    t = start
    prev_t, prev_el = t, _elevation_at(eph, t, lat, lon, alt_km)
    rise_t: datetime | None = None
    max_t, max_el = t, -90.0

    def refine(lo: datetime, hi: datetime) -> datetime:
        """仰角が horizon を横切る時刻を二分法で 1 秒まで詰める。"""
        for _ in range(12):
            mid = lo + (hi - lo) / 2
            if (_elevation_at(eph, mid, lat, lon, alt_km) - horizon) < 0:
                lo = mid
            else:
                hi = mid
        return hi

    while t < end:
        t = min(t + step, end)
        el = _elevation_at(eph, t, lat, lon, alt_km)

        if prev_el < horizon <= el:                 # 昇ってきた
            rise_t = refine(prev_t, t)
            max_t, max_el = t, el
        elif rise_t is not None:
            if el > max_el:
                max_t, max_el = t, el
            if el < horizon <= prev_el:             # 沈んだ
                set_t = _refine_set(eph, prev_t, t, lat, lon, alt_km, horizon)
                peak_t, peak_el = _refine_peak(eph, max_t, step, lat, lon, alt_km)
                if peak_el >= min_elevation:
                    rise_az = look_angles(eph.position_at(rise_t), rise_t, lat, lon, alt_km)[0]
                    peak_az = look_angles(eph.position_at(peak_t), peak_t, lat, lon, alt_km)[0]
                    passes.append({
                        "rise_utc": rise_t,
                        "max_utc": peak_t,
                        "set_utc": set_t,
                        "max_el_deg": peak_el,
                        "rise_az_deg": rise_az,
                        "max_az_deg": peak_az,
                        "duration_sec": (set_t - rise_t).total_seconds(),
                    })
                rise_t = None
                max_el = -90.0
        prev_t, prev_el = t, el

    return passes


def _refine_set(eph, before: datetime, after: datetime,
                lat: float, lon: float, alt_km: float, horizon: float) -> datetime:
    """沈む時刻を二分法で詰める（before は地平線より上、after は下）。"""
    lo, hi = before, after
    for _ in range(12):
        mid = lo + (hi - lo) / 2
        if (_elevation_at(eph, mid, lat, lon, alt_km) - horizon) >= 0:
            lo = mid
        else:
            hi = mid
    return hi


def _refine_peak(eph, around: datetime, step: timedelta,
                 lat: float, lon: float, alt_km: float) -> tuple[datetime, float]:
    """最大仰角を黄金分割法で詰める（粗い刻みの頂点は最大2秒ずれる）。"""
    lo, hi = around - step, around + step
    phi = (math.sqrt(5) - 1) / 2
    a = hi - (hi - lo) * phi
    b = lo + (hi - lo) * phi
    fa = _elevation_at(eph, a, lat, lon, alt_km)
    fb = _elevation_at(eph, b, lat, lon, alt_km)
    for _ in range(20):
        if fa < fb:
            lo, a, fa = a, b, fb
            b = lo + (hi - lo) * phi
            fb = _elevation_at(eph, b, lat, lon, alt_km)
        else:
            hi, b, fb = b, a, fa
            a = hi - (hi - lo) * phi
            fa = _elevation_at(eph, a, lat, lon, alt_km)
    peak = a if fa > fb else b
    return peak, max(fa, fb)
