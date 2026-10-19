"""ISS の通過予測（NASA の軌道暦ベース）のテスト。

TLE + ephem(SGP4) をやめて自前計算にしたので、座標変換が狂っていないかを
ここで押さえる。ネットワークには出ない（testdata/iss_oem_sample.txt を使う）。

検証の考え方:
  ・既知値と突き合わせる（GMST、地心距離、観測地の座標）
  ・往復変換で戻ることを確かめる（測地 → ECEF → 測地）
  ・補間が表の点と一致し、間でも軌道から外れないことを確かめる
  ・通過の形（持続時間・仰角の順序）が物理的にありうる範囲か見る

実機相当の精度（ephem + 最新 TLE との比較で出現時刻の差 3〜8秒、仰角 2度以内）は
ネットワークが要るためここでは測らない。2026-10-19 に手で確認した。

Run:
    pytest test_iss.py -v
"""
import math
import os
from datetime import datetime, timedelta, timezone

import pytest

os.environ.setdefault("OPENAI_API_KEY", "sk-test")
os.environ.setdefault("DB_PATH", "/tmp/test-bridge-iss.db")

from bridge.features.iss import oem, passes  # noqa: E402

_FIXTURE = os.path.join(os.path.dirname(__file__), "testdata", "iss_oem_sample.txt")

# 鎌倉（本番の設置場所）
_LAT, _LON = 35.3140615, 139.5575321


@pytest.fixture(scope="module")
def eph():
    with open(_FIXTURE, encoding="utf-8") as f:
        return oem.parse_oem(f.read())


# ── 解析 ─────────────────────────────────────────────────────────────────────

def test_parses_states_and_metadata(eph):
    assert len(eph.times) == 167
    assert eph.object_name == "ISS"
    assert eph.created_at == datetime(2026, 10, 14, 18, 30, 1, 420000, tzinfo=timezone.utc)
    assert eph.start == datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)


def test_states_are_four_minutes_apart(eph):
    gaps = {(b - a).total_seconds() for a, b in zip(eph.times, eph.times[1:])}
    assert gaps == {240.0}


def test_positions_are_in_low_earth_orbit(eph):
    """地心距離が ISS の軌道半径（約6780km）の範囲に収まること。"""
    for p in eph.positions:
        r = math.sqrt(sum(c * c for c in p))
        assert 6700 < r < 6850, r


def test_rejects_text_without_states():
    with pytest.raises(oem.OemError):
        oem.parse_oem("CCSDS_OEM_VERS = 2.0\nMETA_START\nMETA_STOP\n")


def test_rejects_unsorted_times():
    body = (
        "2026-10-14T12:04:00.000 1 2 3 4 5 6\n"
        "2026-10-14T12:00:00.000 1 2 3 4 5 6\n"
    )
    with pytest.raises(oem.OemError):
        oem.parse_oem(body)


# ── 補間 ─────────────────────────────────────────────────────────────────────

def test_interpolation_matches_table_points(eph):
    """表にある時刻では、補間せずその値が返ること。"""
    for i in (0, 50, 166):
        got = eph.position_at(eph.times[i])
        for a, b in zip(got, eph.positions[i]):
            assert abs(a - b) < 1e-6


def test_interpolated_midpoint_stays_on_orbit(eph):
    """点と点の間（2分後）でも軌道半径が保たれること。

    位置だけの直線補間だと、4分=約1800km の弦を通るため半径が数十km 落ちる。
    速度を使うエルミート補間ならほぼ落ちない。
    """
    mid = eph.times[10] + timedelta(seconds=120)
    r = math.sqrt(sum(c * c for c in eph.position_at(mid)))
    assert 6700 < r < 6850, r


def test_interpolation_beats_straight_line(eph):
    """直線補間よりも軌道からのずれが小さいこと（エルミートを使う理由）。"""
    i = 10
    mid = eph.times[i] + timedelta(seconds=120)
    p0, p1 = eph.positions[i], eph.positions[i + 1]
    straight = tuple((a + b) / 2 for a, b in zip(p0, p1))
    r_ref = (math.sqrt(sum(c * c for c in p0)) + math.sqrt(sum(c * c for c in p1))) / 2
    r_straight = math.sqrt(sum(c * c for c in straight))
    r_hermite = math.sqrt(sum(c * c for c in eph.position_at(mid)))
    assert abs(r_hermite - r_ref) < abs(r_straight - r_ref) / 10


def test_outside_the_range_is_an_error(eph):
    with pytest.raises(oem.OemError):
        eph.position_at(eph.end + timedelta(minutes=1))


# ── 座標変換 ─────────────────────────────────────────────────────────────────

def test_gmst_matches_known_value():
    """2000-01-01 12:00 UTC の GMST は 18h 41m 50.55s（= 18.697374558h）。"""
    g = math.degrees(passes.gmst_rad(datetime(2000, 1, 1, 12, 0, tzinfo=timezone.utc))) / 15
    assert abs(g - 18.697374558) < 1e-5


def test_julian_date_of_j2000():
    assert abs(passes.julian_date(datetime(2000, 1, 1, 12, 0, tzinfo=timezone.utc)) - 2451545.0) < 1e-6


def test_geodetic_round_trip():
    for lat, lon, alt in [(35.0, 139.0, 0.1), (-24.9, 1.08, 425.0), (0.0, 0.0, 0.0)]:
        back = passes.ecef_to_geodetic(passes.geodetic_to_ecef(lat, lon, alt))
        assert abs(back[0] - lat) < 1e-6
        assert abs(back[1] - lon) < 1e-6
        assert abs(back[2] - alt) < 1e-6


def test_precession_is_small_but_not_zero():
    """J2000 から26年で約0.36度。ゼロにすると地上で約40kmずれるので省けない。"""
    when = datetime(2026, 10, 19, tzinfo=timezone.utc)
    v = (7000.0, 0.0, 0.0)
    moved = passes.precess_j2000_to_date(v, when)
    angle = math.degrees(math.acos(
        sum(a * b for a, b in zip(v, moved)) / (7000.0 * math.sqrt(sum(c * c for c in moved)))
    ))
    assert 0.3 < angle < 0.45


def test_subpoint_altitude_is_iss_like(eph):
    """直下点の高度が ISS の運用高度（約400km台）に収まること。"""
    lat, lon, alt = passes.subpoint(eph.positions[0], eph.times[0])
    assert -60 < lat < 60          # 軌道傾斜角 51.6 度
    assert -180 <= lon <= 180
    assert 380 < alt < 460, alt


def test_overhead_satellite_has_90_degree_elevation():
    """観測地の真上に置いた点は仰角90度になること（符号や軸の取り違え検出）。"""
    when = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)
    overhead = passes.ecef_to_eci(passes.geodetic_to_ecef(_LAT, _LON, 420.0), when)
    az, el, rng = passes.look_angles(overhead, when, _LAT, _LON, 0.01)
    assert el > 89.9, el
    assert 400 < rng < 440


def test_eci_ecef_round_trip():
    """往復で戻ること。歳差を片道だけ当てると、仰角が5度ずれる。"""
    when = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)
    v = (2408.586, -3509.473, -5300.909)
    back = passes.ecef_to_eci(passes.eci_to_ecef(v, when), when)
    for a, b in zip(v, back):
        assert abs(a - b) < 1e-6


# ── 通過の探索 ───────────────────────────────────────────────────────────────

def test_finds_passes_in_the_fixture(eph):
    found = passes.find_passes(eph, _LAT, _LON, start=eph.start, hours=11, min_elevation=10)
    assert found, "11時間に1件も通過が無いのはおかしい"
    for p in found:
        assert p["rise_utc"] < p["max_utc"] < p["set_utc"]
        assert 10 <= p["max_el_deg"] <= 90
        assert 0 <= p["rise_az_deg"] < 360
        # ISS の1回の通過は長くても10分強
        assert 60 < p["duration_sec"] < 800, p["duration_sec"]


def test_peak_is_the_highest_point_of_the_pass(eph):
    found = passes.find_passes(eph, _LAT, _LON, start=eph.start, hours=11, min_elevation=10)
    p = max(found, key=lambda x: x["max_el_deg"])
    for offset in (-120, -60, 60, 120):
        t = p["max_utc"] + timedelta(seconds=offset)
        el = passes.look_angles(eph.position_at(t), t, _LAT, _LON, 0.01)[1]
        assert el <= p["max_el_deg"] + 1e-6


def test_min_elevation_filters_passes(eph):
    low = passes.find_passes(eph, _LAT, _LON, start=eph.start, hours=11, min_elevation=10)
    high = passes.find_passes(eph, _LAT, _LON, start=eph.start, hours=11, min_elevation=60)
    assert len(high) <= len(low)
    assert all(p["max_el_deg"] >= 60 for p in high)


def test_search_stays_inside_the_ephemeris(eph):
    """暦の終端を越えて探しに行かないこと（越えると OemError で通知が止まる）。"""
    found = passes.find_passes(eph, _LAT, _LON, start=eph.end - timedelta(hours=1), hours=48)
    for p in found:
        assert p["set_utc"] <= eph.end


# ── 鮮度の表示 ───────────────────────────────────────────────────────────────

def test_describe_reports_age_and_remaining(eph):
    now = eph.created_at + timedelta(days=3)
    info = oem.describe(eph, now)
    assert info["ageDays"] == 3.0
    assert info["points"] == 167
    assert info["stale"] is True        # この抜粋は11時間分しかない


def test_stale_flag_follows_remaining_time():
    """残り1日を切ったら stale。取りに行く判断と UI の表示がこれを見る。"""
    start = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)
    end = start + timedelta(days=15)
    full = oem.Ephemeris(
        times=[start, end],
        positions=[(6778.0, 0.0, 0.0), (6778.0, 0.0, 0.0)],
        velocities=[(0.0, 7.66, 0.0), (0.0, 7.66, 0.0)],
        created_at=start,
    )
    assert oem.describe(full, start)["stale"] is False
    assert oem.describe(full, end - timedelta(hours=23))["stale"] is True


# ── 取得の状態表示 ───────────────────────────────────────────────────────────

def test_status_reports_absence_without_crashing():
    """暦を1度も取れていなくても、ダッシュボードが読める形を返すこと。"""
    from bridge.features.iss import fetch

    fetch.reset_for_test()
    st = fetch.status()
    assert st["available"] is False
    assert st["lastError"] == ""
    assert st["lastAttempt"] is None
    assert "ephemeris" not in st        # 無いものを空で見せない


def test_status_includes_freshness_when_loaded(eph, monkeypatch):
    """暦があるときは鮮度（作成日時・残り日数）を返すこと。"""
    from bridge.features.iss import fetch

    fetch.reset_for_test()
    monkeypatch.setattr(fetch, "_cache", eph)
    st = fetch.status()
    assert st["available"] is True
    assert st["ephemeris"]["points"] == 167
    assert "createdAt" in st["ephemeris"]
    fetch.reset_for_test()
