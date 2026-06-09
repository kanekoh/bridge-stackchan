"""
Tests for calendar_sync.py

Google API への実際のリクエストは MagicMock で差し替えているため、
認証情報なしで実行できます。

Run:
    pytest test_calendar_sync.py -v
"""
import sqlite3
import threading
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import pytest

from calendar_sync import (
    _calculate_notify_at,
    _parse_google_datetime,
    cleanup_old_items,
    get_token_file,
    sync_calendars,
    sync_tasks,
)

_JST = timezone(timedelta(hours=9))


# ── フィクスチャ ──────────────────────────────────────────────────────────────

@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.execute("""CREATE TABLE items (
        id TEXT PRIMARY KEY, type TEXT NOT NULL, source_id TEXT NOT NULL,
        person_name TEXT NOT NULL, notify BOOLEAN NOT NULL, title TEXT NOT NULL,
        start_at DATETIME, end_at DATETIME, due_at DATETIME, notify_at DATETIME,
        all_day BOOLEAN NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'active',
        synced_at DATETIME NOT NULL
    )""")
    conn.execute("""CREATE TABLE notification_log (
        event_id TEXT PRIMARY KEY, notified_at DATETIME NOT NULL,
        acked BOOLEAN NOT NULL DEFAULT 0, acked_at DATETIME
    )""")
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def lock():
    return threading.Lock()


def _make_cal_svc(events: list[dict]) -> MagicMock:
    svc = MagicMock()
    svc.events.return_value.list.return_value.execute.return_value = {"items": events}
    return svc


def _make_tasks_svc(tasks: list[dict]) -> MagicMock:
    svc = MagicMock()
    svc.tasks.return_value.list.return_value.execute.return_value = {"items": tasks}
    return svc


def _future(hours: float = 2) -> str:
    return (datetime.now(_JST) + timedelta(hours=hours)).isoformat()


def _past(hours: float = 2) -> str:
    return (datetime.now(_JST) - timedelta(hours=hours)).isoformat()


# ── get_token_file ────────────────────────────────────────────────────────────

def test_token_file_default():
    assert get_token_file(".", "default") == "./token.json"

def test_token_file_named_key():
    assert get_token_file("/tokens", "papa") == "/tokens/token_papa.json"

def test_token_file_named_key_japanese_not_needed():
    # token_key はASCII。日本語名は person_name で管理
    assert get_token_file(".", "mama") == "./token_mama.json"


# ── _parse_google_datetime ────────────────────────────────────────────────────

def test_parse_datetime_jst():
    dt = _parse_google_datetime("2026-06-10T10:00:00+09:00")
    assert dt is not None and dt.hour == 10 and dt.tzinfo is not None

def test_parse_datetime_utc_converts_to_jst():
    dt = _parse_google_datetime("2026-06-10T01:00:00Z")
    assert dt is not None and dt.hour == 10  # UTC 01:00 → JST 10:00

def test_parse_datetime_date_only():
    dt = _parse_google_datetime("2026-06-10")
    assert dt is not None and dt.year == 2026 and dt.month == 6 and dt.day == 10

def test_parse_datetime_none_returns_none():
    assert _parse_google_datetime(None) is None

def test_parse_datetime_invalid_returns_none():
    assert _parse_google_datetime("not-a-date") is None


# ── _calculate_notify_at ─────────────────────────────────────────────────────

def test_notify_at_uses_default_minutes():
    start = datetime(2026, 6, 10, 10, 0, tzinfo=_JST)
    result = _calculate_notify_at(start, {}, default_minutes=15)
    assert result == datetime(2026, 6, 10, 9, 45, tzinfo=_JST)

def test_notify_at_uses_popup_override():
    start = datetime(2026, 6, 10, 10, 0, tzinfo=_JST)
    reminders = {"useDefault": False, "overrides": [{"method": "popup", "minutes": 30}]}
    result = _calculate_notify_at(start, reminders, default_minutes=15)
    assert result == datetime(2026, 6, 10, 9, 30, tzinfo=_JST)

def test_notify_at_falls_back_when_no_popup():
    # email リマインダーのみ → デフォルト値を使用
    start = datetime(2026, 6, 10, 10, 0, tzinfo=_JST)
    reminders = {"useDefault": False, "overrides": [{"method": "email", "minutes": 10}]}
    result = _calculate_notify_at(start, reminders, default_minutes=15)
    assert result == datetime(2026, 6, 10, 9, 45, tzinfo=_JST)


# ── cleanup_old_items ─────────────────────────────────────────────────────────

def test_cleanup_removes_ended_event(db, lock):
    db.execute(
        "INSERT INTO items VALUES ('e1','event','cal1','パパ',1,'会議',NULL,?,NULL,NULL,0,'active',?)",
        (_past(hours=2), datetime.now(_JST).isoformat()),
    )
    db.commit()
    cleanup_old_items(db, lock)
    assert db.execute("SELECT id FROM items WHERE id='e1'").fetchone() is None

def test_cleanup_keeps_future_event(db, lock):
    db.execute(
        "INSERT INTO items VALUES ('e2','event','cal1','パパ',1,'会議',NULL,?,NULL,NULL,0,'active',?)",
        (_future(hours=2), datetime.now(_JST).isoformat()),
    )
    db.commit()
    cleanup_old_items(db, lock)
    assert db.execute("SELECT id FROM items WHERE id='e2'").fetchone() is not None

def test_cleanup_removes_overdue_task(db, lock):
    db.execute(
        "INSERT INTO items VALUES ('t1','task','@default','ママ',1,'タスク',NULL,NULL,?,NULL,0,'active',?)",
        (_past(hours=2), datetime.now(_JST).isoformat()),
    )
    db.commit()
    cleanup_old_items(db, lock)
    assert db.execute("SELECT id FROM items WHERE id='t1'").fetchone() is None


# ── sync_calendars ────────────────────────────────────────────────────────────

def test_sync_calendars_inserts_new_event(db, lock):
    events = [{"id": "evt1", "summary": "朝会", "start": {"dateTime": _future()}, "end": {"dateTime": _future()}}]
    sources = [{"source_id": "cal@gmail.com", "person_name": "パパ", "notify": True}]
    count = sync_calendars(_make_cal_svc(events), db, lock, sources)
    assert count == 1
    row = db.execute("SELECT title, person_name, status FROM items WHERE id='evt1'").fetchone()
    assert row == ("朝会", "パパ", "active")

def test_sync_calendars_upserts_on_title_change(db, lock):
    db.execute(
        "INSERT INTO items VALUES ('evt2','event','cal@gmail.com','パパ',1,'旧タイトル',?,?,NULL,NULL,0,'active',?)",
        (_future(), _future(), datetime.now(_JST).isoformat()),
    )
    db.commit()
    events = [{"id": "evt2", "summary": "新タイトル", "start": {"dateTime": _future()}, "end": {"dateTime": _future()}}]
    sources = [{"source_id": "cal@gmail.com", "person_name": "パパ", "notify": True}]
    sync_calendars(_make_cal_svc(events), db, lock, sources)
    row = db.execute("SELECT title FROM items WHERE id='evt2'").fetchone()
    assert row[0] == "新タイトル"

def test_sync_calendars_marks_deleted_when_not_returned(db, lock):
    db.execute(
        "INSERT INTO items VALUES ('old_evt','event','cal@gmail.com','パパ',1,'消えた会議',?,?,NULL,NULL,0,'active',?)",
        (_future(), _future(), datetime.now(_JST).isoformat()),
    )
    db.commit()
    sources = [{"source_id": "cal@gmail.com", "person_name": "パパ", "notify": True}]
    sync_calendars(_make_cal_svc([]), db, lock, sources)
    row = db.execute("SELECT status FROM items WHERE id='old_evt'").fetchone()
    assert row[0] == "deleted"

def test_sync_calendars_does_not_delete_other_sources(db, lock):
    # cal_b のイベントが cal_a の同期で deleted にならないことを確認
    db.execute(
        "INSERT INTO items VALUES ('evt_b','event','cal_b@gmail.com','ママ',1,'ママの予定',?,?,NULL,NULL,0,'active',?)",
        (_future(), _future(), datetime.now(_JST).isoformat()),
    )
    db.commit()
    sources = [{"source_id": "cal_a@gmail.com", "person_name": "パパ", "notify": True}]
    sync_calendars(_make_cal_svc([]), db, lock, sources)
    row = db.execute("SELECT status FROM items WHERE id='evt_b'").fetchone()
    assert row[0] == "active"

def test_sync_calendars_all_day_event_has_no_notify_at(db, lock):
    events = [{"id": "allday1", "summary": "祝日", "start": {"date": "2026-06-15"}, "end": {"date": "2026-06-16"}}]
    sources = [{"source_id": "cal@gmail.com", "person_name": "パパ", "notify": True}]
    sync_calendars(_make_cal_svc(events), db, lock, sources)
    row = db.execute("SELECT all_day, notify_at FROM items WHERE id='allday1'").fetchone()
    assert row[0] == 1 and row[1] is None


# ── sync_tasks ────────────────────────────────────────────────────────────────

def test_sync_tasks_inserts_new_task(db, lock):
    tasks = [{"id": "task1", "title": "買い物", "status": "needsAction", "due": _future()}]
    sources = [{"source_id": "@default", "person_name": "ママ", "notify": True}]
    count = sync_tasks(_make_tasks_svc(tasks), db, lock, sources)
    assert count == 1
    row = db.execute("SELECT title, person_name, status FROM items WHERE id='task1'").fetchone()
    assert row == ("買い物", "ママ", "active")

def test_sync_tasks_marks_done_when_disappeared(db, lock):
    db.execute(
        "INSERT INTO items VALUES ('task2','task','@default','ママ',1,'完了タスク',NULL,NULL,?,NULL,0,'active',?)",
        (_future(), datetime.now(_JST).isoformat()),
    )
    db.commit()
    sources = [{"source_id": "@default", "person_name": "ママ", "notify": True}]
    sync_tasks(_make_tasks_svc([]), db, lock, sources)
    row = db.execute("SELECT status FROM items WHERE id='task2'").fetchone()
    assert row[0] == "done"

def test_sync_tasks_skips_completed_status(db, lock):
    tasks = [{"id": "task3", "title": "完了済み", "status": "completed", "due": _future()}]
    sources = [{"source_id": "@default", "person_name": "パパ", "notify": True}]
    sync_tasks(_make_tasks_svc(tasks), db, lock, sources)
    assert db.execute("SELECT id FROM items WHERE id='task3'").fetchone() is None

def test_sync_tasks_does_not_affect_other_sources(db, lock):
    db.execute(
        "INSERT INTO items VALUES ('task_b','task','list_b','パパ',1,'パパのタスク',NULL,NULL,?,NULL,0,'active',?)",
        (_future(), datetime.now(_JST).isoformat()),
    )
    db.commit()
    sources = [{"source_id": "@default", "person_name": "ママ", "notify": True}]
    sync_tasks(_make_tasks_svc([]), db, lock, sources)
    row = db.execute("SELECT status FROM items WHERE id='task_b'").fetchone()
    assert row[0] == "active"
