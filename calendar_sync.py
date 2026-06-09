import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

_JST = timezone(timedelta(hours=9))

SCOPES = [
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/tasks.readonly",
]


def get_token_file(token_dir: str, token_key: str) -> str:
    """token_key='default' → token.json、それ以外 → token_{key}.json"""
    filename = "token.json" if token_key == "default" else f"token_{token_key}.json"
    return os.path.join(token_dir, filename)


def build_services(credentials_file: str, token_file: str):
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build

    creds = None
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(token_file, "w") as f:
                f.write(creds.to_json())
        else:
            raise RuntimeError(
                f"トークンが見つかりません: {token_file}\n"
                f"実行してください: python calendar_sync.py --auth [--key <key>]"
            )

    return build("calendar", "v3", credentials=creds), build("tasks", "v1", credentials=creds)


def _parse_google_datetime(dt_str: str | None) -> datetime | None:
    if not dt_str:
        return None
    try:
        if "T" in dt_str:
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            return dt.astimezone(_JST)
        d = datetime.strptime(dt_str, "%Y-%m-%d")
        return d.replace(tzinfo=_JST)
    except Exception:
        return None


def _calculate_notify_at(
    start_dt: datetime,
    reminders: dict,
    default_minutes: int,
) -> datetime:
    if not reminders.get("useDefault", True):
        popup = next(
            (r for r in reminders.get("overrides", []) if r["method"] == "popup"),
            None,
        )
        if popup:
            return start_dt - timedelta(minutes=int(popup["minutesBefore"]))
    return start_dt - timedelta(minutes=default_minutes)


def sync_calendars(
    calendar_svc,
    db_conn: sqlite3.Connection,
    db_lock: threading.Lock,
    sources: list[dict],
    default_notify_minutes: int = 15,
    sync_days_ahead: int = 7,
) -> int:
    """sources: [{"source_id": ..., "person_name": ..., "notify": ...}]"""
    now_utc = datetime.now(timezone.utc)
    time_min = now_utc.isoformat()
    time_max = (now_utc + timedelta(days=sync_days_ahead)).isoformat()
    fetched_ids: set[str] = set()
    synced_source_ids = {s["source_id"] for s in sources}

    for src in sources:
        cal_id = src["source_id"]
        person_name = src["person_name"]
        notify = int(src.get("notify", True))
        try:
            result = calendar_svc.events().list(
                calendarId=cal_id,
                timeMin=time_min,
                timeMax=time_max,
                singleEvents=True,
                orderBy="startTime",
            ).execute()
        except Exception as e:
            logger.error("Calendar fetch failed: calendar_id=%s error=%s", cal_id, e)
            continue

        for event in result.get("items", []):
            eid = event["id"]
            fetched_ids.add(eid)
            title = event.get("summary", "(タイトルなし)")
            start = event.get("start", {})
            end = event.get("end", {})
            all_day = "date" in start and "dateTime" not in start
            start_dt = _parse_google_datetime(start.get("dateTime") or start.get("date"))
            end_dt = _parse_google_datetime(end.get("dateTime") or end.get("date"))

            notify_at = None
            if not all_day and start_dt:
                na = _calculate_notify_at(start_dt, event.get("reminders", {}), default_notify_minutes)
                if na > datetime.now(_JST):
                    notify_at = na

            synced_at = datetime.now(_JST).isoformat()
            with db_lock:
                db_conn.execute(
                    """
                    INSERT INTO items
                        (id, type, source_id, person_name, notify, title,
                         start_at, end_at, due_at, notify_at, all_day, status, synced_at)
                    VALUES (?, 'event', ?, ?, ?, ?, ?, ?, NULL, ?, ?, 'active', ?)
                    ON CONFLICT(id) DO UPDATE SET
                        title     = excluded.title,
                        start_at  = excluded.start_at,
                        end_at    = excluded.end_at,
                        notify_at = excluded.notify_at,
                        notify    = excluded.notify,
                        status    = CASE WHEN items.status = 'deleted' THEN 'active' ELSE items.status END,
                        synced_at = excluded.synced_at
                    """,
                    (
                        eid, cal_id, person_name, notify, title,
                        start_dt.isoformat() if start_dt else None,
                        end_dt.isoformat() if end_dt else None,
                        notify_at.isoformat() if notify_at else None,
                        int(all_day), synced_at,
                    ),
                )
                db_conn.commit()

    # 今回同期したソースの中で取得できなかったイベントを deleted にマーク
    src_phs = ",".join("?" * len(synced_source_ids))
    with db_lock:
        if fetched_ids:
            id_phs = ",".join("?" * len(fetched_ids))
            db_conn.execute(
                f"UPDATE items SET status = 'deleted' WHERE type = 'event' AND status = 'active'"
                f" AND source_id IN ({src_phs}) AND id NOT IN ({id_phs})",
                list(synced_source_ids) + list(fetched_ids),
            )
        else:
            db_conn.execute(
                f"UPDATE items SET status = 'deleted' WHERE type = 'event' AND status = 'active'"
                f" AND source_id IN ({src_phs})",
                list(synced_source_ids),
            )
        db_conn.commit()

    logger.info("Calendar sync done: %d events from %d sources", len(fetched_ids), len(sources))
    return len(fetched_ids)


def sync_tasks(
    tasks_svc,
    db_conn: sqlite3.Connection,
    db_lock: threading.Lock,
    sources: list[dict],
    default_notify_minutes: int = 15,
) -> int:
    """sources: [{"source_id": ..., "person_name": ..., "notify": ...}]"""
    fetched_ids: set[str] = set()
    synced_source_ids = {s["source_id"] for s in sources}

    for src in sources:
        tl_id = src["source_id"]
        person_name = src["person_name"]
        notify = int(src.get("notify", True))
        try:
            result = tasks_svc.tasks().list(
                tasklist=tl_id,
                showCompleted=False,
                showHidden=False,
            ).execute()
        except Exception as e:
            logger.error("Tasks fetch failed: tasklist_id=%s error=%s", tl_id, e)
            continue

        for task in result.get("items", []):
            if task.get("status") == "completed":
                continue
            tid = task["id"]
            fetched_ids.add(tid)
            title = task.get("title", "(タイトルなし)")
            due_dt = _parse_google_datetime(task.get("due"))

            notify_at = None
            if due_dt:
                na = due_dt - timedelta(minutes=default_notify_minutes)
                if na > datetime.now(_JST):
                    notify_at = na

            synced_at = datetime.now(_JST).isoformat()
            with db_lock:
                db_conn.execute(
                    """
                    INSERT INTO items
                        (id, type, source_id, person_name, notify, title,
                         start_at, end_at, due_at, notify_at, all_day, status, synced_at)
                    VALUES (?, 'task', ?, ?, ?, ?, NULL, NULL, ?, ?, 0, 'active', ?)
                    ON CONFLICT(id) DO UPDATE SET
                        title     = excluded.title,
                        due_at    = excluded.due_at,
                        notify_at = excluded.notify_at,
                        notify    = excluded.notify,
                        status    = CASE WHEN items.status = 'done' THEN 'active' ELSE items.status END,
                        synced_at = excluded.synced_at
                    """,
                    (
                        tid, tl_id, person_name, notify, title,
                        due_dt.isoformat() if due_dt else None,
                        notify_at.isoformat() if notify_at else None,
                        synced_at,
                    ),
                )
                db_conn.commit()

    # アクティブタスクで今回取得できなかったもの = 完了 or 削除 → done にマーク
    src_phs = ",".join("?" * len(synced_source_ids))
    with db_lock:
        if fetched_ids:
            id_phs = ",".join("?" * len(fetched_ids))
            db_conn.execute(
                f"UPDATE items SET status = 'done' WHERE type = 'task' AND status = 'active'"
                f" AND source_id IN ({src_phs}) AND id NOT IN ({id_phs})",
                list(synced_source_ids) + list(fetched_ids),
            )
        else:
            db_conn.execute(
                f"UPDATE items SET status = 'done' WHERE type = 'task' AND status = 'active'"
                f" AND source_id IN ({src_phs})",
                list(synced_source_ids),
            )
        db_conn.commit()

    logger.info("Task sync done: %d tasks from %d sources", len(fetched_ids), len(sources))
    return len(fetched_ids)


def cleanup_old_items(db_conn: sqlite3.Connection, db_lock: threading.Lock) -> None:
    cutoff = (datetime.now(_JST) - timedelta(hours=1)).isoformat()
    with db_lock:
        c = db_conn.execute(
            """DELETE FROM items WHERE
               (end_at IS NOT NULL AND end_at < ?)
               OR (type = 'task' AND due_at IS NOT NULL AND due_at < ?)""",
            (cutoff, cutoff),
        )
        db_conn.commit()
    if c.rowcount:
        logger.info("Cleaned up %d old items", c.rowcount)


def sync_all_from_db(
    db_conn: sqlite3.Connection,
    db_lock: threading.Lock,
    credentials_file: str,
    token_dir: str,
    default_notify_minutes: int = 15,
    sync_days_ahead: int = 7,
) -> None:
    with db_lock:
        rows = db_conn.execute(
            "SELECT source_type, source_id, person_name, notify, token_key "
            "FROM calendar_sources WHERE enabled = 1"
        ).fetchall()

    if not rows:
        logger.info("No calendar sources registered — skipping sync")
        return

    by_token: dict[str, dict[str, list]] = {}
    for source_type, source_id, person_name, notify, token_key in rows:
        entry = by_token.setdefault(token_key, {"calendar": [], "tasklist": []})
        entry[source_type].append({
            "source_id": source_id,
            "person_name": person_name,
            "notify": bool(notify),
        })

    for token_key, sources in by_token.items():
        token_file = get_token_file(token_dir, token_key)
        try:
            cal_svc, tasks_svc = build_services(credentials_file, token_file)
        except Exception as e:
            logger.error("Auth failed: token_key=%s error=%s", token_key, e)
            continue

        if sources["calendar"]:
            sync_calendars(cal_svc, db_conn, db_lock, sources["calendar"], default_notify_minutes, sync_days_ahead)
        if sources["tasklist"]:
            sync_tasks(tasks_svc, db_conn, db_lock, sources["tasklist"], default_notify_minutes)

    cleanup_old_items(db_conn, db_lock)


def run_sync_loop(
    db_conn: sqlite3.Connection,
    db_lock: threading.Lock,
    credentials_file: str,
    token_dir: str,
    interval_minutes: int,
    default_notify_minutes: int = 15,
    sync_days_ahead: int = 7,
) -> None:
    logger.info("Calendar sync thread started: interval=%dmin", interval_minutes)
    while True:
        try:
            sync_all_from_db(db_conn, db_lock, credentials_file, token_dir, default_notify_minutes, sync_days_ahead)
        except Exception as e:
            logger.error("Calendar sync error: %s", e)
        time.sleep(interval_minutes * 60)


def start_sync_thread(
    db_conn: sqlite3.Connection,
    db_lock: threading.Lock,
    credentials_file: str,
    token_dir: str,
    interval_minutes: int = 30,
    default_notify_minutes: int = 15,
    sync_days_ahead: int = 7,
) -> threading.Thread:
    t = threading.Thread(
        target=run_sync_loop,
        args=(db_conn, db_lock, credentials_file, token_dir, interval_minutes, default_notify_minutes, sync_days_ahead),
        daemon=True,
        name="calendar-sync",
    )
    t.start()
    return t


if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    credentials_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "secrets/credentials.json")
    token_dir = os.getenv("GOOGLE_TOKEN_DIR", "secrets")

    key = "default"
    if "--key" in sys.argv:
        idx = sys.argv.index("--key")
        key = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else "default"

    token_file = get_token_file(token_dir, key)

    if "--auth" in sys.argv:
        from google_auth_oauthlib.flow import InstalledAppFlow
        flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
        creds = flow.run_local_server(port=0)
        with open(token_file, "w") as f:
            f.write(creds.to_json())
        print(f"認証完了！トークンを保存しました: {token_file}")

    elif "--list" in sys.argv:
        cal_svc, tasks_svc = build_services(credentials_file, token_file)
        print(f"=== カレンダー（key={key}）===")
        for cal in cal_svc.calendarList().list().execute().get("items", []):
            print(f"  source_id={cal['id']}  name={cal.get('summary', '')}")
        print(f"\n=== タスクリスト（key={key}）===")
        for tl in tasks_svc.tasklists().list().execute().get("items", []):
            print(f"  source_id={tl['id']}  name={tl.get('title', '')}")

    elif "--register-all" in sys.argv:
        # --person パパ  で声かけに使う日本語名を指定
        person_name = "unknown"
        if "--person" in sys.argv:
            idx = sys.argv.index("--person")
            person_name = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else "unknown"

        import sqlite3 as _sqlite3
        db_path = os.getenv("DB_PATH", "data/bridge.db")
        conn = _sqlite3.connect(db_path)
        conn.execute("""CREATE TABLE IF NOT EXISTS calendar_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_type TEXT NOT NULL CHECK(source_type IN ('calendar', 'tasklist')),
            source_id TEXT NOT NULL, person_name TEXT NOT NULL,
            notify BOOLEAN NOT NULL DEFAULT 1, token_key TEXT NOT NULL DEFAULT 'default',
            enabled BOOLEAN NOT NULL DEFAULT 1,
            created_at DATETIME NOT NULL, updated_at DATETIME NOT NULL,
            UNIQUE(source_type, source_id)
        )""")
        conn.commit()

        cal_svc, tasks_svc = build_services(credentials_file, token_file)
        now = datetime.now(_JST).isoformat()

        print(f"登録中... person_name={person_name}  token_key={key}")
        cal_count = task_count = 0

        for cal in cal_svc.calendarList().list().execute().get("items", []):
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO calendar_sources"
                    " (source_type, source_id, person_name, notify, token_key, enabled, created_at, updated_at)"
                    " VALUES ('calendar', ?, ?, 1, ?, 1, ?, ?)",
                    (cal["id"], person_name, key, now, now),
                )
                cal_count += 1
                print(f"  [calendar] {cal['id']}  ({cal.get('summary', '')})")
            except Exception as e:
                print(f"  ※ スキップ: {cal['id']} — {e}")

        for tl in tasks_svc.tasklists().list().execute().get("items", []):
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO calendar_sources"
                    " (source_type, source_id, person_name, notify, token_key, enabled, created_at, updated_at)"
                    " VALUES ('tasklist', ?, ?, 1, ?, 1, ?, ?)",
                    (tl["id"], person_name, key, now, now),
                )
                task_count += 1
                print(f"  [tasklist] {tl['id']}  ({tl.get('title', '')})")
            except Exception as e:
                print(f"  ※ スキップ: {tl['id']} — {e}")

        conn.commit()
        conn.close()
        print(f"\n完了: カレンダー {cal_count}件 / タスクリスト {task_count}件 を登録しました")

    else:
        print("使い方:")
        print("  python calendar_sync.py --auth [--key papa]                      # OAuth 認証")
        print("  python calendar_sync.py --list [--key papa]                      # ID一覧を表示")
        print("  python calendar_sync.py --register-all --key papa --person パパ  # 全件をDBに自動登録")
