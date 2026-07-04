"""Web checks CRUD and run endpoints."""
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from bridge.config import _JST
from bridge.core.db import _db_lock, _db_conn
from bridge.features.weather.notify import _run_web_check

router = APIRouter()


class WebCheckCreate(BaseModel):
    name: str
    url: str = ""
    check_prompt: str = "申し込みが現在受付中かどうかを判定してください。"
    mode: str = "check"
    enabled: bool = True
    notify_time: str = "07:55"
    notify_expression: str = "happy"


class WebCheckUpdate(BaseModel):
    name: str | None = None
    url: str | None = None
    check_prompt: str | None = None
    mode: str | None = None
    enabled: bool | None = None
    notify_time: str | None = None
    notify_expression: str | None = None


@router.get("/api/web-checks")
def list_web_checks():
    with _db_lock:
        rows = _db_conn.execute(
            "SELECT id, name, url, check_prompt, enabled, notify_time, notify_expression, mode, "
            "last_checked_at, last_status, last_notified_date, created_at, updated_at "
            "FROM web_checks ORDER BY id"
        ).fetchall()
    return {
        "items": [
            {
                "id": r[0], "name": r[1], "url": r[2], "check_prompt": r[3],
                "enabled": bool(r[4]), "notify_time": r[5], "notify_expression": r[6],
                "mode": r[7] or "check",
                "last_checked_at": r[8], "last_status": r[9],
                "last_notified_date": r[10], "created_at": r[11], "updated_at": r[12],
            }
            for r in rows
        ]
    }


@router.post("/api/web-checks", status_code=201)
def create_web_check(req: WebCheckCreate):
    now = datetime.now(_JST).isoformat()
    try:
        with _db_lock:
            cursor = _db_conn.execute(
                "INSERT INTO web_checks "
                "(name, url, check_prompt, mode, enabled, notify_time, notify_expression, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (req.name, req.url, req.check_prompt, req.mode, int(req.enabled),
                 req.notify_time, req.notify_expression, now, now),
            )
            _db_conn.commit()
            row_id = cursor.lastrowid
    except Exception as e:
        if "UNIQUE" in str(e):
            raise HTTPException(status_code=409, detail=f"「{req.name}」はすでに登録されています")
        raise HTTPException(status_code=500, detail=str(e))
    return {"id": row_id, "name": req.name}


@router.put("/api/web-checks/{wc_id}")
def update_web_check(wc_id: int, req: WebCheckUpdate):
    now = datetime.now(_JST).isoformat()
    fields: list[str] = []
    vals: list = []
    if req.name is not None:
        fields.append("name = ?"); vals.append(req.name)
    if req.url is not None:
        fields.append("url = ?"); vals.append(req.url)
    if req.check_prompt is not None:
        fields.append("check_prompt = ?"); vals.append(req.check_prompt)
    if req.mode is not None:
        fields.append("mode = ?"); vals.append(req.mode)
    if req.enabled is not None:
        fields.append("enabled = ?"); vals.append(int(req.enabled))
    if req.notify_time is not None:
        fields.append("notify_time = ?"); vals.append(req.notify_time)
    if req.notify_expression is not None:
        fields.append("notify_expression = ?"); vals.append(req.notify_expression)
    if not fields:
        raise HTTPException(status_code=422, detail="更新するフィールドがありません")
    fields.append("updated_at = ?"); vals.append(now)
    vals.append(wc_id)
    with _db_lock:
        c = _db_conn.execute(
            f"UPDATE web_checks SET {', '.join(fields)} WHERE id = ?", vals
        )
        _db_conn.commit()
    if c.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"id={wc_id} は見つかりません")
    return {"updated": wc_id}


@router.delete("/api/web-checks/{wc_id}", status_code=204)
def delete_web_check(wc_id: int):
    with _db_lock:
        c = _db_conn.execute("DELETE FROM web_checks WHERE id = ?", (wc_id,))
        _db_conn.commit()
    if c.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"id={wc_id} は見つかりません")


@router.post("/api/web-checks/{wc_id}/run")
async def run_web_check_now(wc_id: int):
    """手動で今すぐチェックを実行する（read モードは実際に読み上げも行う）。"""
    with _db_lock:
        row = _db_conn.execute(
            "SELECT id, name, url, check_prompt, mode, notify_expression FROM web_checks WHERE id = ?", (wc_id,)
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"id={wc_id} は見つかりません")
    _, name, url, check_prompt, mode, notify_expression = row
    if not url:
        raise HTTPException(status_code=422, detail="URL が未設定です")
    now_jst = datetime.now(_JST)
    result = await _run_web_check(
        wc_id, name, url, check_prompt, now_jst, today_str=None,
        mode=mode or "check", notify_expression=notify_expression or "happy",
    )
    return {"name": name, **result}
