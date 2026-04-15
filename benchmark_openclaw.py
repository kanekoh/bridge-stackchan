#!/usr/bin/env python3
"""Benchmark script for OpenClaw /v1/responses latency.

Usage:
    python benchmark_openclaw.py

Edit REQUEST_BODY below to change the request payload.
Connection settings are loaded from .env automatically.
"""

import os
from datetime import datetime, timezone, timedelta

import httpx
from dotenv import load_dotenv

load_dotenv()

# -------------------------------------------------------------------
# Connection settings (loaded from .env)
# -------------------------------------------------------------------
OPENCLAW_BASE_URL      = os.getenv("OPENCLAW_BASE_URL", "http://localhost:18789/v1")
OPENCLAW_MODEL         = os.getenv("OPENCLAW_MODEL", "openclaw")
OPENCLAW_GATEWAY_TOKEN = os.getenv("OPENCLAW_GATEWAY_TOKEN", "")
OPENCLAW_SESSION_KEY   = os.getenv("OPENCLAW_SESSION_KEY", "")
_raw = os.getenv("OPENCLAW_MAX_OUTPUT_TOKENS", "")
OPENCLAW_MAX_OUTPUT_TOKENS: int | None = int(_raw) if _raw.strip() else None

# -------------------------------------------------------------------
# Request body (edit freely)
# -------------------------------------------------------------------
REQUEST_BODY: dict = {
    "input": "hello",
    # "instructions": "You are a helpful assistant.",  # uncomment to add a system prompt
}

# -------------------------------------------------------------------
# Internals
# -------------------------------------------------------------------
_JST = timezone(timedelta(hours=9))
_DT_FORMAT = "%Y-%m-%dT%H:%M:%S.%f+09:00"


def _now_jst() -> datetime:
    return datetime.now(_JST)


def _fmt(dt: datetime) -> str:
    """Format datetime as 2026-04-15T08:18:03.467+09:00 (milliseconds, JST)."""
    s = dt.strftime(_DT_FORMAT)
    # strftime's %f is 6-digit microseconds — truncate to 3-digit milliseconds
    dot = s.index(".")
    return s[: dot + 4] + "+09:00"


def main() -> None:
    url = OPENCLAW_BASE_URL.rstrip("/") + "/responses"

    headers: dict = {
        "Content-Type": "application/json",
        "x-openclaw-scopes": "operator.read,operator.write",
    }
    if OPENCLAW_GATEWAY_TOKEN:
        headers["Authorization"] = f"Bearer {OPENCLAW_GATEWAY_TOKEN}"
    if OPENCLAW_SESSION_KEY:
        headers["x-openclaw-session-key"] = OPENCLAW_SESSION_KEY

    payload: dict = {
        "model": OPENCLAW_MODEL,
        **REQUEST_BODY,
    }
    if OPENCLAW_MAX_OUTPUT_TOKENS is not None:
        payload["max_output_tokens"] = OPENCLAW_MAX_OUTPUT_TOKENS

    print(f"URL   : {url}")
    print(f"Model : {OPENCLAW_MODEL}")
    print(f"Input : {REQUEST_BODY.get('input', '')!r}")
    print()

    start = _now_jst()
    print(f"Start : {_fmt(start)}")

    try:
        with httpx.Client(timeout=120) as client:
            resp = client.post(url, json=payload, headers=headers)
    except httpx.RequestError as e:
        end = _now_jst()
        elapsed = (end - start).total_seconds()
        print(f"End   : {_fmt(end)}")
        print(f"Elapsed: {elapsed:.3f} s")
        print(f"Error : {type(e).__name__}: {e}")
        return

    end = _now_jst()
    elapsed = (end - start).total_seconds()

    print(f"End   : {_fmt(end)}")
    print(f"Elapsed: {elapsed:.3f} s")
    print(f"Status: {resp.status_code}")
    print()

    if not resp.is_success:
        print(f"[ERROR] HTTP {resp.status_code}")
        print(resp.text[:500])
        return

    data = resp.json()

    # Prefer the output_text convenience field; fall back to scanning output array
    reply: str | None = data.get("output_text")
    if not reply:
        for item in data.get("output", []):
            for content in item.get("content", []):
                if content.get("type") == "output_text":
                    reply = content["text"]
                    break
            if reply:
                break

    if reply:
        print(f"Reply : {reply}")
    else:
        print("[RAW response]")
        import json
        print(json.dumps(data, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
