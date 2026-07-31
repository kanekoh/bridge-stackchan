#!/usr/bin/env python3
"""
benchmark_responses.py

OpenAI Responses API（main.py の LLM_BACKEND=openai と同じ実装方式）で、
各モデルの応答速度を比較する。音声合成（VOICEVOX）は使わず、テキスト入力のみで測定する。

2つのシナリオを測定する:
  plain      : ツールなしの素の応答速度
  tool_call  : Tool（set_timer）を実際に呼ばせて完了するまでの往復速度・成功可否

Usage:
    python benchmark_responses.py
    python benchmark_responses.py --models gpt-4o-mini gpt-5.6-luna
"""

import argparse
import asyncio
import json
import os
import sys
import time
import types
from dataclasses import dataclass
from typing import Optional

import httpx

# bridge.llm.tools は main モジュールの存在を lazy import 時にしか使わないため、
# ダミーの main モジュールを登録してから import する（実サーバ起動なしで使うため）。
if "main" not in sys.modules:
    sys.modules["main"] = types.ModuleType("main")

from bridge.config import OPENAI_API_KEY, OPENAI_RESPONSES_BASE_URL  # noqa: E402
from bridge.llm.tools import _TIMER_TOOLS  # noqa: E402

DEFAULT_MODELS: list[str] = [
    "gpt-4o",
    "gpt-4o-mini",
    "gpt-4.1",
    "gpt-4.1-mini",
    "gpt-4.1-nano",
    "gpt-5.6-luna",
]

PLAIN_PROMPT = "こんにちは、調子はどう？短く一文で。"
TOOL_PROMPT = "3分後にお風呂の時間だよって教えて。"

INSTRUCTIONS = "あなたはかわいい家族向けアシスタントです。短く一文で答えてください。"


@dataclass
class ModelResult:
    model: str
    plain_ms: Optional[float] = None
    plain_reply: str = ""
    plain_error: Optional[str] = None
    tool_ms: Optional[float] = None
    tool_called: bool = False
    tool_reply: str = ""
    tool_error: Optional[str] = None


async def _post(client: httpx.AsyncClient, payload: dict) -> dict:
    url = OPENAI_RESPONSES_BASE_URL.rstrip("/") + "/responses"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}",
    }
    resp = await client.post(url, json=payload, headers=headers, timeout=60)
    if not resp.is_success:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
    return resp.json()


def _extract_text(data: dict) -> str:
    text = data.get("output_text") or ""
    if text:
        return text
    for item in data.get("output", []):
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                return content["text"]
    return ""


async def bench_plain(client: httpx.AsyncClient, model: str) -> tuple[float, str]:
    payload = {"model": model, "input": PLAIN_PROMPT, "instructions": INSTRUCTIONS}
    t0 = time.perf_counter()
    data = await _post(client, payload)
    ms = (time.perf_counter() - t0) * 1000
    return ms, _extract_text(data)


async def bench_tool_call(client: httpx.AsyncClient, model: str) -> tuple[float, bool, str]:
    """set_timer を実際に呼ばせて完了するまでの合計往復時間を測る（本物の実行はしないダミー実行）。"""
    payload = {
        "model": model,
        "input": TOOL_PROMPT,
        "instructions": INSTRUCTIONS,
        "tools": _TIMER_TOOLS,
    }
    t0 = time.perf_counter()
    data = await _post(client, payload)

    function_calls = [item for item in data.get("output", []) if item.get("type") == "function_call"]
    if not function_calls:
        ms = (time.perf_counter() - t0) * 1000
        return ms, False, _extract_text(data)

    # ダミーのツール実行結果を返して 2 ターン目を発行する
    outputs = []
    for fc in function_calls:
        call_id = fc.get("call_id") or fc.get("id", "")
        outputs.append({
            "type": "function_call_output",
            "call_id": call_id,
            "output": json.dumps({"ok": True, "timer_id": "bench-dummy"}, ensure_ascii=False),
        })

    payload2 = {
        "model": model,
        "input": outputs,
        "instructions": INSTRUCTIONS,
        "tools": _TIMER_TOOLS,
        "previous_response_id": data.get("id"),
    }
    data2 = await _post(client, payload2)
    ms = (time.perf_counter() - t0) * 1000
    return ms, True, _extract_text(data2)


async def bench_model(client: httpx.AsyncClient, model: str) -> ModelResult:
    result = ModelResult(model=model)

    print(f"  {model} : plain ...", end="", flush=True)
    try:
        ms, reply = await bench_plain(client, model)
        result.plain_ms, result.plain_reply = ms, reply
        print(f" {ms:.0f} ms  → {reply!r}")
    except Exception as e:
        result.plain_error = f"{type(e).__name__}: {e}"
        print(f" ERROR: {result.plain_error}")

    print(f"  {model} : tool_call ...", end="", flush=True)
    try:
        ms, called, reply = await bench_tool_call(client, model)
        result.tool_ms, result.tool_called, result.tool_reply = ms, called, reply
        tag = "呼んだ" if called else "呼ばなかった"
        print(f" {ms:.0f} ms  [{tag}]  → {reply!r}")
    except Exception as e:
        result.tool_error = f"{type(e).__name__}: {e}"
        print(f" ERROR: {result.tool_error}")

    return result


def print_summary(results: list[ModelResult]) -> None:
    W = 100
    print()
    print("=" * W)
    print("  OpenAI Responses API ベンチマーク結果（音声合成なし・テキストのみ）")
    print("=" * W)
    hdr = f"{'モデル':<20}{'plain(ms)':>12}{'tool往復(ms)':>14}{'Tool呼出':>10}"
    print(hdr)
    print("-" * W)
    for r in results:
        plain_s = f"{r.plain_ms:.0f}" if r.plain_ms is not None else "ERR"
        tool_s = f"{r.tool_ms:.0f}" if r.tool_ms is not None else "ERR"
        called_s = "○" if r.tool_called else ("✕" if r.tool_ms is not None else "-")
        print(f"{r.model:<20}{plain_s:>12}{tool_s:>14}{called_s:>10}")
    print()

    print("【plain 応答速度 昇順】")
    for r in sorted([r for r in results if r.plain_ms is not None], key=lambda r: r.plain_ms):
        print(f"  {r.plain_ms:>8.0f} ms  {r.model}")
    print()

    print("【tool往復 速度 昇順（Tool を実際に呼んだものだけ）】")
    tool_ok = [r for r in results if r.tool_ms is not None and r.tool_called]
    for r in sorted(tool_ok, key=lambda r: r.tool_ms):
        print(f"  {r.tool_ms:>8.0f} ms  {r.model}")
    not_called = [r.model for r in results if r.tool_ms is not None and not r.tool_called]
    if not_called:
        print(f"  ※ Tool を呼ばなかったモデル: {', '.join(not_called)}")
    print()


async def main() -> None:
    parser = argparse.ArgumentParser(description="OpenAI Responses API モデル比較ベンチマーク")
    parser.add_argument("--models", nargs="+", default=DEFAULT_MODELS, metavar="MODEL")
    args = parser.parse_args()

    if not OPENAI_API_KEY:
        print("ERROR: 環境変数 OPENAI_API_KEY が設定されていません", file=sys.stderr)
        sys.exit(1)

    print(f"Base URL: {OPENAI_RESPONSES_BASE_URL}")
    print(f"Models  : {args.models}")
    print()

    async with httpx.AsyncClient() as client:
        results = [await bench_model(client, model) for model in args.models]

    print_summary(results)


if __name__ == "__main__":
    asyncio.run(main())
