#!/usr/bin/env python3
"""
benchmark_stt.py

VOICEVOX で生成した音声を使い、OpenAI モデルの性能を比較する。

Pipeline A: VOICEVOX → STT (whisper-1 / gpt-4o-transcribe 等) → Text LLM
Pipeline B: VOICEVOX → Audio LLM (gpt-4o-audio-preview 等) に直接音声入力

Usage:
    python benchmark_stt.py [--text "..."] [--speaker 1] [--pipeline both]

Environment variables (main.py / .env と同じ変数名を使用):
    OPENAI_API_KEY      OpenAI API key (必須)
    VOICEVOX_URL        VOICEVOX API の base URL
                          ローカル例: http://localhost:50021
                          Web版例  : https://api.tts.quest/v3/voicevox
    VOICEVOX_SPEAKER    スピーカー ID (デフォルト: 1)
    VOICEVOX_API_KEY    Web版 API key (設定時は Web版として動作)
"""

import argparse
import asyncio
import base64
import io
import os
import sys
import time
from dataclasses import dataclass
from typing import Optional

import httpx
import openai

# ── 設定 ─────────────────────────────────────────────────────────────────────
VOICEVOX_URL: str = os.environ.get("VOICEVOX_URL", "http://localhost:50021")
VOICEVOX_SPEAKER: int = int(os.environ.get("VOICEVOX_SPEAKER", "1"))
VOICEVOX_API_KEY: str = os.environ.get("VOICEVOX_API_KEY", "")
OPENAI_API_KEY: str = os.environ.get("OPENAI_API_KEY", "")

DEFAULT_TEXT: str = "今日の天気はどうですか？"

SYSTEM_PROMPT: str = "あなたはかわいい家族向けアシスタントです。短く一文で答えてください。"
AUDIO_LLM_USER_TEXT: str = "この音声の内容に答えてください。短く一文で答えてください。"

# Pipeline A: STT モデル
STT_MODELS: list[str] = [
    "gpt-4o-transcribe",
    "gpt-4o-mini-transcribe",
    "gpt-transcribe",  # 2026-07-28 GA。gpt-4o-transcribe の後継（非同期・ファイル向け）
]

# Pipeline A: Text LLM モデル
TEXT_LLM_MODELS: list[str] = [
    "gpt-4o",
    "gpt-4o-mini",
    "gpt-4.1",
    "gpt-4.1-mini",
    "gpt-4.1-nano",
    "gpt-5.6-luna",  # 2026-07 GA。低価格・低レイテンシ枠（$1/$6 per 1M tokens）
]

# Pipeline B: 音声を直接受け付ける LLM モデル
AUDIO_LLM_MODELS: list[str] = [
    "gpt-4o-audio-preview",
    "gpt-4o-mini-audio-preview",
]


# ── 結果データ ────────────────────────────────────────────────────────────────
@dataclass
class PipelineAResult:
    stt_model: str
    llm_model: str
    transcript: str
    reply: str
    stt_ms: float
    llm_ms: float
    total_ms: float
    error: Optional[str] = None


@dataclass
class PipelineBResult:
    audio_model: str
    reply: str
    total_ms: float
    error: Optional[str] = None


# ── VOICEVOX ──────────────────────────────────────────────────────────────────
async def generate_audio(text: str, speaker: int, http: httpx.AsyncClient) -> tuple[bytes, str]:
    """音声バイト列と形式 ("wav" or "mp3") を返す。

    VOICEVOX_API_KEY が設定されている場合は Web版 (api.tts.quest) を使用し、
    mp3DownloadUrl からダウンロードして MP3 バイト列を返す。
    未設定の場合はローカル VOICEVOX の audio_query + synthesis で WAV を返す。
    """
    if VOICEVOX_API_KEY:
        # Web版: GET {VOICEVOX_URL}/synthesis → JSON → mp3DownloadUrl → download
        r = await http.get(
            f"{VOICEVOX_URL}/synthesis",
            params={"speaker": speaker, "text": text, "key": VOICEVOX_API_KEY},
            timeout=30.0,
        )
        r.raise_for_status()
        data = r.json()
        if not data.get("success"):
            raise RuntimeError(f"VOICEVOX Web API error: {data}")
        mp3_url = data.get("mp3DownloadUrl")
        if not mp3_url:
            raise RuntimeError(f"No mp3DownloadUrl in response: {data}")
        # 音声生成が非同期のため、準備できるまでリトライ
        for attempt in range(10):
            dl = await http.get(mp3_url, timeout=30.0)
            if dl.status_code == 200:
                return dl.content, "mp3"
            await asyncio.sleep(1.0)
        raise RuntimeError(f"MP3 download failed after retries: {mp3_url}")
    else:
        # ローカル: POST /audio_query → POST /synthesis → WAV
        r = await http.post(
            f"{VOICEVOX_URL}/audio_query",
            params={"text": text, "speaker": speaker},
            timeout=30.0,
        )
        r.raise_for_status()
        r2 = await http.post(
            f"{VOICEVOX_URL}/synthesis",
            params={"speaker": speaker},
            content=r.content,
            headers={"Content-Type": "application/json", "Accept": "audio/wav"},
            timeout=30.0,
        )
        r2.raise_for_status()
        return r2.content, "wav"


# ── Pipeline A: STT ───────────────────────────────────────────────────────────
async def run_stt(audio: bytes, fmt: str, model: str, client: openai.AsyncOpenAI) -> tuple[str, float]:
    filename = f"audio.{fmt}"
    mime = "audio/mpeg" if fmt == "mp3" else "audio/wav"
    audio_file = (filename, io.BytesIO(audio), mime)
    t0 = time.perf_counter()
    result = await client.audio.transcriptions.create(
        model=model,
        file=audio_file,  # type: ignore[arg-type]
        language="ja",
    )
    ms = (time.perf_counter() - t0) * 1000
    return result.text, ms


# ── Pipeline A: Text LLM ──────────────────────────────────────────────────────
async def run_text_llm(transcript: str, model: str, client: openai.AsyncOpenAI) -> tuple[str, float]:
    # gpt-5 系は Chat Completions で max_tokens が使えず max_completion_tokens が必要
    token_kwarg = {"max_completion_tokens": 200} if model.startswith("gpt-5") else {"max_tokens": 200}
    t0 = time.perf_counter()
    resp = await client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": transcript},
        ],
        **token_kwarg,
    )
    ms = (time.perf_counter() - t0) * 1000
    return resp.choices[0].message.content or "", ms


# ── Pipeline B: Audio LLM ─────────────────────────────────────────────────────
async def run_audio_llm(audio: bytes, fmt: str, model: str, client: openai.AsyncOpenAI) -> tuple[str, float]:
    """音声を base64 で渡し、音声対応 LLM に直接テキスト返答させる。

    gpt-audio-* 系モデルは modalities=["text","audio"] + audio パラメータが必要。
    テキスト応答は message.content、または音声応答のトランスクリプト (message.audio.transcript) から取得する。
    """
    audio_b64 = base64.b64encode(audio).decode()
    t0 = time.perf_counter()
    resp = await client.chat.completions.create(
        model=model,
        modalities=["text", "audio"],
        audio={"voice": "alloy", "format": "wav"},
        messages=[
            {
                "role": "system",
                "content": SYSTEM_PROMPT,
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_audio",
                        "input_audio": {"data": audio_b64, "format": fmt},
                    },
                    {"type": "text", "text": AUDIO_LLM_USER_TEXT},
                ],
            },
        ],
        max_tokens=200,
    )
    ms = (time.perf_counter() - t0) * 1000
    msg = resp.choices[0].message
    # テキスト応答優先、なければ音声のトランスクリプトを使用
    text = msg.content or ""
    if not text and hasattr(msg, "audio") and msg.audio:
        text = getattr(msg.audio, "transcript", "") or ""
    return text, ms


# ── ベンチマーク実行 ───────────────────────────────────────────────────────────
async def run_pipeline_a(
    audio: bytes, fmt: str, oai: openai.AsyncOpenAI,
    stt_models: list[str], text_llm_models: list[str],
) -> list[PipelineAResult]:
    results: list[PipelineAResult] = []
    for stt_model in stt_models:
        print(f"  STT: {stt_model} ...", end="", flush=True)
        try:
            transcript, stt_ms = await run_stt(audio, fmt, stt_model, oai)
            print(f" {stt_ms:.0f} ms  → {transcript!r}")
        except Exception as e:
            print(f" ERROR: {e}")
            for llm_model in text_llm_models:
                results.append(PipelineAResult(
                    stt_model=stt_model, llm_model=llm_model,
                    transcript="", reply="", stt_ms=0, llm_ms=0, total_ms=0,
                    error=f"STT: {e}",
                ))
            continue

        for llm_model in text_llm_models:
            print(f"    LLM: {llm_model} ...", end="", flush=True)
            try:
                reply, llm_ms = await run_text_llm(transcript, llm_model, oai)
                total_ms = stt_ms + llm_ms
                print(f" {llm_ms:.0f} ms  → {reply!r}")
                results.append(PipelineAResult(
                    stt_model=stt_model, llm_model=llm_model,
                    transcript=transcript, reply=reply,
                    stt_ms=stt_ms, llm_ms=llm_ms, total_ms=total_ms,
                ))
            except Exception as e:
                print(f" ERROR: {e}")
                results.append(PipelineAResult(
                    stt_model=stt_model, llm_model=llm_model,
                    transcript=transcript, reply="", stt_ms=stt_ms, llm_ms=0,
                    total_ms=stt_ms, error=f"LLM: {e}",
                ))
    return results


async def run_pipeline_b(
    audio: bytes, fmt: str, oai: openai.AsyncOpenAI,
    audio_llm_models: list[str],
) -> list[PipelineBResult]:
    results: list[PipelineBResult] = []
    for model in audio_llm_models:
        print(f"  Audio LLM: {model} ...", end="", flush=True)
        try:
            reply, total_ms = await run_audio_llm(audio, fmt, model, oai)
            print(f" {total_ms:.0f} ms  → {reply!r}")
            results.append(PipelineBResult(audio_model=model, reply=reply, total_ms=total_ms))
        except Exception as e:
            print(f" ERROR: {e}")
            results.append(PipelineBResult(audio_model=model, reply="", total_ms=0, error=str(e)))
    return results


# ── 結果表示 ──────────────────────────────────────────────────────────────────
def print_results(
    a_results: list[PipelineAResult],
    b_results: list[PipelineBResult],
    wav_size: int,
    bench_text: str,
) -> None:
    W = 110
    print()
    print("=" * W)
    print("  OpenAI 音声認識 & 音声LLM ベンチマーク結果")
    print("=" * W)
    print(f"  入力テキスト : {bench_text!r}")
    print(f"  音声サイズ   : {wav_size:,} bytes")
    print()

    # ── Pipeline A ──
    if a_results:
        print("【Pipeline A: VOICEVOX → STT → Text LLM】")
        print("-" * W)
        hdr = f"{'STT モデル':<32}{'LLM モデル':<22}{'STT(ms)':>9}{'LLM(ms)':>9}{'計(ms)':>9}"
        print(hdr)
        print("-" * W)

        prev_stt = None
        for r in a_results:
            if r.stt_model != prev_stt:
                if prev_stt is not None:
                    print()
                prev_stt = r.stt_model

            if r.error:
                row = f"{r.stt_model:<32}{r.llm_model:<22}{'ERR':>9}{'ERR':>9}{'ERR':>9}  !! {r.error}"
            else:
                row = f"{r.stt_model:<32}{r.llm_model:<22}{r.stt_ms:>9.0f}{r.llm_ms:>9.0f}{r.total_ms:>9.0f}"
            print(row)
            if not r.error:
                indent = " " * 54
                print(f"{indent}transcript: {r.transcript!r}")
                print(f"{indent}reply     : {r.reply!r}")
        print()

    # ── Pipeline B ──
    if b_results:
        print("【Pipeline B: VOICEVOX → Audio LLM (音声直接入力)】")
        print("-" * W)
        hdr = f"{'Audio LLM モデル':<40}{'計(ms)':>9}"
        print(hdr)
        print("-" * W)
        for r in b_results:
            if r.error:
                print(f"{r.audio_model:<40}{'ERR':>9}  !! {r.error}")
            else:
                print(f"{r.audio_model:<40}{r.total_ms:>9.0f}")
                print(f"{'':40}  reply: {r.reply!r}")
            print()

    # ── 比較サマリ ──
    print("【比較サマリ (レイテンシ昇順)】")
    print("-" * W)
    entries: list[tuple[float, str]] = []

    # Pipeline A: STT モデルごとに最速 LLM との組み合わせを選ぶ
    best_a: dict[str, PipelineAResult] = {}
    for r in a_results:
        if r.error:
            continue
        if r.stt_model not in best_a or r.total_ms < best_a[r.stt_model].total_ms:
            best_a[r.stt_model] = r
    for r in best_a.values():
        label = f"A [{r.stt_model} + {r.llm_model}]"
        entries.append((r.total_ms, label))

    for r in b_results:
        if not r.error:
            entries.append((r.total_ms, f"B [{r.audio_model}]"))

    for ms, label in sorted(entries):
        print(f"  {ms:>8.0f} ms  {label}")
    print()


# ── エントリポイント ───────────────────────────────────────────────────────────
async def main() -> None:
    parser = argparse.ArgumentParser(
        description="VOICEVOX + OpenAI STT/Audio LLM ベンチマーク",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--text", default=DEFAULT_TEXT, help="VOICEVOX に読ませるテキスト")
    parser.add_argument(
        "--speaker", type=int, default=VOICEVOX_SPEAKER,
        help=f"VOICEVOX スピーカー ID (env: VOICEVOX_SPEAKER, 現在値: {VOICEVOX_SPEAKER})",
    )
    parser.add_argument(
        "--pipeline",
        choices=["a", "b", "both"],
        default="both",
        help="実行するパイプライン: a=STT+LLM, b=AudioLLM, both=両方 (デフォルト: both)",
    )
    parser.add_argument(
        "--stt-models",
        nargs="+",
        default=STT_MODELS,
        metavar="MODEL",
        help="Pipeline A で使う STT モデル一覧",
    )
    parser.add_argument(
        "--text-llm-models",
        nargs="+",
        default=TEXT_LLM_MODELS,
        metavar="MODEL",
        help="Pipeline A で使う Text LLM モデル一覧",
    )
    parser.add_argument(
        "--audio-llm-models",
        nargs="+",
        default=AUDIO_LLM_MODELS,
        metavar="MODEL",
        help="Pipeline B で使う Audio LLM モデル一覧",
    )
    args = parser.parse_args()

    stt_models = args.stt_models
    text_llm_models = args.text_llm_models
    audio_llm_models = args.audio_llm_models

    if not OPENAI_API_KEY:
        print("ERROR: 環境変数 OPENAI_API_KEY が設定されていません", file=sys.stderr)
        sys.exit(1)

    mode = "Web版 (api.tts.quest)" if VOICEVOX_API_KEY else "ローカル"
    print(f"入力テキスト : {args.text!r}")
    print(f"スピーカー   : {args.speaker}")
    print(f"VOICEVOX     : {mode}  ({VOICEVOX_URL})")
    print()

    print("VOICEVOX で音声生成中...", end="", flush=True)
    async with httpx.AsyncClient() as http:
        try:
            audio, fmt = await generate_audio(args.text, args.speaker, http)
        except Exception as e:
            print(f" ERROR: {e}", file=sys.stderr)
            sys.exit(1)
    print(f" 完了 ({len(audio):,} bytes, format={fmt})")
    print()

    oai = openai.AsyncOpenAI(api_key=OPENAI_API_KEY)

    a_results: list[PipelineAResult] = []
    b_results: list[PipelineBResult] = []

    if args.pipeline in ("a", "both"):
        print("=== Pipeline A: STT → Text LLM ===")
        a_results = await run_pipeline_a(audio, fmt, oai, stt_models, text_llm_models)
        print()

    if args.pipeline in ("b", "both"):
        print("=== Pipeline B: Audio LLM ===")
        b_results = await run_pipeline_b(audio, fmt, oai, audio_llm_models)
        print()

    print_results(a_results, b_results, len(audio), args.text)


if __name__ == "__main__":
    asyncio.run(main())
