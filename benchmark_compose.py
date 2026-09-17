"""作曲（compose_song）のモデル比較ベンチマーク。

「安いモデルでも歌が作れるか」を実測する。測るのは2つ。

  形式: 楽譜が parse_score() を一発で通るか、作り直しが要るか、諦めるか
  音楽: 通ったとして、聞いて歌に聞こえるものか

形式は自動で判定できるが、音楽のほうは最後は耳で決めるしかない。そこで
生成した楽譜をすべて YAML で残し、サイン波のプレビュー WAV も書き出す
（VOICEVOX ENGINE が無い環境でも旋律の正誤を確かめられるようにするため）。

実際の作曲経路（bridge/features/song/compose.py）をそのまま動かし、
LLM 呼び出しだけを差し替えてトークン数とレイテンシを記録する。

Usage:
    python benchmark_compose.py                    # 全モデル × 全課題
    python benchmark_compose.py --runs 3           # 各組み合わせ3回
    python benchmark_compose.py --models gpt-4o-mini,gpt-5.6-luna
"""
import argparse
import asyncio
import json
import math
import os
import re
import struct
import sys
import time
import wave
from dataclasses import dataclass, field

import httpx
import yaml
from dotenv import load_dotenv

load_dotenv()
os.environ.setdefault("OPENAI_API_KEY", "")
# 本番の DB を汚さない
os.environ["DB_PATH"] = os.environ.get("BENCH_DB_PATH", "/tmp/bench-compose.db")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import main  # noqa: E402  compose は sys.modules["main"].chat_with_llm を呼ぶ
import bridge.core.db as _db_mod  # noqa: E402
from bridge.features.song import compose as _compose  # noqa: E402
from bridge.features.song import library  # noqa: E402
from bridge.features.song.score import (  # noqa: E402
    DEFAULT_FRAME_RATE, Score, ScoreError, build_notes, score_to_dict,
)

API_URL = os.getenv("OPENAI_RESPONSES_BASE_URL", "https://api.openai.com/v1").rstrip("/") + "/responses"
API_KEY = os.getenv("OPENAI_API_KEY", "")

OUT_DIR = os.getenv("BENCH_OUT_DIR", "data/benchmark_songs")

# 1Mトークンあたりの USD（bridge/api/settings.py の _LLM_MODEL_OPTIONS と同じ値）
PRICES = {
    "gpt-4.1-nano": (0.10, 0.40),
    "gpt-4o-mini":  (0.15, 0.60),
    "gpt-5.6-luna": (0.20, 1.20),
    "gpt-4.1-mini": (0.40, 1.60),
}

DEFAULT_MODELS = list(PRICES)

# gpt-5 系だけ reasoning effort を受け付ける。作曲は「フレーズを組み立てる」仕事なので
# 効き目があるはず、という仮説を確かめるために効果の違いも測る。
EFFORT_VARIANTS = {"gpt-5.6-luna": ["none", "low", "medium"]}


@dataclass
class Task:
    key: str
    theme: str
    mood: str = ""
    # 既知の旋律を指定した課題では、正解の音程の並びと突き合わせる
    truth_intervals: list[int] | None = None
    truth_name: str = ""


# 「歓喜の歌」主題（ハ長調）: ミミファソ ソファミレ ドドレミ ミレレ
_ODE_MIDI = [64, 64, 65, 67, 67, 65, 64, 62, 60, 60, 62, 64, 64, 62, 62]
_ODE_INTERVALS = [b - a for a, b in zip(_ODE_MIDI, _ODE_MIDI[1:])]

TASKS = [
    Task(
        key="hurry",
        theme="時間がないときに急かす歌",
        mood="hurry",
    ),
    Task(
        key="ode",
        theme="ベートーベンの交響曲第9番「歓喜の歌」の、いちばん有名な主題のところを「ら」で歌う",
        mood="happy",
        truth_intervals=_ODE_INTERVALS,
        truth_name="歓喜の歌",
    ),
    Task(
        key="classic",
        theme="有名なクラシックの曲を1つ選んで、その曲のいちばん有名な旋律を「ら」で歌う。曲名をタイトルにする",
        mood="",
    ),
]


@dataclass
class Result:
    model: str
    effort: str
    task: str
    run: int
    ok: bool = False
    attempts: int = 0
    title: str = ""
    error: str = ""
    parse_errors: list[str] = field(default_factory=list)
    latency_ms: int = 0
    tokens_in: int = 0
    tokens_out: int = 0
    reasoning_tokens: int = 0
    metrics: dict = field(default_factory=dict)
    path: str = ""

    @property
    def cost_usd(self) -> float:
        pin, pout = PRICES.get(self.model, (0, 0))
        return (self.tokens_in * pin + self.tokens_out * pout) / 1_000_000


# ── LLM 呼び出し（compose からはこれが chat_with_llm に見える） ──────────────

class _Recorder:
    """compose.compose_score が呼ぶ LLM を差し替え、使用量とレイテンシを記録する。

    プロンプトと検証と作り直しのロジックは本物のまま動かしたいので、
    差し替えるのはこの1点だけにしている。
    """

    def __init__(self, model: str, effort: str):
        self.model, self.effort = model, effort
        self.calls = 0
        self.latency_ms = 0
        self.tokens_in = self.tokens_out = self.reasoning = 0

    async def __call__(self, prompt: str, **kwargs) -> str:
        self.calls += 1
        payload: dict = {
            "model": self.model,
            "input": prompt,
            "max_output_tokens": 4000,
        }
        if self.effort and self.model.startswith(("gpt-5", "o1", "o3", "o4")):
            payload["reasoning"] = {"effort": self.effort}

        t0 = time.monotonic()
        async with httpx.AsyncClient(timeout=180) as client:
            resp = await client.post(
                API_URL, json=payload,
                headers={"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"},
            )
        self.latency_ms += round((time.monotonic() - t0) * 1000)
        resp.raise_for_status()
        data = resp.json()

        usage = data.get("usage", {})
        self.tokens_in += usage.get("input_tokens", 0)
        self.tokens_out += usage.get("output_tokens", 0)
        self.reasoning += usage.get("output_tokens_details", {}).get("reasoning_tokens", 0)

        text = data.get("output_text") or ""
        if not text:
            for item in data.get("output", []):
                for content in item.get("content", []):
                    if content.get("type") == "output_text":
                        text = content["text"]
                        break
                if text:
                    break
        return text


# ── 音楽の機械的な目安 ───────────────────────────────────────────────────────

def melody_metrics(score: Score, truth: list[int] | None) -> dict:
    """歌に聞こえるかの機械的な目安。最終判断は耳で行う前提の補助指標。"""
    keys = [n["key"] for n in build_notes(score) if n["key"] is not None]
    if not keys:
        return {"note_count": 0}

    intervals = [b - a for a, b in zip(keys, keys[1:])]
    leaps = [abs(i) for i in intervals]

    # フレーズの繰り返し: 長さ4の音型がのべ何割再登場するか
    motifs = [tuple(keys[i:i + 4]) for i in range(len(keys) - 3)]
    repeat_ratio = 0.0
    if motifs:
        seen: dict[tuple, int] = {}
        for m in motifs:
            seen[m] = seen.get(m, 0) + 1
        repeat_ratio = sum(c - 1 for c in seen.values()) / len(motifs)

    m = {
        "note_count": len(keys),
        "distinct_pitches": len(set(keys)),
        "range_semitones": max(keys) - min(keys),
        "max_leap": max(leaps) if leaps else 0,
        "big_leap_ratio": round(sum(1 for l in leaps if l > 7) / len(leaps), 3) if leaps else 0.0,
        "same_note_ratio": round(sum(1 for i in intervals if i == 0) / len(intervals), 3) if intervals else 0.0,
        "repeat_ratio": round(repeat_ratio, 3),
        # 最後の音が、いちばん多く出てくる音（主音とみなす）と一致するか
        "ends_on_common_tone": keys[-1] == max(set(keys), key=keys.count),
    }
    if truth:
        m["truth_match"] = round(_interval_match(intervals, truth), 3)
    return m


def _interval_match(got: list[int], truth: list[int]) -> float:
    """音程の並びが正解とどれだけ一致するか（移調は問わない）。

    正解の並びが、生成された並びの中に最もよく当てはまる位置を探し、
    その位置での一致率を返す。0.0〜1.0。
    """
    if not got or not truth:
        return 0.0
    best = 0.0
    for start in range(max(1, len(got) - len(truth) + 1)):
        window = got[start:start + len(truth)]
        if not window:
            continue
        hit = sum(1 for a, b in zip(window, truth) if a == b)
        best = max(best, hit / len(truth))
    return best


# ── サイン波プレビュー（ENGINE が無くても旋律を確かめられるように） ──────────

def render_preview_wav(score: Score, path: str, sample_rate: int = 16000) -> None:
    """楽譜をサイン波の WAV にする。歌声ではなく電子音だが、音程とリズムは同じ。"""
    notes = build_notes(score, DEFAULT_FRAME_RATE)
    frames = bytearray()
    for n in notes:
        dur = n["frame_length"] / DEFAULT_FRAME_RATE
        count = int(dur * sample_rate)
        if n["key"] is None:
            frames += b"\x00\x00" * count
            continue
        freq = 440.0 * (2 ** ((n["key"] - 69) / 12))
        # 音の切れ目でプツッと鳴らないよう、前後を短くなだらかにする
        attack = min(int(0.008 * sample_rate), count // 4)
        release = min(int(0.03 * sample_rate), count // 3)
        for i in range(count):
            env = 1.0
            if i < attack:
                env = i / max(1, attack)
            elif i > count - release:
                env = max(0.0, (count - i) / max(1, release))
            # 倍音を少し混ぜて、純粋なサイン波より聞き取りやすくする
            t = i / sample_rate
            v = math.sin(2 * math.pi * freq * t) + 0.25 * math.sin(4 * math.pi * freq * t)
            frames += struct.pack("<h", int(max(-1.0, min(1.0, v / 1.25)) * 20000 * env))

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with wave.open(path, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        wf.writeframes(bytes(frames))


_UNSAFE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def safe_name(text: str) -> str:
    return _UNSAFE.sub("_", text).strip().replace(" ", "_")[:60]


# ── 実行 ─────────────────────────────────────────────────────────────────────

async def run_one(model: str, effort: str, task: Task, run: int) -> Result:
    label = model if not effort else f"{model}[{effort}]"
    r = Result(model=model, effort=effort, task=task.key, run=run)
    rec = _Recorder(model, effort)

    prev = main.chat_with_llm
    main.chat_with_llm = rec
    try:
        score = await _compose.compose_score(theme=task.theme, mood=task.mood)
        r.ok = True
        r.title = score.title
    except ScoreError as e:
        r.error = str(e)
    except Exception as e:
        r.error = f"{type(e).__name__}: {e}"
    finally:
        main.chat_with_llm = prev

    r.attempts = rec.calls
    r.latency_ms = rec.latency_ms
    r.tokens_in, r.tokens_out, r.reasoning_tokens = rec.tokens_in, rec.tokens_out, rec.reasoning

    if r.ok:
        r.metrics = melody_metrics(score, task.truth_intervals)
        base = f"{safe_name(label)}__{task.key}{run}__{safe_name(score.title)}"
        wav_path = os.path.join(OUT_DIR, f"{base}.wav")
        render_preview_wav(score, wav_path)
        data = score_to_dict(score)
        data["id"] = base
        data["title"] = f"{score.title}（{label}）"
        with open(os.path.join(OUT_DIR, f"{base}.yaml"), "w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)
        r.path = wav_path
        library.delete_song(score.id)   # ベンチの曲を本番のライブラリに残さない

    status = "OK " if r.ok else "NG "
    extra = f" match={r.metrics.get('truth_match')}" if r.metrics.get("truth_match") is not None else ""
    print(f"  {status}{label:22s} {task.key:8s} run{run} "
          f"{r.latency_ms:6d}ms attempts={r.attempts} "
          f"{('「' + r.title + '」') if r.ok else r.error[:60]}{extra}", flush=True)
    return r


async def main_async(models: list[str], runs: int, tasks: list[Task], concurrency: int) -> list[Result]:
    combos = []
    for model in models:
        for effort in EFFORT_VARIANTS.get(model, [""]):
            for task in tasks:
                for run in range(1, runs + 1):
                    combos.append((model, effort, task, run))

    print(f"作曲ベンチマーク: {len(combos)} 回の呼び出し "
          f"（{len(models)}モデル × {len(tasks)}課題 × {runs}回、Luna は effort 3種）\n")

    sem = asyncio.Semaphore(concurrency)

    async def guarded(args):
        async with sem:
            return await run_one(*args)

    return await asyncio.gather(*(guarded(c) for c in combos))


def report(results: list[Result]) -> None:
    by_label: dict[str, list[Result]] = {}
    for r in results:
        by_label.setdefault(r.model if not r.effort else f"{r.model}[{r.effort}]", []).append(r)

    print("\n" + "=" * 100)
    print("形式（楽譜が parse_score を通るか）")
    print("=" * 100)
    print(f"{'モデル':24s} {'成功':>6s} {'一発':>6s} {'作り直し':>8s} {'失敗':>6s} "
          f"{'平均ms':>8s} {'推論tok':>8s} {'1曲あたり':>10s}")
    for label, rs in by_label.items():
        ok = [r for r in rs if r.ok]
        first = [r for r in ok if r.attempts == 1]
        retry = [r for r in ok if r.attempts > 1]
        avg_ms = round(sum(r.latency_ms for r in rs) / len(rs))
        avg_reason = round(sum(r.reasoning_tokens for r in rs) / len(rs))
        avg_cost = sum(r.cost_usd for r in rs) / len(rs)
        print(f"{label:24s} {len(ok):>3d}/{len(rs):<3d} {len(first):>6d} {len(retry):>8d} "
              f"{len(rs) - len(ok):>6d} {avg_ms:>8d} {avg_reason:>8d} ${avg_cost:>9.5f}")

    print("\n" + "=" * 100)
    print("音楽（機械的な目安。最終判断は耳で）")
    print("=" * 100)
    print(f"{'モデル':24s} {'課題':8s} {'音数':>5s} {'音種':>5s} {'音域':>5s} "
          f"{'最大跳躍':>8s} {'同音率':>7s} {'反復率':>7s} {'主音終止':>8s} {'一致率':>7s}")
    for label, rs in by_label.items():
        for r in sorted(rs, key=lambda x: (x.task, x.run)):
            if not r.ok:
                continue
            m = r.metrics
            tm = m.get("truth_match")
            print(f"{label:24s} {r.task:8s} {m['note_count']:>5d} {m['distinct_pitches']:>5d} "
                  f"{m['range_semitones']:>5d} {m['max_leap']:>8d} {m['same_note_ratio']:>7.2f} "
                  f"{m['repeat_ratio']:>7.2f} {str(m['ends_on_common_tone']):>8s} "
                  f"{(f'{tm:.2f}' if tm is not None else '-'):>7s}")

    fails = [r for r in results if not r.ok]
    if fails:
        print("\n" + "=" * 100)
        print("失敗の内訳")
        print("=" * 100)
        for r in fails:
            label = r.model if not r.effort else f"{r.model}[{r.effort}]"
            print(f"  {label:24s} {r.task:8s} run{r.run}: {r.error[:110]}")

    total = sum(r.cost_usd for r in results)
    print(f"\n合計コスト: ${total:.4f}")
    print(f"生成物: {OUT_DIR}/ （*.wav = サイン波プレビュー、*.yaml = 楽譜）")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="作曲モデルの比較ベンチマーク")
    ap.add_argument("--models", default=",".join(DEFAULT_MODELS))
    ap.add_argument("--runs", type=int, default=2)
    ap.add_argument("--tasks", default=",".join(t.key for t in TASKS))
    ap.add_argument("--concurrency", type=int, default=4)
    args = ap.parse_args()

    _db_mod._init_db()
    library.load_scores("/nonexistent")   # 同梱の楽譜は読まない（id 衝突を避ける）

    wanted = set(args.tasks.split(","))
    tasks = [t for t in TASKS if t.key in wanted]
    results = asyncio.run(main_async(
        args.models.split(","), args.runs, tasks, args.concurrency))
    report(results)

    with open(os.path.join(OUT_DIR, "results.json"), "w", encoding="utf-8") as f:
        json.dump([{**r.__dict__, "cost_usd": r.cost_usd} for r in results],
                  f, ensure_ascii=False, indent=2)
