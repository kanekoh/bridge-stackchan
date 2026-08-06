"""会話ログから長期記憶を抽出する夜間バッチ。

何を覚えるかをこちらでルール化しない（「家族構成なら覚える」「天気は覚えない」
のような分類を書かない）のが方針。LLM に「後で思い出せたら嬉しいこと」を
自由に書き出させ、意味的な検索で引く。想定していなかった種類の記憶も拾える。

重要でない情報（天気・小さい地震など）の除外は、プロンプトのルールではなく
構造で行う。抽出対象は家族が話しかけた会話（conversations テーブル）だけで、
スタックちゃん側から喋った通知はそもそも入っていない。
"""
import asyncio
import json
import logging
import sys
from datetime import datetime

from bridge.config import _JST
from bridge.core.db import (
    _fetch_conversations, _save_memory, _get_setting, _set_setting,
    _fetch_memories_with_embeddings,
)
from bridge.llm.embeddings import embed_texts, EMBED_DIM, search

logger = logging.getLogger(__name__)

# 抽出は毎日この時刻以降に1回だけ走らせる（家が静かな時間帯）
_EXTRACT_HOUR = 3
_CHECK_INTERVAL_SEC = 1800

_EXTRACT_PROMPT = """以下は、ある家族とロボット「スタックちゃん」の今日の会話です。
この中から、スタックちゃんが後で思い出せたら嬉しいことを書き出してください。

- 分類やカテゴリは考えなくて構いません。覚えておきたいと感じたものを挙げてください
- 覚える価値がなければ、無理に挙げず空の配列を返してください
- 一度きりの雑談や、その場限りのやり取りは省いて構いません

hide_from はとくに慎重に決めてください。ここを間違えると、スタックちゃんが
サプライズを本人に喋ってしまいます。
- hide_from には「この記憶を知られたくない人」の名前を入れます
- 例: パパが「しおりの誕生日プレゼントは自転車。内緒ね」と話した
      → その記憶の hide_from は "しおり"（プレゼントをもらう本人に隠す）
      パパに隠す必要はありません
- 「内緒」「ないしょ」「秘密」「サプライズ」「言わないで」が出てきたら必ず設定します
- 隠す必要がなければ空文字 "" にしてください

各項目は次の形式の JSON 配列で返してください。説明文は不要です。

[
  {
    "content":   "記憶の内容を、後から読んで分かる一文で",
    "about":     "誰についての記憶か（会話に出てくる名前。特定の人でなければ null）",
    "hide_from": "この記憶を知られたくない人の名前。なければ \"\"",
    "kind":      "profile（好み・誕生日・家族構成など変わりにくい情報）か episode（出来事）"
  }
]

会話:
"""


def _format_conversations(rows: list[dict]) -> str:
    lines = []
    for c in reversed(rows):          # 古い順に並べ直す
        who = c.get("speaker") or "だれか"
        lines.append(f"{who}: {c['user_text']}")
        if c.get("reply_text"):
            lines.append(f"スタックちゃん: {c['reply_text']}")
    return "\n".join(lines)


def _parse_items(raw: str) -> list[dict]:
    """LLM の返答から JSON 配列を取り出す。コードフェンス等が付いても拾えるようにする。"""
    text = (raw or "").strip()
    if text.startswith("```"):
        text = text.split("```")[1] if "```" in text[3:] else text
        text = text.lstrip("json").strip()
    start, end = text.find("["), text.rfind("]")
    if start == -1 or end == -1:
        return []
    try:
        items = json.loads(text[start:end + 1])
    except json.JSONDecodeError as e:
        logger.warning("memory extraction: JSON parse failed: %s", e)
        return []
    return [i for i in items if isinstance(i, dict) and i.get("content")]


# これ以上似ていれば同じことを言い直しただけとみなす
_DUPLICATE_SCORE = 0.92


async def _extract_one_batch(rows: list[dict]) -> dict:
    """会話 rows から記憶を抽出して保存する（1バッチ分）。"""
    prompt = _EXTRACT_PROMPT + _format_conversations(rows)
    try:
        raw = await sys.modules["main"].chat_with_llm(
            prompt,
            session_key="",          # 会話履歴に混ぜない（記憶の抽出は独立した処理）
            use_functions=False,
            purpose="notify",        # 単純な抽出なので安いモデルで足りる
        )
    except Exception as e:
        logger.error("memory extraction LLM failed: %s: %s", type(e).__name__, e)
        return {"extracted": 0, "saved": 0, "embedded": 0, "skipped_as_duplicate": 0,
                "error": str(e)}

    items = _parse_items(raw)
    if not items:
        return {"extracted": 0, "saved": 0, "embedded": 0, "skipped_as_duplicate": 0}

    # 埋め込みはまとめて1回のリクエストで作る
    contents = [str(i["content"]).strip() for i in items]
    vectors = await embed_texts(contents)

    # 既存の記憶と近すぎるものは「言い直し」とみなして保存しない
    existing = _fetch_memories_with_embeddings(None) if vectors else []

    saved = embedded = skipped = 0
    for idx, item in enumerate(items):
        blob = vectors[idx] if vectors and idx < len(vectors) else None
        if blob and existing and search(blob, existing, top_k=1, min_score=_DUPLICATE_SCORE):
            skipped += 1
            continue
        ok = _save_memory(
            content=str(item["content"]).strip(),
            about=(item.get("about") or None),
            source=None,
            hide_from=str(item.get("hide_from") or "").strip(),
            kind=str(item.get("kind") or "episode"),
            happened_on=(rows[-1].get("created_at") or "")[:10] or None,
            embedding=blob,
            embed_dim=EMBED_DIM if blob else None,
        )
        if ok:
            saved += 1
            if blob:
                embedded += 1
                existing.append({"content": item["content"], "embedding": blob,
                                 "embed_dim": EMBED_DIM})
    return {"extracted": len(items), "saved": saved, "embedded": embedded,
            "skipped_as_duplicate": skipped}


async def extract_memories(limit: int = 300, reprocess: bool = False,
                           max_batches: int = 10) -> dict:
    """未処理の会話ログから記憶を抽出して保存する。

    処理済みの会話 id を記録し、次回はその続きから読む。日付での絞り込みは
    しない。日付で絞ると、ブリッジが止まっていた期間の会話が範囲から外れた
    まま id だけ前進し、その間の会話が二度と抽出されなくなるため。

    未処理が limit を超える場合は古い順に複数バッチに分けて処理する
    （新しい順に切ると、あふれた古い会話が取り残される）。
    reprocess=True で最初から取り直す。
    """
    if reprocess:
        _set_setting("memory_extracted_until_id", "")

    total = {"conversations": 0, "extracted": 0, "saved": 0, "embedded": 0,
             "skipped_as_duplicate": 0, "batches": 0}
    for _ in range(max_batches):
        last = _get_setting("memory_extracted_until_id", "")
        after_id = int(last) if last.isdigit() else None
        rows = _fetch_conversations(limit=limit, after_id=after_id, oldest_first=True)
        if not rows:
            break

        result = await _extract_one_batch(rows)
        if result.get("error"):
            total["error"] = result["error"]
            break

        # 実際に処理できたバッチの分だけ id を進める
        max_id = max(r["id"] for r in rows)
        _set_setting("memory_extracted_until_id", str(max_id))

        total["conversations"] += len(rows)
        total["batches"] += 1
        for k in ("extracted", "saved", "embedded", "skipped_as_duplicate"):
            total[k] += result.get(k, 0)
        total["until_id"] = max_id

        if len(rows) < limit:      # 追いついた
            break

    if total["conversations"]:
        logger.info("memory extraction done: %s", total)
    return total


async def memory_extract_loop() -> None:
    """毎日 _EXTRACT_HOUR 以降に1回だけ抽出を走らせる。"""
    logger.info("Memory extract loop started: hour=%d", _EXTRACT_HOUR)
    while True:
        try:
            now = datetime.now(_JST)
            today = now.date().isoformat()
            done = _get_setting("memory_extracted_date", "")
            if now.hour >= _EXTRACT_HOUR and done != today:
                result = await extract_memories()
                _set_setting("memory_extracted_date", today)
                logger.info("nightly memory extraction: %s", result)
        except Exception:
            logger.exception("memory extract loop error")
        await asyncio.sleep(_CHECK_INTERVAL_SEC)
