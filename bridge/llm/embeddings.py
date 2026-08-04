"""記憶検索用の埋め込み生成とベクトル検索。

家庭規模（数千〜数万件）では専用のベクトル DB は不要で、numpy の内積による
総当たりで十分速い（実測: 1万件で 1ms 未満）。SQLite に float32 の生バイト列で
持ち、検索時にメモリへ展開する。

次元は 256 に落としている。実測では 1536 次元と精度が変わらず（むしろ短い
発話では良い結果だった）、保存量と転送量が 1/6 になる。
"""
import logging
import struct
import sys

import numpy as np

from bridge.config import OPENAI_API_KEY, OPENAI_RESPONSES_BASE_URL

logger = logging.getLogger(__name__)

EMBED_MODEL = "text-embedding-3-small"
EMBED_DIM = 256


def _http_client():
    main_mod = sys.modules.get("main")
    return getattr(main_mod, "_http_client", None) if main_mod else None


async def embed_texts(texts: list[str]) -> list[bytes] | None:
    """複数テキストをまとめて埋め込み、float32 のバイト列にして返す。

    失敗しても記憶の保存自体は続けたいので、例外は投げず None を返す。
    """
    texts = [t for t in texts if t and t.strip()]
    if not texts:
        return []
    client = _http_client()
    if client is None or not OPENAI_API_KEY:
        logger.warning("embed_texts: http client or API key not available")
        return None
    url = OPENAI_RESPONSES_BASE_URL.rstrip("/") + "/embeddings"
    try:
        resp = await client.post(
            url,
            json={"model": EMBED_MODEL, "input": texts, "dimensions": EMBED_DIM},
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {OPENAI_API_KEY}"},
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("embedding request failed: %s: %s", type(e).__name__, e)
        return None

    out: list[bytes] = []
    for item in sorted(data.get("data", []), key=lambda d: d.get("index", 0)):
        vec = np.asarray(item["embedding"], dtype="float32")
        n = np.linalg.norm(vec)
        if n > 0:
            vec = vec / n          # 正規化しておけば検索時は内積だけで済む
        out.append(vec.tobytes())
    return out


async def embed_one(text: str) -> bytes | None:
    vecs = await embed_texts([text])
    return vecs[0] if vecs else None


def _to_array(blob: bytes, dim: int | None) -> np.ndarray | None:
    if not blob:
        return None
    d = dim or EMBED_DIM
    if len(blob) != d * 4:
        return None
    return np.frombuffer(blob, dtype="float32")


def search(query_blob: bytes, rows: list[dict], top_k: int = 5,
           min_score: float = 0.25) -> list[dict]:
    """埋め込み済みの記憶から、問い合わせに近いものを上位 top_k 件返す。

    min_score 未満は「関係ない」とみなして落とす。埋め込み検索は必ず何かを
    返してしまうため、無関係な記憶を思い出したことにしないための足切り。
    """
    q = _to_array(query_blob, EMBED_DIM)
    if q is None or not rows:
        return []
    usable = [(r, _to_array(r.get("embedding"), r.get("embed_dim"))) for r in rows]
    usable = [(r, v) for r, v in usable if v is not None and v.shape == q.shape]
    if not usable:
        return []
    matrix = np.stack([v for _, v in usable])
    scores = matrix @ q
    order = np.argsort(-scores)[:top_k]
    results = []
    for i in order:
        if float(scores[i]) < min_score:
            continue
        r = dict(usable[int(i)][0])
        r.pop("embedding", None)
        r["score"] = round(float(scores[i]), 3)
        results.append(r)
    return results
