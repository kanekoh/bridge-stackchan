"""撮った写真を見て、短い説明文（caption）を作る。

用途は2つ。
  ・その場の質問に答える（「洗濯物ある？」→ 写真を見て答える）
  ・あとから探せるようにする（説明文があれば、記憶の意味検索に載せられる）

会話モデルに画像をそのまま渡す。実測で gpt-5.6-luna / gpt-6-luna とも
画像を読めることを確認済み（2026-10-25）。
"""
import base64
import logging

from bridge.config import OPENAI_API_KEY, OPENAI_RESPONSES_BASE_URL
from bridge.core.db import _get_setting, _set_capture_caption
from bridge.core.http import get_http_client

logger = logging.getLogger(__name__)

_DEFAULT_PROMPT = (
    "この写真に写っているものを、日本語の短い1〜2文で説明してください。"
    "見たままを書き、推測や感想は書かないでください。"
    "人が写っている場合は「人がいる」程度にとどめ、誰かの判定はしないでください。"
)


class VisionError(Exception):
    """写真を見られなかった。"""


def _model() -> str:
    """画像を読むモデル。会話モデルに合わせる（別指定も可）。"""
    return (
        _get_setting("openai_responses_model_vision", "")
        or _get_setting("openai_responses_model", "")
        or "gpt-5.6-luna"
    )


async def describe(
    image_bytes: bytes, *, content_type: str = "image/jpeg",
    question: str = "", capture_id: str = "",
) -> str:
    """写真を見て答える。question が空なら、あとで探すための説明文を作る。

    失敗しても例外は投げず、空文字を返す。写真自体は保存済みなので、
    説明が付かないことで撮影まで失敗扱いにする必要はない。
    """
    if not OPENAI_API_KEY:
        return ""
    prompt = question.strip() or _DEFAULT_PROMPT
    data_url = f"data:{content_type};base64,{base64.b64encode(image_bytes).decode()}"
    payload = {
        "model": _model(),
        "max_output_tokens": 300,
        "reasoning": {"effort": "none"},
        "input": [{"role": "user", "content": [
            {"type": "input_text", "text": prompt},
            {"type": "input_image", "image_url": data_url},
        ]}],
    }
    try:
        resp = await get_http_client().post(
            OPENAI_RESPONSES_BASE_URL.rstrip("/") + "/responses",
            json=payload,
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            timeout=60,
        )
        resp.raise_for_status()
        text = _extract_text(resp.json()).strip()
    except Exception as e:
        logger.warning("写真を見られませんでした（保存は済んでいます）: %s", e)
        return ""

    # 質問への回答は「その場の答え」なので説明文として保存しない。
    # 保存するのは、あとから探すための説明だけ。
    if text and capture_id and not question.strip():
        _set_capture_caption(capture_id, text)
    return text


def _extract_text(data: dict) -> str:
    parts = []
    for item in data.get("output", []) or []:
        for content in item.get("content", []) or []:
            if content.get("type") == "output_text":
                parts.append(content.get("text", ""))
    return "".join(parts)
