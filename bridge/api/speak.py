"""UI speak test endpoint."""
import logging
import uuid

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from bridge.core.audio import resolve_audio_url
from bridge.core.expression import _parse_expression, _resolve_expression
from bridge.devices.mqtt import publish_speak
from bridge.llm.backends import chat_with_llm

logger = logging.getLogger(__name__)
router = APIRouter()


class UiSpeakRequest(BaseModel):
    text: str
    mode: str = "say"  # "say" | "speak"


@router.post("/api/ui/speak")
async def api_ui_speak(req: UiSpeakRequest):
    if not req.text.strip():
        raise HTTPException(status_code=400, detail="text は必須です")
    if req.mode == "speak":
        speak_instruction = (
            "以下はスタックちゃんがその場にいる人に向けて話す内容の原文です。"
            "この内容をスタックちゃんらしい口調に変換してください。"
        )
        reply = await chat_with_llm(req.text, system_prompt_append=speak_instruction, use_functions=False)
        expression, text_to_say = _parse_expression(reply)
    else:
        expression, text_to_say = "neutral", req.text
    speaker_id, stackchan_expr = _resolve_expression(expression)
    audio_url, streaming_url = await resolve_audio_url(text_to_say, speaker_id)
    req_id = str(uuid.uuid4())
    publish_speak(audio_url, streaming_url, text_to_say, "ui", "normal", req_id, stackchan_expr)
    return {"requestId": req_id, "text": text_to_say, "expression": stackchan_expr}
