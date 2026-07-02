import logging
import sys
import openai
from bridge.core.audio import resolve_audio_url

logger = logging.getLogger(__name__)

_OPENAI_ERROR_REPLIES: dict[str, str] = {
    "insufficient_quota": "ごめんね〜、いまわたしのおさいふがからっぽで…あとでまた話しかけてね！",
    "rate_limit_exceeded": "いまちょっとこんでるみたい！少し待ってからまた話しかけてね〜",
}


def _classify_api_error(e: Exception) -> str | None:
    """Known OpenAI API error → Stack-chan message. Unknown error → None."""
    if isinstance(e, openai.RateLimitError):
        err_str = str(e)
        if "insufficient_quota" in err_str:
            return _OPENAI_ERROR_REPLIES["insufficient_quota"]
        return _OPENAI_ERROR_REPLIES["rate_limit_exceeded"]
    if isinstance(e, (openai.AuthenticationError, openai.PermissionDeniedError)):
        return "ごめんね、いまうまく動けなくて…ちょっと待ってね！"
    if isinstance(e, openai.APIConnectionError):
        return "ネットワークにつながれないみたい…また後で話しかけてね！"
    return None


async def _deliver_error_reply(
    error_reply: str,
    source: str,
    priority: str,
    req_id: str,
    mode: str,
) -> dict:
    """Speak an error message via MQTT (async) or return audioUrl (sync)."""
    audio_url, streaming_url = await resolve_audio_url(error_reply)
    if mode != "sync":
        # Look up publish_speak from main at call time to avoid circular import
        main_mod = sys.modules.get("main")
        publish_speak = getattr(main_mod, "publish_speak")
        publish_speak(audio_url, streaming_url, error_reply, source, priority, req_id)
        return {"requestId": req_id}
    resp: dict = {"requestId": req_id, "reply": error_reply, "audioUrl": audio_url}
    if streaming_url:
        resp["audioStreamingUrl"] = streaming_url
    return resp
