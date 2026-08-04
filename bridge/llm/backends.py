"""LLM Backend Protocol and implementations (OpenClaw, OpenAI Responses)."""
import asyncio
import logging
import sys
from typing import Protocol

import bridge.config as _cfg
from bridge.core.expression import _STACKCHAN_SYSTEM_PROMPT
from bridge.core.db import (
    _SessionData, _get_session_data, _save_session, _summarize_and_reset_session,
    _get_setting,
)
from bridge.llm.persona import _build_datetime_context, _build_location_context, _build_birthday_context, _build_family_context

logger = logging.getLogger(__name__)


def _get_main():
    """Return the main module (lazy lookup to avoid circular import)."""
    return sys.modules.get("main")


def _cfg_val(name: str):
    """Get a config value from main module (allows test patching) or fall back to bridge.config."""
    main_mod = _get_main()
    if main_mod is not None and hasattr(main_mod, name):
        return getattr(main_mod, name)
    return getattr(_cfg, name)


_REASONING_MODEL_PREFIXES = ("gpt-5", "o1", "o3", "o4")


def _supports_reasoning_effort(model: str) -> bool:
    """reasoning.effort パラメータを受け付けるモデルか（非対応モデルに送ると 400 エラーになる）。"""
    return model.startswith(_REASONING_MODEL_PREFIXES)


class LLMBackend(Protocol):
    async def chat(
        self,
        text: str,
        audio: bytes | None,
        speaker: str | None,
        system_prompt_append: str,
        session_key: str,
        notify_context: dict | None,
        use_functions: bool,
    ) -> str: ...


class OpenClawResponsesBackend:
    async def chat(
        self,
        text: str,
        audio: bytes | None,
        speaker: str | None,
        system_prompt_append: str,
        session_key: str,
        notify_context: dict | None,
        use_functions: bool,
    ) -> str:
        main_mod = _get_main()
        _http_client = getattr(main_mod, "_http_client", None)
        _handle_function_calls = getattr(main_mod, "_handle_function_calls", None)
        _TIMER_TOOLS = getattr(main_mod, "_TIMER_TOOLS", [])
        _WEATHER_TOOLS = getattr(main_mod, "_WEATHER_TOOLS", [])
        _CALENDAR_TOOLS = getattr(main_mod, "_CALENDAR_TOOLS", [])
        _MESSAGE_TOOLS = getattr(main_mod, "_MESSAGE_TOOLS", [])
        _ALERT_TOOLS = getattr(main_mod, "_ALERT_TOOLS", [])

        OPENCLAW_BASE_URL = _cfg_val("OPENCLAW_BASE_URL")
        OPENCLAW_GATEWAY_TOKEN = _cfg_val("OPENCLAW_GATEWAY_TOKEN")
        OPENCLAW_SESSION_KEY = _cfg_val("OPENCLAW_SESSION_KEY")
        OPENCLAW_MODEL = _cfg_val("OPENCLAW_MODEL")
        OPENCLAW_MAX_OUTPUT_TOKENS = _cfg_val("OPENCLAW_MAX_OUTPUT_TOKENS")
        CALENDAR_ENABLED = _cfg_val("CALENDAR_ENABLED")
        P2PQUAKE_ENABLED = _cfg_val("P2PQUAKE_ENABLED")

        url = OPENCLAW_BASE_URL.rstrip("/") + "/responses"
        headers: dict = {
            "Content-Type": "application/json",
            "x-openclaw-scopes": "operator.read,operator.write",
        }
        if OPENCLAW_GATEWAY_TOKEN:
            headers["Authorization"] = f"Bearer {OPENCLAW_GATEWAY_TOKEN}"
        effective_session = session_key or OPENCLAW_SESSION_KEY
        if effective_session:
            headers["x-openclaw-session-key"] = effective_session

        user_input: str | list = f"[話者: {speaker}] {text}" if speaker else text
        instructions_parts = [_build_datetime_context()]
        fam_ctx = _build_family_context()
        if fam_ctx:
            instructions_parts.append(fam_ctx)
        loc_ctx = _build_location_context()
        if loc_ctx:
            instructions_parts.append(loc_ctx)
        birthday_ctx = _build_birthday_context()
        if birthday_ctx:
            instructions_parts.append(birthday_ctx)
        if system_prompt_append:
            instructions_parts.append(system_prompt_append)
        tools = list(_TIMER_TOOLS) if use_functions else []
        if use_functions:
            tools.extend(_WEATHER_TOOLS)
        if use_functions and CALENDAR_ENABLED:
            tools.extend(_CALENDAR_TOOLS)
        if use_functions:
            tools.extend(_MESSAGE_TOOLS)
        if use_functions and P2PQUAKE_ENABLED:
            tools.extend(_ALERT_TOOLS)

        logger.info(
            "OpenClaw request: url=%s model=%s session_key=%s",
            url, OPENCLAW_MODEL, OPENCLAW_SESSION_KEY or "(none)",
        )

        for _ in range(5):  # Function calling ループ（最大 5 回）
            payload: dict = {
                "model": OPENCLAW_MODEL,
                "input": user_input,
                "instructions": "\n\n".join(instructions_parts),
            }
            if OPENCLAW_MAX_OUTPUT_TOKENS is not None:
                payload["max_output_tokens"] = OPENCLAW_MAX_OUTPUT_TOKENS
            if tools:
                payload["tools"] = tools
            try:
                resp = await _http_client.post(url, json=payload, headers=headers)
                if not resp.is_success:
                    logger.error("OpenClaw HTTP %d: body=%s", resp.status_code, resp.text[:500])
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error("OpenClaw error detail: type=%s message=%s", type(e).__name__, e)
                raise

            output = data.get("output", [])
            function_outputs = await _handle_function_calls(output, notify_context or {})
            if function_outputs is None:
                if "output_text" in data:
                    return data["output_text"]
                for item in output:
                    for content in item.get("content", []):
                        if content.get("type") == "output_text":
                            return content["text"]
                raise RuntimeError(f"OpenClaw response に返答テキストが見つかりません: {data}")
            user_input = function_outputs

        raise RuntimeError("OpenClaw function calling loop exceeded max iterations")


class OpenAIResponsesBackend:
    async def chat(
        self,
        text: str,
        audio: bytes | None,
        speaker: str | None,
        system_prompt_append: str,
        session_key: str,
        notify_context: dict | None,
        use_functions: bool,
    ) -> str:
        main_mod = _get_main()
        _http_client = getattr(main_mod, "_http_client", None)
        _handle_function_calls = getattr(main_mod, "_handle_function_calls", None)
        _TIMER_TOOLS = getattr(main_mod, "_TIMER_TOOLS", [])
        _WEATHER_TOOLS = getattr(main_mod, "_WEATHER_TOOLS", [])
        _CALENDAR_TOOLS = getattr(main_mod, "_CALENDAR_TOOLS", [])
        _MESSAGE_TOOLS = getattr(main_mod, "_MESSAGE_TOOLS", [])
        _ALERT_TOOLS = getattr(main_mod, "_ALERT_TOOLS", [])
        _REQUEST_WEB_SEARCH_TOOL = getattr(main_mod, "_REQUEST_WEB_SEARCH_TOOL", None)

        OPENAI_RESPONSES_BASE_URL = _cfg_val("OPENAI_RESPONSES_BASE_URL")
        OPENAI_API_KEY = _cfg_val("OPENAI_API_KEY")
        OPENAI_RESPONSES_MODEL = _get_setting("openai_responses_model", "") or _cfg_val("OPENAI_RESPONSES_MODEL")
        OPENAI_RESPONSES_MAX_OUTPUT_TOKENS = _cfg_val("OPENAI_RESPONSES_MAX_OUTPUT_TOKENS")
        OPENAI_RESPONSES_REASONING_EFFORT = _get_setting("openai_responses_reasoning_effort", "") or _cfg_val("OPENAI_RESPONSES_REASONING_EFFORT")
        OPENAI_RESPONSES_WEB_SEARCH = _cfg_val("OPENAI_RESPONSES_WEB_SEARCH")
        OPENAI_RESPONSES_WEB_SEARCH_ON_DEMAND = _cfg_val("OPENAI_RESPONSES_WEB_SEARCH_ON_DEMAND")
        OPENAI_RESPONSES_WEB_SEARCH_TOOL = _cfg_val("OPENAI_RESPONSES_WEB_SEARCH_TOOL")
        CALENDAR_ENABLED = _cfg_val("CALENDAR_ENABLED")
        P2PQUAKE_ENABLED = _cfg_val("P2PQUAKE_ENABLED")
        DISABLE_SESSION_HISTORY = _cfg_val("DISABLE_SESSION_HISTORY")
        DISABLE_TOOLS = _cfg_val("DISABLE_TOOLS")
        SESSION_SUMMARY_THRESHOLD = _cfg_val("SESSION_SUMMARY_THRESHOLD")

        url = OPENAI_RESPONSES_BASE_URL.rstrip("/") + "/responses"
        headers: dict = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OPENAI_API_KEY}",
        }

        user_input: str | list = f"[話者: {speaker}] {text}" if speaker else text

        # _handle_function_calls から enable_web_search フラグを書き戻すため、ここで必ず辞書化する
        notify_ctx: dict = notify_context if notify_context is not None else {}

        instructions_parts = [_STACKCHAN_SYSTEM_PROMPT, _build_datetime_context()]
        fam_ctx = _build_family_context()
        if fam_ctx:
            instructions_parts.append(fam_ctx)
        loc_ctx = _build_location_context()
        if loc_ctx:
            instructions_parts.append(loc_ctx)
        birthday_ctx = _build_birthday_context()
        if birthday_ctx:
            instructions_parts.append(birthday_ctx)
        if OPENAI_RESPONSES_WEB_SEARCH and OPENAI_RESPONSES_WEB_SEARCH_ON_DEMAND:
            instructions_parts.append(
                "Web検索ガイドライン:\n"
                "- 雑談・感情表現・既に知っている内容ではWeb検索を使わない\n"
                "- 「最新」「今日」「いま」「天気」「ニュース」など現在の情報が必要なときだけ "
                "request_web_search を呼ぶ"
            )
        if system_prompt_append:
            instructions_parts.append(system_prompt_append)

        session = _get_session_data(session_key) if session_key else _SessionData(None, 0, 0, None)
        previous_response_id = session.response_id if not DISABLE_SESSION_HISTORY else None

        # previous_response_id がない（新規 or リセット後）かつサマリがあれば過去の文脈として注入
        if not previous_response_id and session.summary:
            instructions_parts.append(
                f"【過去の会話の要約】\n{session.summary}"
            )

        tools = list(_TIMER_TOOLS) if (use_functions and not DISABLE_TOOLS) else []
        if use_functions and not DISABLE_TOOLS:
            tools.extend(_WEATHER_TOOLS)
        if use_functions and CALENDAR_ENABLED and not DISABLE_TOOLS:
            tools.extend(_CALENDAR_TOOLS)
        if use_functions and not DISABLE_TOOLS:
            tools.extend(_MESSAGE_TOOLS)
        if use_functions and P2PQUAKE_ENABLED and not DISABLE_TOOLS:
            tools.extend(_ALERT_TOOLS)
        if OPENAI_RESPONSES_WEB_SEARCH and not DISABLE_TOOLS:
            if OPENAI_RESPONSES_WEB_SEARCH_ON_DEMAND:
                if _REQUEST_WEB_SEARCH_TOOL:
                    tools.append(_REQUEST_WEB_SEARCH_TOOL)
            else:
                tools.append({"type": OPENAI_RESPONSES_WEB_SEARCH_TOOL})

        logger.info(
            "OpenAI Responses request: model=%s session_key=%s previous_response_id=%s "
            "char_in=%d char_out=%d has_summary=%s web_search=%s on_demand=%s",
            OPENAI_RESPONSES_MODEL, session_key or "(none)", previous_response_id or "(none)",
            session.char_count_in, session.char_count_out, bool(session.summary),
            OPENAI_RESPONSES_WEB_SEARCH, OPENAI_RESPONSES_WEB_SEARCH_ON_DEMAND,
        )

        for _ in range(5):  # Function calling ループ（最大 5 回）
            # ON_DEMAND モードで LLM が前ターンに request_web_search を呼んでいたら、
            # ここで本物の web_search_preview に差し替える（Pass 2 への昇格）
            if (
                OPENAI_RESPONSES_WEB_SEARCH
                and OPENAI_RESPONSES_WEB_SEARCH_ON_DEMAND
                and notify_ctx.get("enable_web_search")
            ):
                tools = [t for t in tools if t.get("name") != "request_web_search"]
                if not any(t.get("type") == OPENAI_RESPONSES_WEB_SEARCH_TOOL for t in tools):
                    tools.append({"type": OPENAI_RESPONSES_WEB_SEARCH_TOOL})
                    logger.info("Web search promoted to Pass 2")
                notify_ctx["enable_web_search"] = False  # 多重昇格防止

            payload: dict = {
                "model": OPENAI_RESPONSES_MODEL,
                "input": user_input,
                "instructions": "\n\n".join(instructions_parts),
            }
            if previous_response_id:
                payload["previous_response_id"] = previous_response_id
            if OPENAI_RESPONSES_MAX_OUTPUT_TOKENS is not None:
                payload["max_output_tokens"] = OPENAI_RESPONSES_MAX_OUTPUT_TOKENS
            if OPENAI_RESPONSES_REASONING_EFFORT and _supports_reasoning_effort(OPENAI_RESPONSES_MODEL):
                payload["reasoning"] = {"effort": OPENAI_RESPONSES_REASONING_EFFORT}
            if tools:
                payload["tools"] = tools

            try:
                resp = await _http_client.post(url, json=payload, headers=headers)

                # previous_response_id が壊れた状態（未解決の function_call が残っている）の場合、
                # リセットして同じ入力で再試行する。会話の連続性は失われるが処理は継続できる。
                # 注意: function_call_output 送信中（user_input がリスト）はリセットしない。
                #       そこで 400 が出るのは call_id の不一致など別の問題であり、
                #       リセットすると function_call_output だけが残って状況が悪化する。
                if (
                    resp.status_code == 400
                    and previous_response_id
                    and isinstance(user_input, str)
                    and "tool" in resp.text.lower()
                ):
                    logger.warning(
                        "previous_response_id has unresolved function call, resetting and retrying: "
                        "session_key=%s previous_response_id=%s",
                        session_key, previous_response_id,
                    )
                    previous_response_id = None
                    payload.pop("previous_response_id", None)
                    resp = await _http_client.post(url, json=payload, headers=headers)

                if not resp.is_success:
                    logger.error("OpenAI Responses HTTP %d: body=%s", resp.status_code, resp.text[:500])
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error("OpenAI Responses error: type=%s message=%s", type(e).__name__, e)
                raise

            response_id = data.get("id")
            output = data.get("output", [])
            function_outputs = await _handle_function_calls(output, notify_ctx)

            if function_outputs is None:
                # 最終テキストレスポンス → ここでのみ DB に保存する
                # （function_call の中間レスポンス ID を保存すると次回の会話で 400 エラーになるため）
                reply_text = data.get("output_text") or ""
                if not reply_text:
                    for item in output:
                        for content in item.get("content", []):
                            if content.get("type") == "output_text":
                                reply_text = content["text"]
                                break
                        if reply_text:
                            break
                if not reply_text:
                    raise RuntimeError(f"OpenAI Responses response に返答テキストが見つかりません: {data}")

                if session_key:
                    new_in  = session.char_count_in  + len(text)
                    new_out = session.char_count_out + len(reply_text)
                    _save_session(
                        session_key=session_key,
                        response_id=response_id,
                        char_count_in=new_in,
                        char_count_out=new_out,
                        summary=session.summary,
                    )
                    logger.info(
                        "Session saved: session_key=%s response_id=%s char_in=%d char_out=%d total=%d",
                        session_key, response_id, new_in, new_out, new_in + new_out,
                    )
                    # 閾値を超えたら要約してリセット（次回リクエストからクリーンな状態になる）
                    if response_id and (new_in + new_out) >= SESSION_SUMMARY_THRESHOLD:
                        logger.info(
                            "Session char threshold reached (%d >= %d), summarizing: session_key=%s",
                            new_in + new_out, SESSION_SUMMARY_THRESHOLD, session_key,
                        )
                        asyncio.create_task(_summarize_and_reset_session(session_key, response_id))

                return reply_text

            # Function call あり → ループ内での previous_response_id を更新して継続
            # （DB には保存しない。function_call の未解決状態を DB に残さないため）
            if response_id:
                previous_response_id = response_id
            user_input = function_outputs

        raise RuntimeError("OpenAI Responses function calling loop exceeded max iterations")


_BACKENDS: dict[str, LLMBackend] = {
    "openclaw": OpenClawResponsesBackend(),
    "openai": OpenAIResponsesBackend(),
}


async def chat_with_openclaw(
    text: str,
    speaker: str | None = None,
    system_prompt_append: str = "",
    notify_context: dict | None = None,
    use_functions: bool = True,
) -> str:
    return await _BACKENDS["openclaw"].chat(
        text, None, speaker, system_prompt_append, "", notify_context, use_functions
    )


async def chat_with_openai_responses(
    text: str,
    speaker: str | None = None,
    system_prompt_append: str = "",
    session_key: str = "",
    notify_context: dict | None = None,
    use_functions: bool = True,
) -> str:
    return await _BACKENDS["openai"].chat(
        text, None, speaker, system_prompt_append, session_key, notify_context, use_functions
    )


async def chat_with_llm(
    text: str,
    speaker: str | None = None,
    system_prompt_append: str = "",
    session_key: str = "",
    notify_context: dict | None = None,
    use_functions: bool = True,
) -> str:
    """Dispatch to the configured LLM backend (LLM_BACKEND env).

    notify_context: {"session_key": str, "slack_channel": str | None}
    use_functions: False にすると Function Calling ツールを含めない（タイマー発火時など）
    """
    LLM_BACKEND = _cfg_val("LLM_BACKEND")
    backend = _BACKENDS.get(LLM_BACKEND)
    if backend is None:
        raise ValueError(f"Unknown LLM_BACKEND: {LLM_BACKEND!r}")
    return await backend.chat(text, None, speaker, system_prompt_append, session_key, notify_context, use_functions)
