"""Slack Bot integration for Stack-chan.

All main.py functions are accessed lazily via sys.modules["main"].
"""
import asyncio
import logging
import re
import sys
import uuid
from datetime import datetime, timedelta

from bridge.config import (
    _JST,
    SLACK_BOT_TOKEN, SLACK_APP_TOKEN, MQTT_DEVICE_ID,
)

logger = logging.getLogger(__name__)

_MENTION_RE = re.compile(r"<@[A-Z0-9]+>")


async def _slack_handle_mention(event: dict, say) -> None:
    """app_mention: チャンネルで @stackchan されたときに Slack へテキストで返信する（MQTT 発話なし）。"""
    text = _MENTION_RE.sub("", event.get("text", "")).strip()
    if not text:
        return

    channel = event["channel"]
    user = event.get("user", "")
    # 音声会話・/speak と同じスレッドを共有し、記憶が繋がるようにする
    session_key = MQTT_DEVICE_ID
    sys.modules["main"]._record_slack_user(user)
    sender_name = sys.modules["main"]._resolve_display_name(user, "")
    logger.info("Slack mention: channel=%s sender=%s text=%s", channel, sender_name or "(unknown)", text[:60])

    try:
        reply = await sys.modules["main"].chat_with_llm(
            text,
            speaker=sender_name or None,
            session_key=session_key,
            notify_context={"session_key": session_key, "slack_channel": channel, "speaker": sender_name or None},
        )
    except Exception as e:
        logger.error("Slack mention LLM error: %s", e)
        await say(sys.modules["main"]._classify_api_error(e) or "ごめんね、うまく考えられなかったよ〜 もう一度試してみて！")
        return

    _, clean_reply = sys.modules["main"]._parse_expression(reply)
    # 音声会話と同じく、要約される前の生の会話を記憶のもとして残す。
    # Slack は slack_user_id から家族名を確実に引けるため、音声の話者識別より
    # 話者が正確に付く。
    sys.modules["main"]._save_conversation(
        session_key=session_key, speaker=sender_name or None,
        user_text=text, reply_text=clean_reply, source="slack:mention",
    )
    await say(clean_reply)


async def _slack_handle_dm(event: dict, say) -> None:
    """message.im: スタックちゃんへの DM に Slack テキストで返信する（MQTT 発話なし）。"""
    if event.get("channel_type") != "im":
        return
    if event.get("bot_id"):  # ボット自身の発言は無視
        return

    text = event.get("text", "").strip()
    if not text:
        return

    channel = event.get("channel", "")
    user = event["user"]
    session_key = f"slack:dm:{user}"
    sys.modules["main"]._record_slack_user(user)
    sender_name = sys.modules["main"]._resolve_display_name(user, "")
    logger.info("Slack DM: user=%s sender=%s text=%s", user, sender_name or "(unknown)", text[:60])

    try:
        reply = await sys.modules["main"].chat_with_llm(
            text,
            speaker=sender_name or None,
            session_key=session_key,
            notify_context={"session_key": session_key, "slack_channel": channel, "speaker": sender_name or None},
        )
    except Exception as e:
        logger.error("Slack DM LLM error: %s", e)
        await say(sys.modules["main"]._classify_api_error(e) or "ごめんね、うまく考えられなかったよ〜 もう一度試してみて！")
        return

    _, clean_reply = sys.modules["main"]._parse_expression(reply)
    sys.modules["main"]._save_conversation(
        session_key=session_key, speaker=sender_name or None,
        user_text=text, reply_text=clean_reply, source="slack:dm",
    )
    await say(clean_reply)


async def _deliver_pending_messages_after(
    main_reply: str, source: str, priority: str, session_key: str = "", speaker: str | None = None,
) -> None:
    """メイン返答の再生推定時間後に未読伝言を MQTT で届ける。
    日本語の平均読み上げ速度 ~5.5文字/秒 + バッファ3秒で待機する。
    宛先付きの伝言は、話者が宛先本人と一致する場合のみ届ける。
    """
    wait_sec = len(main_reply) / 5.5 + 3.0
    await asyncio.sleep(wait_sec)

    _main = sys.modules["main"]
    messages = _main._filter_messages_for_speaker(_main._fetch_pending_messages(), speaker)
    if not messages:
        return

    for msg in messages:
        sender = msg["sender"]
        recipient = msg["recipient"]
        content = msg["content"]

        recipient_part = f"（{recipient}への伝言）" if recipient else ""
        prompt = (
            f"以下の伝言{recipient_part}を、スタックちゃんとして読み上げてください。\n"
            "必ず「そういえば」「あ、そうだ」「ちなみに」などの話題転換の言葉を文頭に入れてください。\n"
            "自然な話し言葉で短くまとめてください。\n\n"
            f"送り主: {sender}\n"
            f"内容: {content}"
        )
        try:
            reply = await sys.modules["main"].chat_with_llm(
                prompt,
                system_prompt_append="",
                session_key=session_key,
                notify_context={"session_key": session_key, "slack_channel": None},
                use_functions=False,
            )
        except Exception as e:
            logger.error("Message delivery LLM error: msg_id=%d %s", msg["id"], e)
            continue

        expression, clean_reply = sys.modules["main"]._parse_expression(reply)
        speaker_id, stackchan_expr = sys.modules["main"]._resolve_expression(expression)
        try:
            audio_url, streaming_url = await sys.modules["main"].resolve_audio_url(clean_reply, speaker_id)
            req_id = str(uuid.uuid4())
            sys.modules["main"].publish_speak(audio_url, streaming_url, clean_reply, source, priority, req_id, stackchan_expr)
            sys.modules["main"]._mark_message_delivered(msg["id"])
            logger.info("Message delivered: id=%d text=%s", msg["id"], clean_reply[:60])
            await _notify_message_delivered(msg)
        except Exception as e:
            logger.error("Message delivery speak error: msg_id=%d %s", msg["id"], e)

        if len(messages) > 1:
            await asyncio.sleep(3.0)


async def _notify_message_delivered(msg: dict) -> None:
    """伝言が読まれたことを送信者に Slack DM で通知する。"""
    slack_id = msg.get("sender_slack_id")
    _main = sys.modules["main"]
    if not slack_id or not _main._slack_app:
        return
    recipient_part = f"{msg['recipient']}への" if msg["recipient"] else ""
    try:
        await _main._slack_app.client.chat_postMessage(
            channel=slack_id,
            text=f"📬 {recipient_part}伝言が届いたよ！「{msg['content']}」",
        )
        logger.info("Delivery notification sent: msg_id=%d slack_id=%s", msg["id"], slack_id)
    except Exception as e:
        logger.error("Delivery notification error: msg_id=%d %s", msg["id"], e)


def _record_slack_user_from_body(body: dict) -> None:
    """スラッシュコマンドの body から Slack ユーザーを記録する。"""
    user_id = body.get("user_id", "")
    user_name = body.get("user_name")
    if user_id:
        sys.modules["main"]._record_slack_user(user_id, user_name)


async def _slack_handle_say(ack, body: dict, respond) -> None:
    """/say コマンド: テキストを LLM 変換なしでそのまま VOICEVOX → MQTT 送信。"""
    await ack()
    _record_slack_user_from_body(body)

    text = body.get("text", "").strip()
    if not text:
        await respond("読み上げる内容を入力してください。例: `/say おはようございます`")
        return

    logger.info("Slack /say: channel=%s text=%s", body.get("channel_id"), text[:60])
    req_id = str(uuid.uuid4())
    try:
        audio_url, streaming_url = await sys.modules["main"].resolve_audio_url(text)
        sys.modules["main"]._pending_acks[req_id] = asyncio.Event()
        sys.modules["main"].publish_speak(audio_url, streaming_url, text, "slack", "normal", req_id)
    except Exception as e:
        sys.modules["main"]._pending_acks.pop(req_id, None)
        logger.error("Slack /say error: %s", e)
        await respond(f"音声の送信に失敗したよ。テキストはこれ：「{text}」")
        return

    ack_ok = await sys.modules["main"].wait_for_ack(req_id)
    if ack_ok:
        await respond(f"話すよ！「{text}」", response_type="in_channel")
    else:
        await respond(f"⚠️ スタックちゃんから応答がなかったよ。届いてないかも。「{text}」", response_type="in_channel")


async def _slack_handle_register(ack, body: dict, respond) -> None:
    """/register コマンド: 自分の Slack アカウントを家族メンバーとして登録する。
    書式: /register <呼び名>
    例:   /register パパ
    """
    await ack()
    _record_slack_user_from_body(body)

    name = body.get("text", "").strip()
    if not name:
        await respond("使い方: `/register <呼び名>`\n例: `/register パパ`")
        return

    user_id = body.get("user_id", "")
    now = datetime.now(_JST).isoformat()
    try:
        with sys.modules["bridge.core.db"]._db_lock:
            sys.modules["bridge.core.db"]._db_conn.execute(  # type: ignore[union-attr]
                """INSERT INTO family_members (name, slack_user_id, created_at, updated_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(name) DO UPDATE SET slack_user_id=excluded.slack_user_id, updated_at=excluded.updated_at""",
                (name, user_id, now, now),
            )
            sys.modules["bridge.core.db"]._db_conn.commit()
        logger.info("Slack /register: user_id=%s name=%s", user_id, name)
        await respond(f"✅ 「{name}」として登録したよ！")
    except Exception as e:
        logger.error("Slack /register error: %s", e)
        await respond("登録に失敗したよ。もう一度試してみて！")


async def _slack_handle_tell(ack, body: dict, respond) -> None:
    """/tell コマンド: 伝言を DB に保存。次回の会話時にスタックちゃんが読み上げる。
    書式: /tell [宛名] <内容>
    例:   /tell しおり 明日の習い事は16時からだよ
          /tell 夕食は7時です（宛名なしは全員向け）
    """
    await ack()
    _record_slack_user_from_body(body)

    text = body.get("text", "").strip()
    if not text:
        await respond(
            "使い方: `/tell [宛名] <内容>`\n"
            "例: `/tell しおり 明日の習い事は16時からだよ`\n"
            "　　`/tell 夕食は7時です`（宛名なしは全員向け）"
        )
        return

    # 先頭トークンが6文字以内なら宛名とみなす（日本語の名前は概ね短い）
    tokens = text.split(None, 1)
    if len(tokens) == 2 and len(tokens[0]) <= 6:
        recipient, content = tokens[0], tokens[1]
    else:
        recipient, content = None, text

    sender_slack_id = body.get("user_id")
    fallback_name = body.get("user_name") or sender_slack_id or "だれか"
    sender = sys.modules["main"]._resolve_display_name(sender_slack_id, fallback_name)
    msg_id = sys.modules["main"]._save_message(sender, recipient, content, sender_slack_id)
    logger.info("Message saved: id=%d sender=%s recipient=%s", msg_id, sender, recipient)

    if recipient:
        await respond(f"📬 {recipient}への伝言を預かったよ！次に話しかけてもらったときに伝えるね。")
    else:
        await respond(f"📬 みんなへの伝言を預かったよ！次に話しかけてもらったときに伝えるね。")


async def _slack_handle_speak(ack, body: dict, respond) -> None:
    """/speak コマンド: テキストをスタックちゃん口調に変換して MQTT 送信。"""
    await ack()
    _record_slack_user_from_body(body)

    text = body.get("text", "").strip()
    if not text:
        await respond("話す内容を入力してください。例: `/speak おはようございます`")
        return

    channel_id = body.get("channel_id", "")
    user_id = body.get("user_id", "")
    sender_name = sys.modules["main"]._resolve_display_name(user_id, body.get("user_name") or "だれか")
    # ingest-audio と同じセッションを共有することで、音声会話と Slack /speak の記憶が繋がる
    session_key = MQTT_DEVICE_ID
    logger.info("Slack /speak: channel=%s sender=%s session=%s text=%s", channel_id, sender_name, session_key, text[:60])

    try:
        # /speak は「みんなへの発信」なので、依頼者への返答にならないよう指示を加える
        # 送信者名を LLM に渡すことで「パパが〜って言ってたよ」のような表現が可能になる
        speak_instruction = (
            f"{sender_name}から家族全員へのメッセージです。"
            "以下の内容をスタックちゃんらしい口調で読み上げてください。"
            "特定の個人への呼びかけにはせず、その場にいる全員に向けて話してください。"
        )
        reply = await sys.modules["main"].chat_with_llm(
            text,
            system_prompt_append=speak_instruction,
            session_key=session_key,
            notify_context={"session_key": session_key, "slack_channel": channel_id},
            use_functions=False,
        )
    except Exception as e:
        logger.error("Slack /speak LLM error: %s", e)
        await respond("ごめん、うまく変換できなかったよ。もう一度試してね！")
        return

    expression, clean_reply = sys.modules["main"]._parse_expression(reply)
    speaker_id, stackchan_expr = sys.modules["main"]._resolve_expression(expression)
    # /speak は家族から全員への連絡（「明日は運動会だよ」など）。応答ではないが
    # 家族の出来事そのものなので記憶のもとに残す。
    sys.modules["main"]._save_conversation(
        session_key=session_key, speaker=sender_name or None,
        user_text=text, reply_text=clean_reply, source="slack:speak",
    )
    try:
        audio_url, streaming_url = await sys.modules["main"].resolve_audio_url(clean_reply, speaker_id)
        req_id = str(uuid.uuid4())
        # ACK が publish_speak より先に届いても取りこぼさないよう、先に event を登録する
        sys.modules["main"]._pending_acks[req_id] = asyncio.Event()
        sys.modules["main"].publish_speak(audio_url, streaming_url, clean_reply, "slack", "normal", req_id, stackchan_expr)
    except Exception as e:
        sys.modules["main"]._pending_acks.pop(req_id, None)
        logger.error("Slack /speak speak error: %s", e)
        await respond(f"音声の送信に失敗したよ。テキストはこれ：「{clean_reply}」")
        return

    ack_ok = await sys.modules["main"].wait_for_ack(req_id)
    if ack_ok:
        await respond(f"話すよ！「{clean_reply}」", response_type="in_channel")
    else:
        await respond(f"⚠️ スタックちゃんから応答がなかったよ。届いてないかも。「{clean_reply}」", response_type="in_channel")


_DURATION_RE = re.compile(
    r"^(?:(\d{1,2}):(\d{2}))"       # HH:MM
    r"|(?:(\d+)\s*(h|m|s|時間|分|秒))"  # 数値 + 単位
    r"|(\d+)$",                       # 数値のみ（分とみなす）
    re.IGNORECASE,
)


def _parse_duration(token: str) -> int | None:
    """時間指定トークンを秒数に変換する。解析不能の場合は None を返す。
    例: '3m' → 180, '1h' → 3600, '10s' → 10, '14:30' → 今日の 14:30 JST まで, '30' → 1800
    """
    m = _DURATION_RE.match(token.strip())
    if not m:
        return None

    hh, mm, num, unit, bare = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5)

    if hh is not None:
        # 絶対時刻 HH:MM
        now = datetime.now(_JST)
        target = now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        return max(1, int((target - now).total_seconds()))

    if num is not None:
        n = int(num)
        u = unit.lower()
        if u in ("h", "時間"):
            return n * 3600
        if u in ("m", "分"):
            return n * 60
        if u in ("s", "秒"):
            return n
        return None

    if bare is not None:
        return int(bare) * 60  # 数値のみ → 分

    return None


async def _slack_handle_timer(ack, body: dict, respond) -> None:
    """/timer コマンド: 構造化フォーマットでタイマーを設定する。

    書式: /timer <時間> <ラベル>
      時間例: 3m, 1h, 30s, 14:30, 90（分）
      ラベル例: 宿題確認, おやつの時間
    """
    await ack()
    _record_slack_user_from_body(body)

    raw = body.get("text", "").strip()
    if not raw:
        await respond(
            "使い方: `/timer <時間> <ラベル>`\n"
            "時間の例: `3m`（3分）, `1h`（1時間）, `30s`（30秒）, `14:30`（14時30分）\n"
            "例: `/timer 30m 宿題確認`"
        )
        return

    parts = raw.split(None, 1)
    duration_token = parts[0]
    label = parts[1].strip() if len(parts) > 1 else duration_token

    seconds = _parse_duration(duration_token)
    if seconds is None:
        await respond(
            f"⚠️ 時間の指定が解析できなかったよ：`{duration_token}`\n"
            "例: `3m`, `1h`, `30s`, `14:30`, `90`（分）"
        )
        return

    channel_id = body.get("channel_id", "")
    timer_id = sys.modules["main"]._register_timer(
        label=label,
        seconds=seconds,
        session_key="",
        slack_channel=channel_id,
    )

    minutes, secs = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        human = f"{hours}時間{minutes}分後" if minutes else f"{hours}時間後"
    elif minutes:
        human = f"{minutes}分{secs}秒後" if secs else f"{minutes}分後"
    else:
        human = f"{secs}秒後"

    await respond(
        f"⏰ タイマーをセットしたよ！\n"
        f"・ラベル: {label}\n"
        f"・時間: {human}\n"
        f"・ID: `{timer_id}`",
        response_type="in_channel",
    )
    logger.info("Slack /timer: channel=%s label=%s seconds=%d timer_id=%s", channel_id, label, seconds, timer_id)


def _setup_slack():
    """Slack アプリを初期化してハンドラを登録する。トークン未設定時は None を返す。"""
    _main = sys.modules["main"]
    if not SLACK_BOT_TOKEN or not SLACK_APP_TOKEN:
        logger.info("Slack tokens not set — Slack Bot disabled")
        return None

    from slack_bolt.async_app import AsyncApp
    from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler

    _main._slack_app = AsyncApp(token=SLACK_BOT_TOKEN)
    _main._slack_app.event("app_mention")(_slack_handle_mention)
    _main._slack_app.event("message")(_slack_handle_dm)
    _main._slack_app.command("/register")(_slack_handle_register)
    _main._slack_app.command("/say")(_slack_handle_say)
    _main._slack_app.command("/speak")(_slack_handle_speak)
    _main._slack_app.command("/tell")(_slack_handle_tell)
    _main._slack_app.command("/timer")(_slack_handle_timer)

    return AsyncSocketModeHandler(_main._slack_app, SLACK_APP_TOKEN)

