# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview
Stack-chan（M5Stack ロボット）をかわいい家族アシスタントとして動作させる Bridge API サービス。Slack・音声入力からテキストを受け取り、LLM で返答を生成し、VOICEVOX で音声合成して MQTT 経由で Stack-chan に再生させる。

主なデータフロー:
- **Slack → Stack-chan**: Slack `/speak` コマンド → LLM → VOICEVOX Web API → MQTT publish → Stack-chan 再生
- **Stack-chan 音声入力**: `/ingest-audio` WAV 受信 → STT (OpenAI Whisper) → LLM → VOICEVOX → MQTT
- **カレンダー通知**: Google Calendar/Tasks 定期同期 → 通知タイミングで VOICEVOX → MQTT

## Commands

```bash
# 開発サーバー起動（ローカル）
uvicorn main:app --reload

# コンテナビルド + 起動（podman、Raspberry Pi 本番用）
./start.sh          # build + run
./stop.sh           # 停止

# Docker でビルドのみ
./build.sh

# テスト
pytest                              # 全テスト
pytest test_main.py                 # API テスト
pytest test_calendar_sync.py        # カレンダー同期テスト
pytest -k "test_parse_expression"   # 単一テスト

# 起動中サービスへの E2E インターフェーステスト
./test_interface.sh
./test_interface.sh "テストメッセージ"

# Google Calendar 初回認証（secrets/token_{key}.json が生成される）
python calendar_sync.py --auth
python calendar_sync.py --auth --key shiori
```

## Code Architecture

### ファイル構成
- `main.py` — FastAPI アプリ本体（約 1900 行、すべての API ロジックを含む）
- `calendar_sync.py` — Google Calendar / Tasks の同期ロジック（スレッドで実行）
- `config/expression_map.yaml` — 感情ラベル → VOICEVOX 話者ID + Stack-chan 表情名のマッピング
- `data/bridge.db` — SQLite DB（llm_sessions / items / notification_log / calendar_sources）
- `secrets/` — Google OAuth トークンファイル（`token.json`, `token_{key}.json`）

### main.py の構造

**起動・初期化（`lifespan`）**
- `_main_loop` にイベントループを保存（MQTT スレッド → asyncio の橋渡し用）
- SQLite 初期化（`_init_db`）
- Slack Socket Mode ハンドラ起動
- カレンダー同期スレッド起動（`CALENDAR_ENABLED=true` のとき）

**LLM バックエンド（`chat_with_llm`）**
- `LLM_BACKEND=openclaw`: OpenClaw（OpenAI 互換 API）を使用。`OPENCLAW_BASE_URL` に接続
- `LLM_BACKEND=openai`: OpenAI Responses API を直接使用。会話履歴は `previous_response_id` チェーンで管理
- セッション履歴は SQLite の `llm_sessions` テーブルで管理。文字数が `SESSION_SUMMARY_THRESHOLD` を超えると自動要約してリセット

**LLM の返答フォーマット**
LLM には必ず感情ラベルを1行目に出力させる:
```
happy
スイミング、明日の16時からだよ！
```
`_parse_expression()` で分割し、`_resolve_expression()` で `expression_map.yaml` から VOICEVOX 話者ID と Stack-chan 表情名を解決する。

**VOICEVOX（`resolve_audio_url`）**
現在は Web 高速版（api.tts.quest）を使用。ローカルに MP3 をダウンロードせず URL をそのまま MQTT に渡す。`get_audio_url_web()` が `(mp3DownloadUrl, mp3StreamingUrl)` を返す。

**MQTT（`_MqttConnection`）**
paho-mqtt の永続接続クラス。`publish()` 呼び出し時に未接続なら自動再接続する。
- 発行トピック: `stackchan/{MQTT_DEVICE_ID}/speak`
- 購読トピック: `stackchan/ack`（Stack-chan からの受信確認）

**MQTT ACK 待機**
- `_pending_acks: dict[str, asyncio.Event]` で requestId ごとにイベントを管理
- `on_message`（MQTT スレッド）→ `call_soon_threadsafe(event.set)`（asyncio スレッドへ通知）
- `wait_for_ack(req_id, timeout=MQTT_ACK_TIMEOUT)` で最大 15 秒待機
- **重要**: `publish_speak` を呼ぶ前に `_pending_acks[req_id]` を登録しないと ACK を取りこぼす（Slack パスは実装済み、`/speak` と `/ingest-audio` エンドポイントは ACK 待機なし）

**API エンドポイント**
- `POST /speak` — テキストを VOICEVOX → MQTT に直接送信（ACK 待機なし）
- `POST /ingest-audio` — WAV 受信 → STT → LLM → VOICEVOX → MQTT。`mode=sync` で MQTT なし
- `GET /healthz` — ヘルスチェック
- `GET /debug/connectivity` — MQTT/VOICEVOX への TCP 疎通確認
- `POST /calendar-sources` など — カレンダーソース管理 CRUD

**Slack 統合**
Socket Mode（WebSocket）で動作。`SLACK_BOT_TOKEN` と `SLACK_APP_TOKEN` の両方が必要。
- `/speak <text>` — LLM 変換 → VOICEVOX → MQTT。ACK 受信後に Slack へ結果通知
- `/timer <時間> <ラベル>` — タイマー設定。発火時に LLM で声かけ文を生成して MQTT 送信

**タイマー**
`asyncio.Task` で管理。`_active_timers[timer_id]` に格納。発火後オプションでスヌーズあり。

**カレンダー同期**
`calendar_sync.py` がバックグラウンドスレッドで Google Calendar/Tasks を定期取得 → SQLite の `items` テーブルへ upsert。`main.py` の通知ループ（`_calendar_notify_loop`）が `notify_at` を監視して MQTT 発話を実行。複数人分のトークンを `token_{key}.json` で管理。

## Stack-chan Personality

LLM に渡すシステムプロンプト（`_STACKCHAN_SYSTEM_PROMPT`）のルール:
- デフォルト日本語。英語で話しかけられてもかわいいカタカナ英語まじりの日本語で返す
- 短く・シンプル・かわいく・話し言葉
- ビジネス的な堅い表現は避ける
- 家族みんなが使うため、特定の一人に寄りすぎない
- ウェブ検索結果は要点を2〜3文で、URL・出典・引用表現は読み上げない
- 返答フォーマット: 1行目が感情ラベル（neutral/happy/sad/sleepy/angry/doubt）、2行目以降が本文

## MQTT Payload

`stackchan/{deviceId}/speak` への発行ペイロード:
```json
{
  "type": "speak",
  "audioUrl": "https://...",
  "audioStreamingUrl": "https://...",
  "text": "おはよう！",
  "source": "slack",
  "priority": "normal",
  "requestId": "uuid",
  "expression": "happy"
}
```

Stack-chan からの ACK（`stackchan/ack`）:
```json
{ "id": "<requestId>", "status": "received", "message": "" }
```

## Key Environment Variables

主要な設定（`.env.example` に全量あり）:
- `LLM_BACKEND` — `openclaw`（デフォルト）or `openai`
- `MQTT_ACK_TIMEOUT` — ACK タイムアウト秒数（デフォルト 15.0）
- `CALENDAR_ENABLED` — `true` にするとカレンダー同期・通知が有効
- `DISABLE_SESSION_HISTORY` / `DISABLE_TOOLS` — デバッグ・性能切り分け用フラグ
- `OPENAI_RESPONSES_WEB_SEARCH_ON_DEMAND` — Web 検索を LLM の判断で行う2パス方式（実験的）
