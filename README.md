# bridge-stackchan

Slack・音声入力・Google カレンダーと Stack-chan をつなぐブリッジ API サーバーです。
Raspberry Pi 上で動作し、テキスト・音声・カレンダー通知を Stack-chan のスピーチに変換します。

---

## アーキテクチャ

### テキスト → スピーチ（Phase 1）

```
Slack /speak コマンド
  → LLM でスタックちゃん口調に変換
  → VOICEVOX → MQTT → Stack-chan
```

### 音声会話（Phase 2）

```
Stack-chan(マイク) → POST /ingest-audio
  ├─ [並行] Whisper STT → テキスト
  └─ [並行] Speaker-ID  → 話者名
         ↓
  LLM（日時コンテキスト + 話者名 + テキスト + 会話履歴）
         ↓
  VOICEVOX
         ↓
  mode=async: MQTT → Stack-chan
  mode=sync : HTTP レスポンスで audioUrl を返す
```

### カレンダー通知（Phase 3）

```
Google Calendar / Tasks API（30分ごと同期）
  → SQLite DB（items テーブル）
  → 通知チェックループ（60秒ごと）
  → LLM で声かけ文を生成
  → VOICEVOX → MQTT → Stack-chan
```

### Slack Bot

```
Slack（Socket Mode）
  ├─ @stackchan メンション → LLM → Slack テキスト返信
  ├─ DM                   → LLM → Slack テキスト返信
  ├─ /speak <テキスト>    → LLM → VOICEVOX → MQTT → Stack-chan
  └─ /timer <時間> <ラベル> → タイマー設定 → 発火時に MQTT
```

---

## セットアップ

### 必要なもの

- Python 3.11+（コンテナは 3.12）
- [VOICEVOX](https://voicevox.hiroshiba.jp/)（ローカル）または VOICEVOX Web 高速版 API キー
- MQTT ブローカー（ローカル or HiveMQ Cloud 等）
- OpenAI API キー（STT 用、LLM バックエンドとして使う場合も）
- OpenClaw（LLM バックエンドとして使う場合）
- [speaker-id](https://github.com/kanekoh/speaker-id)（話者識別、任意）
- Google Cloud OAuth 認証情報（カレンダー連携を使う場合）

### インストール

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 環境変数

`.env.example` をコピーして設定してください。

```bash
cp .env.example .env
```

#### VOICEVOX

| 変数 | デフォルト | 説明 |
|---|---|---|
| `VOICEVOX_URL` | `http://localhost:50021` | VOICEVOX エンドポイント |
| `VOICEVOX_SPEAKER` | `1` | スピーカー ID |
| `VOICEVOX_API_KEY` | *(空)* | Web 高速版を使う場合に設定 |

#### MQTT

| 変数 | デフォルト | 説明 |
|---|---|---|
| `MQTT_BROKER` | `localhost` | ブローカーホスト |
| `MQTT_PORT` | `1883` | ポート（TLS 時は `8883`） |
| `MQTT_USERNAME` | *(空)* | 認証ユーザー名 |
| `MQTT_PASSWORD` | *(空)* | 認証パスワード |
| `MQTT_TLS` | `false` | TLS 接続を使う場合は `true` |
| `MQTT_DEVICE_ID` | `default` | Stack-chan のデバイス ID |
| `MQTT_QOS` | `1` | QoS レベル |
| `MQTT_ACK_TIMEOUT` | `15.0` | ACK 待ちタイムアウト（秒） |

#### LLM バックエンド

| 変数 | デフォルト | 説明 |
|---|---|---|
| `LLM_BACKEND` | `openclaw` | `"openclaw"` または `"openai"` |
| `OPENCLAW_BASE_URL` | `http://localhost:18789/v1` | OpenClaw Gateway URL |
| `OPENCLAW_MODEL` | `openclaw` | エージェント ID |
| `OPENCLAW_GATEWAY_TOKEN` | *(空)* | Bearer トークン |
| `OPENCLAW_SESSION_KEY` | *(空)* | 会話セッションキー |
| `OPENCLAW_MAX_OUTPUT_TOKENS` | *(制限なし)* | 最大出力トークン数 |
| `OPENAI_API_KEY` | *(空)* | OpenAI API キー |
| `OPENAI_RESPONSES_BASE_URL` | `https://api.openai.com/v1` | Responses API エンドポイント |
| `OPENAI_RESPONSES_MODEL` | `gpt-4o-mini` | モデル名 |
| `OPENAI_RESPONSES_MAX_OUTPUT_TOKENS` | *(制限なし)* | 最大出力トークン数 |
| `OPENAI_RESPONSES_WEB_SEARCH` | `false` | Web 検索ツールを有効にする |
| `OPENAI_RESPONSES_WEB_SEARCH_ON_DEMAND` | `false` | LLM が必要と判断したときだけ検索（レイテンシ最適化） |

#### 会話セッション

| 変数 | デフォルト | 説明 |
|---|---|---|
| `SESSION_SUMMARY_THRESHOLD` | `3000` | 文字数がこの値を超えたら要約してリセット |
| `SESSION_SUMMARY_MAX_TOKENS` | `500` | 要約の最大トークン数 |
| `DISABLE_SESSION_HISTORY` | `false` | `true` で毎回新規会話（デバッグ用） |
| `DISABLE_TOOLS` | `false` | `true` で Function Calling を無効化（デバッグ用） |

#### STT・話者識別

| 変数 | デフォルト | 説明 |
|---|---|---|
| `STT_MODEL` | `whisper-1` | Whisper モデル名 |
| `SPEAKER_ID_URL` | *(空)* | speaker-id サービス URL。未設定で話者識別スキップ |
| `SPEAKER_ID_API_KEY` | *(空)* | Bearer トークン |
| `SPEAKER_ID_THRESHOLD` | `0.75` | 識別スコアの閾値（未満は「不明」扱い） |

#### Slack

| 変数 | デフォルト | 説明 |
|---|---|---|
| `SLACK_BOT_TOKEN` | *(空)* | Bot Token（`xoxb-...`） |
| `SLACK_APP_TOKEN` | *(空)* | App Token（`xapp-...`、Socket Mode 用） |

両方設定されている場合のみ Slack Bot が有効になります。

#### DB

| 変数 | デフォルト | 説明 |
|---|---|---|
| `DB_PATH` | `data/bridge.db` | SQLite DB ファイルパス |

#### Google カレンダー

| 変数 | デフォルト | 説明 |
|---|---|---|
| `CALENDAR_ENABLED` | `false` | `true` にすると同期・通知が有効 |
| `GOOGLE_CREDENTIALS_FILE` | `credentials.json` | Google Cloud の OAuth 設定ファイル |
| `GOOGLE_TOKEN_DIR` | `.` | トークンファイルを置くディレクトリ |
| `CALENDAR_SYNC_INTERVAL_MINUTES` | `30` | Google API への同期間隔（分） |
| `CALENDAR_DEFAULT_NOTIFY_MINUTES` | `15` | リマインダー未設定時の通知タイミング（分前） |
| `CALENDAR_SYNC_DAYS_AHEAD` | `7` | 何日先まで同期するか |
| `CALENDAR_NOTIFY_CHECK_INTERVAL` | `60` | 通知チェック間隔（秒） |
| `CALENDAR_NOTIFY_GRACE_MINUTES` | `60` | 再起動時の通知猶予（この分以内の通知のみ送信） |

### 起動

```bash
# 直接起動
uvicorn main:app --host 0.0.0.0 --port 8000

# Podman / Docker で起動
./start.sh

# 停止
./stop.sh
```

---

## API エンドポイント

### `GET /healthz`

死活確認。

```bash
curl http://localhost:8000/healthz
# {"status":"ok"}
```

---

### `POST /speak`

テキストを VOICEVOX で音声化して MQTT 経由で Stack-chan に送信します。LLM は経由しません。

**リクエスト（JSON）**

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| `text` | string | ✓ | 読み上げるテキスト |
| `source` | string | | 送信元ラベル（デフォルト: `"unknown"`） |
| `priority` | string | | 優先度（デフォルト: `"normal"`） |
| `request_id` | string | | 冪等キー（省略時は自動生成） |

```bash
curl -X POST http://localhost:8000/speak \
  -H "Content-Type: application/json" \
  -d '{"text": "おはよう！今日もいい天気だよ。"}'
```

**レスポンス**

```json
{
  "requestId": "abc-123",
  "audioUrl": "https://..."
}
```

---

### `POST /ingest-audio`

Stack-chan からの WAV 音声を受け取り、STT・話者識別（並行）→ LLM → VOICEVOX のパイプラインを実行します。

**リクエスト（multipart/form-data）**

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| `file` | file | ✓ | WAV ファイル |
| `mode` | string | | `"async"`（デフォルト）または `"sync"` |
| `system_prompt_append` | string | | システムプロンプトへの追記 |
| `source` | string | | 送信元ラベル（デフォルト: `"stackchan"`） |
| `priority` | string | | 優先度（デフォルト: `"normal"`） |
| `request_id` | string | | 冪等キー（省略時は自動生成） |
| `session_key` | string | | 会話セッション識別子（省略時は `MQTT_DEVICE_ID`） |

**mode=async（デフォルト）**

MQTT で Stack-chan に音声 URL を配信します。レスポンスは `requestId` のみ。

```bash
curl -X POST http://localhost:8000/ingest-audio -F "file=@voice.wav"
# {"requestId": "abc-123"}
```

**mode=sync**

MQTT を使わず HTTP レスポンスで直接返します。MQTT ラウンドトリップがない分、低レイテンシです。

```bash
curl -X POST http://localhost:8000/ingest-audio -F "file=@voice.wav" -F "mode=sync"
```

```json
{
  "requestId": "abc-123",
  "transcript": "今日の天気を教えて",
  "speaker": "パパ",
  "reply": "今日はとってもいいお天気だよ〜！",
  "audioUrl": "https://...",
  "audioStreamingUrl": "https://..."
}
```

> `speaker` は話者識別できない場合 `null`。`audioStreamingUrl` は VOICEVOX Web API が返した場合のみ含まれます。

---

### `GET /calendar/sources`

登録済みのカレンダー・タスクリスト一覧を返します。

```bash
curl http://localhost:8000/calendar/sources
```

---

### `POST /calendar/sources`

カレンダーまたはタスクリストを登録します。

**リクエスト（JSON）**

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| `source_type` | string | ✓ | `"calendar"` または `"tasklist"` |
| `source_id` | string | ✓ | Google カレンダー ID またはタスクリスト ID |
| `person_name` | string | ✓ | 声かけに使う名前（日本語可、例: `"パパ"`） |
| `token_key` | string | | 認証トークンキー（デフォルト: `"default"`） |
| `notify` | boolean | | 通知するか（デフォルト: `true`） |
| `enabled` | boolean | | 同期するか（デフォルト: `true`） |

```bash
curl -X POST http://localhost:8000/calendar/sources \
  -H "Content-Type: application/json" \
  -d '{"source_type":"calendar","source_id":"papa@gmail.com","person_name":"パパ","token_key":"papa"}'
```

---

### `DELETE /calendar/sources/{id}`

カレンダーソースの登録を削除します。

```bash
curl -X DELETE http://localhost:8000/calendar/sources/1
```

---

### デバッグエンドポイント

| エンドポイント | 説明 |
|---|---|
| `GET /debug/sessions` | LLM セッション一覧（会話履歴・要約） |
| `GET /debug/timers` | 現在アクティブなタイマー一覧 |
| `GET /debug/connectivity` | VOICEVOX・MQTT・OpenClaw への疎通確認 |
| `GET /debug/calendar-items` | DB 内の予定・タスク一覧（`notify_at` を含む） |

---

## MQTT トピック

| トピック | 方向 | 用途 |
|---|---|---|
| `stackchan/{deviceId}/speak` | Bridge → Stack-chan | 音声再生指示 |
| `stackchan/ack` | Stack-chan → Bridge | 再生完了 ACK |

**`speak` ペイロード**

```json
{
  "type": "speak",
  "audioUrl": "https://...",
  "audioStreamingUrl": "https://...",
  "text": "パパ、会議まであと15分だよ！",
  "source": "calendar",
  "priority": "normal",
  "requestId": "abc-123"
}
```

> `audioStreamingUrl` は VOICEVOX Web API がストリーミング URL を返した場合のみ含まれます。

**`ack` ペイロード**

```json
{ "id": "abc-123" }
```

---

## Slack Bot

`SLACK_BOT_TOKEN` と `SLACK_APP_TOKEN` を両方設定すると有効になります（Socket Mode）。

| 操作 | 動作 |
|---|---|
| チャンネルで `@stackchan <メッセージ>` | LLM で回答 → Slack にテキスト返信 |
| Stack-chan に DM | LLM で回答 → Slack にテキスト返信 |
| `/speak <テキスト>` | LLM でスタックちゃん口調に変換 → VOICEVOX → MQTT |
| `/timer <時間> <ラベル>` | タイマー設定。発火時に VOICEVOX → MQTT |

**`/timer` の時間指定例**

| 指定 | 意味 |
|---|---|
| `3m` | 3 分後 |
| `1h` | 1 時間後 |
| `30s` | 30 秒後 |
| `14:30` | 今日の 14:30 |
| `90` | 90 分後（数値のみは分） |

---

## Google カレンダー連携

カレンダーイベントとタスクを定期的に取得し、開始前に Stack-chan が声で通知します。

### 前提

1. [Google Cloud Console](https://console.cloud.google.com/) でプロジェクトを作成
2. **Google Calendar API** と **Google Tasks API** を有効化
3. 「OAuth 2.0 クライアント ID」を作成し、`credentials.json` をダウンロード
4. `.env` で `CALENDAR_ENABLED=true` を設定

### セットアップ手順

```bash
# 1. 人ごとに認証（ブラウザが開きます）
#    token_key はファイル名に使う ASCII 識別子
python calendar_sync.py --auth --key papa   # → token_papa.json
python calendar_sync.py --auth --key mama   # → token_mama.json
python calendar_sync.py --auth              # → token.json（1人の場合）

# 2. 全カレンダー・タスクリストを自動登録（推奨）
#    person_name が声かけの日本語名になります
python calendar_sync.py --register-all --key papa --person パパ
python calendar_sync.py --register-all --key mama --person ママ

# 3. 登録内容を確認
curl http://localhost:8000/calendar/sources

# 4. 不要なカレンダーを削除（特定のカレンダーだけ使いたい場合）
curl -X DELETE http://localhost:8000/calendar/sources/{id}
```

### 通知タイミング

| 条件 | 通知タイミング |
|---|---|
| カレンダーに popup リマインダーあり | そのリマインダー時刻 |
| リマインダーなし | `CALENDAR_DEFAULT_NOTIFY_MINUTES` 分前（デフォルト: 15 分前） |
| 終日イベント | 通知しない |

### イベント・タスクのステータス遷移

| 状態 | 説明 |
|---|---|
| `active` | 通知対象 |
| `done` | タスクが完了（Google Tasks から消えた） |
| `deleted` | イベントがキャンセル・削除された |

`done` / `deleted` は通知されません。終了から 1 時間後に DB から自動削除されます。

---

## 開発ステータス

| フェーズ | 内容 | 状態 |
|---|---|---|
| Phase 1 | Slack → LLM → スピーチ（MQTT） | 完了 |
| Phase 2 | 音声入力 → STT + 話者識別 → LLM → スピーチ | 完了 |
| Phase 3 | Google カレンダー・タスク連携、定期通知 | 完了 |
