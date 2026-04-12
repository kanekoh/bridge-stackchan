# bridge-stackchan

Slack・音声入力とStack-chanをつなぐブリッジAPIサーバーです。
Raspberry Pi上で動作し、テキストや音声をStack-chanのスピーチに変換します。

## アーキテクチャ

### Phase 1 — テキスト→スピーチ

```
Slack → OpenClaw → POST /speak → VOICEVOX → MQTT → Stack-chan
```

### Phase 2 — 音声会話

```
Stack-chan(マイク) → POST /ingest-audio
  ├─ [並行] Whisper STT → テキスト
  └─ [並行] speaker-id  → 話者名
         ↓ 両方揃ったら
  OpenClaw（日時コンテキスト + 話者名 + テキスト）
         ↓
  VOICEVOX
         ↓
  mode=async: MQTT → Stack-chan   （非同期、レスポンスは requestId のみ）
  mode=sync:  HTTP レスポンスで audioUrl を返す（MQTT なし）
```

## セットアップ

### 必要なもの

- Python 3.11+
- [VOICEVOX](https://voicevox.hiroshiba.jp/)（ローカル）または VOICEVOX Web API キー
- MQTTブローカー（ローカル or HiveMQ Cloud等）
- OpenAI API キー（Whisper STT + OpenClaw用）
- [speaker-id](https://github.com/kanekoh/speaker-id)（話者識別、任意）

### インストール

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 環境変数

`.env.example` をコピーして `.env` を作成し、各値を設定してください。

```bash
cp .env.example .env
```

| 変数 | デフォルト | 説明 |
|---|---|---|
| `VOICEVOX_URL` | `http://localhost:50021` | VOICEVOXのエンドポイント |
| `VOICEVOX_SPEAKER` | `1` | スピーカーID |
| `VOICEVOX_API_KEY` | *(空)* | Web高速版を使う場合に設定。空の場合はローカルVOICEVOX |
| `MQTT_BROKER` | `localhost` | MQTTブローカーのホスト |
| `MQTT_PORT` | `1883` | MQTTポート（TLS時は`8883`） |
| `MQTT_USERNAME` | *(空)* | MQTT認証ユーザー名 |
| `MQTT_PASSWORD` | *(空)* | MQTT認証パスワード |
| `MQTT_TLS` | `false` | TLS接続を使う場合は`true` |
| `MQTT_DEVICE_ID` | `default` | Stack-chanのデバイスID |
| `OPENAI_API_KEY` | *(空)* | OpenAI APIキー（Whisper STT用） |
| `OPENCLAW_BASE_URL` | `http://localhost:18789/v1` | OpenClaw Gateway の URL（`/v1` まで含める） |
| `OPENCLAW_MODEL` | `openclaw` | エージェントID（`openclaw` または `openclaw/<agentId>`） |
| `OPENCLAW_GATEWAY_TOKEN` | *(空)* | Gateway の Bearer トークン |
| `OPENCLAW_SESSION_KEY` | *(空)* | Slack チャンネルと会話履歴を共有する場合に設定（`agent:<agentId>:slack:channel:<channelId>`） |
| `SPEAKER_ID_URL` | *(空)* | speaker-idサービスのURL。未設定なら話者識別をスキップ |
| `SPEAKER_ID_API_KEY` | *(空)* | speaker-idのBearerトークン |
| `SPEAKER_ID_THRESHOLD` | `0.75` | 識別スコアの閾値（0.0〜1.0）。未満は「不明」扱い |

### 起動

```bash
# 直接起動
uvicorn main:app --host 0.0.0.0 --port 8000

# Dockerで起動
./start.sh

# Dockerで停止
./stop.sh
```

## APIエンドポイント

### `GET /healthz`

サーバーの死活確認。

```bash
curl http://localhost:8000/healthz
# {"status":"ok"}
```

---

### `POST /speak`

テキストをVOICEVOXで音声化してStack-chanに送信します。

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
  -d '{"text": "おはよう！今日もいい天気だよ。", "source": "slack"}'
```

**レスポンス**

```json
{
  "requestId": "abc-123",
  "audioUrl": "http://raspberry-pi:8000/audio/abc-123.mp3"
}
```

---

### `POST /ingest-audio`

M5StackからWAVを受け取り、STT・話者識別を並行実行したうえで OpenClaw → VOICEVOX のパイプラインを実行します。
`mode` パラメータで音声の配信方法を選択できます。

**リクエスト（multipart/form-data）**

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| `file` | file | ✓ | M5Stackが録音したWAVファイル |
| `mode` | string | | `"async"`（デフォルト）または `"sync"` |
| `system_prompt_append` | string | | ベースプロンプトへの追記（文脈・状況の補足など） |
| `source` | string | | 送信元ラベル（デフォルト: `"stackchan"`） |
| `priority` | string | | 優先度（デフォルト: `"normal"`） |
| `request_id` | string | | 冪等キー（省略時は自動生成） |

**mode=async（デフォルト）**

VOICEVOX で取得した音声 URL を MQTT で Stack-chan に配信します。
レスポンスには `requestId` のみ返します。Stack-chan は MQTT メッセージを受信して再生します。

```bash
curl -X POST http://localhost:8000/ingest-audio \
  -F "file=@voice.wav"
```

```json
{
  "requestId": "abc-123"
}
```

**mode=sync**

MQTT を使わず、音声 URL を HTTP レスポンスで直接返します。
Stack-chan が POST の応答を受け取り次第 `audioUrl` を再生できるため、MQTT のラウンドトリップがなく低レイテンシです。
Stack-chan の会話フロー（音声入力 → 返答再生）に適しています。

```bash
curl -X POST http://localhost:8000/ingest-audio \
  -F "file=@voice.wav" \
  -F "mode=sync"
```

```json
{
  "requestId": "abc-123",
  "transcript": "今日の天気を教えて",
  "speaker": "パパ",
  "reply": "今日はとってもいいお天気だよ〜！お出かけ日和だね。",
  "audioUrl": "https://audio1.tts.quest/v1/data/.../audio.mp3",
  "audioStreamingUrl": "https://audio1.tts.quest/v1/data/.../audio.mp3s"
}
```

> `speaker` は話者識別できなかった場合 `null` になります。`SPEAKER_ID_URL` 未設定の場合も常に `null` です。
> `audioStreamingUrl` は VOICEVOX Web API がストリーミング URL を返した場合のみ含まれます。

---

## MQTTトピック

| トピック | 用途 |
|---|---|
| `stackchan/{deviceId}/speak` | 音声再生指示 |

**`speak` ペイロード例**

```json
{
  "type": "speak",
  "audioUrl": "http://raspberry-pi:8000/audio/abc-123.mp3",
  "text": "今日もよろしくね！",
  "source": "stackchan",
  "priority": "normal",
  "requestId": "abc-123"
}
```

## 開発ステータス

| フェーズ | 内容 | 状態 |
|---|---|---|
| Phase 1 | Slack → テキスト → スピーチ | 完了 |
| Phase 2 | 音声入力 → STT + 話者識別（並行）→ OpenClaw → スピーチ | 完了 |
| Phase 3 | Googleカレンダー・Trello連携、定期通知 | 未着手 |
