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
Stack-chan(マイク) → POST /ingest-audio → Whisper STT → OpenClaw → VOICEVOX → MQTT → Stack-chan
```

## セットアップ

### 必要なもの

- Python 3.11+
- [VOICEVOX](https://voicevox.hiroshiba.jp/)（ローカル）または VOICEVOX Web API キー
- MQTTブローカー（ローカル or HiveMQ Cloud等）
- OpenAI API キー（Whisper STT + OpenClaw用）

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
| `AUDIO_DIR` | `/tmp/bridge-audio` | 生成MP3の保存ディレクトリ |
| `AUDIO_BASE_URL` | `http://localhost:8000` | MP3配信のベースURL（Raspberry PiのIP等） |
| `OPENAI_API_KEY` | *(空)* | OpenAI APIキー（Whisper + OpenClaw用） |
| `OPENCLAW_BASE_URL` | `https://api.openai.com/v1` | OpenAI互換エンドポイント |
| `OPENCLAW_MODEL` | `gpt-4o` | 使用するモデル名 |
| `OPENCLAW_SYSTEM_PROMPT` | *(スタックちゃん人格)* | ベースシステムプロンプト（省略時はデフォルト） |

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

M5StackからMP3を受け取り、Whisper STT → OpenClaw → VOICEVOX → MQTTのフルパイプラインを実行します。

**リクエスト（multipart/form-data）**

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| `file` | file | ✓ | M5Stackが録音したMP3 |
| `system_prompt_append` | string | | ベースプロンプトへの追記（文脈・状況の補足など） |
| `source` | string | | 送信元ラベル（デフォルト: `"stackchan"`） |
| `priority` | string | | 優先度（デフォルト: `"normal"`） |
| `request_id` | string | | 冪等キー（省略時は自動生成） |

```bash
curl -X POST http://localhost:8000/ingest-audio \
  -F "file=@voice.mp3" \
  -F "system_prompt_append=今日はパパの誕生日です。お祝いのひとことを添えてください。"
```

**レスポンス**

```json
{
  "requestId": "abc-123",
  "transcript": "今日の天気を教えて",
  "reply": "今日はとってもいいお天気だよ〜！お出かけ日和だね。",
  "audioUrl": "http://raspberry-pi:8000/audio/abc-123.mp3"
}
```

---

### `GET /audio/{filename}.mp3`

生成された音声ファイルの静的配信。Stack-chanがこのURLで音声を取得します。

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
| Phase 2 | 音声入力 → STT → OpenClaw → スピーチ | 完了 |
| Phase 3 | Googleカレンダー・Trello連携、定期通知 | 未着手 |
