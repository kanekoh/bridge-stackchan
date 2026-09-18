# bridge-stackchan

Slack・音声入力・Google カレンダーと Stack-chan をつなぐブリッジ API サーバーです。
Raspberry Pi 上で動作し、テキスト・音声・カレンダー通知・伝言を Stack-chan のスピーチに変換します。

---

## アーキテクチャ

### テキスト → スピーチ

```
Slack /say <テキスト>    → そのまま VOICEVOX → MQTT → Stack-chan
Slack /speak <テキスト>  → LLM でスタックちゃん口調に変換 → VOICEVOX → MQTT → Stack-chan
```

### 音声会話

```
Stack-chan(マイク) → POST /ingest-audio
  ├─ [並行] Whisper STT → テキスト
  └─ [並行] Speaker-ID  → 話者名
         ↓
  LLM（日時コンテキスト + 話者名 + テキスト + 会話履歴 + ツール）
    ├─ 通常の会話 → VOICEVOX → HTTP レスポンス(sync) または MQTT(async)
    ├─ 「伝言ある？」→ get_pending_messages ツール → 伝言を返答
    ├─ タイマー指示 → set_timer ツール
    └─ 予定・タスク → get_upcoming_items ツール
         ↓（sync モード、返答完了後）
  未読伝言チェック → LLM「そういえば〜」→ VOICEVOX → MQTT
```

### カレンダー通知

```
Google Calendar / Tasks API（30分ごと同期）
  → SQLite DB（items テーブル）
  → 通知チェックループ（60秒ごと）
  → LLM で声かけ文を生成
  → VOICEVOX → MQTT → Stack-chan
```

### 伝言板

```
Slack /tell [宛名] <内容>
  → DB（messages テーブル）に保存
  → Stack-chan への次の発話完了後に「そういえば〜」と LLM 経由で読み上げ
  → 読み上げ完了後、送信者に Slack DM で配信通知
```

### 歌

出発が迫ったときなどに、事前に用意した鼻歌を鳴らします。合成はリアルタイムでは行わず、
起動時に生成して `data/songs/` にキャッシュし、Pi から HTTP で配信します。

```
config/songs/*.yaml（人が書いた楽譜）┐
songs テーブル（つくった歌）        ┴→ 同じ Score
  → VOICEVOX ENGINE /sing_frame_audio_query → /frame_synthesis（WAV 24kHz）
  → ffmpeg（MP3 24kHz mono・発話と同じ形式）→ data/songs/ にキャッシュ
  → gatekeeper（1回きり・深夜抑制・重複防止）
  → MQTT publish（発話と同じ stackchan/{deviceId}/speak）
```

歌は LLM にその場で作らせることもできます（`compose_song`）。LLM が書くのは
人が YAML に書くのと同じ楽譜なので、おかしな楽譜は手書きと同じ関門で弾かれます。
作った歌は保存され、次からは合成せずすぐ歌えます。

発話（api.tts.quest）には歌唱 API がないため、歌だけはローカルの
VOICEVOX ENGINE 0.25.2 を使います。ENGINE が起動していなければ歌だけが無効になり、
他の機能には影響しません。詳しくは [歌](#歌-1) を参照してください。

### Slack Bot

```
Slack（Socket Mode）
  ├─ @stackchan メンション → LLM → Slack テキスト返信
  ├─ DM                    → LLM → Slack テキスト返信
  ├─ /say <テキスト>       → そのまま VOICEVOX → MQTT → Stack-chan
  ├─ /speak <テキスト>     → LLM 変換 → VOICEVOX → MQTT → Stack-chan
  ├─ /tell [宛名] <内容>   → 伝言 DB に保存
  ├─ /register <呼び名>    → Slack ID と呼び名をひも付け登録
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

### 起動

```bash
# 直接起動
uvicorn main:app --host 0.0.0.0 --port 8000

# Podman / Docker でビルド + 起動
./start.sh

# 停止
./stop.sh
```

---

## 環境変数

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
| `LLM_BACKEND` | `openai` | `"openclaw"` または `"openai"` |
| `OPENCLAW_BASE_URL` | `http://localhost:18789/v1` | OpenClaw Gateway URL |
| `OPENCLAW_MODEL` | `openclaw` | エージェント ID |
| `OPENCLAW_GATEWAY_TOKEN` | *(空)* | Bearer トークン |
| `OPENCLAW_MAX_OUTPUT_TOKENS` | *(制限なし)* | 最大出力トークン数 |
| `OPENAI_API_KEY` | *(空)* | OpenAI API キー |
| `OPENAI_RESPONSES_MODEL` | `gpt-4o-mini` | モデル名 |
| `OPENAI_RESPONSES_WEB_SEARCH` | `false` | Web 検索ツールを有効にする |
| `OPENAI_RESPONSES_WEB_SEARCH_ON_DEMAND` | `false` | LLM が必要と判断したときだけ検索 |

#### 会話セッション

| 変数 | デフォルト | 説明 |
|---|---|---|
| `SESSION_SUMMARY_THRESHOLD` | `3000` | 文字数がこの値を超えたら要約してリセット |
| `DISABLE_SESSION_HISTORY` | `false` | `true` で毎回新規会話（デバッグ用） |
| `DISABLE_TOOLS` | `false` | `true` で Function Calling を無効化（デバッグ用） |

#### STT・話者識別

| 変数 | デフォルト | 説明 |
|---|---|---|
| `STT_MODEL` | `whisper-1` | Whisper モデル名 |
| `SPEAKER_ID_URL` | *(空)* | speaker-id サービス URL（サーバー間通信用）。未設定で話者識別スキップ |
| `SPEAKER_ID_BROWSER_URL` | *(空)* | speaker-id サービスへのブラウザアクセス URL（例: `http://raspberrypi:8082`）。設定すると Web UI のナビに話者登録・テストリンクが表示される |
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

#### 歌

| 変数 | デフォルト | 説明 |
|---|---|---|
| `VOICEVOX_SING_URL` | `http://127.0.0.1:50021` | 歌声合成用 VOICEVOX ENGINE。コンテナからホストを見る場合は `http://10.0.2.2:50021` |
| `VOICEVOX_SING_TIMEOUT` | `180` | 1 曲の合成にかける上限（秒） |
| `SONG_CONFIG_DIR` | `config/songs` | 楽譜 YAML の置き場 |
| `SONG_DIR` | `data/songs` | 生成済み MP3 の置き場 |
| `SONG_PUBLIC_BASE_URL` | *(空)* | **必須**。Stack-chan から見た Bridge の URL（例: `http://raspberrypi.local:8000`） |
| `SONG_SAMPLE_RATE` | `24000` | 出力サンプリングレート。発話と同じ値。変えるとデバイスが無音になることがある |
| `SONG_BITRATE` | `64k` | 出力ビットレート |
| `SONG_PREBUILD_ON_START` | `true` | 起動時に全曲を事前生成する |
| `FFMPEG_BIN` | `ffmpeg` | ffmpeg のパス |
| `SONG_TRIGGER_ENABLED` | `false` | カレンダー起点トリガーの既定値（実運用は `/ui/songs` で切り替え） |
| `SONG_TRIGGER_LEAD_MINUTES` | `10` | 出発リミットの何分前から歌うか |
| `SONG_TRIGGER_PREP_MINUTES` | `10` | 準備バッファ（分） |
| `SONG_TRIGGER_TRAVEL_MINUTES` | `15` | 移動時間の既定値（分） |
| `SONG_QUIET_START` / `SONG_QUIET_END` | `22:00` / `07:00` | 歌わない時間帯（設置場所のタイムゾーン基準） |
| `SONG_COOLDOWN_MINUTES` | `30` | 前に歌ってから次を歌うまでの最短間隔（分） |
| `SONG_IDLE_ENABLED` | `false` | 用事がなくてもたまに歌い出す |
| `SONG_IDLE_MIN_HOURS` | `6` | 前に歌ってから最低これだけ空ける（時間） |
| `SONG_IDLE_CHANCE` | `0.25` | 条件を満たした回ごとに歌う確率（15分ごとに判定） |
| `SONG_IDLE_START` / `SONG_IDLE_END` | `09:00` / `20:00` | 歌い出してよい時間帯 |

---

## Web UI

`http://<ホスト>:8000/ui` でアクセスできます。

| ページ | URL | 説明 |
|---|---|---|
| 家族メンバー管理 | `/ui/members` | 呼び名・Slack ID・MAC アドレスの登録・編集・削除。Slack でやりとりした未登録ユーザーを候補表示 |
| 伝言管理 | `/ui/messages` | 未読・既読の伝言一覧。配信日時の確認・削除 |
| テスト | `/ui/test` | テキストを直接または LLM 変換して Stack-chan に読み上げさせる |
| 歌 | `/ui/songs` | 曲の一覧・試聴・作曲・楽譜の確認（ピアノロール）・削除、トリガーと「ふと歌う」の設定、歌った履歴 |
| 設定 | `/ui/settings` | Speaker-ID の URL・しきい値など、DB に保存される設定を管理 |

`SPEAKER_ID_BROWSER_URL` が設定されているとナビバーに「話者登録 ↗」「話者テスト ↗」リンクが表示されます。

---

## Slack コマンド

| コマンド | 動作 |
|---|---|
| `/say <テキスト>` | LLM を通さずそのまま読み上げ |
| `/speak <テキスト>` | スタックちゃん口調に LLM 変換して読み上げ |
| `/tell [宛名] <内容>` | 伝言を DB に保存（次の会話時に「そういえば〜」と読み上げ） |
| `/register <呼び名>` | 自分の Slack アカウントを家族メンバーとして登録 |
| `/timer <時間> <ラベル>` | タイマー設定。発火時に VOICEVOX → MQTT |

**`/tell` の書式例**

```
/tell しおり 明日の習い事は16時からだよ   # 宛名付き
/tell 夕食は7時です                       # 宛名なし（全員向け）
```

**`/timer` の時間指定例**

| 指定 | 意味 |
|---|---|
| `3m` | 3 分後 |
| `1h` | 1 時間後 |
| `30s` | 30 秒後 |
| `14:30` | 今日の 14:30 |
| `90` | 90 分後（数値のみは分） |

---

## LLM ツール（Function Calling）

会話中に LLM が自律的に呼び出すツールです。

| ツール | 発動タイミング | 動作 |
|---|---|---|
| `set_timer` | 「3分後に教えて」など | タイマーを設定 |
| `list_timers` | 「タイマーは今いくつ？」など | アクティブなタイマー一覧を返す |
| `get_upcoming_items` | 「今日の予定は？」など | DB からカレンダー予定・タスクを返す |
| `get_pending_messages` | 「伝言ある？」「なにか連絡来てた？」など | 未読伝言を返す。読み上げ後に送信者へ Slack 通知 |
| `play_song` | 「遅刻しそう」「なにか歌って」など | 歌を鳴らす（`song_id` か `mood` を指定） |
| `list_songs` | 「どんな歌うたえるの？」「この前つくった歌なんだっけ？」など | 曲の一覧。お題・作った相手・歌った回数・最後に歌った日も返すので思い出し話ができる |
| `compose_song` | 「歌つくって」「おふろの歌うたって」など | その場で1曲つくって歌う。作った歌は保存される |

---

## 伝言板

### 伝言の流れ

1. Slack で `/tell しおり 明日の習い事は16時からだよ` と送信
2. DB（`messages` テーブル）に保存
3. 誰かが Stack-chan に話しかける（`/ingest-audio` sync モード）
4. メイン返答の音声長さを文字数から推定して待機（≒ `文字数 ÷ 5.5 + 3` 秒）
5. LLM が「そういえば〜」「あ、そうだ〜」などの話題転換フレーズで伝言を読み上げ
6. VOICEVOX → MQTT で Stack-chan が発話
7. Slack で送信者に「しおりへの伝言が届いたよ！」と DM 通知

### 「伝言ある？」への応答

音声で「伝言ある？」「なにか連絡来てた？」と聞くと、LLM が `get_pending_messages` ツールを呼び出し、前置きなしで直接伝言を返します。

### 家族メンバー登録との連携

`/ui/members` で Slack ユーザー ID と呼び名（例: `パパ`）を登録すると、伝言の送信者名に呼び名が使われます。登録方法：

- **Web UI**: `/ui/members` → 「Slack でやりとりした未登録ユーザー」から「登録する」をクリック
- **Slack コマンド**: 本人が `/register パパ` と打つだけで自動登録

---

## API エンドポイント

### スピーチ

#### `POST /speak`

テキストを VOICEVOX で音声化して MQTT 経由で Stack-chan に送信します。LLM は経由しません。

```bash
curl -X POST http://localhost:8000/speak \
  -H "Content-Type: application/json" \
  -d '{"text": "おはよう！今日もいい天気だよ。"}'
```

#### `POST /ingest-audio`

Stack-chan からの WAV 音声を受け取り、STT・話者識別（並行）→ LLM → VOICEVOX のパイプラインを実行します。

| フィールド | 型 | 説明 |
|---|---|---|
| `file` | file | WAV ファイル |
| `mode` | string | `"async"`（MQTT）または `"sync"`（HTTP レスポンス） |
| `session_key` | string | 会話セッション識別子 |
| `source` / `priority` | string | MQTT ペイロードのラベル |

### 家族メンバー

| メソッド | パス | 説明 |
|---|---|---|
| `GET` | `/api/family-members` | 一覧取得 |
| `POST` | `/api/family-members` | 追加（name, slack_user_id, mac_address） |
| `PUT` | `/api/family-members/{id}` | 更新 |
| `DELETE` | `/api/family-members/{id}` | 削除 |
| `GET` | `/api/slack-seen-users` | 未登録の Slack ユーザー候補一覧 |

### 伝言

| メソッド | パス | 説明 |
|---|---|---|
| `GET` | `/api/messages?status=pending\|delivered\|all` | 伝言一覧 |
| `DELETE` | `/api/messages/{id}` | 削除 |

### 設定

| メソッド | パス | 説明 |
|---|---|---|
| `GET` | `/api/settings` | 設定一覧（現在値・env デフォルト） |
| `PUT` | `/api/settings/{key}` | 設定を DB に保存 |
| `DELETE` | `/api/settings/{key}` | DB の値を削除して env デフォルトに戻す |

### カレンダー

| メソッド | パス | 説明 |
|---|---|---|
| `GET` | `/calendar/sources` | 登録済みソース一覧 |
| `POST` | `/calendar/sources` | ソース登録 |
| `DELETE` | `/calendar/sources/{id}` | ソース削除 |

### デバッグ

| エンドポイント | 説明 |
|---|---|
| `GET /healthz` | 死活確認 |
| `GET /debug/sessions` | LLM セッション一覧 |
| `GET /debug/timers` | アクティブなタイマー一覧 |
| `GET /debug/connectivity` | VOICEVOX・MQTT・OpenClaw への疎通確認 |
| `GET /debug/calendar-items` | DB 内の予定・タスク一覧 |

---

## MQTT トピック

| トピック | 方向 | 用途 |
|---|---|---|
| `stackchan/{deviceId}/speak` | Bridge → Stack-chan | 音声再生指示 |
| `stackchan/ack` | Stack-chan → Bridge | 再生受信 ACK |

**`speak` ペイロード**

```json
{
  "type": "speak",
  "audioUrl": "https://...",
  "audioStreamingUrl": "https://...",
  "text": "パパ、会議まであと15分だよ！",
  "source": "calendar",
  "priority": "normal",
  "requestId": "abc-123",
  "expression": "happy"
}
```

**`ack` ペイロード**（Stack-chan から即座に返す）

```json
{ "id": "abc-123", "status": "received", "message": "" }
```

---

## Google カレンダー連携

```bash
# 人ごとに OAuth 認証（ブラウザが開きます）
python calendar_sync.py --auth --key papa   # → secrets/token_papa.json
python calendar_sync.py --auth --key mama   # → secrets/token_mama.json

# カレンダー・タスクリストを自動登録
python calendar_sync.py --register-all --key papa --person パパ
python calendar_sync.py --register-all --key mama --person ママ

# 登録内容を確認
curl http://localhost:8000/calendar/sources
```

### 通知タイミング

| 条件 | 通知タイミング |
|---|---|
| カレンダーに popup リマインダーあり | そのリマインダー時刻 |
| リマインダーなし | `CALENDAR_DEFAULT_NOTIFY_MINUTES` 分前 |
| 終日イベント | 通知しない |

---

## 歌

### VOICEVOX ENGINE の用意

歌唱は Web 高速版（api.tts.quest）では使えないため、ローカルに VOICEVOX ENGINE 0.25.2 を置きます。

```bash
# リリースの voicevox_engine-linux-cpu-{x64|arm64}-0.25.2.vvpp（中身は zip・約1.9GB）を展開して
./run --host 127.0.0.1 --port 50021

# 起動確認
curl http://127.0.0.1:50021/version
```

systemd などで常駐させてください。起動していない場合は歌だけが無効になり、
発話・通知など他の機能はそのまま動きます。

### 楽譜の書き方

`config/songs/<曲ID>.yaml` に置きます。ファイルを足すだけで曲が増えます
（再起動せずに反映するには `/ui/songs` の「楽譜を読み直す」）。

```yaml
id: hurry
title: いそげのうた
bpm: 152
transpose: 0            # 半音単位で移調（±24）
mood: [urgent, hurry]   # play_song(mood=...) で選ぶためのタグ
default_lyric: "ん"      # 鼻歌。はっきり聞かせたいときは "ら"
style:
  sing: 6000            # 音程・タイミングを決める（GET /singers の type=sing）
  frame_decode: 3007    # 声色を決める（type=frame_decode）
notes:
  - {note: G4,   beats: 0.25}          # 音名は C4 = middle C
  - {note: C5,   beats: 0.5, lyric: ら} # lyric 省略時は default_lyric
  - {note: rest, beats: 0.25}          # 休符
```

- **先頭の無音は書きません。** 変換時に自動で入ります（ENGINE が先頭無音を前提にしているため）
- `beats` は拍数（4分音符 = 1.0）。拍→フレームの変換は累積時間から求めるので、音を並べてもテンポがずれません
- `lyric` はひらがな・カタカナ1モーラ（「きゃ」のような拗音も可）

同梱の楽譜:

| 曲 | 用途 |
|---|---|
| `hurry` | いそげのうた。出発リミットのトリガーで鳴らす短い鼻歌 |
| `twinkle` | きらきら星。旋律が既知なので、変換が正しいか耳で確かめる基準曲 |
| `galop` | うきうきギャロップ。カンカン風の陽気な鼻歌 |

style_id は `GET /singers` で確認できます。0.25.2 では `sing` は 6000（波音リツ）のみ、
`frame_decode` は 3003 ずんだもん ノーマル / 3001 あまあま / 3007 ツンツン / 3076 なみだめ などです。
**公開デモでは「VOICEVOX:ずんだもん」のクレジットを表示してください。**

### トリガー

判定はすべてルールベースで、LLM には任せていません。

1. **カレンダー起点** — `予定の開始時刻 − 移動時間 − 準備バッファ` を出発リミットとし、
   残り `SONG_TRIGGER_LEAD_MINUTES` 分を切った時点で在宅なら歌います。
   移動時間・準備バッファは `/ui/songs` の既定値を、予定ごとに上書きできます
   （`song_trigger_overrides`）。
2. **発話起点** — 「遅刻しそう」「なにか歌って」などの発話から LLM が `play_song` を呼びます。

「在宅」は Wi-Fi 在席検知が未実装のため、**旅行中でない**（`trips` に未終了のレコードがない）かつ
**直近5分以内に `device/state` を受信している**ことで代用しています。

### 歌をつくる

`/ui/songs` の「歌をつくる」、または会話で「歌つくって」と頼むと、LLM が楽譜を書きます。

```
お題・雰囲気 → LLM が楽譜 JSON を書く
  → score.parse_score() で検証（手書きの楽譜とまったく同じ関門）
  → songs テーブルに保存 → 合成してキャッシュ → 歌う
```

- 検証はプロンプトの「お願い」ではなく `parse_score()` が担保します。1回目で弾かれた場合は
  エラーの理由を添えてもう一度だけ作り直させます（それでもだめなら諦めます）
- 合成に失敗した歌は保存を取り消します（歌えない曲が一覧に残らないように）
- 作曲に使うモデルは `/ui/settings` の「作曲モデル」で選べます。安いモデルだと楽譜が
  形式どおりにならず作り直しが増えるため、会話モデルと同等以上を推奨します

作った歌は一覧に「つくった歌」として並び、お題・誰のために作ったか・歌った回数・
最後に歌った日が残ります。`list_songs` はこれをそのまま返すので、
「そういえば前に○○の歌つくったよね」という思い出し方ができます。

### ふと歌う

用事がなくても、日中に在宅していてしばらく歌っていなければ、たまに勝手に歌い出します。
毎回きっちり同じ間隔で鳴ると機械らしくなるので、条件を満たした回ごとに確率で決めます
（15分ごとに判定し、既定では 25%）。直近に歌った曲は避けて選びます。

### gatekeeper

発火の抑制は `bridge/features/gatekeeper.py` に集約しています。

| 条件 | 内容 |
|---|---|
| once | 同じ予定では二度と鳴らさない |
| クールダウン | 前に歌ってから `SONG_COOLDOWN_MINUTES` 分は歌わない |
| 深夜抑制 | `SONG_QUIET_START`〜`SONG_QUIET_END` は歌わない（設置場所のタイムゾーン基準） |
| 重複防止 | 前の曲が鳴り終わるまで次を重ねない |

本人が声で頼んだ場合（`play_song` ツール）は深夜抑制を通り抜けます。
UI の試聴は gatekeeper を通しません。

---

## DB テーブル

| テーブル | 用途 |
|---|---|
| `llm_sessions` | LLM 会話履歴・要約 |
| `items` | カレンダー予定・タスク |
| `notification_log` | カレンダー通知済み記録 |
| `calendar_sources` | Google カレンダー・タスクリスト登録 |
| `messages` | 伝言板（送信者・宛名・内容・配信日時） |
| `family_members` | 家族メンバー（呼び名・Slack ID・MAC アドレス） |
| `slack_seen_users` | Slack でやりとりした全ユーザーの記録 |
| `app_settings` | Web UI から変更可能な設定（DB 優先、env フォールバック） |
| `gate_log` | 発火の抑制記録（1回きり判定・クールダウン） |
| `songs` | スタックちゃんが作った歌（楽譜 JSON・お題・作った相手） |
| `song_play_log` | 歌った履歴 |
| `song_trigger_overrides` | 予定ごとの移動時間・準備バッファ・曲の上書き |

---

## 開発ステータス

| フェーズ | 内容 | 状態 |
|---|---|---|
| Phase 1 | Slack コマンド体系整備（/say / /speak / /tell / /register）、伝言板基盤 | 完了 |
| Phase 2 | Wi-Fi 在席検知・MAC アドレス登録 UI | 未実装 |
| Phase 3① | 会話ベースの伝言配信（ingest-audio sync 後フォローアップ） | 完了 |
| Phase 3② | 帰宅検知トリガーによる伝言配信 | 未実装（Phase 2 依存） |
| Phase 4 | Stack-chan 設定 Web UI（MQTT config プロトコル） | 未実装 |
