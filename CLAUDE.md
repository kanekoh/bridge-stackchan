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
pytest test_song.py                 # 楽譜変換・gatekeeper・出発リミット
pytest test_capture.py              # 写真の一時保存と記憶の区別・掃除
pytest test_device_wakeword.py      # ウェイクワード操作（HTTP優先・MQTT退避）
pytest test_iss.py                  # ISS の軌道暦の解析・座標変換・通過探索
pytest test_ui_maps.py              # 地図タイルが鍵なしで使える配信元か
pytest test_speaker_id.py           # 話者識別スコアの記録・集計
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
- `config/songs/*.yaml` — 歌の楽譜（歌詞・音名・拍数・BPM・移調）
- `data/bridge.db` — SQLite DB（llm_sessions / items / notification_log / calendar_sources / gate_log / song_play_log / song_trigger_overrides / captures / speaker_id_log）
- `data/songs/*.mp3` — 事前生成した歌（`/songs` で配信。キャッシュなので消してよい）
- `data/captures/YYYY-MM/*.jpg` — Stack-chan が撮った写真（**記憶に昇格したものは消してはいけない**）
- `data/iss/ISS_OEM.txt` — NASA の ISS 軌道暦（15日分）。取得先が落ちている間はこれで通知を続ける
- `testdata/iss_oem_sample.txt` — 上記の抜粋（11時間分）。`test_iss.py` が使う
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

**ウェイクワード**
`device/state`（30秒間隔）には `wakeWord` / `wakeWordRegistered` / `wakeWordThreshold` / `wakeWordLastDistance` が載る。距離（DTW）は**小さいほど似ている**で、しきい値以下なら反応する。距離は話すたびに動くため 30 秒間隔では調整できず、`/ui/device` の距離メーターは `GET /api/device/live`（デバイス本体への直接 HTTP）を 1.5 秒ごとに読む。ブラウザから直接デバイスを叩くと UI が HTTPS のとき混在コンテンツで止まるので、必ず Bridge が代理で取りに行く。

**カメラ設定**
`device/state` に `cameraAeLevel` / `cameraBrightness` / `cameraWarmupFrames` / `cameraJpegQuality` / `cameraFrameSize` が載る。調整は `/ui/device` の「カメラ」から行い、変えたらその場で試し撮りして確かめる。
- **暗いときは露出（`cameraAeLevel`、-2〜2）から。** `cameraBrightness` は後段の補正でノイズも持ち上がる
- `cameraWarmupFrames`（0〜30）は AEC/AGC が落ち着くまでの捨て枚数。暗所で色が転ぶなら増やす
- `cameraFrameSize` は QQVGA/QVGA/CIF/VGA。**VGA(640×480) が GC0308 の上限**
- **0 や -2 を「変更なし」と混同しないこと。** 送信前に `None` のものだけ落とす（混同すると暗いまま直らない）

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
- `GET /api/device/live` — デバイス本体の `GET /device` を Bridge 経由で読む（ウェイクワードの距離調整用の短間隔ポーリング。届かなくても 200 + `available:false`）
- `POST /api/device/wakeword` — ウェイクワードの ON/OFF・登録・しきい値。**HTTP を先に試し、駄目なら MQTT の `device/set` に回る**
- `POST /api/device/camera` — カメラの露出・明るさ補正・ウォームアップ枚数・JPEG画質・解像度。同じく HTTP 優先・MQTT 退避。`capture: true` は試し撮り（**HTTP でしか送れない**）
- `GET /api/songs` — 曲一覧・キャッシュ状態・ENGINE 状態・設定
- `POST /api/songs/{id}/play` — 試聴（gatekeeper を通さない）
- `POST /api/songs/{id}/rebuild` / `POST /api/songs/prebuild` — 再生成
- `POST /api/songs/compose` — その場で1曲つくる
- `GET /api/songs/{id}/score` — 楽譜と ENGINE に渡す notes（UI のピアノロール用）
- `DELETE /api/songs/{id}` — つくった歌を消す（手書きは消せない）
- `GET /api/songs/triggers` — 直近の予定と出発リミットの計算結果
- `GET /songs/{file}.mp3` — 事前生成した歌の配信（StaticFiles）
- `POST /ingest-photo` — Stack-chan が撮った写真を受信（`retention=temp|permanent`）
- `POST /api/captures/request` — MQTT で「いま撮って」と依頼し、写真が届くまで待つ
- `GET /api/captures` — 写真の一覧と統計（`keep=temp|permanent` で絞り込み）
- `GET /api/captures/{id}/file` — 写真の実体を配信
- `POST /api/captures/{id}/keep` / `unkeep` — 記憶への昇格・一時保存への降格
- `DELETE /api/captures/{id}` — 削除（実体を消し、行には消した時刻を残す）

**応答時間（2026-10-20 時点の実測）**
本番（ラズパイ）から実際の回線で測った値。中央値で全体 4.4〜4.8 秒。
- STT 約 1,400ms / LLM 2,300〜4,100ms / VOICEVOX 約 180ms / 話者識別 約 480ms（STT と並行なので隠れる）
- **LLM はモデルで2倍変わる。** 同条件（effort=none）5回ずつで gpt-5.6-luna 1,469ms 対 gpt-6-luna 3,006ms。会話は速い方、通知・記憶・作曲は安い方に分けてある
- **音声を圧縮して送るのは無駄**（検証済み、やり直さないこと）。ラズパイ→OpenAI の送信量は待ち時間に効かない。
  同じ音声で順番を入れ替えて5周した結果、Opus はサイズ 9% でも送信+文字起こしが 76ms しか縮まず、変換 145ms で差し引き遅くなる。
  形式の差より1回ごとの揺らぎ（同じ WAV で 842〜1,095ms）の方が大きい。
  最初に「FLAC が 437ms 速い」と出たのは、形式ごとに連続実行して最初の WAV に接続の立ち上げが乗った測定ミス
- 文字起こしの結果は**同じ音声でも毎回同じとは限らない**（モデル側の非決定性。圧縮の影響と誤認しないこと）
- 残る改善策はリアルタイム文字起こし（録音しながら進める）。`/ingest-audio-stream` は実装・テスト済みだが
  **実運用では一度も使われていない**（記録507件すべて一括送信）。デバイス側 SD の `streamPath` を設定すれば切り替わる

**Slack 統合**
Socket Mode（WebSocket）で動作。`SLACK_BOT_TOKEN` と `SLACK_APP_TOKEN` の両方が必要。
- `/speak <text>` — LLM 変換 → VOICEVOX → MQTT。ACK 受信後に Slack へ結果通知
- `/timer <時間> <ラベル>` — タイマー設定。発火時に LLM で声かけ文を生成して MQTT 送信

**タイマー**
`asyncio.Task` で管理。`_active_timers[timer_id]` に格納。発火後オプションでスヌーズあり。

**カレンダー同期**
`calendar_sync.py` がバックグラウンドスレッドで Google Calendar/Tasks を定期取得 → SQLite の `items` テーブルへ upsert。`main.py` の通知ループ（`_calendar_notify_loop`）が `notify_at` を監視して MQTT 発話を実行。複数人分のトークンを `token_{key}.json` で管理。

**歌（`bridge/features/song/`）**
発話（api.tts.quest）には歌唱 API がないため、歌だけはローカルの VOICEVOX ENGINE 0.25.2 を使う。
- `score.py` — 楽譜 → ENGINE の notes 配列。**純関数のみ**（ENGINE にも DB にも触らない）。先頭無音の挿入と、累積フレームの差分によるテンポずれ防止がここの責務。ユニットテストの主対象（`test_song.py`）
  - **長音符「ー」は母音に開いてから ENGINE に渡す**（`resolve_long_vowel`）。ENGINE は 1 ノート 1 モーラ前提で「ー」だけ 400 を返す（`ッ` `ん` は通る）。「ハッピーバースデー」のような自然な表記で落ちていたため。先頭や「ん」「っ」の直後は開けないので理由を添えて弾く
- `engine.py` — `/sing_frame_audio_query`（sing style_id で音程）→ `/frame_synthesis`（frame_decode style_id で声色）の2段階
- `cache.py` — 起動時に事前生成 → ffmpeg で **24kHz mono MP3、ID3/Xing なし**（デバイスは audioStreamingUrl を生の MP3 フレーム列として読むため、先頭にタグがあると同期できず無音になる）→ `data/songs/`。**リアルタイム合成はしない**。楽譜・style_id・ENGINE バージョン・出力フォーマットのハッシュがキャッシュキー
- `library.py` — 曲を引く唯一の入口。`config/songs/*.yaml`（手書き＝builtin）と `songs` テーブル（つくった歌＝composed）の両方を読む。id 衝突時は YAML 優先。気分タグでの選曲・再生履歴・統計もここ
- `compose.py` — LLM に楽譜 JSON を書かせる。**検証はプロンプトではなく `parse_score()` が担保**（手書きの楽譜と同じ関門）。不正なら理由を添えて1回だけ作り直させる。合成に失敗したら保存を取り消す
- `play.py` — gatekeeper を通してから `publish_speak` で MQTT へ。**新トピックは作らず発話と同じ `stackchan/{deviceId}/speak` に audioUrl を載せる**（ファーム変更が要らない）
- `triggers.py` — 出発リミット（予定の開始 − 移動時間 − 準備バッファ）の監視ループと、「ふと歌う」ループ（日中・在宅・前回から N 時間以上を満たした回ごとに確率で発火）

ENGINE が起動していなければ歌だけが無効になる（他機能は無影響）。`SONG_PUBLIC_BASE_URL` 未設定だと audioUrl を組み立てられないため歌えない。

**キャプチャ（`bridge/features/capture/`）**
Stack-chan（M5Stack CoreS3）が撮った写真を受け取って保存する。**一時保存（既定 7 日で自動削除）と記憶（ずっと残す）を分ける**のが要点で、区別は `captures.keep_until` だけで表す（**NULL なら記憶**、日付が入っていれば一時保存）。
- `store.py` — ファイル（`data/captures/YYYY-MM/{id}.jpg`）と DB 行の保存・昇格・削除。保持期限を決めるのは `retention_deadline()` の1か所だけ
- `cleanup.py` — 期限切れを消すループ。**記憶には絶対に触らない**（`_fetch_expired_captures` が `keep_until IS NOT NULL` で絞る）
- 受け取り口は `POST /ingest-photo`（multipart、`/ingest-audio` と同じ形）。ファーム未対応でも curl で試せる
- `requests.py` — 「いま撮って」の依頼。**依頼は MQTT（`stackchan/{deviceId}/capture` に `{type, requestId, reason}`）、写真は HTTP** で返る。経路が分かれるので `requestId` で突き合わせ、`/ingest-photo` の到着で待機を解除する。ACK は「依頼を受け取った」ことしか示さないため、ACK 待ちとは別に到着を待つ。応答なし（`no_ack`）と、ACK 後に写真が来ない（`timeout`）を区別して返す
- **MQTT に画像は載せない**（ペイロード上限とデバイスのメモリの制約）。種類ごとにトピックを分けず 1 本に `type` を載せる
- **写真を外部に送らない（2026-10-25 決定・変更しないこと）。** 家の中には子どもや来客が写る。撮られた本人の同意なく外部の事業者へ渡さない方針。画像を OpenAI に送って説明させる `vision.py` は削除した。`captures.caption` 列は残っているが自動生成はしない
- LLM ツール `take_photo` — 撮って保存するだけ。**スタックちゃん自身は写真を見られない**ので、「いま部屋どうなってる？」には答えられない（道具の説明文にそう書いてあり、見られないことを正直に伝える）。応答なし／写真が来ない、を分けて返す
- `test_capture.py` が「画像を外に送るコードが入り込んでいないか」を機械的に見張る
- **実体の配信は `/api/captures/{id}/file` だけ**。家族の顔が写るため、歌のような認証なしの StaticFiles には置かない

**ISS 通過通知（`bridge/features/iss/`）**
軌道データは **NASA/JSC の OEM（軌道暦）**を使う。1ファイルに15日分の位置・速度が4分間隔で入っているため、取得先が数日落ちても通知は続く（以前は CelesTrak の TLE を1日分メモリに持つだけで、2026-10-17 からの障害で通知が完全に止まった）。
- `oem.py` — OEM の解析と**エルミート補間**（速度を使うので4分間隔でも軌道の曲がりを再現できる）。純関数のみ
- `passes.py` — EME2000 → 歳差 → GMST で地球固定 → ENU → 方位・仰角。通過は30秒刻みで探し、地平線の交差と最大仰角を二分法・黄金分割で詰める。**歳差を省くと地上で約40kmずれる**ので入れてある（章動と極運動は省略、仰角への影響は0.01度未満）
- `fetch.py` — 取得・ファイル保存（`data/iss/ISS_OEM.txt`）・鮮度の管理。**取りに行けなければ保存してある暦を使い続ける**。失敗後は `ISS_OEM_REFRESH_HOURS` 空けて再挑戦する
- 精度は ephem(SGP4)+最新 TLE との比較で、出現時刻の差 3〜8秒・最大仰角の差 2度以内（2026-10-19 実測）
- `GET /api/debug/iss` に暦の鮮度（作成日時・残り日数・最後の取得エラー）が出る。ダッシュボードの「ISS 次の通過」カードもこれを読む（取得に失敗していても保存分で動き続けるため、気づけるように鮮度を併記する）

**gatekeeper（`bridge/features/gatekeeper.py`）**
発火の抑制を1か所にまとめた汎用モジュール。`allow(kind, key, ...)` → 実行 → `record(kind, key, busy_sec=...)` の順で使う。1回きり（once）・クールダウン・深夜抑制（設置場所のタイムゾーン基準）・再生中の重複防止。判定は**すべてルールベース**で LLM には問い合わせない。記録は `gate_log` テーブル。

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

撮影・録音の依頼 `stackchan/{deviceId}/capture`:
```json
{ "type": "photo", "requestId": "uuid", "reason": "ui" }
```
写真そのものはここを通らない。デバイスは撮ったものを `POST /ingest-photo` に `request_id` 付きで送り返す。

Stack-chan からの ACK（`stackchan/ack`）:
```json
{ "id": "<requestId>", "status": "received", "message": "" }
```

## Key Environment Variables

主要な設定（`.env.example` に全量あり）:
- `LLM_BACKEND` — `openclaw`（デフォルト）or `openai`
- `OPENAI_RESPONSES_MODEL` — 会話モデル。既定は `gpt-5.6-luna`。**gpt-6-luna は半額だが2倍遅い**（同条件5回ずつで中央値 1469ms 対 3006ms、2026-10-20 実測）ため、会話は速さ優先、通知・記憶・作曲は `gpt-6-luna` に分けている
- `MQTT_ACK_TIMEOUT` — ACK タイムアウト秒数（デフォルト 15.0）
- `CALENDAR_ENABLED` — `true` にするとカレンダー同期・通知が有効
- `DISABLE_SESSION_HISTORY` / `DISABLE_TOOLS` — デバッグ・性能切り分け用フラグ
- `OPENAI_RESPONSES_WEB_SEARCH_ON_DEMAND` — Web 検索を LLM の判断で行う2パス方式（実験的）
- `VOICEVOX_SING_URL` — 歌声合成用の VOICEVOX ENGINE（コンテナからホストは `http://10.0.2.2:50021`）
- `SONG_PUBLIC_BASE_URL` — Stack-chan から見た Bridge の URL。歌の audioUrl 組み立てに必須
- `SONG_TRIGGER_ENABLED` — 出発リミットで歌うトリガー（実運用の切り替えは `/ui/songs`）
- `CAPTURE_TEMP_DAYS` — 写真の一時保存の日数（既定 7。記憶に昇格したものは対象外）
- `DEVICE_HTTP_URL` — デバイス本体の HTTP アドレス。ウェイクワードの距離メーターに必要（UI の `device_http_url` が優先）
