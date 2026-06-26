# 雨予報エンジン設計書

## 概要

AMeDAS 多点観測データから雨の移動ベクトルを推定し、Open-Meteo の数値予報と組み合わせて
設置場所への雨の到達を予測するエンジン。

主目的: 洗濯物の取り込み通知（30〜60分先に雨が来るか）

---

## データソース

### 1. 気象庁 AMeDAS（観測値）

| 項目 | 内容 |
|------|------|
| URL | `https://www.jma.go.jp/bosai/amedas/data/map/{YYYYMMDDHHMMSS}.json` |
| 時刻形式 | UTC (`latest_time.txt` は JST → 変換要) |
| 更新頻度 | 10分ごと |
| 主要フィールド | `precipitation10m` (10分間降水量 mm), `temp` |
| 観測点密度 | 半径100km以内に約63点（鎌倉基準） |
| 特性 | **観測値のみ**。将来予測なし。 |

```
最新時刻取得: GET https://www.jma.go.jp/bosai/amedas/data/latest_time.txt
観測点マスタ: GET https://www.jma.go.jp/bosai/amedas/const/amedastable.json
マップデータ: GET https://www.jma.go.jp/bosai/amedas/data/map/20260625142000.json
```

### 2. Open-Meteo（数値予報モデル）

| 項目 | 内容 |
|------|------|
| URL | `https://api.open-meteo.com/v1/forecast` |
| 更新頻度 | 15分（`past_minutely_15` + `forecast_minutely_15`） |
| 主要フィールド | `precipitation` (15分間降水量 mm) |
| 特性 | **予報あり**（60分先まで）。局地性は低い。 |

---

## アルゴリズム

### ステップ 1: AMeDAS スナップショット収集

過去 60 分を 10 分刻みで **7 スナップショット**（t-60, t-50, ..., t-0）を並列取得。
対象は設置場所から半径 100km 以内の観測点のみ（鎌倉基準: 63点）。

```
t-60  t-50  t-40  t-30  t-20  t-10  t-0（最新）
 snap0  snap1  snap2  snap3  snap4  snap5  snap6
```

各スナップショットは `{station_id: {precipitation10m: [値, ...], ...}}` の辞書。

### ステップ 2: 降水開始時刻の特定

過去1時間に一度でも降水（`precipitation10m > 0`）を記録した観測点について、
**最初に降水を記録した時刻インデックス**（onset_index）を求める。

```
onset_times = {}
for i, snapshot in enumerate(snapshots):
    for station_id, obs in snapshot.items():
        if station_id not in onset_times and obs.precipitation10m > 0:
            onset_times[station_id] = i  # 0=t-60, 6=t-0
```

### ステップ 3: 移動ベクトルの推定

#### 方法: 時空間線形回帰

降水開始時刻 `t_i` は観測点座標 `(x_i, y_i)` の線形関数と仮定する：

```
t_i = a * x_i + b * y_i + c
```

ここで `(x, y)` は設置場所を原点とした **km 単位の平面座標**：

```
x_i = (lon_i - lon_target) * 111 * cos(lat_target)   [km, 東が正]
y_i = (lat_i - lat_target) * 111                       [km, 北が正]
```

`t_i` の単位は分（t-60 → 0, t-0 → 6）。

最小二乗法（`numpy.linalg.lstsq`）で `(a, b, c)` を推定。

#### 移動ベクトルの解釈

勾配ベクトル `(a, b)` は「t（降水開始時刻）が増える方向」＝**雨が進んでいる方向（移動先）**。

```
# 雨の移動方向（勾配がそのまま移動方向）
direction_deg = atan2(a, b)   # bearing: 0°=北, 90°=東

# 移動速度 [km/h]
speed_kmh = (1.0 / sqrt(a**2 + b**2)) * 60
```

#### 到達時刻の予測

```
# 設置場所 (0, 0) への onset_time を予測
t_target = c   (a*0 + b*0 + c = c)

# 現在 (snap6) からの到達までの分数
arrival_min = (6 - t_target) * 10  # スナップ単位 → 分
```

`arrival_min > 0`: まだ到達していない（あと arrival_min 分で来る）  
`arrival_min <= 0`: すでに到達しているはず

### ステップ 4: Open-Meteo と照合・統合判定

#### `now_dry`（現在乾燥か）

AMeDAS と Open-Meteo の **両方** が乾燥を示す場合のみ `now_dry=True`。
どちらか一方でも雨を検知していれば `now_dry=False`（観測データ優先）。

```
now_dry = (AMeDAS 20km 内に降水局なし) AND (Open-Meteo 現在値 < 閾値)
```

#### 予報照合

```
AMeDAS 推定                Open-Meteo 予報
───────────────           ─────────────────
arrival_min ≤ 30   AND    forecast_prec > 0    → 確信度: HIGH
arrival_min ≤ 60   OR     forecast_prec > 0    → 確信度: MEDIUM
arrival_min > 60   AND    forecast_prec = 0    → 確信度: LOW（雨来ない可能性大）
```

#### 通知フロー（`_check_rain_notification`）

```
now_dry=False
  └─ 通知未送信  → 「気づいたら雨が降り始めてる！」（予報外れ通知）
  └─ 通知済み   → スルー（クールダウン維持）

now_dry=True AND soon_wet=False
  └─ AMeDAS 接近中  → クールダウン保持（まだリセットしない）
  └─ 接近なし      → クールダウンリセット

now_dry=True AND soon_wet=True
  └─ 3時間クールダウン確認 → 「30分以内に雨」または「急な雨」通知
```

---

## データフロー

```
latest_time.txt
      │
      ▼
[UTC タイムスタンプ] ──────────────────────────────┐
      │                                              │
      ▼ ×7並列                                      │
map/{ts}.json × 7スナップ                           │
      │                                              │
      ▼                                              ▼
amedastable.json      Open-Meteo forecast_minutely_15
      │                          │
      ▼                          │
半径100km内に絞込                │
      │                          │
      ▼                          │
onset_time 行列 (N点×7時刻)     │
      │                          │
      ▼                          │
最小二乗回帰 → (a, b, c)        │
      │                          │
      ▼                          │
移動方向・速度・到達時刻推定      │
      │                          │
      └──────────┬───────────────┘
                 ▼
          統合予報結果
          {
            now_dry: bool,          # AMeDAS AND Open-Meteo 両方が乾燥
            soon_wet: bool,         # 30分以内に雨（AMeDAS OR Open-Meteo）
            sudden: bool,
            openmeteo_confirms: bool,
            amedas: {
              approaching: bool,
              arrival_min: int | None,
              direction_str: str,
              speed_kmh: float,
              confidence: "high"|"medium"|"low",
              wet_now: [...],       # 現在降水中の近隣局
            },
            openmeteo: { timeline: [...], ... },
          }
```

---

## 実装場所

| 関数 | 役割 |
|------|------|
| `_load_amedas_station_table()` | 観測点マスタをメモリキャッシュ（初回のみ取得） |
| `_fetch_amedas_snapshots(lat, lon)` | 7スナップを並列取得・半径フィルタ |
| `_estimate_rain_movement(snapshots, meta, target)` | 移動ベクトル推定コア |
| `_fetch_amedas_openmeteo_rain_data(lat, lon)` | 両ソース統合、最終予報生成 |
| `_check_rain_notification()` | 通知判定・送信。`now_dry=False` の予報外れ検知も担う |
| `_rain_llm_comment(sudden, time_label, hour, unexpected)` | LLM による状況コメント生成 |

`rain_source = "amedas+openmeteo"` として `_EDITABLE_SETTINGS` に追加。  
`_check_rain_notification()` で既存の `nowcast` / `openmeteo` と同列に扱う。

---

## 制約・注意事項

| 項目 | 内容 |
|------|------|
| 空間解像度 | 観測点間距離 15〜30km → 局地的な雨（数km規模）は捉えられない |
| 時間解像度 | 10分刻み → 速度推定誤差 ±20〜30% |
| 回帰の前提 | 雨前線が直線状に一定速度で移動していると仮定 |
| 最低データ数 | onset_time が判明した観測点が 3点以上必要（それ未満は低確信度） |
| AMeDAS の欠測 | 一部観測点が欠測することがある（robust に扱う） |
| 夜間の雷雨 | 局地的・急速に発達するため本手法は不向き（Open-Meteo 優先） |

---

## 比較: 各ソースの特性

| ソース | 現況精度 | 30分先予報 | 局地性 | コスト |
|-------|---------|-----------|-------|-------|
| AMeDAS 単体 | ◎ | × | △ | 無料 |
| Open-Meteo 単体 | ○ | ○ | △ | 無料 |
| JMA ナウキャスト | ◎ | ◎ | ◎ | 無料（非公式） |
| **AMeDAS + Open-Meteo** | **◎** | **○** | **△** | **無料・公式** |

---

## 将来の改善案

- 観測点間の距離逆数重み付きで回帰精度向上
- 降水量（強さ）を onset_time ではなく連続量で扱う
- 梅雨前線・台風など気圧配置パターンに応じたモデル切り替え
- AMeDAS の水平内挿で任意地点の降水推定（Kriging 等）
