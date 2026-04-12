#!/usr/bin/env bash
# test_interface.sh - Bridge API インターフェーステスト
#
# 使い方:
#   ./start.sh && ./test_interface.sh                          # デフォルトメッセージで実行
#   ./start.sh && ./test_interface.sh "こんにちは、テストだよ！"  # メッセージを引数で指定
#
# 起動・停止は start.sh / stop.sh に任せる。
# このスクリプトは既に起動済みのサービスに対してテストのみ実行する。

set -euo pipefail

# ── 設定 ─────────────────────────────────────────────
MESSAGE="${1:-おはよう！今日もよろしくね。テストメッセージだよ。}"
PORT="${PORT:-8000}"
BASE_URL="http://localhost:${PORT}"

# ── 色出力 ────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

PASS=0
FAIL=0

pass() { echo -e "  ${GREEN}✓ PASS${RESET}  $1"; PASS=$((PASS + 1)); }
fail() { echo -e "  ${RED}✗ FAIL${RESET}  $1"; FAIL=$((FAIL + 1)); }
info() { echo -e "${CYAN}▶${RESET} $1"; }
warn() { echo -e "${YELLOW}⚠${RESET}  $1"; }
header() { echo -e "\n${BOLD}$1${RESET}"; }

# ── テスト用WAVファイル生成 ───────────────────────────
WAV_FILE=$(mktemp /tmp/test_audio_XXXXXX.wav)
python3 - <<'EOF' > "${WAV_FILE}"
import io, sys, wave
buf = io.BytesIO()
with wave.open(buf, "wb") as wf:
    wf.setnchannels(1)
    wf.setsampwidth(2)
    wf.setframerate(16000)
    wf.writeframes(b"\x00\x00" * 16000)  # 1秒の無音
sys.stdout.buffer.write(buf.getvalue())
EOF
trap 'rm -f "${WAV_FILE}"' EXIT

# ── サービス起動確認 ──────────────────────────────────
if ! curl -sf "${BASE_URL}/healthz" > /dev/null 2>&1; then
  warn "サービスが起動していません: ${BASE_URL}"
  warn "先に ./start.sh を実行してください。"
  exit 1
fi
info "サービス確認OK: ${BASE_URL}"

# ── テスト実行 ────────────────────────────────────────
header "インターフェーステスト"
echo -e "  テストメッセージ: ${BOLD}${MESSAGE}${RESET}\n"

# --- /debug/connectivity ---
echo -e "${BOLD}[GET /debug/connectivity]${RESET}"
HTTP_CODE=$(curl -s -o /tmp/debug_body.json -w "%{http_code}" "${BASE_URL}/debug/connectivity")
BODY=$(cat /tmp/debug_body.json)
if [[ "${HTTP_CODE}" == "200" ]]; then
  echo "${BODY}" | python3 -c "
import json, sys
d = json.load(sys.stdin)
print('  ENV:')
for k, v in d.get('env', {}).items():
    print(f'    {k}: {v}')
print('  TCP:')
for k, v in d.get('tcp', {}).items():
    mark = '✓' if v == 'ok' else '✗'
    print(f'    {mark} {k}: {v}')
"
  ALL_TCP_OK=$(echo "${BODY}" | python3 -c "
import json, sys
d = json.load(sys.stdin)
bad = [k for k, v in d.get('tcp', {}).items() if v != 'ok']
print(' '.join(bad))
")
  if [[ -z "${ALL_TCP_OK}" ]]; then
    pass "全サービスへの TCP 接続 OK"
  else
    fail "TCP 接続失敗: ${ALL_TCP_OK}"
  fi
else
  fail "HTTP ${HTTP_CODE}"
fi

# --- /healthz ---
echo -e "${BOLD}[GET /healthz]${RESET}"
HTTP_CODE=$(curl -s -o /tmp/healthz_body.json -w "%{http_code}" "${BASE_URL}/healthz")
BODY=$(cat /tmp/healthz_body.json)
if [[ "${HTTP_CODE}" == "200" ]] && echo "${BODY}" | grep -q '"ok"'; then
  pass "HTTP 200, body=${BODY}"
else
  fail "HTTP ${HTTP_CODE}, body=${BODY}"
fi

# --- /speak ---
echo -e "\n${BOLD}[POST /speak]${RESET}"
SPEAK_PAYLOAD=$(python3 -c "
import json, sys
print(json.dumps({
  'text': sys.argv[1],
  'source': 'test_interface.sh',
  'priority': 'normal'
}, ensure_ascii=False))
" "${MESSAGE}")

HTTP_CODE=$(curl -s -o /tmp/speak_body.json -w "%{http_code}" \
  -X POST "${BASE_URL}/speak" \
  -H "Content-Type: application/json" \
  -d "${SPEAK_PAYLOAD}")
BODY=$(cat /tmp/speak_body.json)

if [[ "${HTTP_CODE}" == "200" ]]; then
  REQUEST_ID=$(echo "${BODY}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('requestId',''))" 2>/dev/null || true)
  AUDIO_URL=$(echo "${BODY}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('audioUrl',''))" 2>/dev/null || true)
  pass "HTTP 200"
  echo -e "     requestId : ${REQUEST_ID}"
  echo -e "     audioUrl  : ${AUDIO_URL}"
  if [[ -n "${REQUEST_ID}" ]]; then
    pass "requestId が返却された"
  else
    fail "requestId が空"
  fi
elif [[ "${HTTP_CODE}" == "502" ]]; then
  DETAIL=$(echo "${BODY}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('detail',''))" 2>/dev/null || echo "${BODY}")
  fail "HTTP 502 — ${DETAIL}"
else
  fail "HTTP ${HTTP_CODE}, body=${BODY}"
fi

# --- /ingest-audio ---
echo -e "\n${BOLD}[POST /ingest-audio]${RESET}"
info "WAVファイル: ${WAV_FILE} ($(wc -c < "${WAV_FILE}") bytes)"
HTTP_CODE=$(curl -s -o /tmp/ingest_body.json -w "%{http_code}" \
  -X POST "${BASE_URL}/ingest-audio" \
  -F "file=@${WAV_FILE};filename=test.wav;type=audio/wav")
BODY=$(cat /tmp/ingest_body.json)

if [[ "${HTTP_CODE}" == "200" ]]; then
  TRANSCRIPT=$(echo "${BODY}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('transcript',''))" 2>/dev/null || true)
  REPLY=$(echo "${BODY}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('reply',''))" 2>/dev/null || true)
  pass "HTTP 200 (STT + OpenClaw + VOICEVOX + MQTT すべて成功)"
  echo -e "     transcript : ${TRANSCRIPT}"
  echo -e "     reply      : ${REPLY}"
elif [[ "${HTTP_CODE}" == "503" ]]; then
  warn "HTTP 503 (OPENAI_API_KEY 未設定 — STT は使用不可)"
  PASS=$((PASS + 1))
elif [[ "${HTTP_CODE}" == "502" ]]; then
  DETAIL=$(echo "${BODY}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('detail',''))" 2>/dev/null || echo "${BODY}")
  fail "HTTP 502 — ${DETAIL}"
else
  fail "HTTP ${HTTP_CODE}, body=${BODY}"
fi

# ── 結果サマリ ────────────────────────────────────────
header "結果"
TOTAL=$((PASS + FAIL))
echo -e "  合計: ${TOTAL}  ${GREEN}PASS: ${PASS}${RESET}  ${RED}FAIL: ${FAIL}${RESET}"

if [[ ${FAIL} -eq 0 ]]; then
  echo -e "\n${GREEN}${BOLD}全テスト成功！${RESET}"
  exit 0
else
  echo -e "\n${RED}${BOLD}${FAIL}件のテストが失敗しました。${RESET}"
  exit 1
fi
