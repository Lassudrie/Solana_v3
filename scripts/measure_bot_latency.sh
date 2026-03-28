#!/usr/bin/env bash
set -euo pipefail

endpoint="${1:-http://127.0.0.1:8081}"
report="${2:-/tmp/bot-latency.txt}"
warmup_seconds="${BOT_LATENCY_WARMUP_SECONDS:-30}"
overview_tmp="$(mktemp /tmp/bot-overview.XXXXXX.json)"

cleanup() {
    rm -f "$overview_tmp"
}

trap cleanup EXIT

log() {
    printf '[%s] %s\n' "$(date -Is)" "$*" | tee -a "$report"
}

rm -f "$report"
touch "$report"

log "waiting for bot monitor on ${endpoint}"
until curl -fsS --max-time 2 "${endpoint}/monitor/overview" >"$overview_tmp" 2>/dev/null; do
    log "monitor unavailable"
    sleep 5
done

log "monitor reachable, waiting for ready=true"
until python3 - "$overview_tmp" <<'PY'
import json
import sys

with open(sys.argv[1]) as f:
    data = json.load(f)

raise SystemExit(0 if data.get("ready") else 1)
PY
do
    curl -fsS --max-time 2 "${endpoint}/monitor/overview" >"$overview_tmp" 2>/dev/null || true
    log "bot not ready yet"
    sleep 5
done

log "bot ready, collecting ${warmup_seconds}s of warm samples"
sleep "$warmup_seconds"

{
    printf '=== botctl status (%s) ===\n' "$(date -Is)"
    /usr/local/bin/botctl --endpoint "$endpoint" status
    printf '\n=== botctl signals 1m (%s) ===\n' "$(date -Is)"
    /usr/local/bin/botctl --endpoint "$endpoint" signals --window 1m --limit 20
} | tee -a "$report"

log "wrote ${report}"
exec tail -n +1 -f "$report"
