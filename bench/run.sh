#!/usr/bin/env bash
# Benchmark harness for PAGI::Server HTTP/1.1 serving.
#
# Starts bin/pagi-server (single worker, access log off) with
# bench/apps/bench-app.pl and drives it with hey, printing the
# requests/sec for each run plus the median.
#
# Usage (from the repo root, with the right perl on PATH):
#   bench/run.sh [scenario ...]      # default: all scenarios
#
# Scenarios: hello echo big nokeepalive
#
# Environment overrides:
#   PORT=5199 DURATION=10s CONNS=32 RUNS=3 SERVER_ARGS='...' bench/run.sh
#
# Results are written to stdout as TSV: scenario, runs..., median.

set -euo pipefail

PORT="${PORT:-5199}"
DURATION="${DURATION:-10s}"
CONNS="${CONNS:-32}"
RUNS="${RUNS:-3}"
SERVER_ARGS="${SERVER_ARGS:-}"
BASE="http://127.0.0.1:${PORT}"

cd "$(dirname "$0")/.."

command -v hey >/dev/null || { echo "hey(1) not found on PATH" >&2; exit 1; }

SCENARIOS=("$@")
[ ${#SCENARIOS[@]} -eq 0 ] && SCENARIOS=(hello echo big nokeepalive)

TMPDIR_BENCH="$(mktemp -d)"
trap 'cleanup' EXIT
SERVER_PID=

cleanup() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -TERM "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$TMPDIR_BENCH"
}

# 1KB request body for the echo scenario
head -c 1024 /dev/zero | tr '\0' 'a' > "$TMPDIR_BENCH/body-1k.txt"

echo "# starting pagi-server (workers=1, no access log) on :$PORT" >&2
# shellcheck disable=SC2086
perl -Ilib bin/pagi-server --workers 1 --port "$PORT" --no-access-log \
    $SERVER_ARGS bench/apps/bench-app.pl > "$TMPDIR_BENCH/server.log" 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 100); do
    if curl -sf -o /dev/null "$BASE/hello"; then
        break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "server died during startup:" >&2
        cat "$TMPDIR_BENCH/server.log" >&2
        exit 1
    fi
    sleep 0.1
done
curl -sf -o /dev/null "$BASE/hello" || { echo "server never became ready" >&2; exit 1; }

hey_rps() {
    hey "$@" | awk '/Requests\/sec/ {print $2}'
}

run_scenario() {
    local name="$1"; shift
    local -a args=("$@")

    # warmup (not recorded)
    hey -z 2s -c "$CONNS" "${args[@]}" > /dev/null

    local -a results=()
    for _ in $(seq 1 "$RUNS"); do
        results+=("$(hey_rps -z "$DURATION" -c "$CONNS" "${args[@]}")")
    done

    local median
    median=$(printf '%s\n' "${results[@]}" | sort -n | awk '{a[NR]=$1} END {print (NR%2) ? a[(NR+1)/2] : (a[NR/2]+a[NR/2+1])/2}')
    printf '%s\t%s\tmedian=%s\n' "$name" "$(IFS=,; echo "${results[*]}")" "$median"
}

for s in "${SCENARIOS[@]}"; do
    case "$s" in
        hello)       run_scenario hello "$BASE/hello" ;;
        echo)        run_scenario echo -m POST -D "$TMPDIR_BENCH/body-1k.txt" "$BASE/echo" ;;
        big)         run_scenario big "$BASE/big" ;;
        nokeepalive) run_scenario nokeepalive --disable-keepalive "$BASE/hello" ;;
        *) echo "unknown scenario: $s" >&2; exit 1 ;;
    esac
done
