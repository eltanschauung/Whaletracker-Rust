#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BIN="$ROOT_DIR/target/debug/whaletracker-rust"
LOG_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/whaletracker-rust"
PKILL_PATTERN='target/debug/whaletracker-rust'

mkdir -p "$LOG_DIR"
chmod 700 "$LOG_DIR"

if [[ ! -x "$BIN" ]]; then
  echo "missing executable: $BIN" >&2
  exit 1
fi

# Database settings come from the service environment. Keep each listener's
# durable state isolated so their independent processes never share a journal.
pkill -u "$USER" -f "$PKILL_PATTERN" 2>/dev/null || true
sleep 0.5

pids=()

cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done

  for pid in "${pids[@]:-}"; do
    wait "$pid" 2>/dev/null || true
  done
}

trap cleanup EXIT INT TERM

start_listener() {
  local port="$1"

  env WT_RUST_BIND="127.0.0.1:$port" \
    WT_RUST_PENDING_JOURNAL_PATH="$LOG_DIR/${port}_pending_journal.log" \
    WT_RUST_DEAD_LETTER_PATH="$LOG_DIR/${port}_dead_letters.log" \
    "$BIN" >>"$LOG_DIR/${port}.log" 2>&1 &
  pids+=("$!")
}

start_listener 28017
start_listener 28018

wait -n "${pids[@]}"
exit $?
