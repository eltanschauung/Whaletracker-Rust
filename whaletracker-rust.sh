#!/usr/bin/env bash
set -euo pipefail

# Stop any existing Rust SQL sink instances started from this project.
pkill -u "$USER" -f 'target/debug/whaletracker-rust' 2>/dev/null || true
sleep 0.5

cd ~/yourdirectoryhere/Whaletracker-Rust/

LOG_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/whaletracker-rust"
mkdir -p "$LOG_DIR"
chmod 700 "$LOG_DIR"

BUILD_LOG="$LOG_DIR/build.log"
LOG1="$LOG_DIR/28017.log"
LOG2="$LOG_DIR/28018.log"

export WT_DB_DRIVER=mysql
export WT_DB_HOST=127.0.0.1
export WT_DB_PORT=3306
export WT_DB_NAME=sourcemod
export WT_DB_USER=database_user_here
export WT_DB_PASS='passwordhere'

export WT_RUST_FLUSH_MS=100
export WT_RUST_MAX_BATCH_ROWS=256

cargo build >"$BUILD_LOG" 2>&1

WT_RUST_BIND=127.0.0.1:28017 ./target/debug/whaletracker-rust >"$LOG1" 2>&1 &
PID1=$!
WT_RUST_BIND=127.0.0.1:28018 ./target/debug/whaletracker-rust >"$LOG2" 2>&1 &
PID2=$!

echo "Started whaletracker-rust instances: $PID1 (28017), $PID2 (28018)"
echo "Logs: $BUILD_LOG $LOG1 $LOG2"
