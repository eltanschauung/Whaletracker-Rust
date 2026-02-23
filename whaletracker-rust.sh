#!/usr/bin/env bash
set -euo pipefail

# Stop any existing Rust SQL sink instances started from this project.
pkill -f '/yourdirectory/youruser/Whaletracker-Rust/target/debug/whaletracker-rust' 2>/dev/null || true
sleep 0.5

cd ~/yourdirectoryhere/Whaletracker-Rust/

export WT_DB_DRIVER=mysql
export WT_DB_HOST=127.0.0.1
export WT_DB_PORT=3306
export WT_DB_NAME=sourcemod
export WT_DB_USER=database_user_here
export WT_DB_PASS='passwordhere'

export WT_RUST_FLUSH_MS=100
export WT_RUST_MAX_BATCH_ROWS=256

cargo build >/tmp/whaletracker-rust-build.log 2>&1

WT_RUST_BIND=127.0.0.1:28017 ./target/debug/whaletracker-rust >/tmp/whaletracker-rust-28017.log 2>&1 &
PID1=$!
WT_RUST_BIND=127.0.0.1:28018 ./target/debug/whaletracker-rust >/tmp/whaletracker-rust-28018.log 2>&1 &
PID2=$!

echo "Started whaletracker-rust instances: $PID1 (28017), $PID2 (28018)"
echo "Logs: /tmp/whaletracker-rust-build.log /tmp/whaletracker-rust-28017.log /tmp/whaletracker-rust-28018.log"

