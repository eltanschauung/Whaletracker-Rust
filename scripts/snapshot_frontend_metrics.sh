#!/usr/bin/env bash
set -euo pipefail

repo_dir="${WT_RUST_REPO:-$HOME/Whaletracker-Rust}"
env_file="${WT_FRONTEND_SNAPSHOT_ENV:-$HOME/.config/whaletracker-rust/snapshot.env}"
binary="${WT_FRONTEND_SNAPSHOT_BIN:-$repo_dir/target/release/frontend_metric_snapshot}"

if [[ -f "$env_file" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "$env_file"
  set +a
fi

cd "$repo_dir"

if [[ ! -x "$binary" ]]; then
  cargo build --release --bin frontend_metric_snapshot >/dev/null
fi

exec "$binary"
