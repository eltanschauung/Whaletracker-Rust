# whaletracker-rust

Rust SQL outlet backend + SourceMod build of WhaleTracker that defers most runtime SQL writes to Rust over a persistent TCP socket. SourceMod owns gameplay/stat event generation; Rust owns queued SQL write execution.

## sourcepawn cvars (rust outlet)

- `sm_whaletracker_rust_sql_outlet` (`1` = enabled, `0` = disable and keep local writes)
- `sm_whaletracker_rust_host` (default `127.0.0.1`)
- `sm_whaletracker_rust_port` (default `28017`)
- `sm_whaletracker_rust_queue_max` (default `4096`)
- `sm_whaletracker_rust_batch_max` (default `64`)
- `sm_whaletracker_rust_server_id` (optional)
- `sm_whaletracker_rust_auth_token` (optional shared secret, protected cvar)
- `sm_whaletracker_rust_sql_debug` (default `0`)

## protocol

Transport is persistent TCP with NDJSON framing: one JSON object per line.

SourceMod sends `hello`, `sql_batch`, and `health`. Rust sends `hello_ack`, `ack`, `error`, and `health`.

`ack` is sent after accepted writes have completed worker execution and successful event ids have been marked done in the pending journal.

Example `sql_batch` payload:

```json
{"type":"sql_batch","batch_id":12,"sent_at":1700000000,"writes":[{"sql":"INSERT INTO whaletracker ...","user_id":34,"event_id":"kogasa.tf:27015:1700000000:123"}]}
```

When `WT_RUST_AUTH_TOKEN` is non-empty, SourceMod must send the same value through `sm_whaletracker_rust_auth_token` in `hello` before `sql_batch` or `health` messages are accepted.

## rust environment knobs

- `WT_RUST_BIND` (default `127.0.0.1:28017`)
- `WT_RUST_FLUSH_MS` (default `100`)
- `WT_RUST_MAX_BATCH_ROWS` (default `256`)
- `WT_RUST_MAX_QUEUE_ROWS` (default `8192`)
- `WT_RUST_MAX_FRAME_BYTES` (default `32768`)
- `WT_RUST_MAX_INBOUND_WRITES` (default `256`)
- `WT_RUST_MAX_SQL_BYTES` (default `8192`)
- `WT_RUST_DEDUPE_EVENTS` (default `65536`)
- `WT_RUST_AUTH_TOKEN` (optional)
- `WT_RUST_PENDING_JOURNAL_PATH` (default `sql_pending_journal.log`, empty disables startup replay)
- `WT_RUST_DEAD_LETTER_PATH` (default `sql_dead_letters.log`, empty disables)
