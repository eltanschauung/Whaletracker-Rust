# whaletracker-rust

<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/df33e1ec-e877-41cc-ab87-910527647951" />

Rust SQL outlet backend + SourceMod build of WhaleTracker that defers most runtime SQL writes to Rust over a persistent TCP socket. This provides the benefit of multithreading.

# TODO

Create a 64bit branch of https://github.com/JoinedSenses/sm-ext-socket for 64bit Source game support.
See the original Whaletracker here: https://github.com/babasproke2/Whaletracker

## what this is

`scripting/whaletracker_rust.sp` is the current compilepoint, it keeps WhaleTracker gameplay/stat logic in SourcePawn, it intercepts `QueueSaveQuery(...)` and sends SQL writes to the Rust backend (`sql_batch` over NDJSON), and read/query commands like `sm_points`, `sm_rank`, and `sm_ranks` still use SourceMod's database directly.

This is a hybrid right now:
- SourcePawn still owns game event handling and SQL query generation
- Rust is the write executor/outlet for multithreading

## requirements

- `sm-ext-socket` installed and loaded in SourceMod
- Rust backend running on localhost TCP (default `127.0.0.1:28017`)
- Rust backend and env variables configured to use the same db as WhaleTracker

## protocol (current)

transport:
- persistent TCP
- NDJSON framing (`one JSON object per line`)

SourceMod -> Rust:
- `hello`
- `sql_batch`
- `health`

Rust -> SourceMod:
- `hello_ack`
- `ack`
- `error`
- `health`

Example `sql_batch` payload:

```json
{"type":"sql_batch","batch_id":12,"sent_at":1700000000,"writes":[{"sql":"INSERT INTO whaletracker ...","user_id":34}]}
```

## sourcepawn cvars (rust outlet)

- `sm_whaletracker_rust_sql_outlet` (`1` = enabled, `0` = disable and keep local writes)
- `sm_whaletracker_rust_host` (default `127.0.0.1`)
- `sm_whaletracker_rust_port` (default `28017`)
- `sm_whaletracker_rust_queue_max` (default `2048`)
- `sm_whaletracker_rust_batch_max` (default `32`)
- `sm_whaletracker_rust_server_id` (optional)
