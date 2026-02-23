# Rust Whaletracker (Structure Notes)

## Goal
- Keep gameplay/event/stat accumulation logic in SourcePawn.
- Keep SQL query construction in SourcePawn (same semantics as current `whaletracker.sp`).
- Move only DB execution to Rust (async queue + worker + retries/backpressure).

## Boundary (Updated)
- `SourcePawn`:
  - hooks TF2/TF2C events
  - updates `g_Stats` / `g_MapStats`
  - builds the exact SQL writes (`INSERT/REPLACE/UPDATE/DELETE`) it already uses
  - sends SQL write batches to Rust over persistent TCP
- `Rust daemon`:
  - accepts SQL batches
  - validates message shape (not gameplay semantics)
  - queues SQL writes
  - executes writes against DB
  - returns ACK / error / health responses

## Why This Is Better For This Port
- Lowest migration risk: existing whaletracker SQL behavior stays authoritative.
- Easy parity: SourcePawn still produces the same SQL strings.
- Rust only solves what SourcePawn is bad at: threaded IO, queueing, batching, retries.
- You can replace `QueueSaveQuery()` with a socket enqueue path without rewriting stat logic first.

## Transport (Chosen)
- `sm-ext-socket` persistent TCP client in SourcePawn
- Rust TCP listener on `127.0.0.1:<port>`
- `NDJSON` framing (`json + \n`)

## Protocol (SQL Outlet)
- SourcePawn -> Rust:
  - `hello`
  - `sql_batch`
  - `health` (optional request)
- Rust -> SourcePawn:
  - `hello_ack`
  - `ack` (batch accepted/enqueued)
  - `error`
  - `health`

### `sql_batch` shape (example)
```json
{"type":"sql_batch","batch_id":12,"sent_at":1700000000,"writes":[{"sql":"REPLACE INTO ...","user_id":0},{"sql":"DELETE FROM ...","user_id":52}]}
```

## Data Flow (Updated)
1. SourcePawn event happens
2. SourcePawn updates stats state
3. SourcePawn builds SQL write string (same as current plugin)
4. SourcePawn queues SQL write in socket bridge queue
5. SourcePawn flushes `sql_batch` over TCP
6. Rust enqueues SQL writes and sends ACK
7. Rust DB worker executes queued writes asynchronously

## SourcePawn Integration Plan (Main Plugin)
- Replace / wrap current DB write outlet functions:
  - `QueueSaveQuery(...)`
  - `PumpSaveQueue()` (or bypass with Rust batch queue)
  - `RunSaveQuerySync(...)` (fallback path if Rust disabled)
- Keep current SQL builders unchanged:
  - `InsertPlayerLogRecord(...)`
  - `InsertMatchLogRecord(...)`
  - `QueueStatsSave(...)`
  - points cache updates
  - schema create/alter statements (initially can stay local or move to Rust)

## Rust Daemon Responsibilities (Strict)
- Do not interpret gameplay events.
- Do not reconstruct stat logic.
- Treat incoming SQL as opaque write operations.
- Enforce queueing, batching, execution order, and error reporting.

## Queue / Reliability Rules
- ACK means "accepted into Rust queue" (not necessarily committed yet).
- SourcePawn can use one-batch-in-flight for simplicity.
- If Rust disconnects before ACK:
  - simple mode: possible loss (acceptable during bring-up)
  - stronger mode: keep/resend in-flight batch using `batch_id`
- Rust worker should log DB errors with `batch_id` + `user_id` + query preview.

## Batch Strategy
- Batch SQL writes at the transport layer (SourcePawn -> Rust), not by trying to merge SQL semantics.
- Rust may execute sequentially or in DB transactions depending on backend and risk tolerance.
- Keep write ordering stable within a batch and across batches.

## Migration Plan (Practical)
1. Keep existing whaletracker DB writes working (baseline)
2. Add Rust SQL outlet bridge in parallel (test-only SQL writes)
3. Redirect low-risk writes first (`whaletracker_online`, server heartbeats)
4. Redirect log/player upserts
5. Redirect main stats upserts and points cache writes
6. Move schema create/alter to Rust (optional; parity-first)
7. Remove direct SourceMod DB write path after verification

## Querys/Reads (Later)
- Reads can remain SourcePawn->DB directly at first.
- Later, route read queries to Rust for consistency/caching if desired.
- This is optional for the first migration milestone.

## Current Repo Direction
- `scripting/whaletracker_rust.sp` now acts as a SQL outlet bridge (persistent TCP + `sql_batch` + ACK handling).
- `src/main.rs` now acts as a Rust SQL sink daemon skeleton (TCP NDJSON + queue + DB worker mock).
