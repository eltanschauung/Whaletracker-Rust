# whaletracker-rust

Rust SQL outlet backend + SourceMod build of WhaleTracker that defers most runtime SQL writes to Rust over a persistent TCP socket. This provides the benefit of multithreading.

## sourcepawn cvars (rust outlet)

- `sm_whaletracker_rust_sql_outlet` (`1` = enabled, `0` = disable and keep local writes)
- `sm_whaletracker_rust_host` (default `127.0.0.1`)
- `sm_whaletracker_rust_port` (default `28017`)
- `sm_whaletracker_rust_queue_max` (default `2048`)
- `sm_whaletracker_rust_batch_max` (default `32`)
- `sm_whaletracker_rust_server_id` (optional)
