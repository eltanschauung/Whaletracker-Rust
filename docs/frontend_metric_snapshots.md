# Frontend Metric Snapshots

`frontend_metric_snapshot` records daily point-in-time values for frontend summary metrics into the statistics database.

## Current Metrics

The default metric set mirrors the `/stats` summary cards:

- `stats_summary.registered_players_total`
  - Window: `all_time`
  - Source query: `COUNT(*) FROM whaletracker`
- `stats_summary.active_players_month`
  - Window: `month_to_date`
  - Source query: distinct players in `whaletracker_log_players` joined to `whaletracker_logs` from the current month.
- `stats_summary.active_players_week`
  - Window: `rolling_7_days`
  - Source query: distinct players in `whaletracker_log_players` joined to `whaletracker_logs` from the last 7 days.

Rows are written to `frontend_statistics_metric_snapshots` by default. The writer uses a daily `snapshot_key`, so rerunning the job on the same day updates that day's values instead of creating duplicate rows.

## Configuration

The binary uses the same `WT_DB_*` environment convention as WhaleTracker Rust:

- `WT_DB_HOST`
- `WT_DB_PORT`
- `WT_DB_NAME`
- `WT_DB_USER`
- `WT_DB_PASS`

Optional snapshot-specific overrides:

- `WT_FRONTEND_SNAPSHOT_TABLE`, default `frontend_statistics_metric_snapshots`
- `WT_FRONTEND_SNAPSHOT_SOURCE`, default `frontend_stats_summary`

The wrapper script `scripts/snapshot_frontend_metrics.sh` loads:

```text
$HOME/.config/whaletracker-rust/snapshot.env
```

This keeps credentials out of crontab and out of git.

## Cron

The installed cron job runs once daily at 6 AM, matching the server restart window:

```cron
0 6 * * * /usr/bin/flock -n /tmp/frontend_metric_snapshot.lock /home/<USER>/Whaletracker-Rust/scripts/snapshot_frontend_metrics.sh >> /home/<USER>/.local/state/whaletracker-rust/frontend_metric_snapshot.log 2>&1
```

To add a similar job for another metric family, use the same pattern:

- Put the calculation in a small binary or script.
- Load secrets from an untracked env file.
- Write to a narrow append/update snapshot table with `sampled_at`, `snapshot_key`, metric identity, window bounds, and value.
- Redirect stdout/stderr into a repo-local ignored log file.
