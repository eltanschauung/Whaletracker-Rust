use mysql::{params, prelude::Queryable, OptsBuilder, Pool, PooledConn};
use std::env;

#[derive(Clone)]
struct Config {
    db_host: String,
    db_port: u16,
    db_name: String,
    db_user: String,
    db_pass: String,
    snapshot_table: String,
    snapshot_source: String,
}

struct MetricDefinition {
    metric_group: &'static str,
    metric_name: &'static str,
    window_kind: &'static str,
    window_start_sql: &'static str,
    window_end_sql: &'static str,
    value_sql: &'static str,
}

struct MetricSnapshot {
    metric_group: &'static str,
    metric_name: &'static str,
    window_kind: &'static str,
    sampled_at: i64,
    snapshot_key: String,
    window_start: i64,
    window_end: i64,
    value: i64,
}

fn main() {
    let config = Config::from_env();

    if !is_safe_identifier(&config.snapshot_table) {
        eprintln!(
            "unsafe PLUGIN_STATS_SNAPSHOT_TABLE={} (use letters, numbers, or underscores)",
            config.snapshot_table
        );
        std::process::exit(2);
    }

    let pool = create_pool(&config).unwrap_or_else(|err| {
        eprintln!("failed to create MySQL pool: {err}");
        std::process::exit(1);
    });

    let mut conn = pool.get_conn().unwrap_or_else(|err| {
        eprintln!("failed to connect to MySQL: {err}");
        std::process::exit(1);
    });

    ensure_schema(&mut conn, &config).unwrap_or_else(|err| {
        eprintln!("failed to ensure snapshot schema: {err}");
        std::process::exit(1);
    });

    let mut written = 0;
    for metric in metric_definitions() {
        let snapshot = fetch_metric_snapshot(&mut conn, &metric).unwrap_or_else(|err| {
            eprintln!(
                "failed to calculate {}.{}: {err}",
                metric.metric_group, metric.metric_name
            );
            std::process::exit(1);
        });

        write_snapshot(&mut conn, &config, &snapshot).unwrap_or_else(|err| {
            eprintln!(
                "failed to write {}.{}: {err}",
                metric.metric_group, metric.metric_name
            );
            std::process::exit(1);
        });
        written += 1;
    }

    println!(
        "wrote {written} frontend metric snapshot(s) to {}",
        config.snapshot_table
    );
}

impl Config {
    fn from_env() -> Self {
        Self {
            db_host: env::var("WT_DB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
            db_port: env_u16("WT_DB_PORT", 3306),
            db_name: env::var("WT_DB_NAME").unwrap_or_else(|_| "sourcemod".to_string()),
            db_user: env::var("WT_DB_USER").unwrap_or_else(|_| "root".to_string()),
            db_pass: env::var("WT_DB_PASS").unwrap_or_default(),
            snapshot_table: env::var("WT_FRONTEND_SNAPSHOT_TABLE")
                .unwrap_or_else(|_| "frontend_statistics_metric_snapshots".to_string()),
            snapshot_source: env::var("WT_FRONTEND_SNAPSHOT_SOURCE")
                .unwrap_or_else(|_| "frontend_stats_summary".to_string()),
        }
    }
}

fn metric_definitions() -> Vec<MetricDefinition> {
    vec![
        MetricDefinition {
            metric_group: "stats_summary",
            metric_name: "registered_players_total",
            window_kind: "all_time",
            window_start_sql: "0",
            window_end_sql: "UNIX_TIMESTAMP(NOW())",
            value_sql: "SELECT COUNT(*) FROM whaletracker",
        },
        MetricDefinition {
            metric_group: "stats_summary",
            metric_name: "active_players_month",
            window_kind: "month_to_date",
            window_start_sql: "UNIX_TIMESTAMP(DATE_FORMAT(NOW(), '%Y-%m-01 00:00:00'))",
            window_end_sql: "UNIX_TIMESTAMP(DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 1 MONTH), '%Y-%m-01 00:00:00'))",
            value_sql: "SELECT COUNT(DISTINCT lp.steamid) \
                        FROM whaletracker_log_players lp \
                        INNER JOIN whaletracker_logs l ON l.log_id = lp.log_id \
                        WHERE l.started_at >= UNIX_TIMESTAMP(DATE_FORMAT(NOW(), '%Y-%m-01 00:00:00')) \
                          AND l.started_at < UNIX_TIMESTAMP(DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 1 MONTH), '%Y-%m-01 00:00:00'))",
        },
        MetricDefinition {
            metric_group: "stats_summary",
            metric_name: "active_players_week",
            window_kind: "rolling_7_days",
            window_start_sql: "UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 7 DAY))",
            window_end_sql: "UNIX_TIMESTAMP(NOW())",
            value_sql: "SELECT COUNT(DISTINCT lp.steamid) \
                        FROM whaletracker_log_players lp \
                        INNER JOIN whaletracker_logs l ON l.log_id = lp.log_id \
                        WHERE l.started_at >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 7 DAY)) \
                          AND l.started_at < UNIX_TIMESTAMP(NOW())",
        },
    ]
}

fn create_pool(config: &Config) -> mysql::Result<Pool> {
    let builder = OptsBuilder::new()
        .ip_or_hostname(Some(config.db_host.clone()))
        .tcp_port(config.db_port)
        .db_name(Some(config.db_name.clone()))
        .user(Some(config.db_user.clone()))
        .pass(Some(config.db_pass.clone()));

    Pool::new(builder)
}

fn ensure_schema(conn: &mut PooledConn, config: &Config) -> mysql::Result<()> {
    let table = quote_identifier(&config.snapshot_table);
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {table} (\
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,\
            sampled_at INT NOT NULL,\
            snapshot_key VARCHAR(64) NOT NULL,\
            metric_group VARCHAR(64) NOT NULL,\
            metric_name VARCHAR(64) NOT NULL,\
            window_kind VARCHAR(32) NOT NULL,\
            window_start INT NOT NULL,\
            window_end INT NOT NULL,\
            value BIGINT NOT NULL,\
            source VARCHAR(64) NOT NULL,\
            created_at INT NOT NULL,\
            PRIMARY KEY (id),\
            UNIQUE KEY uniq_snapshot_metric (snapshot_key, metric_group, metric_name, window_kind, source),\
            KEY idx_sampled_at (sampled_at),\
            KEY idx_metric_sampled (metric_group, metric_name, sampled_at),\
            KEY idx_window (window_kind, window_start, window_end)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
    );

    conn.query_drop(sql)
}

fn fetch_metric_snapshot(
    conn: &mut PooledConn,
    metric: &MetricDefinition,
) -> mysql::Result<MetricSnapshot> {
    let sql = format!(
        "SELECT \
            UNIX_TIMESTAMP(NOW()) AS sampled_at,\
            DATE_FORMAT(NOW(), '%Y-%m-%d') AS snapshot_key,\
            ({}) AS window_start,\
            ({}) AS window_end,\
            ({}) AS value",
        metric.window_start_sql, metric.window_end_sql, metric.value_sql
    );

    conn.query_first::<(i64, String, i64, i64, i64), _>(sql)
        .map(|row| {
            let (sampled_at, snapshot_key, window_start, window_end, value) =
                row.unwrap_or_else(|| (0, String::new(), 0, 0, 0));

            MetricSnapshot {
                metric_group: metric.metric_group,
                metric_name: metric.metric_name,
                window_kind: metric.window_kind,
                sampled_at,
                snapshot_key,
                window_start,
                window_end,
                value,
            }
        })
}

fn write_snapshot(
    conn: &mut PooledConn,
    config: &Config,
    snapshot: &MetricSnapshot,
) -> mysql::Result<()> {
    let table = quote_identifier(&config.snapshot_table);
    let sql = format!(
        "INSERT INTO {table} \
        (sampled_at, snapshot_key, metric_group, metric_name, window_kind, window_start, window_end, value, source, created_at) \
        VALUES \
        (:sampled_at, :snapshot_key, :metric_group, :metric_name, :window_kind, :window_start, :window_end, :value, :source, UNIX_TIMESTAMP(NOW())) \
        ON DUPLICATE KEY UPDATE \
            sampled_at = VALUES(sampled_at),\
            window_start = VALUES(window_start),\
            window_end = VALUES(window_end),\
            value = VALUES(value),\
            created_at = VALUES(created_at)"
    );

    conn.exec_drop(
        sql,
        params! {
            "sampled_at" => snapshot.sampled_at,
            "snapshot_key" => snapshot.snapshot_key.as_str(),
            "metric_group" => snapshot.metric_group,
            "metric_name" => snapshot.metric_name,
            "window_kind" => snapshot.window_kind,
            "window_start" => snapshot.window_start,
            "window_end" => snapshot.window_end,
            "value" => snapshot.value,
            "source" => config.snapshot_source.as_str(),
        },
    )
}

fn env_u16(name: &str, default_value: u16) -> u16 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(default_value)
}

fn is_safe_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
}

fn quote_identifier(value: &str) -> String {
    format!("`{value}`")
}
