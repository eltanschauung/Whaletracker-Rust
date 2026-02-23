use mysql::prelude::Queryable;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_BIND: &str = "127.0.0.1:28017";
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 100;
const DEFAULT_MAX_BATCH_ROWS: usize = 256;
const DEFAULT_DB_HOST: &str = "127.0.0.1";
const DEFAULT_DB_PORT: u16 = 3306;
const DEFAULT_DB_NAME: &str = "appdb";
const DEFAULT_DB_USER: &str = "dbuser";
const DEFAULT_DB_DRIVER: &str = "mysql";

trait DbExecutor: Send {
    fn execute(&mut self, sql: &str) -> Result<(), String>;
}

struct MysqlDbExecutor {
    pool: mysql::Pool,
}

impl MysqlDbExecutor {
    fn connect(cfg: &DbConfig) -> Result<Self, String> {
        if !cfg.driver.eq_ignore_ascii_case("mysql") {
            return Err(format!(
                "unsupported WT_DB_DRIVER '{}' (only 'mysql' is supported)",
                cfg.driver
            ));
        }

        let builder = mysql::OptsBuilder::new()
            .ip_or_hostname(Some(cfg.host.clone()))
            .tcp_port(cfg.port)
            .db_name(Some(cfg.database.clone()))
            .user(Some(cfg.user.clone()))
            .pass(Some(cfg.pass.clone()));

        let pool = mysql::Pool::new(builder).map_err(|e| e.to_string())?;
        Ok(Self { pool })
    }
}

impl DbExecutor for MysqlDbExecutor {
    fn execute(&mut self, sql: &str) -> Result<(), String> {
        let mut conn = self.pool.get_conn().map_err(|e| e.to_string())?;
        conn.query_drop(sql).map_err(|e| e.to_string())
    }
}

#[derive(Debug, Clone)]
struct QueuedSqlWrite {
    sql: String,
    user_id: Option<u32>,
    source_batch_id: Option<i64>,
    enqueued_at_ms: u128,
}

#[derive(Debug, Clone)]
struct SinkConfig {
    flush_interval: Duration,
    max_batch_rows: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlLane {
    Online,
    Stats,
    Logs,
}

impl SqlLane {
    const ALL: [SqlLane; 3] = [SqlLane::Online, SqlLane::Stats, SqlLane::Logs];

    fn label(self) -> &'static str {
        match self {
            SqlLane::Online => "online",
            SqlLane::Stats => "stats",
            SqlLane::Logs => "logs",
        }
    }
}

#[derive(Debug, Clone)]
struct DbConfig {
    driver: String,
    host: String,
    port: u16,
    database: String,
    user: String,
    pass: String,
}

impl DbConfig {
    fn from_env() -> Self {
        let driver = std::env::var("WT_DB_DRIVER").unwrap_or_else(|_| DEFAULT_DB_DRIVER.to_string());
        let host = std::env::var("WT_DB_HOST").unwrap_or_else(|_| DEFAULT_DB_HOST.to_string());
        let port = std::env::var("WT_DB_PORT")
            .ok()
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(DEFAULT_DB_PORT);
        let database = std::env::var("WT_DB_NAME").unwrap_or_else(|_| DEFAULT_DB_NAME.to_string());
        let user = std::env::var("WT_DB_USER").unwrap_or_else(|_| DEFAULT_DB_USER.to_string());
        let pass = std::env::var("WT_DB_PASS").unwrap_or_default();

        Self {
            driver,
            host,
            port,
            database,
            user,
            pass,
        }
    }
}

#[derive(Default)]
struct SinkStats {
    accepted_writes: AtomicU64,
    executed_writes: AtomicU64,
    db_errors: AtomicU64,
    parse_errors: AtomicU64,
    dropped_writes: AtomicU64,
}

#[derive(Default)]
struct LaneStats {
    accepted_writes: AtomicU64,
    executed_writes: AtomicU64,
    db_errors: AtomicU64,
}

struct LaneWorker {
    lane: SqlLane,
    cfg: SinkConfig,
    queue: Mutex<VecDeque<QueuedSqlWrite>>,
    notify: Condvar,
    db: Mutex<Box<dyn DbExecutor>>,
    stats: LaneStats,
}

impl LaneWorker {
    fn new(lane: SqlLane, db: Box<dyn DbExecutor>, cfg: SinkConfig) -> Self {
        Self {
            lane,
            cfg,
            queue: Mutex::new(VecDeque::new()),
            notify: Condvar::new(),
            db: Mutex::new(db),
            stats: LaneStats::default(),
        }
    }

    fn queue_depth(&self) -> usize {
        self.queue.lock().expect("lane queue mutex poisoned").len()
    }

    fn worker_loop(self: Arc<Self>, global_stats: Arc<SinkStats>) {
        loop {
            let mut batch = Vec::with_capacity(self.cfg.max_batch_rows);

            {
                let mut q = self.queue.lock().expect("lane queue mutex poisoned");
                if q.is_empty() {
                    let (guard, _timeout) = self
                        .notify
                        .wait_timeout(q, self.cfg.flush_interval)
                        .expect("condvar wait failed");
                    q = guard;
                }

                while batch.len() < self.cfg.max_batch_rows {
                    let Some(item) = q.pop_front() else {
                        break;
                    };
                    batch.push(item);
                }
            }

            if batch.is_empty() {
                continue;
            }

            for item in batch {
                let kind = classify_sql(&item.sql);
                println!(
                    "[sql-sink:{}] exec start kind={} user_id={:?} batch_id={:?} age_ms={} sql={}",
                    self.lane.label(),
                    kind,
                    item.user_id,
                    item.source_batch_id,
                    now_ms().saturating_sub(item.enqueued_at_ms),
                    preview_sql(&item.sql, 220)
                );
                let result = {
                    let mut db = self.db.lock().expect("db mutex poisoned");
                    db.execute(&item.sql)
                };

                match result {
                    Ok(()) => {
                        self.stats.executed_writes.fetch_add(1, Ordering::Relaxed);
                        global_stats.executed_writes.fetch_add(1, Ordering::Relaxed);
                        println!(
                            "[sql-sink:{}] exec ok kind={} batch_id={:?}",
                            self.lane.label(),
                            kind,
                            item.source_batch_id
                        );
                    }
                    Err(err) => {
                        self.stats.db_errors.fetch_add(1, Ordering::Relaxed);
                        global_stats.db_errors.fetch_add(1, Ordering::Relaxed);
                        eprintln!(
                            "[sql-sink:{}] db error (user_id={:?}, batch_id={:?}, age_ms={}): {} | sql={}",
                            self.lane.label(),
                            item.user_id,
                            item.source_batch_id,
                            now_ms().saturating_sub(item.enqueued_at_ms),
                            err,
                            preview_sql(&item.sql, 256)
                        );
                    }
                }
            }
        }
    }
}

struct SqlSink {
    lanes: Vec<Arc<LaneWorker>>,
    stats: Arc<SinkStats>,
}

impl SqlSink {
    fn new(db_cfg: &DbConfig, cfg: SinkConfig) -> Result<Self, String> {
        let mut lanes = Vec::with_capacity(SqlLane::ALL.len());
        for lane in SqlLane::ALL {
            let db = MysqlDbExecutor::connect(db_cfg)?;
            lanes.push(Arc::new(LaneWorker::new(lane, Box::new(db), cfg.clone())));
        }
        Ok(Self {
            lanes,
            stats: Arc::new(SinkStats::default()),
        })
    }

    fn spawn_workers(self: &Arc<Self>) {
        for lane in &self.lanes {
            let lane = Arc::clone(lane);
            let global_stats = Arc::clone(&self.stats);
            thread::spawn(move || lane.worker_loop(global_stats));
        }
    }

    fn lane_for_sql(sql: &str) -> SqlLane {
        match classify_sql(sql) {
            "whaletracker_online" | "whaletracker_servers" => SqlLane::Online,
            "whaletracker_main" | "whaletracker_points_cache" => SqlLane::Stats,
            _ => SqlLane::Logs,
        }
    }

    fn lane_by_kind(&self, lane: SqlLane) -> &Arc<LaneWorker> {
        match lane {
            SqlLane::Online => &self.lanes[0],
            SqlLane::Stats => &self.lanes[1],
            SqlLane::Logs => &self.lanes[2],
        }
    }

    fn enqueue_batch(&self, batch_id: Option<i64>, writes: Vec<SqlWrite>) -> usize {
        let now_ms = now_ms();
        let mut accepted = 0usize;
        let mut touched_online = false;
        let mut touched_stats = false;
        let mut touched_logs = false;

        for write in writes {
            if write.sql.trim().is_empty() {
                self.stats.dropped_writes.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            let lane_kind = Self::lane_for_sql(&write.sql);
            let lane = self.lane_by_kind(lane_kind);
            let mut q = lane.queue.lock().expect("lane queue mutex poisoned");
            q.push_back(QueuedSqlWrite {
                sql: write.sql,
                user_id: write.user_id,
                source_batch_id: batch_id,
                enqueued_at_ms: now_ms,
            });
            drop(q);
            lane.stats.accepted_writes.fetch_add(1, Ordering::Relaxed);
            match lane_kind {
                SqlLane::Online => touched_online = true,
                SqlLane::Stats => touched_stats = true,
                SqlLane::Logs => touched_logs = true,
            }
            accepted += 1;
        }

        if accepted > 0 {
            self.stats
                .accepted_writes
                .fetch_add(accepted as u64, Ordering::Relaxed);
            if touched_online {
                self.lane_by_kind(SqlLane::Online).notify.notify_one();
            }
            if touched_stats {
                self.lane_by_kind(SqlLane::Stats).notify.notify_one();
            }
            if touched_logs {
                self.lane_by_kind(SqlLane::Logs).notify.notify_one();
            }
        }

        accepted
    }

    fn queue_depth(&self) -> usize {
        self.lanes.iter().map(|lane| lane.queue_depth()).sum()
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InboundMessage {
    Hello {
        service: Option<String>,
        proto: Option<u32>,
        server_id: Option<String>,
        ts: Option<i64>,
    },
    SqlBatch {
        batch_id: Option<i64>,
        sent_at: Option<i64>,
        writes: Vec<SqlWrite>,
    },
    Health,
}

#[derive(Debug, Deserialize)]
struct SqlWrite {
    sql: String,
    #[serde(default)]
    user_id: Option<u32>,
    #[serde(default)]
    force_sync: Option<bool>,
}

#[derive(Debug, Serialize)]
struct AckResponse<'a> {
    r#type: &'a str,
    batch_id: Option<i64>,
    accepted: usize,
    queue_depth: usize,
    ts: u64,
}

#[derive(Debug, Serialize)]
struct ErrorResponse<'a> {
    r#type: &'a str,
    message: &'a str,
    ts: u64,
}

#[derive(Debug, Serialize)]
struct HelloResponse<'a> {
    r#type: &'a str,
    service: &'a str,
    proto: u32,
    ts: u64,
}

#[derive(Debug, Serialize)]
struct HealthResponse<'a> {
    r#type: &'a str,
    queue_depth: usize,
    accepted_writes: u64,
    executed_writes: u64,
    db_errors: u64,
    parse_errors: u64,
    ts: u64,
}

fn main() -> std::io::Result<()> {
    let bind_addr = std::env::var("WT_RUST_BIND").unwrap_or_else(|_| DEFAULT_BIND.to_string());
    let flush_interval_ms = std::env::var("WT_RUST_FLUSH_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_FLUSH_INTERVAL_MS);
    let max_batch_rows = std::env::var("WT_RUST_MAX_BATCH_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_BATCH_ROWS);
    let db_cfg = DbConfig::from_env();

    println!(
        "[sql-sink] db config from env: driver={} host={} port={} database={} user={} pass_set={}",
        db_cfg.driver,
        db_cfg.host,
        db_cfg.port,
        db_cfg.database,
        db_cfg.user,
        if db_cfg.pass.is_empty() { "0" } else { "1" }
    );
    if db_cfg.pass.is_empty() {
        eprintln!("[sql-sink] warning: WT_DB_PASS is empty; set it to match SourceMod databases.cfg \"default\"");
    }

    let sink = Arc::new(
        SqlSink::new(
            &db_cfg,
            SinkConfig {
                flush_interval: Duration::from_millis(flush_interval_ms.max(1)),
                max_batch_rows: max_batch_rows.max(1),
            },
        )
        .map_err(|e| std::io::Error::other(format!("db init failed: {e}")))?,
    );
    println!("[sql-sink] mysql connection pools ready (lanes=3: online, stats, logs)");
    sink.spawn_workers();

    let listener = TcpListener::bind(&bind_addr)?;
    println!("[sql-sink] listening on {}", bind_addr);

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let sink = Arc::clone(&sink);
                thread::spawn(move || {
                    if let Err(e) = handle_client(stream, sink) {
                        eprintln!("[sql-sink] client handler error: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("[sql-sink] accept error: {}", e);
            }
        }
    }

    Ok(())
}

fn handle_client(stream: TcpStream, sink: Arc<SqlSink>) -> std::io::Result<()> {
    let peer = stream.peer_addr().ok();
    let reader_stream = stream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);
    let mut writer = stream;

    println!("[sql-sink] client connected: {:?}", peer);

    let mut buf = Vec::with_capacity(4096);
    loop {
        buf.clear();
        let n = reader.read_until(b'\n', &mut buf)?;
        if n == 0 {
            break;
        }

        if let Some(b'\n') = buf.last().copied() {
            buf.pop();
        }
        if let Some(b'\r') = buf.last().copied() {
            buf.pop();
        }
        if buf.is_empty() {
            continue;
        }

        let line = match std::str::from_utf8(&buf) {
            Ok(s) => s,
            Err(_) => {
                sink.stats.parse_errors.fetch_add(1, Ordering::Relaxed);
                send_json_line(
                    &mut writer,
                    &ErrorResponse {
                        r#type: "error",
                        message: "invalid utf8",
                        ts: now_secs(),
                    },
                )?;
                continue;
            }
        };

        let parsed: Result<InboundMessage, serde_json::Error> = serde_json::from_str(line);
        match parsed {
            Ok(InboundMessage::Hello {
                service,
                proto,
                server_id,
                ts,
            }) => {
                println!(
                    "[sql-sink] hello from {:?}: service={:?} proto={:?} server_id={:?} ts={:?}",
                    peer, service, proto, server_id, ts
                );
                send_json_line(
                    &mut writer,
                    &HelloResponse {
                        r#type: "hello_ack",
                        service: "whaletracker_sql_sink",
                        proto: 1,
                        ts: now_secs(),
                    },
                )?;
            }
            Ok(InboundMessage::SqlBatch {
                batch_id,
                sent_at,
                writes,
            }) => {
                let total = writes.len();
                let online_writes = writes
                    .iter()
                    .filter(|w| {
                        let lower = w.sql.to_ascii_lowercase();
                        lower.contains("whaletracker_online") || lower.contains("whaletracker_servers")
                    })
                    .count();
                let force_sync_count = writes
                    .iter()
                    .filter(|w| w.force_sync.unwrap_or(false))
                    .count();
                if force_sync_count > 0 {
                    // accepted, but currently handled the same as queued writes.
                    println!(
                        "[sql-sink] batch {:?} requested {} force_sync writes (treated as queued)",
                        batch_id, force_sync_count
                    );
                }
                let accepted = sink.enqueue_batch(batch_id, writes);
                if let Some(sent_at) = sent_at {
                    println!(
                        "[sql-sink] batch {:?}: accepted {}/{} writes (queue_depth={}, sent_at={}, online_related={})",
                        batch_id,
                        accepted,
                        total,
                        sink.queue_depth(),
                        sent_at,
                        online_writes
                    );
                }
                send_json_line(
                    &mut writer,
                    &AckResponse {
                        r#type: "ack",
                        batch_id,
                        accepted,
                        queue_depth: sink.queue_depth(),
                        ts: now_secs(),
                    },
                )?;
            }
            Ok(InboundMessage::Health) => {
                send_json_line(
                    &mut writer,
                    &HealthResponse {
                        r#type: "health",
                        queue_depth: sink.queue_depth(),
                        accepted_writes: sink.stats.accepted_writes.load(Ordering::Relaxed),
                        executed_writes: sink.stats.executed_writes.load(Ordering::Relaxed),
                        db_errors: sink.stats.db_errors.load(Ordering::Relaxed),
                        parse_errors: sink.stats.parse_errors.load(Ordering::Relaxed),
                        ts: now_secs(),
                    },
                )?;
            }
            Err(e) => {
                sink.stats.parse_errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("[sql-sink] parse error from {:?}: {} | line={}", peer, e, preview_sql(line, 256));
                send_json_line(
                    &mut writer,
                    &ErrorResponse {
                        r#type: "error",
                        message: "invalid json message",
                        ts: now_secs(),
                    },
                )?;
            }
        }
    }

    println!("[sql-sink] client disconnected: {:?}", peer);
    Ok(())
}

fn send_json_line<T: Serialize>(writer: &mut TcpStream, value: &T) -> std::io::Result<()> {
    serde_json::to_writer(&mut *writer, value).map_err(std::io::Error::other)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn preview_sql(sql: &str, max_len: usize) -> String {
    if sql.len() <= max_len {
        return sql.to_string();
    }
    let mut out = sql[..max_len].to_string();
    out.push_str("...");
    out
}

fn classify_sql(sql: &str) -> &'static str {
    let lower = sql.to_ascii_lowercase();
    if lower.contains("whaletracker_online") {
        return "whaletracker_online";
    }
    if lower.contains("whaletracker_servers") {
        return "whaletracker_servers";
    }
    if lower.contains("whaletracker_points_cache") {
        return "whaletracker_points_cache";
    }
    if lower.contains("whaletracker_log_players") {
        return "whaletracker_log_players";
    }
    if lower.contains("whaletracker_logs") {
        return "whaletracker_logs";
    }
    if lower.contains("insert into whaletracker ") || lower.contains("update whaletracker ") {
        return "whaletracker_main";
    }
    "other"
}
