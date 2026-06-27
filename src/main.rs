use mysql::{params, prelude::Queryable};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_BIND: &str = "127.0.0.1:28017";
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 100;
const DEFAULT_MAX_BATCH_ROWS: usize = 256;
const DEFAULT_MAX_FRAME_BYTES: usize = 32768;
const DEFAULT_MAX_INBOUND_WRITES: usize = 256;
const DEFAULT_MAX_SQL_BYTES: usize = 8192;
const DEFAULT_MAX_QUEUE_ROWS: usize = 8192;
const DEFAULT_DEDUPE_EVENTS: usize = 65536;
const DEFAULT_DEAD_LETTER_PATH: &str = "sql_dead_letters.log";
const DEFAULT_PENDING_JOURNAL_PATH: &str = "sql_pending_journal.log";
const DEFAULT_DB_HOST: &str = "127.0.0.1";
const DEFAULT_DB_PORT: u16 = 3306;
const DEFAULT_DB_NAME: &str = "appdb";
const DEFAULT_DB_USER: &str = "dbuser";
const DEFAULT_DB_DRIVER: &str = "mysql";
const DEFAULT_POINTS_CACHE_OWNER_PORT: u16 = 28017;
const DEFAULT_POINTS_CACHE_DEBOUNCE_MS: u64 = 3000;
const DEFAULT_POINTS_CACHE_POLL_MS: u64 = 1000;
const DEFAULT_POINTS_CACHE_TOUCH_MS: u64 = 1000;
const DEFAULT_SCHEMA_POLL_MS: u64 = 1000;
const WHALETRACKER_SCHEMA_VERSION: u32 = 3;
const MAX_LOG_DAMAGE_PER_MINUTE: f64 = 3000.0;
const POINTS_CACHE_STATE_KEY: &str = "global";
const SCHEMA_VERSION_TABLE: &str = "whaletracker_schema_migrations";
const WHALE_RANK_MIN_KD_SUM: i32 = 200;
const WHALE_RANK_MIN_PLAYTIME_SECONDS: i32 = 10800;
const WHALE_POINTS_SQL_EXPR: &str = r#"ROUND(1000.0 * SQRT(((CASE WHEN ((CASE WHEN kills > 0 THEN kills ELSE 0 END) + (CASE WHEN deaths > 0 THEN deaths ELSE 0 END)) > 0 THEN ((CASE WHEN kills > 0 THEN kills ELSE 0 END) + (CASE WHEN deaths > 0 THEN deaths ELSE 0 END)) ELSE 1 END)) / (((CASE WHEN ((CASE WHEN kills > 0 THEN kills ELSE 0 END) + (CASE WHEN deaths > 0 THEN deaths ELSE 0 END)) > 0 THEN ((CASE WHEN kills > 0 THEN kills ELSE 0 END) + (CASE WHEN deaths > 0 THEN deaths ELSE 0 END)) ELSE 1 END)) + 400.0)) * (((CASE WHEN playtime > 0 THEN playtime ELSE 0 END) / 3600.0) / (((CASE WHEN playtime > 0 THEN playtime ELSE 0 END) / 3600.0) + 20.0)) * ((5.0 * (((CASE WHEN kills > 0 THEN kills ELSE 0 END) + ((CASE WHEN assists > 0 THEN assists ELSE 0 END) * 0.35)) / ((CASE WHEN deaths > 0 THEN deaths ELSE 0 END) + 20.0))) + LN(1.0 + ((CASE WHEN damage_dealt > 0 THEN damage_dealt ELSE 0 END) / (150.0 * ((CASE WHEN ((CASE WHEN kills > 0 THEN kills ELSE 0 END) + (CASE WHEN deaths > 0 THEN deaths ELSE 0 END)) > 0 THEN ((CASE WHEN kills > 0 THEN kills ELSE 0 END) + (CASE WHEN deaths > 0 THEN deaths ELSE 0 END)) ELSE 1 END))))) + (0.60 * LN(1.0 + ((CASE WHEN healing > 0 THEN healing ELSE 0 END) / (100.0 * ((CASE WHEN ((CASE WHEN kills > 0 THEN kills ELSE 0 END) + (CASE WHEN deaths > 0 THEN deaths ELSE 0 END)) > 0 THEN ((CASE WHEN kills > 0 THEN kills ELSE 0 END) + (CASE WHEN deaths > 0 THEN deaths ELSE 0 END)) ELSE 1 END)))))) + (0.90 * LN(1.0 + ((60.0 * (CASE WHEN total_ubers > 0 THEN total_ubers ELSE 0 END)) / ((CASE WHEN ((CASE WHEN kills > 0 THEN kills ELSE 0 END) + (CASE WHEN deaths > 0 THEN deaths ELSE 0 END)) > 0 THEN ((CASE WHEN kills > 0 THEN kills ELSE 0 END) + (CASE WHEN deaths > 0 THEN deaths ELSE 0 END)) ELSE 1 END)))))))"#;

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

        let pool = open_mysql_pool(cfg)?;
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
    event_id: Option<String>,
    source_batch_id: Option<i64>,
    enqueued_at_ms: u128,
    completion: Option<Arc<BatchCompletion>>,
}

#[derive(Debug)]
struct BatchCompletion {
    state: Mutex<BatchCompletionState>,
    notify: Condvar,
}

#[derive(Debug)]
struct BatchCompletionState {
    remaining: usize,
    executed: usize,
    db_errors: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BatchCompletionSnapshot {
    executed: usize,
    db_errors: usize,
}

impl BatchCompletion {
    fn new(remaining: usize) -> Self {
        Self {
            state: Mutex::new(BatchCompletionState {
                remaining,
                executed: 0,
                db_errors: 0,
            }),
            notify: Condvar::new(),
        }
    }

    fn finish(&self, success: bool) {
        let mut state = self.state.lock().expect("batch completion mutex poisoned");
        state.remaining = state.remaining.saturating_sub(1);
        if success {
            state.executed += 1;
        } else {
            state.db_errors += 1;
        }
        if state.remaining == 0 {
            self.notify.notify_all();
        }
    }

    fn wait(&self) -> BatchCompletionSnapshot {
        let mut state = self.state.lock().expect("batch completion mutex poisoned");
        while state.remaining > 0 {
            state = self.notify.wait(state).expect("batch completion wait failed");
        }
        BatchCompletionSnapshot {
            executed: state.executed,
            db_errors: state.db_errors,
        }
    }
}

struct EnqueueResult {
    accepted: usize,
    completion: Option<Arc<BatchCompletion>>,
}

#[derive(Debug)]
struct EnqueueError {
    message: &'static str,
    queued: usize,
    incoming: usize,
    limit: usize,
}

struct DedupeCache {
    state: Mutex<DedupeState>,
    max_entries: usize,
}

struct DedupeState {
    seen: HashSet<String>,
    order: VecDeque<String>,
}

impl DedupeCache {
    fn new(max_entries: usize) -> Self {
        Self {
            state: Mutex::new(DedupeState {
                seen: HashSet::new(),
                order: VecDeque::new(),
            }),
            max_entries,
        }
    }

    fn contains(&self, event_id: &str) -> bool {
        if event_id.is_empty() {
            return false;
        }
        self.state
            .lock()
            .expect("dedupe mutex poisoned")
            .seen
            .contains(event_id)
    }

    fn remember(&self, event_id: &str) {
        if event_id.is_empty() || self.max_entries == 0 {
            return;
        }

        let mut state = self.state.lock().expect("dedupe mutex poisoned");
        if !state.seen.insert(event_id.to_string()) {
            return;
        }
        state.order.push_back(event_id.to_string());
        while state.order.len() > self.max_entries {
            if let Some(oldest) = state.order.pop_front() {
                state.seen.remove(&oldest);
            }
        }
    }

    fn len(&self) -> usize {
        self.state
            .lock()
            .expect("dedupe mutex poisoned")
            .seen
            .len()
    }

    fn capacity(&self) -> usize {
        self.max_entries
    }
}

struct DeadLetterWriter {
    file: Option<Mutex<File>>,
}

#[derive(Serialize)]
struct DeadLetterEntry<'a> {
    ts_ms: u64,
    reason: &'a str,
    detail: Option<&'a str>,
    kind: &'a str,
    user_id: Option<u32>,
    batch_id: Option<i64>,
    event_id: Option<&'a str>,
    sql: &'a str,
}

impl DeadLetterWriter {
    fn from_env() -> Self {
        let path = std::env::var("WT_RUST_DEAD_LETTER_PATH")
            .unwrap_or_else(|_| DEFAULT_DEAD_LETTER_PATH.to_string());
        if path.trim().is_empty() {
            println!("[sql-sink] dead-letter journal disabled");
            return Self { file: None };
        }

        match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(file) => {
                println!("[sql-sink] dead-letter journal path={}", path);
                Self {
                    file: Some(Mutex::new(file)),
                }
            }
            Err(err) => {
                eprintln!(
                    "[sql-sink] failed to open dead-letter journal {}: {}",
                    path, err
                );
                Self { file: None }
            }
        }
    }

    fn record(
        &self,
        reason: &str,
        detail: Option<&str>,
        sql: &str,
        user_id: Option<u32>,
        batch_id: Option<i64>,
        event_id: Option<&str>,
    ) {
        let Some(file) = &self.file else {
            return;
        };
        let entry = DeadLetterEntry {
            ts_ms: now_ms_u64(),
            reason,
            detail,
            kind: classify_sql(sql),
            user_id,
            batch_id,
            event_id,
            sql,
        };

        let mut file = file.lock().expect("dead-letter mutex poisoned");
        if let Err(err) = serde_json::to_writer(&mut *file, &entry) {
            eprintln!("[sql-sink] failed to encode dead-letter entry: {}", err);
            return;
        }
        if let Err(err) = file.write_all(b"\n").and_then(|_| file.flush()) {
            eprintln!("[sql-sink] failed to write dead-letter entry: {}", err);
        }
    }
}

struct PendingJournal {
    file: Option<Mutex<File>>,
    path: Option<String>,
}

#[derive(Debug, Clone)]
struct JournalPendingWrite {
    event_id: String,
    sql: String,
    user_id: Option<u32>,
    batch_id: Option<i64>,
    ts_ms: u64,
}

#[derive(Debug, Default)]
struct JournalReplayState {
    pending: Vec<JournalPendingWrite>,
    recent_done: Vec<String>,
    done_records: usize,
    bad_lines: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PendingJournalRecord {
    Pending {
        event_id: String,
        sql: String,
        user_id: Option<u32>,
        batch_id: Option<i64>,
        ts_ms: u64,
    },
    Done {
        event_id: String,
        ts_ms: u64,
    },
}

impl PendingJournal {
    fn from_env() -> Result<Self, String> {
        let path = std::env::var("WT_RUST_PENDING_JOURNAL_PATH")
            .unwrap_or_else(|_| DEFAULT_PENDING_JOURNAL_PATH.to_string());
        if path.trim().is_empty() {
            println!("[sql-sink] pending journal disabled");
            return Ok(Self {
                file: None,
                path: None,
            });
        }

        Self::open_path(path)
    }

    fn open_path(path: String) -> Result<Self, String> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(|err| format!("failed to open pending journal {}: {}", path, err))?;
        println!("[sql-sink] pending journal path={}", path);
        Ok(Self {
            file: Some(Mutex::new(file)),
            path: Some(path),
        })
    }

    fn append_pending(
        &self,
        event_id: &str,
        sql: &str,
        user_id: Option<u32>,
        batch_id: Option<i64>,
    ) -> Result<(), String> {
        self.append_record(&PendingJournalRecord::Pending {
            event_id: event_id.to_string(),
            sql: sql.to_string(),
            user_id,
            batch_id,
            ts_ms: now_ms_u64(),
        })
    }

    fn append_done(&self, event_id: &str) -> Result<(), String> {
        self.append_record(&PendingJournalRecord::Done {
            event_id: event_id.to_string(),
            ts_ms: now_ms_u64(),
        })
    }

    fn append_record(&self, record: &PendingJournalRecord) -> Result<(), String> {
        let Some(file) = &self.file else {
            return Ok(());
        };

        let mut file = file.lock().expect("pending journal mutex poisoned");
        serde_json::to_writer(&mut *file, record).map_err(|err| err.to_string())?;
        file.write_all(b"\n").map_err(|err| err.to_string())?;
        file.flush().map_err(|err| err.to_string())?;
        Ok(())
    }

    fn load_replay_state(&self, dedupe: &DedupeCache) -> JournalReplayState {
        let Some(path) = &self.path else {
            return JournalReplayState::default();
        };
        Self::load_replay_state_from_path(path, dedupe)
    }

    fn compact_from_state(&self, state: &JournalReplayState) -> Result<(), String> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let Some(file_mutex) = &self.file else {
            return Ok(());
        };

        let mut file = file_mutex.lock().expect("pending journal mutex poisoned");
        file.sync_all().map_err(|err| err.to_string())?;

        let tmp_path = format!("{}.compact.{}", path, now_ms_u64());
        let result = (|| -> Result<(), String> {
            let mut tmp = File::create(&tmp_path).map_err(|err| err.to_string())?;
            for pending in &state.pending {
                serde_json::to_writer(
                    &mut tmp,
                    &PendingJournalRecord::Pending {
                        event_id: pending.event_id.clone(),
                        sql: pending.sql.clone(),
                        user_id: pending.user_id,
                        batch_id: pending.batch_id,
                        ts_ms: pending.ts_ms,
                    },
                )
                .map_err(|err| err.to_string())?;
                tmp.write_all(b"\n").map_err(|err| err.to_string())?;
            }
            for event_id in &state.recent_done {
                serde_json::to_writer(
                    &mut tmp,
                    &PendingJournalRecord::Done {
                        event_id: event_id.clone(),
                        ts_ms: now_ms_u64(),
                    },
                )
                .map_err(|err| err.to_string())?;
                tmp.write_all(b"\n").map_err(|err| err.to_string())?;
            }
            tmp.sync_all().map_err(|err| err.to_string())?;
            fs::rename(&tmp_path, path).map_err(|err| err.to_string())?;
            *file = OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(path)
                .map_err(|err| err.to_string())?;
            Ok(())
        })();

        if result.is_err() {
            let _ = fs::remove_file(&tmp_path);
        }
        result
    }

    fn load_replay_state_from_path(path: &str, dedupe: &DedupeCache) -> JournalReplayState {
        let file = match File::open(path) {
            Ok(file) => file,
            Err(err) => {
                eprintln!("[sql-sink] failed to read pending journal {}: {}", path, err);
                return JournalReplayState::default();
            }
        };

        let mut pending = HashMap::<String, JournalPendingWrite>::new();
        let mut recent_done = VecDeque::<String>::new();
        let mut done_records = 0usize;
        let mut bad_lines = 0usize;
        let done_capacity = dedupe.capacity();
        for line in BufReader::new(file).lines() {
            let Ok(line) = line else {
                bad_lines += 1;
                continue;
            };
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<PendingJournalRecord>(&line) {
                Ok(PendingJournalRecord::Pending {
                    event_id,
                    sql,
                    user_id,
                    batch_id,
                    ts_ms,
                }) => {
                    pending.insert(
                        event_id.clone(),
                        JournalPendingWrite {
                            event_id,
                            sql,
                            user_id,
                            batch_id,
                            ts_ms,
                        },
                    );
                }
                Ok(PendingJournalRecord::Done { event_id, .. }) => {
                    pending.remove(&event_id);
                    dedupe.remember(&event_id);
                    if done_capacity > 0 {
                        recent_done.push_back(event_id);
                        while recent_done.len() > done_capacity {
                            recent_done.pop_front();
                        }
                    }
                    done_records += 1;
                }
                Err(err) => {
                    bad_lines += 1;
                    eprintln!("[sql-sink] bad pending journal line: {} | {}", err, line);
                }
            }
        }

        let mut pending = pending.into_values().collect::<Vec<_>>();
        pending.sort_by(|a, b| a.ts_ms.cmp(&b.ts_ms).then_with(|| a.event_id.cmp(&b.event_id)));

        JournalReplayState {
            pending,
            recent_done: recent_done.into_iter().collect(),
            done_records,
            bad_lines,
        }
    }
}

#[derive(Debug, Clone)]
struct SinkConfig {
    flush_interval: Duration,
    max_batch_rows: usize,
    max_queue_rows: usize,
    max_dedupe_events: usize,
}

#[derive(Clone)]
struct ProtocolConfig {
    max_frame_bytes: usize,
    max_inbound_writes: usize,
    max_sql_bytes: usize,
    auth_token: String,
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

fn open_mysql_pool(cfg: &DbConfig) -> Result<mysql::Pool, String> {
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

    mysql::Pool::new(builder).map_err(|e| e.to_string())
}

#[derive(Debug)]
struct Migration {
    version: u32,
    name: &'static str,
    statements: Vec<String>,
}

struct SchemaManager {
    bind_port: u16,
    owner_port: u16,
    poll_ms: u64,
    pool: mysql::Pool,
}

impl SchemaManager {
    fn new(
        db_cfg: &DbConfig,
        bind_port: u16,
        owner_port: u16,
        poll_ms: u64,
    ) -> Result<Self, String> {
        Ok(Self {
            bind_port,
            owner_port,
            poll_ms: poll_ms.max(1),
            pool: open_mysql_pool(db_cfg)?,
        })
    }

    fn is_owner(&self) -> bool {
        self.bind_port > 0 && self.bind_port == self.owner_port
    }

    fn ensure_version_table(&self) -> Result<(), String> {
        let mut conn = self.pool.get_conn().map_err(|e| e.to_string())?;
        conn.query_drop(format!(
            "CREATE TABLE IF NOT EXISTS {} (\
             version INTEGER PRIMARY KEY,\
             name VARCHAR(128) NOT NULL,\
             applied_at BIGINT DEFAULT 0\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
            SCHEMA_VERSION_TABLE
        ))
        .map_err(|e| e.to_string())
    }

    fn current_version(&self) -> Result<u32, String> {
        self.ensure_version_table()?;
        let mut conn = self.pool.get_conn().map_err(|e| e.to_string())?;
        let sql = format!("SELECT COALESCE(MAX(version), 0) FROM {}", SCHEMA_VERSION_TABLE);
        let version = conn
            .query_first::<u32, _>(sql)
            .map_err(|e| e.to_string())?
            .unwrap_or(0);
        Ok(version)
    }

    fn prepare(&self) -> Result<(), String> {
        if self.is_owner() {
            println!(
                "[schema] bind_port={} owner_port={} role=owner target_version={}",
                self.bind_port, self.owner_port, WHALETRACKER_SCHEMA_VERSION
            );
            self.apply_migrations()
        } else {
            println!(
                "[schema] bind_port={} owner_port={} role=follower target_version={}",
                self.bind_port, self.owner_port, WHALETRACKER_SCHEMA_VERSION
            );
            self.wait_until_ready()
        }
    }

    fn apply_migrations(&self) -> Result<(), String> {
        self.ensure_version_table()?;
        let current = self.current_version()?;
        let migrations = schema_migrations();

        for migration in migrations.into_iter().filter(|m| m.version > current) {
            println!(
                "[schema] apply start version={} name={}",
                migration.version, migration.name
            );
            let mut conn = self.pool.get_conn().map_err(|e| e.to_string())?;
            for statement in &migration.statements {
                conn.query_drop(statement).map_err(|e| e.to_string())?;
            }
            conn.exec_drop(
                format!(
                    "INSERT INTO {} (version, name, applied_at) VALUES (:version, :name, :applied_at)",
                    SCHEMA_VERSION_TABLE
                ),
                params! {
                    "version" => migration.version,
                    "name" => migration.name,
                    "applied_at" => now_ms_u64(),
                },
            )
            .map_err(|e| e.to_string())?;
            println!(
                "[schema] apply ok version={} name={}",
                migration.version, migration.name
            );
        }

        let final_version = self.current_version()?;
        if final_version < WHALETRACKER_SCHEMA_VERSION {
            return Err(format!(
                "schema version {} below required {} after apply",
                final_version, WHALETRACKER_SCHEMA_VERSION
            ));
        }
        Ok(())
    }

    fn wait_until_ready(&self) -> Result<(), String> {
        loop {
            match self.current_version() {
                Ok(version) if version >= WHALETRACKER_SCHEMA_VERSION => return Ok(()),
                Ok(_) => {}
                Err(err) => eprintln!("[schema] wait retry: {}", err),
            }
            thread::sleep(Duration::from_millis(self.poll_ms));
        }
    }
}

fn build_create_table_sql(name: &str, data_columns: Vec<String>, key_defs: Vec<String>) -> String {
    let mut parts = data_columns;
    parts.extend(key_defs);
    format!(
        "CREATE TABLE IF NOT EXISTS `{}` ({}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        name,
        parts.join(", ")
    )
}

fn push_add_columns(statements: &mut Vec<String>, table: &str, columns: &[String]) {
    for column in columns {
        statements.push(format!(
            "ALTER TABLE `{}` ADD COLUMN IF NOT EXISTS {}",
            table, column
        ));
    }
}

fn class_slugs() -> [&'static str; 9] {
    [
        "scout",
        "sniper",
        "soldier",
        "demoman",
        "medic",
        "heavy",
        "pyro",
        "spy",
        "engineer",
    ]
}

fn weapon_category_columns() -> Vec<String> {
    let mut columns = Vec::new();
    for slug in [
        "shotguns",
        "scatterguns",
        "pistols",
        "rocketlaunchers",
        "grenadelaunchers",
        "stickylaunchers",
        "snipers",
        "revolvers",
    ] {
        columns.push(format!("`shots_{}` INTEGER DEFAULT 0", slug));
        columns.push(format!("`hits_{}` INTEGER DEFAULT 0", slug));
    }
    columns
}

fn per_class_columns() -> Vec<String> {
    let mut columns = vec!["`classes_mask` INTEGER DEFAULT 0".to_string()];
    for slug in class_slugs() {
        columns.push(format!("`shots_{}` INTEGER DEFAULT 0", slug));
        columns.push(format!("`hits_{}` INTEGER DEFAULT 0", slug));
    }
    columns
}

fn weapon_slot_columns(max_slots: usize) -> Vec<String> {
    let mut columns = Vec::new();
    for slot in 1..=max_slots {
        columns.push(format!("`weapon{}_name` VARCHAR(128) DEFAULT ''", slot));
        columns.push(format!("`weapon{}_shots` INTEGER DEFAULT 0", slot));
        columns.push(format!("`weapon{}_hits` INTEGER DEFAULT 0", slot));
        columns.push(format!("`weapon{}_damage` INTEGER DEFAULT 0", slot));
        columns.push(format!("`weapon{}_defindex` INTEGER DEFAULT 0", slot));
    }
    columns
}

fn online_weapon_slot_columns(max_slots: usize) -> Vec<String> {
    let mut columns = Vec::new();
    for slot in 1..=max_slots {
        columns.push(format!("`weapon{}_name` VARCHAR(64) DEFAULT ''", slot));
        columns.push(format!("`weapon{}_accuracy` FLOAT DEFAULT 0", slot));
        columns.push(format!("`weapon{}_shots` INTEGER DEFAULT 0", slot));
        columns.push(format!("`weapon{}_hits` INTEGER DEFAULT 0", slot));
    }
    columns
}

fn schema_migrations() -> Vec<Migration> {
    let mut whaletracker_columns = vec![
        "`steamid` VARCHAR(32) NOT NULL".to_string(),
        "`first_seen` INTEGER DEFAULT NULL".to_string(),
        "`kills` INTEGER DEFAULT 0".to_string(),
        "`deaths` INTEGER DEFAULT 0".to_string(),
        "`shots` INTEGER DEFAULT 0".to_string(),
        "`hits` INTEGER DEFAULT 0".to_string(),
        "`healing` INTEGER DEFAULT 0".to_string(),
        "`total_ubers` INTEGER DEFAULT 0".to_string(),
        "`best_ubers_life` INTEGER DEFAULT 0".to_string(),
        "`medic_drops` INTEGER DEFAULT 0".to_string(),
        "`uber_drops` INTEGER DEFAULT 0".to_string(),
        "`airshots` INTEGER DEFAULT 0".to_string(),
        "`bonusPoints` INTEGER DEFAULT 0".to_string(),
        "`totalCrossbowHits` INTEGER DEFAULT 0".to_string(),
        "`marketGardenHits` INTEGER DEFAULT 0".to_string(),
        "`headshots` INTEGER DEFAULT 0".to_string(),
        "`backstabs` INTEGER DEFAULT 0".to_string(),
        "`best_headshots_life` INTEGER DEFAULT 0".to_string(),
        "`best_backstabs_life` INTEGER DEFAULT 0".to_string(),
        "`best_kills_life` INTEGER DEFAULT 0".to_string(),
        "`best_killstreak` INTEGER DEFAULT 0".to_string(),
        "`best_score_life` INTEGER DEFAULT 0".to_string(),
        "`assists` INTEGER DEFAULT 0".to_string(),
        "`best_assists_life` INTEGER DEFAULT 0".to_string(),
        "`playtime` INTEGER DEFAULT 0".to_string(),
        "`is_admin` TINYINT DEFAULT 0".to_string(),
        "`damage_dealt` INTEGER DEFAULT 0".to_string(),
        "`damage_taken` INTEGER DEFAULT 0".to_string(),
        "`last_seen` INTEGER DEFAULT 0".to_string(),
        "`personaname` VARCHAR(128) DEFAULT ''".to_string(),
        "`favorite_class` TINYINT DEFAULT 0".to_string(),
        "`cached_personaname` VARCHAR(255) DEFAULT NULL".to_string(),
        "`cached_personaname_lower` VARCHAR(255) DEFAULT NULL".to_string(),
    ];
    whaletracker_columns.extend(per_class_columns());
    whaletracker_columns.extend(weapon_category_columns());
    whaletracker_columns.push(
        "`sort_weight` DOUBLE AS (CASE WHEN playtime >= 14400 THEN (kills + (0.5 * assists)) / GREATEST(deaths, 1) ELSE -1 END) STORED"
            .to_string(),
    );

    let mut online_columns = vec![
        "`steamid` VARCHAR(32) NOT NULL".to_string(),
        "`personaname` VARCHAR(128) DEFAULT ''".to_string(),
        "`class` TINYINT DEFAULT 0".to_string(),
        "`team` TINYINT DEFAULT 0".to_string(),
        "`alive` TINYINT DEFAULT 0".to_string(),
        "`is_spectator` TINYINT DEFAULT 0".to_string(),
        "`kills` INTEGER DEFAULT 0".to_string(),
        "`deaths` INTEGER DEFAULT 0".to_string(),
        "`assists` INTEGER DEFAULT 0".to_string(),
        "`damage` INTEGER DEFAULT 0".to_string(),
        "`damage_taken` INTEGER DEFAULT 0".to_string(),
        "`healing` INTEGER DEFAULT 0".to_string(),
        "`headshots` INTEGER DEFAULT 0".to_string(),
        "`backstabs` INTEGER DEFAULT 0".to_string(),
        "`medic_drops` INTEGER DEFAULT 0".to_string(),
        "`uber_drops` INTEGER DEFAULT 0".to_string(),
        "`airshots` INTEGER DEFAULT 0".to_string(),
        "`marketGardenHits` INTEGER DEFAULT 0".to_string(),
        "`playtime` INTEGER DEFAULT 0".to_string(),
        "`total_ubers` INTEGER DEFAULT 0".to_string(),
        "`best_streak` INTEGER DEFAULT 0".to_string(),
        "`best_ubers_life` INTEGER DEFAULT 0".to_string(),
        "`current_killstreak` INTEGER DEFAULT 0".to_string(),
        "`current_ubers_life` INTEGER DEFAULT 0".to_string(),
        "`visible_max` INTEGER DEFAULT 0".to_string(),
        "`time_connected` INTEGER DEFAULT 0".to_string(),
        "`shots` INTEGER DEFAULT 0".to_string(),
        "`hits` INTEGER DEFAULT 0".to_string(),
        "`host_ip` VARCHAR(64) DEFAULT ''".to_string(),
        "`host_port` INTEGER DEFAULT 0".to_string(),
        "`playercount` INTEGER DEFAULT 0".to_string(),
        "`map_name` VARCHAR(128) DEFAULT ''".to_string(),
        "`last_update` INTEGER DEFAULT 0".to_string(),
    ];
    online_columns.extend(per_class_columns());
    online_columns.extend(weapon_category_columns());
    online_columns.extend(online_weapon_slot_columns(6));

    let online_meta_columns = vec![
        "`id` TINYINT NOT NULL".to_string(),
        "`map_name` VARCHAR(128) DEFAULT ''".to_string(),
        "`playercount` INTEGER DEFAULT 0".to_string(),
        "`updated_at` INTEGER DEFAULT 0".to_string(),
        "`host_ip` VARCHAR(64) DEFAULT ''".to_string(),
        "`host_port` INTEGER DEFAULT 0".to_string(),
        "`visible_max` INTEGER DEFAULT 0".to_string(),
    ];

    let servers_columns = vec![
        "`ip` VARCHAR(64) NOT NULL".to_string(),
        "`port` INTEGER NOT NULL".to_string(),
        "`playercount` INTEGER DEFAULT 0".to_string(),
        "`visible_max` INTEGER DEFAULT 0".to_string(),
        "`game` VARCHAR(64) DEFAULT ''".to_string(),
        "`game_url` VARCHAR(32) DEFAULT ''".to_string(),
        "`map` VARCHAR(128) DEFAULT ''".to_string(),
        "`city` VARCHAR(128) DEFAULT ''".to_string(),
        "`country` VARCHAR(8) DEFAULT ''".to_string(),
        "`flags` VARCHAR(256) DEFAULT ''".to_string(),
        "`last_update` INTEGER DEFAULT 0".to_string(),
    ];

    let logs_columns = vec![
        "`log_id` VARCHAR(64) NOT NULL".to_string(),
        "`map` VARCHAR(64) DEFAULT ''".to_string(),
        "`gamemode` VARCHAR(64) DEFAULT 'Unknown'".to_string(),
        "`started_at` INTEGER DEFAULT 0".to_string(),
        "`ended_at` INTEGER DEFAULT 0".to_string(),
        "`duration` INTEGER DEFAULT 0".to_string(),
        "`player_count` INTEGER DEFAULT 0".to_string(),
        "`created_at` INTEGER DEFAULT 0".to_string(),
        "`updated_at` INTEGER DEFAULT 0".to_string(),
    ];

    let mut log_players_columns = vec![
        "`log_id` VARCHAR(64) NOT NULL".to_string(),
        "`steamid` VARCHAR(32) NOT NULL".to_string(),
        "`personaname` VARCHAR(128) DEFAULT ''".to_string(),
        "`kills` INTEGER DEFAULT 0".to_string(),
        "`deaths` INTEGER DEFAULT 0".to_string(),
        "`assists` INTEGER DEFAULT 0".to_string(),
        "`damage` INTEGER DEFAULT 0".to_string(),
        "`damage_taken` INTEGER DEFAULT 0".to_string(),
        "`healing` INTEGER DEFAULT 0".to_string(),
        "`headshots` INTEGER DEFAULT 0".to_string(),
        "`backstabs` INTEGER DEFAULT 0".to_string(),
        "`total_ubers` INTEGER DEFAULT 0".to_string(),
        "`playtime` INTEGER DEFAULT 0".to_string(),
        "`medic_drops` INTEGER DEFAULT 0".to_string(),
        "`uber_drops` INTEGER DEFAULT 0".to_string(),
        "`airshots` INTEGER DEFAULT 0".to_string(),
        "`marketGardenHits` INTEGER DEFAULT 0".to_string(),
        "`shots` INTEGER DEFAULT 0".to_string(),
        "`hits` INTEGER DEFAULT 0".to_string(),
        "`best_streak` INTEGER DEFAULT 0".to_string(),
        "`best_headshots_life` INTEGER DEFAULT 0".to_string(),
        "`best_backstabs_life` INTEGER DEFAULT 0".to_string(),
        "`best_score_life` INTEGER DEFAULT 0".to_string(),
        "`best_kills_life` INTEGER DEFAULT 0".to_string(),
        "`best_assists_life` INTEGER DEFAULT 0".to_string(),
        "`best_ubers_life` INTEGER DEFAULT 0".to_string(),
        "`is_admin` TINYINT DEFAULT 0".to_string(),
        "`last_updated` INTEGER DEFAULT 0".to_string(),
    ];
    log_players_columns.extend(per_class_columns());
    log_players_columns.extend(weapon_category_columns());
    log_players_columns.extend(weapon_slot_columns(6));
    for slug in ["soldier", "demoman", "sniper", "medic"] {
        log_players_columns.push(format!("`airshots_{}` INTEGER DEFAULT 0", slug));
        log_players_columns.push(format!(
            "`airshots_{}_height` INTEGER DEFAULT 0",
            slug
        ));
    }

    let points_cache_columns = vec![
        "`steamid` VARCHAR(32) NOT NULL".to_string(),
        "`points` INTEGER DEFAULT 0".to_string(),
        "`rank` INTEGER DEFAULT 0".to_string(),
        "`name_color` VARCHAR(32) DEFAULT ''".to_string(),
        "`updated_at` INTEGER DEFAULT 0".to_string(),
    ];

    let create_tables = vec![
        build_create_table_sql(
            "whaletracker",
            whaletracker_columns.clone(),
            vec![
                "PRIMARY KEY (`steamid`)".to_string(),
                "KEY `idx_cached_personaname_lower` (`cached_personaname_lower`)".to_string(),
                "KEY `idx_last_seen` (`last_seen`)".to_string(),
                "KEY `idx_sort_weight` (`sort_weight` DESC, `kills` DESC)".to_string(),
            ],
        ),
        build_create_table_sql(
            "whaletracker_online",
            online_columns.clone(),
            vec!["PRIMARY KEY (`steamid`)".to_string()],
        ),
        build_create_table_sql(
            "whaletracker_online_meta",
            online_meta_columns.clone(),
            vec!["PRIMARY KEY (`id`)".to_string()],
        ),
        build_create_table_sql(
            "whaletracker_servers",
            servers_columns.clone(),
            vec!["PRIMARY KEY (`ip`, `port`)".to_string()],
        ),
        build_create_table_sql(
            "whaletracker_logs",
            logs_columns.clone(),
            vec!["PRIMARY KEY (`log_id`)".to_string()],
        ),
        build_create_table_sql(
            "whaletracker_log_players",
            log_players_columns.clone(),
            vec!["PRIMARY KEY (`log_id`, `steamid`)".to_string()],
        ),
        build_create_table_sql(
            "whaletracker_points_cache",
            points_cache_columns.clone(),
            vec!["PRIMARY KEY (`steamid`)".to_string()],
        ),
        "CREATE TABLE IF NOT EXISTS whaletracker_points_cache_build LIKE whaletracker_points_cache"
            .to_string(),
        "CREATE TABLE IF NOT EXISTS whaletracker_points_cache_state (\
         cache_key VARCHAR(64) PRIMARY KEY,\
         dirty TINYINT DEFAULT 0,\
         dirty_updated_at BIGINT DEFAULT 0,\
         last_reason VARCHAR(64) DEFAULT '',\
         last_rebuilt_at BIGINT DEFAULT 0\
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            .to_string(),
    ];

    let mut upgrade_columns = Vec::new();
    push_add_columns(&mut upgrade_columns, "whaletracker", &whaletracker_columns);
    push_add_columns(&mut upgrade_columns, "whaletracker_online", &online_columns);
    push_add_columns(
        &mut upgrade_columns,
        "whaletracker_online_meta",
        &online_meta_columns,
    );
    push_add_columns(&mut upgrade_columns, "whaletracker_servers", &servers_columns);
    push_add_columns(&mut upgrade_columns, "whaletracker_logs", &logs_columns);
    push_add_columns(
        &mut upgrade_columns,
        "whaletracker_log_players",
        &log_players_columns,
    );
    push_add_columns(
        &mut upgrade_columns,
        "whaletracker_points_cache",
        &points_cache_columns,
    );
    push_add_columns(
        &mut upgrade_columns,
        "whaletracker_points_cache_build",
        &points_cache_columns,
    );
    upgrade_columns.extend([
        "CREATE INDEX IF NOT EXISTS idx_cached_personaname_lower ON whaletracker (cached_personaname_lower)".to_string(),
        "CREATE INDEX IF NOT EXISTS idx_last_seen ON whaletracker (last_seen)".to_string(),
        "CREATE INDEX IF NOT EXISTS idx_sort_weight ON whaletracker (sort_weight DESC, kills DESC)".to_string(),
        "ALTER TABLE whaletracker CONVERT TO CHARACTER SET utf8mb4".to_string(),
        "ALTER TABLE whaletracker_online CONVERT TO CHARACTER SET utf8mb4".to_string(),
        "ALTER TABLE whaletracker_online_meta CONVERT TO CHARACTER SET utf8mb4".to_string(),
        "ALTER TABLE whaletracker_servers CONVERT TO CHARACTER SET utf8mb4".to_string(),
        "ALTER TABLE whaletracker_logs CONVERT TO CHARACTER SET utf8mb4".to_string(),
        "ALTER TABLE whaletracker_log_players CONVERT TO CHARACTER SET utf8mb4".to_string(),
        "ALTER TABLE whaletracker_points_cache CONVERT TO CHARACTER SET utf8mb4".to_string(),
        "ALTER TABLE whaletracker_points_cache_build CONVERT TO CHARACTER SET utf8mb4".to_string(),
        "DROP TABLE IF EXISTS whaletracker_mapstats".to_string(),
    ]);

    let remove_dead_columns = vec![
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS totalCrossbowHits INTEGER DEFAULT 0"
            .to_string(),
        "ALTER TABLE whaletracker DROP COLUMN IF EXISTS medicKills".to_string(),
        "ALTER TABLE whaletracker DROP COLUMN IF EXISTS heavyKills".to_string(),
    ];

    vec![
        Migration {
            version: 1,
            name: "create_whaletracker_schema",
            statements: create_tables,
        },
        Migration {
            version: 2,
            name: "upgrade_whaletracker_schema",
            statements: upgrade_columns,
        },
        Migration {
            version: 3,
            name: "remove_dead_whaletracker_columns",
            statements: remove_dead_columns,
        },
    ]
}

#[derive(Debug, Clone)]
struct PointsCacheConfig {
    owner_port: u16,
    debounce_ms: u64,
    poll_ms: u64,
    touch_min_ms: u64,
}

impl PointsCacheConfig {
    fn from_env() -> Self {
        let owner_port = std::env::var("WT_POINTS_CACHE_OWNER_PORT")
            .ok()
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(DEFAULT_POINTS_CACHE_OWNER_PORT);
        let debounce_ms = std::env::var("WT_POINTS_CACHE_DEBOUNCE_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_POINTS_CACHE_DEBOUNCE_MS);
        let poll_ms = std::env::var("WT_POINTS_CACHE_POLL_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_POINTS_CACHE_POLL_MS);
        let touch_min_ms = std::env::var("WT_POINTS_CACHE_TOUCH_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_POINTS_CACHE_TOUCH_MS);

        Self {
            owner_port,
            debounce_ms: debounce_ms.max(1),
            poll_ms: poll_ms.max(1),
            touch_min_ms: touch_min_ms.max(1),
        }
    }
}

struct PointsCacheManager {
    bind_port: u16,
    cfg: PointsCacheConfig,
    pool: mysql::Pool,
    last_dirty_touch_ms: AtomicU64,
}

impl PointsCacheManager {
    fn new(db_cfg: &DbConfig, bind_port: u16, cfg: PointsCacheConfig) -> Result<Self, String> {
        Ok(Self {
            bind_port,
            cfg,
            pool: open_mysql_pool(db_cfg)?,
            last_dirty_touch_ms: AtomicU64::new(0),
        })
    }

    fn is_owner(&self) -> bool {
        self.bind_port > 0 && self.bind_port == self.cfg.owner_port
    }

    fn mark_dirty(&self, reason: &str, force: bool) -> Result<(), String> {
        let now = now_ms_u64();
        if !force {
            let last = self.last_dirty_touch_ms.load(Ordering::Relaxed);
            if last > 0 && now.saturating_sub(last) < self.cfg.touch_min_ms {
                return Ok(());
            }
        }

        let mut conn = self.pool.get_conn().map_err(|e| e.to_string())?;
        conn.exec_drop(
            "INSERT INTO whaletracker_points_cache_state (cache_key, dirty, dirty_updated_at, last_reason, last_rebuilt_at) \
             VALUES (:cache_key, 1, :dirty_updated_at, :last_reason, 0) \
             ON DUPLICATE KEY UPDATE dirty = 1, dirty_updated_at = VALUES(dirty_updated_at), last_reason = VALUES(last_reason)",
            params! {
                "cache_key" => POINTS_CACHE_STATE_KEY,
                "dirty_updated_at" => now,
                "last_reason" => reason,
            },
        )
        .map_err(|e| e.to_string())?;
        self.last_dirty_touch_ms.store(now, Ordering::Relaxed);
        Ok(())
    }

    fn spawn_worker(self: &Arc<Self>) {
        if !self.is_owner() {
            println!(
                "[points-cache] bind_port={} owner_port={} role=follower",
                self.bind_port,
                self.cfg.owner_port
            );
            return;
        }

        println!(
            "[points-cache] bind_port={} owner_port={} role=owner debounce_ms={} poll_ms={}",
            self.bind_port,
            self.cfg.owner_port,
            self.cfg.debounce_ms,
            self.cfg.poll_ms
        );

        let manager = Arc::clone(self);
        thread::spawn(move || manager.worker_loop());
    }

    fn worker_loop(self: Arc<Self>) {
        if let Err(err) = self.mark_dirty("startup", true) {
            eprintln!("[points-cache] failed to mark startup dirty: {}", err);
        }

        loop {
            thread::sleep(Duration::from_millis(self.cfg.poll_ms));

            match self.poll_and_rebuild() {
                Ok(()) => {}
                Err(err) => eprintln!("[points-cache] worker error: {}", err),
            }
        }
    }

    fn poll_and_rebuild(&self) -> Result<(), String> {
        let mut conn = self.pool.get_conn().map_err(|e| e.to_string())?;
        let state: Option<(u8, u64, String)> = conn
            .exec_first(
                "SELECT dirty, dirty_updated_at, last_reason \
                 FROM whaletracker_points_cache_state \
                 WHERE cache_key = :cache_key LIMIT 1",
                params! {
                    "cache_key" => POINTS_CACHE_STATE_KEY,
                },
            )
            .map_err(|e| e.to_string())?;

        let Some((dirty, dirty_updated_at, last_reason)) = state else {
            return Ok(());
        };
        if dirty == 0 {
            return Ok(());
        }

        let now = now_ms_u64();
        if now.saturating_sub(dirty_updated_at) < self.cfg.debounce_ms {
            return Ok(());
        }

        let reason = if last_reason.is_empty() {
            "dirty".to_string()
        } else {
            last_reason
        };

        drop(conn);
        println!(
            "[points-cache] rebuild start reason={} dirty_updated_at={} bind_port={}",
            reason, dirty_updated_at, self.bind_port
        );

        match self.rebuild_points_cache() {
            Ok(()) => {
                let mut conn = self.pool.get_conn().map_err(|e| e.to_string())?;
                conn.exec_drop(
                    "UPDATE whaletracker_points_cache_state \
                     SET dirty = 0, last_rebuilt_at = :last_rebuilt_at, last_reason = :last_reason \
                     WHERE cache_key = :cache_key AND dirty = 1 AND dirty_updated_at = :dirty_updated_at",
                    params! {
                        "cache_key" => POINTS_CACHE_STATE_KEY,
                        "last_rebuilt_at" => now_ms_u64(),
                        "last_reason" => reason.as_str(),
                        "dirty_updated_at" => dirty_updated_at,
                    },
                )
                .map_err(|e| e.to_string())?;
                println!(
                    "[points-cache] rebuild ok reason={} bind_port={}",
                    reason, self.bind_port
                );
                Ok(())
            }
            Err(err) => {
                let mut conn = self.pool.get_conn().map_err(|e| e.to_string())?;
                conn.exec_drop(
                    "UPDATE whaletracker_points_cache_state \
                     SET dirty = 1, dirty_updated_at = :dirty_updated_at, last_reason = :last_reason \
                     WHERE cache_key = :cache_key",
                    params! {
                        "cache_key" => POINTS_CACHE_STATE_KEY,
                        "dirty_updated_at" => now_ms_u64(),
                        "last_reason" => "rebuild_retry",
                    },
                )
                .map_err(|e| e.to_string())?;
                Err(err)
            }
        }
    }

    fn rebuild_points_cache(&self) -> Result<(), String> {
        let mut conn = self.pool.get_conn().map_err(|e| e.to_string())?;
        conn.query_drop(
            "CREATE TABLE IF NOT EXISTS whaletracker_points_cache_build LIKE whaletracker_points_cache",
        )
        .map_err(|e| e.to_string())?;
        conn.query_drop("TRUNCATE TABLE whaletracker_points_cache_build")
            .map_err(|e| e.to_string())?;

        let insert_sql = format!(
            "INSERT INTO whaletracker_points_cache_build (steamid, points, rank, name_color, updated_at) \
             SELECT base.steamid, base.points, COALESCE(ranked.rank, 0), base.color, {now} \
             FROM (\
             SELECT w.steamid, {expr} AS points, \
             COALESCE(NULLIF(f.color COLLATE utf8mb4_uca1400_ai_ci,''), COALESCE(NULLIF(c.name_color,''), 'gold')) AS color \
             FROM whaletracker w \
             LEFT JOIN filters_namecolors f ON f.steamid COLLATE utf8mb4_uca1400_ai_ci = w.steamid \
             LEFT JOIN whaletracker_points_cache c ON c.steamid = w.steamid \
             ) base \
             LEFT JOIN (\
             SELECT eligible.steamid, ROW_NUMBER() OVER (ORDER BY eligible.points DESC, eligible.steamid ASC) AS rank \
             FROM (\
             SELECT w.steamid, {expr} AS points \
             FROM whaletracker w \
             WHERE ((CASE WHEN w.kills > 0 THEN w.kills ELSE 0 END) + (CASE WHEN w.deaths > 0 THEN w.deaths ELSE 0 END)) >= {min_kd_sum} \
             AND (CASE WHEN w.playtime > 0 THEN w.playtime ELSE 0 END) >= {min_playtime}\
             ) eligible\
            ) ranked ON ranked.steamid = base.steamid",
            now = now_secs(),
            expr = WHALE_POINTS_SQL_EXPR,
            min_kd_sum = WHALE_RANK_MIN_KD_SUM,
            min_playtime = WHALE_RANK_MIN_PLAYTIME_SECONDS
        );
        conn.query_drop(insert_sql).map_err(|e| e.to_string())?;
        conn.query_drop(
            "RENAME TABLE \
             whaletracker_points_cache TO whaletracker_points_cache_swap, \
             whaletracker_points_cache_build TO whaletracker_points_cache, \
             whaletracker_points_cache_swap TO whaletracker_points_cache_build",
        )
        .map_err(|e| e.to_string())?;
        Ok(())
    }
}

#[derive(Default)]
struct SinkStats {
    accepted_writes: AtomicU64,
    executed_writes: AtomicU64,
    db_errors: AtomicU64,
    parse_errors: AtomicU64,
    dropped_writes: AtomicU64,
    journal_pending_startup: AtomicU64,
    journal_replayed_startup: AtomicU64,
    journal_done_records_startup: AtomicU64,
    journal_bad_lines_startup: AtomicU64,
    journal_compactions: AtomicU64,
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
    points_cache: Option<Arc<PointsCacheManager>>,
    dedupe: Arc<DedupeCache>,
    dead_letters: Arc<DeadLetterWriter>,
    pending_journal: Arc<PendingJournal>,
}

impl LaneWorker {
    fn new(
        lane: SqlLane,
        db: Box<dyn DbExecutor>,
        cfg: SinkConfig,
        points_cache: Option<Arc<PointsCacheManager>>,
        dedupe: Arc<DedupeCache>,
        dead_letters: Arc<DeadLetterWriter>,
        pending_journal: Arc<PendingJournal>,
    ) -> Self {
        Self {
            lane,
            cfg,
            queue: Mutex::new(VecDeque::new()),
            notify: Condvar::new(),
            db: Mutex::new(db),
            stats: LaneStats::default(),
            points_cache,
            dedupe,
            dead_letters,
            pending_journal,
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

                let success = match result {
                    Ok(()) => {
                        self.stats.executed_writes.fetch_add(1, Ordering::Relaxed);
                        global_stats.executed_writes.fetch_add(1, Ordering::Relaxed);
                        if kind == "whaletracker_main" {
                            if let Some(points_cache) = &self.points_cache {
                                if let Err(err) = points_cache.mark_dirty("stats_write", false) {
                                    eprintln!(
                                        "[points-cache] failed to mark dirty from lane {}: {}",
                                        self.lane.label(),
                                        err
                                    );
                                }
                            }
                        }
                        println!(
                            "[sql-sink:{}] exec ok kind={} batch_id={:?}",
                            self.lane.label(),
                            kind,
                            item.source_batch_id
                        );
                        if let Some(event_id) = &item.event_id {
                            if let Err(err) = self.pending_journal.append_done(event_id) {
                                eprintln!(
                                    "[sql-sink:{}] failed to mark journal done event_id={}: {}",
                                    self.lane.label(),
                                    event_id,
                                    err
                                );
                            }
                            self.dedupe.remember(event_id);
                        }
                        true
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
                        self.dead_letters.record(
                            "db_error",
                            Some(&err),
                            &item.sql,
                            item.user_id,
                            item.source_batch_id,
                            item.event_id.as_deref(),
                        );
                        false
                    }
                };
                if let Some(completion) = &item.completion {
                    completion.finish(success);
                }
            }
        }
    }
}

struct SqlSink {
    lanes: Vec<Arc<LaneWorker>>,
    stats: Arc<SinkStats>,
    dedupe: Arc<DedupeCache>,
    dead_letters: Arc<DeadLetterWriter>,
    pending_journal: Arc<PendingJournal>,
}

impl SqlSink {
    fn new(
        db_cfg: &DbConfig,
        cfg: SinkConfig,
        points_cache: Option<Arc<PointsCacheManager>>,
    ) -> Result<Self, String> {
        let mut lanes = Vec::with_capacity(SqlLane::ALL.len());
        let dedupe = Arc::new(DedupeCache::new(cfg.max_dedupe_events));
        let dead_letters = Arc::new(DeadLetterWriter::from_env());
        let pending_journal = Arc::new(PendingJournal::from_env()?);
        for lane in SqlLane::ALL {
            let db = MysqlDbExecutor::connect(db_cfg)?;
            let lane_points_cache = if lane == SqlLane::Stats {
                points_cache.clone()
            } else {
                None
            };
            lanes.push(Arc::new(LaneWorker::new(
                lane,
                Box::new(db),
                cfg.clone(),
                lane_points_cache,
                Arc::clone(&dedupe),
                Arc::clone(&dead_letters),
                Arc::clone(&pending_journal),
            )));
        }
        Ok(Self {
            lanes,
            stats: Arc::new(SinkStats::default()),
            dedupe,
            dead_letters,
            pending_journal,
        })
    }

    fn spawn_workers(self: &Arc<Self>) {
        for lane in &self.lanes {
            let lane = Arc::clone(lane);
            let global_stats = Arc::clone(&self.stats);
            thread::spawn(move || lane.worker_loop(global_stats));
        }
    }

    fn replay_pending_journal(&self) -> Result<(), String> {
        let replay = self.pending_journal.load_replay_state(&self.dedupe);
        self.pending_journal.compact_from_state(&replay)?;
        self.stats
            .journal_compactions
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .journal_pending_startup
            .store(replay.pending.len() as u64, Ordering::Relaxed);
        self.stats
            .journal_done_records_startup
            .store(replay.done_records as u64, Ordering::Relaxed);
        self.stats
            .journal_bad_lines_startup
            .store(replay.bad_lines as u64, Ordering::Relaxed);
        if replay.pending.is_empty() {
            println!(
                "[sql-sink] pending journal replay: pending=0 recent_done={} done_records={} bad_lines={}",
                replay.recent_done.len(),
                replay.done_records,
                replay.bad_lines
            );
            return Ok(());
        }

        let total = replay.pending.len();
        let mut replayed = 0usize;
        for write in replay.pending {
            if self.enqueue_replayed_write(write)? {
                replayed += 1;
            }
        }
        self.stats
            .journal_replayed_startup
            .store(replayed as u64, Ordering::Relaxed);
        println!(
            "[sql-sink] pending journal replay: replayed={}/{} recent_done={} done_records={} bad_lines={}",
            replayed,
            total,
            replay.recent_done.len(),
            replay.done_records,
            replay.bad_lines
        );
        Ok(())
    }

    fn enqueue_replayed_write(&self, write: JournalPendingWrite) -> Result<bool, String> {
        if self.dedupe.contains(&write.event_id) {
            return Ok(false);
        }
        if let Err(reason) = validate_sql_write(&write.sql) {
            self.stats.dropped_writes.fetch_add(1, Ordering::Relaxed);
            self.dead_letters.record(
                "journal_invalid_write",
                Some(&reason),
                &write.sql,
                write.user_id,
                write.batch_id,
                Some(&write.event_id),
            );
            if let Err(err) = self.pending_journal.append_done(&write.event_id) {
                eprintln!(
                    "[sql-sink] failed to suppress invalid replay event_id={}: {}",
                    write.event_id, err
                );
            }
            return Ok(false);
        }

        let queued = self.queue_depth();
        let queue_limit = self.lanes[0].cfg.max_queue_rows;
        if queued.saturating_add(1) > queue_limit {
            return Err(format!(
                "pending journal replay would exceed queue limit: queued={} limit={}",
                queued, queue_limit
            ));
        }

        let lane_kind = Self::lane_for_sql(&write.sql);
        let lane = self.lane_by_kind(lane_kind);
        let mut q = lane.queue.lock().expect("lane queue mutex poisoned");
        q.push_back(QueuedSqlWrite {
            sql: write.sql,
            user_id: write.user_id,
            event_id: Some(write.event_id),
            source_batch_id: write.batch_id,
            enqueued_at_ms: write.ts_ms as u128,
            completion: None,
        });
        drop(q);
        lane.stats.accepted_writes.fetch_add(1, Ordering::Relaxed);
        self.stats.accepted_writes.fetch_add(1, Ordering::Relaxed);
        lane.notify.notify_one();
        Ok(true)
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

    fn enqueue_batch(
        &self,
        batch_id: Option<i64>,
        writes: Vec<SqlWrite>,
    ) -> Result<EnqueueResult, EnqueueError> {
        let now_ms = now_ms();
        let mut accepted_writes = Vec::new();
        let mut touched_online = false;
        let mut touched_stats = false;
        let mut touched_logs = false;

        for write in writes {
            let sql = write.sql.trim();
            if sql.is_empty() {
                self.stats.dropped_writes.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            if let Err(reason) = validate_sql_write(sql) {
                self.stats.dropped_writes.fetch_add(1, Ordering::Relaxed);
                eprintln!(
                    "[sql-sink] drop invalid write kind={} user_id={:?} batch_id={:?}: {} | sql={}",
                    classify_sql(sql),
                    write.user_id,
                    batch_id,
                    reason,
                    preview_sql(sql, 256)
                );
                self.dead_letters.record(
                    "invalid_write",
                    Some(&reason),
                    sql,
                    write.user_id,
                    batch_id,
                    write.event_id.as_deref(),
                );
                continue;
            }

            if let Some(event_id) = write.event_id.as_deref() {
                if self.dedupe.contains(event_id) {
                    self.stats.dropped_writes.fetch_add(1, Ordering::Relaxed);
                    println!(
                        "[sql-sink] drop duplicate event_id={} user_id={:?} batch_id={:?}",
                        event_id, write.user_id, batch_id
                    );
                    continue;
                }
            }

            let lane_kind = Self::lane_for_sql(sql);
            accepted_writes.push((write, lane_kind));
        }

        let accepted = accepted_writes.len();
        let queued = self.queue_depth();
        let queue_limit = self.lanes[0].cfg.max_queue_rows;
        if accepted > 0 && queued.saturating_add(accepted) > queue_limit {
            self.stats
                .dropped_writes
                .fetch_add(accepted as u64, Ordering::Relaxed);
            return Err(EnqueueError {
                message: "queue full",
                queued,
                incoming: accepted,
                limit: queue_limit,
            });
        }

        for (write, _) in &accepted_writes {
            if let Some(event_id) = write.event_id.as_deref() {
                if let Err(err) = self.pending_journal.append_pending(
                    event_id,
                    &write.sql,
                    write.user_id,
                    batch_id,
                ) {
                    self.stats
                        .dropped_writes
                        .fetch_add(accepted as u64, Ordering::Relaxed);
                    eprintln!(
                        "[sql-sink] failed to append pending journal event_id={} batch_id={:?}: {}",
                        event_id, batch_id, err
                    );
                    return Err(EnqueueError {
                        message: "journal write failed",
                        queued,
                        incoming: accepted,
                        limit: queue_limit,
                    });
                }
            }
        }

        let completion = (accepted > 0).then(|| Arc::new(BatchCompletion::new(accepted)));

        for (write, lane_kind) in accepted_writes {
            let lane = self.lane_by_kind(lane_kind);
            let mut q = lane.queue.lock().expect("lane queue mutex poisoned");
            q.push_back(QueuedSqlWrite {
                sql: write.sql,
                user_id: write.user_id,
                event_id: write.event_id,
                source_batch_id: batch_id,
                enqueued_at_ms: now_ms,
                completion: completion.clone(),
            });
            drop(q);
            lane.stats.accepted_writes.fetch_add(1, Ordering::Relaxed);
            match lane_kind {
                SqlLane::Online => touched_online = true,
                SqlLane::Stats => touched_stats = true,
                SqlLane::Logs => touched_logs = true,
            }
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

        Ok(EnqueueResult {
            accepted,
            completion,
        })
    }

    fn queue_depth(&self) -> usize {
        self.lanes.iter().map(|lane| lane.queue_depth()).sum()
    }

    fn lane_queue_depths(&self) -> (usize, usize, usize) {
        (
            self.lane_by_kind(SqlLane::Online).queue_depth(),
            self.lane_by_kind(SqlLane::Stats).queue_depth(),
            self.lane_by_kind(SqlLane::Logs).queue_depth(),
        )
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InboundMessage {
    Hello {
        service: Option<String>,
        proto: Option<u32>,
        server_id: Option<String>,
        auth: Option<String>,
        ts: Option<i64>,
    },
    SqlBatch {
        batch_id: Option<i64>,
        sent_at: Option<i64>,
        writes: Vec<InboundWrite>,
    },
    Health,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum InboundWrite {
    Raw(SqlWrite),
    Typed(TypedWrite),
}

#[derive(Debug, Deserialize)]
struct SqlWrite {
    sql: String,
    #[serde(default)]
    user_id: Option<u32>,
    #[serde(default)]
    event_id: Option<String>,
    #[serde(default)]
    force_sync: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct TypedWrite {
    kind: String,
    #[serde(default)]
    steamid: Option<String>,
    #[serde(default)]
    host_port: Option<u16>,
    #[serde(default)]
    user_id: Option<u32>,
    #[serde(default)]
    event_id: Option<String>,
}

fn materialize_inbound_writes(writes: Vec<InboundWrite>) -> Result<Vec<SqlWrite>, String> {
    writes.into_iter().map(materialize_inbound_write).collect()
}

fn materialize_inbound_write(write: InboundWrite) -> Result<SqlWrite, String> {
    match write {
        InboundWrite::Raw(write) => Ok(write),
        InboundWrite::Typed(write) => write.into_sql_write(),
    }
}

impl TypedWrite {
    fn into_sql_write(self) -> Result<SqlWrite, String> {
        let sql = match self.kind.as_str() {
            "online_remove" => {
                let steamid = validate_steamid64(
                    self.steamid
                        .as_deref()
                        .ok_or_else(|| "online_remove missing steamid".to_string())?,
                )?;
                let host_port = self
                    .host_port
                    .ok_or_else(|| "online_remove missing host_port".to_string())?;
                format!(
                    "DELETE FROM whaletracker_online WHERE steamid = '{}' AND host_port = {}",
                    sql_escape_string(steamid),
                    host_port
                )
            }
            "online_clear_host" => {
                let host_port = self
                    .host_port
                    .ok_or_else(|| "online_clear_host missing host_port".to_string())?;
                format!("DELETE FROM whaletracker_online WHERE host_port = {}", host_port)
            }
            "server_clear_port" => {
                let host_port = self
                    .host_port
                    .ok_or_else(|| "server_clear_port missing host_port".to_string())?;
                format!("DELETE FROM whaletracker_servers WHERE port = {}", host_port)
            }
            other => return Err(format!("unknown typed write kind: {}", other)),
        };

        Ok(SqlWrite {
            sql,
            user_id: self.user_id,
            event_id: self.event_id,
            force_sync: None,
        })
    }
}

fn validate_steamid64(value: &str) -> Result<&str, String> {
    if value.len() < 16 || value.len() > 20 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("invalid steamid64: {}", value));
    }
    Ok(value)
}

fn sql_escape_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch == '\\' || ch == '\'' {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
}

#[derive(Debug, Serialize)]
struct AckResponse<'a> {
    r#type: &'a str,
    batch_id: Option<i64>,
    accepted: usize,
    executed: usize,
    db_errors: usize,
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
    online_queue_depth: usize,
    stats_queue_depth: usize,
    logs_queue_depth: usize,
    dedupe_events: usize,
    accepted_writes: u64,
    executed_writes: u64,
    db_errors: u64,
    parse_errors: u64,
    dropped_writes: u64,
    journal_pending_startup: u64,
    journal_replayed_startup: u64,
    journal_done_records_startup: u64,
    journal_bad_lines_startup: u64,
    journal_compactions: u64,
    ts: u64,
}

fn main() -> std::io::Result<()> {
    let bind_addr = std::env::var("WT_RUST_BIND").unwrap_or_else(|_| DEFAULT_BIND.to_string());
    let bind_port = bind_addr
        .rsplit(':')
        .next()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(DEFAULT_POINTS_CACHE_OWNER_PORT);
    let flush_interval_ms = std::env::var("WT_RUST_FLUSH_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_FLUSH_INTERVAL_MS);
    let max_batch_rows = std::env::var("WT_RUST_MAX_BATCH_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_BATCH_ROWS);
    let max_queue_rows = std::env::var("WT_RUST_MAX_QUEUE_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_QUEUE_ROWS)
        .clamp(1, 1_000_000);
    let max_dedupe_events = std::env::var("WT_RUST_DEDUPE_EVENTS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_DEDUPE_EVENTS)
        .min(1_000_000);
    let max_frame_bytes = std::env::var("WT_RUST_MAX_FRAME_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_FRAME_BYTES)
        .clamp(1024, 1024 * 1024);
    let max_inbound_writes = std::env::var("WT_RUST_MAX_INBOUND_WRITES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_INBOUND_WRITES)
        .clamp(1, 4096);
    let max_sql_bytes = std::env::var("WT_RUST_MAX_SQL_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_SQL_BYTES)
        .clamp(256, 1024 * 1024);
    let auth_token = std::env::var("WT_RUST_AUTH_TOKEN").unwrap_or_default();
    let protocol_cfg = ProtocolConfig {
        max_frame_bytes,
        max_inbound_writes,
        max_sql_bytes,
        auth_token,
    };
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

    let points_cache_cfg = PointsCacheConfig::from_env();
    let schema = SchemaManager::new(
        &db_cfg,
        bind_port,
        points_cache_cfg.owner_port,
        DEFAULT_SCHEMA_POLL_MS,
    )
    .map_err(|e| std::io::Error::other(format!("schema init failed: {e}")))?;
    schema
        .prepare()
        .map_err(|e| std::io::Error::other(format!("schema prepare failed: {e}")))?;

    let points_cache = Arc::new(
        PointsCacheManager::new(&db_cfg, bind_port, points_cache_cfg.clone())
            .map_err(|e| std::io::Error::other(format!("points cache init failed: {e}")))?,
    );

    let sink = Arc::new(
        SqlSink::new(
            &db_cfg,
            SinkConfig {
                flush_interval: Duration::from_millis(flush_interval_ms.max(1)),
                max_batch_rows: max_batch_rows.max(1),
                max_queue_rows,
                max_dedupe_events,
            },
            Some(Arc::clone(&points_cache)),
        )
        .map_err(|e| std::io::Error::other(format!("db init failed: {e}")))?,
    );
    println!(
        "[sql-sink] mysql connection pools ready (lanes=3: online, stats, logs, max_queue_rows={}, max_dedupe_events={}, max_frame_bytes={}, max_inbound_writes={}, max_sql_bytes={}, auth_required={})",
        max_queue_rows,
        max_dedupe_events,
        protocol_cfg.max_frame_bytes,
        protocol_cfg.max_inbound_writes,
        protocol_cfg.max_sql_bytes,
        protocol_auth_required(&protocol_cfg) as u8
    );
    sink.replay_pending_journal()
        .map_err(|e| std::io::Error::other(format!("pending journal replay failed: {e}")))?;
    sink.spawn_workers();
    points_cache.spawn_worker();

    let listener = TcpListener::bind(&bind_addr)?;
    println!("[sql-sink] listening on {}", bind_addr);

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let sink = Arc::clone(&sink);
                let protocol_cfg = protocol_cfg.clone();
                thread::spawn(move || {
                    if let Err(e) = handle_client(stream, sink, protocol_cfg) {
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

fn handle_client(
    stream: TcpStream,
    sink: Arc<SqlSink>,
    protocol_cfg: ProtocolConfig,
) -> std::io::Result<()> {
    let peer = stream.peer_addr().ok();
    let reader_stream = stream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);
    let mut writer = stream;

    println!("[sql-sink] client connected: {:?}", peer);
    let mut authenticated = !protocol_auth_required(&protocol_cfg);

    let mut buf = Vec::with_capacity(4096);
    loop {
        match read_protocol_frame(&mut reader, protocol_cfg.max_frame_bytes, &mut buf)? {
            FrameRead::Eof => break,
            FrameRead::TooLong => {
                sink.stats.parse_errors.fetch_add(1, Ordering::Relaxed);
                eprintln!(
                    "[sql-sink] frame too large from {:?}: max_frame_bytes={}",
                    peer, protocol_cfg.max_frame_bytes
                );
                send_json_line(
                    &mut writer,
                    &ErrorResponse {
                        r#type: "error",
                        message: "frame too large",
                        ts: now_secs(),
                    },
                )?;
                continue;
            }
            FrameRead::Line => {
                if buf.is_empty() {
                    continue;
                }
            }
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
                auth,
                ts,
            }) => {
                if !protocol_auth_matches(&protocol_cfg, auth.as_deref()) {
                    sink.stats.parse_errors.fetch_add(1, Ordering::Relaxed);
                    eprintln!("[sql-sink] unauthorized hello from {:?}", peer);
                    send_json_line(
                        &mut writer,
                        &ErrorResponse {
                            r#type: "error",
                            message: "unauthorized",
                            ts: now_secs(),
                        },
                    )?;
                    break;
                }
                authenticated = true;
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
                if !authenticated {
                    sink.stats.parse_errors.fetch_add(1, Ordering::Relaxed);
                    send_json_line(
                        &mut writer,
                        &ErrorResponse {
                            r#type: "error",
                            message: "hello required",
                            ts: now_secs(),
                        },
                    )?;
                    continue;
                }
                let total = writes.len();
                let writes = match materialize_inbound_writes(writes) {
                    Ok(writes) => writes,
                    Err(err) => {
                        sink.stats.parse_errors.fetch_add(1, Ordering::Relaxed);
                        eprintln!(
                            "[sql-sink] rejected batch {:?} from {:?}: {}",
                            batch_id, peer, err
                        );
                        send_json_line(
                            &mut writer,
                            &ErrorResponse {
                                r#type: "error",
                                message: "invalid typed write",
                                ts: now_secs(),
                            },
                        )?;
                        continue;
                    }
                };
                if let Err(message) = validate_inbound_batch_shape(&protocol_cfg, &writes) {
                    sink.stats.parse_errors.fetch_add(1, Ordering::Relaxed);
                    eprintln!(
                        "[sql-sink] rejected batch {:?} from {:?}: {} (writes={}, max_writes={}, max_sql_bytes={})",
                        batch_id,
                        peer,
                        message,
                        writes.len(),
                        protocol_cfg.max_inbound_writes,
                        protocol_cfg.max_sql_bytes
                    );
                    send_json_line(
                        &mut writer,
                        &ErrorResponse {
                            r#type: "error",
                            message,
                            ts: now_secs(),
                        },
                    )?;
                    continue;
                }
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
                let enqueue = match sink.enqueue_batch(batch_id, writes) {
                    Ok(enqueue) => enqueue,
                    Err(err) => {
                        eprintln!(
                            "[sql-sink] rejected batch {:?} from {:?}: {} (queued={}, incoming={}, limit={})",
                            batch_id, peer, err.message, err.queued, err.incoming, err.limit
                        );
                        send_json_line(
                            &mut writer,
                            &ErrorResponse {
                                r#type: "error",
                                message: err.message,
                                ts: now_secs(),
                            },
                        )?;
                        continue;
                    }
                };
                let completion = enqueue
                    .completion
                    .as_ref()
                    .map(|completion| completion.wait())
                    .unwrap_or(BatchCompletionSnapshot {
                        executed: 0,
                        db_errors: 0,
                    });
                if let Some(sent_at) = sent_at {
                    println!(
                        "[sql-sink] batch {:?}: accepted {}/{} writes, executed={}, db_errors={} (queue_depth={}, sent_at={}, online_related={})",
                        batch_id,
                        enqueue.accepted,
                        total,
                        completion.executed,
                        completion.db_errors,
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
                        accepted: enqueue.accepted,
                        executed: completion.executed,
                        db_errors: completion.db_errors,
                        queue_depth: sink.queue_depth(),
                        ts: now_secs(),
                    },
                )?;
            }
            Ok(InboundMessage::Health) => {
                if !authenticated {
                    sink.stats.parse_errors.fetch_add(1, Ordering::Relaxed);
                    send_json_line(
                        &mut writer,
                        &ErrorResponse {
                            r#type: "error",
                            message: "hello required",
                            ts: now_secs(),
                        },
                    )?;
                    continue;
                }
                let (online_queue_depth, stats_queue_depth, logs_queue_depth) =
                    sink.lane_queue_depths();
                send_json_line(
                    &mut writer,
                    &HealthResponse {
                        r#type: "health",
                        queue_depth: sink.queue_depth(),
                        online_queue_depth,
                        stats_queue_depth,
                        logs_queue_depth,
                        dedupe_events: sink.dedupe.len(),
                        accepted_writes: sink.stats.accepted_writes.load(Ordering::Relaxed),
                        executed_writes: sink.stats.executed_writes.load(Ordering::Relaxed),
                        db_errors: sink.stats.db_errors.load(Ordering::Relaxed),
                        parse_errors: sink.stats.parse_errors.load(Ordering::Relaxed),
                        dropped_writes: sink.stats.dropped_writes.load(Ordering::Relaxed),
                        journal_pending_startup: sink
                            .stats
                            .journal_pending_startup
                            .load(Ordering::Relaxed),
                        journal_replayed_startup: sink
                            .stats
                            .journal_replayed_startup
                            .load(Ordering::Relaxed),
                        journal_done_records_startup: sink
                            .stats
                            .journal_done_records_startup
                            .load(Ordering::Relaxed),
                        journal_bad_lines_startup: sink
                            .stats
                            .journal_bad_lines_startup
                            .load(Ordering::Relaxed),
                        journal_compactions: sink.stats.journal_compactions.load(Ordering::Relaxed),
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

#[derive(Debug, PartialEq, Eq)]
enum FrameRead {
    Line,
    Eof,
    TooLong,
}

fn read_protocol_frame<R: BufRead>(
    reader: &mut R,
    max_frame_bytes: usize,
    out: &mut Vec<u8>,
) -> std::io::Result<FrameRead> {
    out.clear();

    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            if out.is_empty() {
                return Ok(FrameRead::Eof);
            }
            trim_frame_cr(out);
            return Ok(FrameRead::Line);
        }

        let newline_pos = available.iter().position(|&b| b == b'\n');
        let take_len = newline_pos.unwrap_or(available.len());

        if out.len().saturating_add(take_len) > max_frame_bytes {
            let consume_len = newline_pos.map(|pos| pos + 1).unwrap_or(available.len());
            reader.consume(consume_len);
            if newline_pos.is_none() {
                drain_protocol_frame(reader)?;
            }
            out.clear();
            return Ok(FrameRead::TooLong);
        }

        out.extend_from_slice(&available[..take_len]);
        let consume_len = newline_pos.map(|pos| pos + 1).unwrap_or(available.len());
        reader.consume(consume_len);

        if newline_pos.is_some() {
            trim_frame_cr(out);
            return Ok(FrameRead::Line);
        }
    }
}

fn drain_protocol_frame<R: BufRead>(reader: &mut R) -> std::io::Result<()> {
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(());
        }

        if let Some(newline_pos) = available.iter().position(|&b| b == b'\n') {
            reader.consume(newline_pos + 1);
            return Ok(());
        }

        let consume_len = available.len();
        reader.consume(consume_len);
    }
}

fn trim_frame_cr(frame: &mut Vec<u8>) {
    if frame.last().copied() == Some(b'\r') {
        frame.pop();
    }
}

fn protocol_auth_required(cfg: &ProtocolConfig) -> bool {
    !cfg.auth_token.trim().is_empty()
}

fn protocol_auth_matches(cfg: &ProtocolConfig, provided: Option<&str>) -> bool {
    if !protocol_auth_required(cfg) {
        return true;
    }
    provided == Some(cfg.auth_token.as_str())
}

fn validate_inbound_batch_shape(
    cfg: &ProtocolConfig,
    writes: &[SqlWrite],
) -> Result<(), &'static str> {
    if writes.len() > cfg.max_inbound_writes {
        return Err("too many writes");
    }
    if writes.iter().any(|write| write.sql.len() > cfg.max_sql_bytes) {
        return Err("sql too large");
    }
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

fn now_ms_u64() -> u64 {
    now_ms().min(u64::MAX as u128) as u64
}

fn preview_sql(sql: &str, max_len: usize) -> String {
    if sql.len() <= max_len {
        return sql.to_string();
    }
    let mut out = sql[..max_len].to_string();
    out.push_str("...");
    out
}

fn validate_sql_write(sql: &str) -> Result<(), String> {
    if classify_sql(sql) == "whaletracker_log_players" {
        validate_log_player_write(sql)?;
    }
    Ok(())
}

fn validate_log_player_write(sql: &str) -> Result<(), String> {
    let Some((columns, values)) = parse_insert_columns_values(sql, "whaletracker_log_players") else {
        return Ok(());
    };

    let damage = sql_int_column(&columns, &values, "damage")
        .ok_or_else(|| "missing log-player damage column".to_string())?;
    let damage_taken = sql_int_column(&columns, &values, "damage_taken")
        .ok_or_else(|| "missing log-player damage_taken column".to_string())?;
    let playtime = sql_int_column(&columns, &values, "playtime")
        .ok_or_else(|| "missing log-player playtime column".to_string())?;

    if !is_log_rate_plausible(damage, playtime) {
        return Err(format!(
            "implausible log-player damage rate: damage={} playtime={} max_dpm={}",
            damage, playtime, MAX_LOG_DAMAGE_PER_MINUTE
        ));
    }
    if !is_log_rate_plausible(damage_taken, playtime) {
        return Err(format!(
            "implausible log-player damage-taken rate: damage_taken={} playtime={} max_dpm={}",
            damage_taken, playtime, MAX_LOG_DAMAGE_PER_MINUTE
        ));
    }

    Ok(())
}

fn is_log_rate_plausible(amount: i64, playtime: i64) -> bool {
    if amount < 0 || playtime < 0 {
        return false;
    }
    if amount == 0 {
        return true;
    }
    if playtime <= 0 {
        return false;
    }
    (amount as f64 * 60.0 / playtime as f64) <= MAX_LOG_DAMAGE_PER_MINUTE
}

fn parse_insert_columns_values(sql: &str, table: &str) -> Option<(Vec<String>, Vec<String>)> {
    let trimmed = sql.trim_start();
    let prefix = format!("insert into {}", table);
    if !trimmed.to_ascii_lowercase().starts_with(&prefix) {
        return None;
    }

    let columns_open = trimmed.find('(')?;
    let columns_close = find_matching_paren(trimmed, columns_open)?;
    let after_columns = columns_close + 1;
    let values_rel = find_ascii_case_insensitive(&trimmed[after_columns..], "values")?;
    let values_pos = after_columns + values_rel;
    let values_open = trimmed[values_pos..].find('(')? + values_pos;
    let values_close = find_matching_paren(trimmed, values_open)?;

    let columns = split_sql_list(&trimmed[columns_open + 1..columns_close])
        .into_iter()
        .map(|column| normalize_sql_column(&column))
        .collect::<Vec<_>>();
    let values = split_sql_list(&trimmed[values_open + 1..values_close]);

    if columns.len() != values.len() {
        return None;
    }
    Some((columns, values))
}

fn find_ascii_case_insensitive(haystack: &str, needle: &str) -> Option<usize> {
    haystack
        .to_ascii_lowercase()
        .find(&needle.to_ascii_lowercase())
}

fn find_matching_paren(sql: &str, open: usize) -> Option<usize> {
    let mut in_quote = false;
    let mut escaped = false;
    let mut depth = 0i32;

    for (idx, ch) in sql.char_indices().skip_while(|(idx, _)| *idx < open) {
        if in_quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '\'' {
                in_quote = false;
            }
            continue;
        }

        if ch == '\'' {
            in_quote = true;
            continue;
        }
        if ch == '(' {
            depth += 1;
            continue;
        }
        if ch == ')' {
            depth -= 1;
            if depth == 0 {
                return Some(idx);
            }
        }
    }
    None
}

fn split_sql_list(input: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut in_quote = false;
    let mut escaped = false;
    let mut depth = 0i32;

    for (idx, ch) in input.char_indices() {
        if in_quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '\'' {
                in_quote = false;
            }
            continue;
        }

        match ch {
            '\'' => in_quote = true,
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(input[start..idx].trim().to_string());
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(input[start..].trim().to_string());
    parts
}

fn normalize_sql_column(column: &str) -> String {
    column
        .trim()
        .trim_matches('`')
        .trim()
        .to_ascii_lowercase()
}

fn sql_int_column(columns: &[String], values: &[String], column: &str) -> Option<i64> {
    let column = column.to_ascii_lowercase();
    let index = columns.iter().position(|candidate| candidate == &column)?;
    parse_sql_i64(values.get(index)?)
}

fn parse_sql_i64(value: &str) -> Option<i64> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("null") {
        return Some(0);
    }
    value.trim_matches('\'').parse::<i64>().ok()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_plausible_log_player_insert() {
        let sql = "INSERT INTO whaletracker_log_players (log_id, steamid, personaname, damage, damage_taken, playtime) VALUES ('log', 'steam', 'name', 2400, 1200, 120)";

        assert!(validate_sql_write(sql).is_ok());
    }

    #[test]
    fn rejects_implausible_log_player_dpm() {
        let sql = "INSERT INTO whaletracker_log_players (log_id, steamid, personaname, damage, damage_taken, playtime) VALUES ('log', 'steam', 'name', 4738, 0, 22)";

        assert!(validate_sql_write(sql).is_err());
    }

    #[test]
    fn reads_protocol_frame_with_crlf() {
        let input = std::io::Cursor::new(b"{\"type\":\"health\"}\r\n".to_vec());
        let mut reader = std::io::BufReader::new(input);
        let mut out = Vec::new();

        assert_eq!(
            read_protocol_frame(&mut reader, 64, &mut out).unwrap(),
            FrameRead::Line
        );
        assert_eq!(std::str::from_utf8(&out).unwrap(), "{\"type\":\"health\"}");
    }

    #[test]
    fn rejects_and_drains_oversized_protocol_frame() {
        let input = std::io::Cursor::new(b"abcdef\nok\n".to_vec());
        let mut reader = std::io::BufReader::new(input);
        let mut out = Vec::new();

        assert_eq!(
            read_protocol_frame(&mut reader, 4, &mut out).unwrap(),
            FrameRead::TooLong
        );
        assert!(out.is_empty());
        assert_eq!(
            read_protocol_frame(&mut reader, 4, &mut out).unwrap(),
            FrameRead::Line
        );
        assert_eq!(std::str::from_utf8(&out).unwrap(), "ok");
    }

    #[test]
    fn optional_protocol_auth_matches_only_when_configured() {
        let open_cfg = ProtocolConfig {
            max_frame_bytes: 64,
            max_inbound_writes: 2,
            max_sql_bytes: 64,
            auth_token: String::new(),
        };
        assert!(protocol_auth_matches(&open_cfg, None));

        let locked_cfg = ProtocolConfig {
            max_frame_bytes: 64,
            max_inbound_writes: 2,
            max_sql_bytes: 64,
            auth_token: "secret".to_string(),
        };
        assert!(!protocol_auth_matches(&locked_cfg, None));
        assert!(!protocol_auth_matches(&locked_cfg, Some("wrong")));
        assert!(protocol_auth_matches(&locked_cfg, Some("secret")));
    }

    #[test]
    fn rejects_oversized_inbound_batches_and_writes() {
        let cfg = ProtocolConfig {
            max_frame_bytes: 128,
            max_inbound_writes: 1,
            max_sql_bytes: 8,
            auth_token: String::new(),
        };
        let ok_write = SqlWrite {
            sql: "SELECT 1".to_string(),
            user_id: None,
            event_id: None,
            force_sync: None,
        };
        assert!(validate_inbound_batch_shape(&cfg, &[ok_write]).is_ok());

        let too_many = vec![
            SqlWrite {
                sql: "SELECT 1".to_string(),
                user_id: None,
                event_id: None,
                force_sync: None,
            },
            SqlWrite {
                sql: "SELECT 2".to_string(),
                user_id: None,
                event_id: None,
                force_sync: None,
            },
        ];
        assert_eq!(
            validate_inbound_batch_shape(&cfg, &too_many).unwrap_err(),
            "too many writes"
        );

        let too_large = vec![SqlWrite {
            sql: "SELECT 123".to_string(),
            user_id: None,
            event_id: None,
            force_sync: None,
        }];
        assert_eq!(
            validate_inbound_batch_shape(&cfg, &too_large).unwrap_err(),
            "sql too large"
        );
    }

    #[test]
    fn materializes_typed_online_writes() {
        let writes = materialize_inbound_writes(vec![
            InboundWrite::Typed(TypedWrite {
                kind: "online_remove".to_string(),
                steamid: Some("76561198115534197".to_string()),
                host_port: Some(27015),
                user_id: Some(7),
                event_id: Some("event-online-remove".to_string()),
            }),
            InboundWrite::Typed(TypedWrite {
                kind: "online_clear_host".to_string(),
                steamid: None,
                host_port: Some(27015),
                user_id: None,
                event_id: Some("event-online-clear".to_string()),
            }),
            InboundWrite::Typed(TypedWrite {
                kind: "server_clear_port".to_string(),
                steamid: None,
                host_port: Some(27015),
                user_id: None,
                event_id: Some("event-server-clear".to_string()),
            }),
        ])
        .unwrap();

        assert_eq!(
            writes[0].sql,
            "DELETE FROM whaletracker_online WHERE steamid = '76561198115534197' AND host_port = 27015"
        );
        assert_eq!(writes[0].user_id, Some(7));
        assert_eq!(writes[0].event_id.as_deref(), Some("event-online-remove"));
        assert_eq!(
            writes[1].sql,
            "DELETE FROM whaletracker_online WHERE host_port = 27015"
        );
        assert_eq!(writes[2].sql, "DELETE FROM whaletracker_servers WHERE port = 27015");
    }

    #[test]
    fn rejects_invalid_typed_online_write() {
        let err = materialize_inbound_writes(vec![InboundWrite::Typed(TypedWrite {
            kind: "online_remove".to_string(),
            steamid: Some("not-a-steamid".to_string()),
            host_port: Some(27015),
            user_id: None,
            event_id: None,
        })])
        .unwrap_err();

        assert!(err.contains("invalid steamid64"));
    }

    #[test]
    fn dedupe_cache_remembers_successful_events_with_eviction() {
        let cache = DedupeCache::new(2);
        assert!(!cache.contains("event-a"));

        cache.remember("event-a");
        cache.remember("event-b");
        assert!(cache.contains("event-a"));
        assert!(cache.contains("event-b"));

        cache.remember("event-c");
        assert!(!cache.contains("event-a"));
        assert!(cache.contains("event-b"));
        assert!(cache.contains("event-c"));
    }

    #[test]
    fn pending_journal_replays_only_unfinished_events() {
        let path = std::env::temp_dir().join(format!("whaletracker-journal-{}.log", now_ms_u64()));
        {
            let mut file = File::create(&path).unwrap();
            serde_json::to_writer(
                &mut file,
                &PendingJournalRecord::Pending {
                    event_id: "event-a".to_string(),
                    sql: "SELECT 1".to_string(),
                    user_id: Some(1),
                    batch_id: Some(10),
                    ts_ms: 100,
                },
            )
            .unwrap();
            file.write_all(b"\n").unwrap();
            serde_json::to_writer(
                &mut file,
                &PendingJournalRecord::Pending {
                    event_id: "event-b".to_string(),
                    sql: "SELECT 2".to_string(),
                    user_id: Some(2),
                    batch_id: Some(11),
                    ts_ms: 200,
                },
            )
            .unwrap();
            file.write_all(b"\n").unwrap();
            serde_json::to_writer(
                &mut file,
                &PendingJournalRecord::Done {
                    event_id: "event-a".to_string(),
                    ts_ms: 300,
                },
            )
            .unwrap();
            file.write_all(b"\n").unwrap();
        }

        let dedupe = DedupeCache::new(10);
        let state = PendingJournal::load_replay_state_from_path(path.to_str().unwrap(), &dedupe);
        std::fs::remove_file(path).unwrap();

        assert_eq!(state.pending.len(), 1);
        assert_eq!(state.pending[0].event_id, "event-b");
        assert_eq!(state.recent_done, vec!["event-a".to_string()]);
        assert_eq!(state.done_records, 1);
        assert!(dedupe.contains("event-a"));
        assert!(!dedupe.contains("event-b"));
    }

    #[test]
    fn pending_journal_compaction_keeps_pending_and_recent_done_only() {
        let path = std::env::temp_dir().join(format!(
            "whaletracker-journal-compact-{}-{}.log",
            std::process::id(),
            now_ms_u64()
        ));
        {
            let mut file = File::create(&path).unwrap();
            serde_json::to_writer(
                &mut file,
                &PendingJournalRecord::Pending {
                    event_id: "event-a".to_string(),
                    sql: "SELECT 1".to_string(),
                    user_id: Some(1),
                    batch_id: Some(10),
                    ts_ms: 100,
                },
            )
            .unwrap();
            file.write_all(b"\n").unwrap();
            serde_json::to_writer(
                &mut file,
                &PendingJournalRecord::Pending {
                    event_id: "event-b".to_string(),
                    sql: "SELECT 2".to_string(),
                    user_id: Some(2),
                    batch_id: Some(11),
                    ts_ms: 200,
                },
            )
            .unwrap();
            file.write_all(b"\n").unwrap();
            serde_json::to_writer(
                &mut file,
                &PendingJournalRecord::Done {
                    event_id: "event-a".to_string(),
                    ts_ms: 300,
                },
            )
            .unwrap();
            file.write_all(b"\nnot-json\n").unwrap();
        }

        let journal = PendingJournal::open_path(path.to_str().unwrap().to_string()).unwrap();
        let dedupe = DedupeCache::new(10);
        let state = journal.load_replay_state(&dedupe);
        journal.compact_from_state(&state).unwrap();

        let compacted = std::fs::read_to_string(&path).unwrap();
        std::fs::remove_file(path).unwrap();

        assert!(compacted.contains("event-a"));
        assert!(compacted.contains("event-b"));
        assert!(!compacted.contains("not-json"));
        assert_eq!(compacted.lines().count(), 2);
        assert_eq!(state.pending.len(), 1);
        assert_eq!(state.bad_lines, 1);
    }
}
