use mysql::{params, prelude::Queryable};
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
const DEFAULT_POINTS_CACHE_OWNER_PORT: u16 = 28017;
const DEFAULT_POINTS_CACHE_DEBOUNCE_MS: u64 = 3000;
const DEFAULT_POINTS_CACHE_POLL_MS: u64 = 1000;
const DEFAULT_POINTS_CACHE_TOUCH_MS: u64 = 1000;
const DEFAULT_SCHEMA_POLL_MS: u64 = 1000;
const WHALETRACKER_SCHEMA_VERSION: u32 = 3;
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
        "`medicKills` INTEGER DEFAULT 0".to_string(),
        "`heavyKills` INTEGER DEFAULT 0".to_string(),
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
        "`name` VARCHAR(128) DEFAULT ''".to_string(),
        "`name_color` VARCHAR(32) DEFAULT ''".to_string(),
        "`prename` VARCHAR(64) DEFAULT ''".to_string(),
        "`updated_at` INTEGER DEFAULT 0".to_string(),
    ];

    let chat_columns = vec![
        "`id` INTEGER NOT NULL AUTO_INCREMENT".to_string(),
        "`created_at` INTEGER NOT NULL".to_string(),
        "`steamid` VARCHAR(32) DEFAULT NULL".to_string(),
        "`personaname` VARCHAR(128) DEFAULT NULL".to_string(),
        "`iphash` VARCHAR(64) DEFAULT NULL".to_string(),
        "`message` TEXT NOT NULL".to_string(),
        "`server_ip` VARCHAR(64) DEFAULT NULL".to_string(),
        "`server_port` INTEGER DEFAULT NULL".to_string(),
        "`alert` TINYINT DEFAULT 1".to_string(),
    ];

    let chat_outbox_columns = vec![
        "`id` INTEGER NOT NULL AUTO_INCREMENT".to_string(),
        "`created_at` INTEGER NOT NULL".to_string(),
        "`iphash` VARCHAR(64) NOT NULL".to_string(),
        "`display_name` VARCHAR(128) DEFAULT ''".to_string(),
        "`message` TEXT NOT NULL".to_string(),
        "`server_ip` VARCHAR(64) DEFAULT NULL".to_string(),
        "`server_port` INTEGER DEFAULT NULL".to_string(),
        "`delivered_to` TEXT DEFAULT NULL".to_string(),
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
        build_create_table_sql(
            "whaletracker_chat",
            chat_columns.clone(),
            vec![
                "PRIMARY KEY (`id`)".to_string(),
                "KEY `idx_created_at` (`created_at`)".to_string(),
            ],
        ),
        build_create_table_sql(
            "whaletracker_chat_outbox",
            chat_outbox_columns.clone(),
            vec![
                "PRIMARY KEY (`id`)".to_string(),
                "KEY `idx_created_at` (`created_at`)".to_string(),
            ],
        ),
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
            name: "add_whaletracker_chat_tables",
            statements: vec![
                build_create_table_sql(
                    "whaletracker_chat",
                    chat_columns,
                    vec![
                        "PRIMARY KEY (`id`)".to_string(),
                        "KEY `idx_created_at` (`created_at`)".to_string(),
                    ],
                ),
                build_create_table_sql(
                    "whaletracker_chat_outbox",
                    chat_outbox_columns,
                    vec![
                        "PRIMARY KEY (`id`)".to_string(),
                        "KEY `idx_created_at` (`created_at`)".to_string(),
                    ],
                ),
                "ALTER TABLE whaletracker_chat ADD COLUMN IF NOT EXISTS alert TINYINT DEFAULT 1".to_string(),
                "ALTER TABLE whaletracker_chat CONVERT TO CHARACTER SET utf8mb4".to_string(),
                "ALTER TABLE whaletracker_chat_outbox CONVERT TO CHARACTER SET utf8mb4".to_string(),
            ],
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
            "INSERT INTO whaletracker_points_cache_build (steamid, points, rank, name, name_color, prename, updated_at) \
             SELECT base.steamid, base.points, COALESCE(ranked.rank, 0), base.name, base.color, base.prename, {now} \
             FROM (\
             SELECT w.steamid, {expr} AS points, \
             COALESCE(NULLIF(w.cached_personaname,''), NULLIF(w.personaname,''), COALESCE(NULLIF(c.name,''), w.steamid)) AS name, \
             COALESCE(NULLIF(f.color COLLATE utf8mb4_uca1400_ai_ci,''), COALESCE(NULLIF(c.name_color,''), 'gold')) AS color, \
             COALESCE((SELECT p.newname COLLATE utf8mb4_uca1400_ai_ci FROM prename_rules p WHERE p.pattern COLLATE utf8mb4_uca1400_ai_ci = w.steamid LIMIT 1), COALESCE(NULLIF(c.prename,''), '')) AS prename \
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
}

impl LaneWorker {
    fn new(
        lane: SqlLane,
        db: Box<dyn DbExecutor>,
        cfg: SinkConfig,
        points_cache: Option<Arc<PointsCacheManager>>,
    ) -> Self {
        Self {
            lane,
            cfg,
            queue: Mutex::new(VecDeque::new()),
            notify: Condvar::new(),
            db: Mutex::new(db),
            stats: LaneStats::default(),
            points_cache,
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
    fn new(
        db_cfg: &DbConfig,
        cfg: SinkConfig,
        points_cache: Option<Arc<PointsCacheManager>>,
    ) -> Result<Self, String> {
        let mut lanes = Vec::with_capacity(SqlLane::ALL.len());
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
            )));
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
            },
            Some(Arc::clone(&points_cache)),
        )
        .map_err(|e| std::io::Error::other(format!("db init failed: {e}")))?,
    );
    println!("[sql-sink] mysql connection pools ready (lanes=3: online, stats, logs)");
    sink.spawn_workers();
    points_cache.spawn_worker();

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
