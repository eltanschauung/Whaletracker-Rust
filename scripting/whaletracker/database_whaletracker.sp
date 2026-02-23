public void WhaleTracker_SQLConnect()
{
    if (g_hReconnectTimer != null)
    {
        CloseHandle(g_hReconnectTimer);
        g_hReconnectTimer = null;
    }

    if (g_hDatabase != null)
    {
        delete g_hDatabase;
        g_hDatabase = null;
    }

    g_bDatabaseReady = false;
    g_CvarDatabase.GetString(g_sDatabaseConfig, sizeof(g_sDatabaseConfig));
    g_bShuttingDown = false;
    Database.Connect(T_SQLConnect, g_sDatabaseConfig);
}

public void T_SQLConnect(Database db, const char[] error, any data)
{
    if (db == null)
    {
        LogError("[WhaleTracker] Database connection failed: %s", error);
        g_bDatabaseReady = false;
        WhaleTracker_ScheduleReconnect(5.0);
        return;
    }

    g_hDatabase = db;
    g_bDatabaseReady = true;

    if (!g_hDatabase.SetCharset("utf8mb4"))
    {
        LogError("[WhaleTracker] Failed to set database charset to utf8mb4, names may be truncated.");
    }

    char query[4096];
    Format(query, sizeof(query),
        "CREATE TABLE IF NOT EXISTS `whaletracker` ("
        ... "`steamid` VARCHAR(32) PRIMARY KEY,"
        ... "`first_seen` INTEGER,"
        ... "`kills` INTEGER DEFAULT 0,"
        ... "`deaths` INTEGER DEFAULT 0,"
        ... "`healing` INTEGER DEFAULT 0,"
        ... "`total_ubers` INTEGER DEFAULT 0,"
        ... "`medic_drops` INTEGER DEFAULT 0,"
        ... "`uber_drops` INTEGER DEFAULT 0,"
        ... "`airshots` INTEGER DEFAULT 0,"
        ... "`headshots` INTEGER DEFAULT 0,"
        ... "`backstabs` INTEGER DEFAULT 0,"
        ... "`best_killstreak` INTEGER DEFAULT 0,"
        ... "`assists` INTEGER DEFAULT 0,"
        ... "`playtime` INTEGER DEFAULT 0,"
        ... "`damage_dealt` INTEGER DEFAULT 0,"
        ... "`damage_taken` INTEGER DEFAULT 0,"
        ... "`shots_scatterguns` INTEGER DEFAULT 0,"
        ... "`hits_scatterguns` INTEGER DEFAULT 0,"
        ... "`shots_pistols` INTEGER DEFAULT 0,"
        ... "`hits_pistols` INTEGER DEFAULT 0,"
        ... "`shots_rocketlaunchers` INTEGER DEFAULT 0,"
        ... "`hits_rocketlaunchers` INTEGER DEFAULT 0,"
        ... "`shots_grenadelaunchers` INTEGER DEFAULT 0,"
        ... "`hits_grenadelaunchers` INTEGER DEFAULT 0,"
        ... "`shots_stickylaunchers` INTEGER DEFAULT 0,"
        ... "`hits_stickylaunchers` INTEGER DEFAULT 0,"
        ... "`shots_snipers` INTEGER DEFAULT 0,"
        ... "`hits_snipers` INTEGER DEFAULT 0,"
        ... "`shots_revolvers` INTEGER DEFAULT 0,"
        ... "`hits_revolvers` INTEGER DEFAULT 0"
        ... ")");
    g_hDatabase.Query(WhaleTracker_CreateTable, query);

    Format(query, sizeof(query), "CREATE INDEX IF NOT EXISTS `idx_last_seen` ON `whaletracker` (`last_seen`)");
    g_hDatabase.Query(WhaleTracker_CreateTable, query);

    Format(query, sizeof(query),
        "CREATE TABLE IF NOT EXISTS `whaletracker_online` ("
        ... "`steamid` VARCHAR(32) PRIMARY KEY,"
        ... "`personaname` VARCHAR(128) DEFAULT '',"
        ... "`class` TINYINT DEFAULT 0,"
        ... "`team` TINYINT DEFAULT 0,"
        ... "`alive` TINYINT DEFAULT 0,"
        ... "`is_spectator` TINYINT DEFAULT 0,"
        ... "`kills` INTEGER DEFAULT 0,"
        ... "`deaths` INTEGER DEFAULT 0,"
        ... "`assists` INTEGER DEFAULT 0,"
        ... "`damage` INTEGER DEFAULT 0,"
        ... "`damage_taken` INTEGER DEFAULT 0,"
        ... "`healing` INTEGER DEFAULT 0,"
        ... "`headshots` INTEGER DEFAULT 0,"
        ... "`backstabs` INTEGER DEFAULT 0,"
        ... "`playtime` INTEGER DEFAULT 0,"
        ... "`total_ubers` INTEGER DEFAULT 0,"
        ... "`best_streak` INTEGER DEFAULT 0,"
        ... "`visible_max` INTEGER DEFAULT 0,"
        ... "`time_connected` INTEGER DEFAULT 0,"
        ... "`classes_mask` INTEGER DEFAULT 0,"
        ... "`shots_shotguns` INTEGER DEFAULT 0,"
        ... "`hits_shotguns` INTEGER DEFAULT 0,"
        ... "`shots_scatterguns` INTEGER DEFAULT 0,"
        ... "`hits_scatterguns` INTEGER DEFAULT 0,"
        ... "`shots_pistols` INTEGER DEFAULT 0,"
        ... "`hits_pistols` INTEGER DEFAULT 0,"
        ... "`shots_rocketlaunchers` INTEGER DEFAULT 0,"
        ... "`hits_rocketlaunchers` INTEGER DEFAULT 0,"
        ... "`shots_grenadelaunchers` INTEGER DEFAULT 0,"
        ... "`hits_grenadelaunchers` INTEGER DEFAULT 0,"
        ... "`shots_stickylaunchers` INTEGER DEFAULT 0,"
        ... "`hits_stickylaunchers` INTEGER DEFAULT 0,"
        ... "`shots_snipers` INTEGER DEFAULT 0,"
        ... "`hits_snipers` INTEGER DEFAULT 0,"
        ... "`shots_revolvers` INTEGER DEFAULT 0,"
        ... "`hits_revolvers` INTEGER DEFAULT 0,"
        ... "`host_ip` VARCHAR(64) DEFAULT '',"
        ... "`host_port` INTEGER DEFAULT 0,"
        ... "`playercount` INTEGER DEFAULT 0,"
        ... "`map_name` VARCHAR(128) DEFAULT '',"
        ... "`last_update` INTEGER DEFAULT 0"
        ... ")");
    g_hDatabase.Query(WhaleTracker_CreateOnlineTable, query);

        Format(query, sizeof(query),
            "CREATE TABLE IF NOT EXISTS `whaletracker_servers` ("
            ... "`ip` VARCHAR(64) NOT NULL,"
            ... "`port` INTEGER NOT NULL,"
            ... "`playercount` INTEGER DEFAULT 0,"
            ... "`visible_max` INTEGER DEFAULT 0,"
            ... "`map` VARCHAR(128) DEFAULT '',"
            ... "`city` VARCHAR(128) DEFAULT '',"
            ... "`country` VARCHAR(8) DEFAULT '',"
            ... "`flags` VARCHAR(256) DEFAULT '',"
            ... "`last_update` INTEGER DEFAULT 0,"
            ... "PRIMARY KEY (`ip`, `port`)"
            ... ")");
    g_hDatabase.Query(WhaleTracker_CreateServersTable, query);

    if (WhaleTracker_ShouldUseMatchLogs())
    {
        Format(query, sizeof(query),
            "CREATE TABLE IF NOT EXISTS `whaletracker_logs` ("
            ... "`log_id` VARCHAR(64) PRIMARY KEY,"
            ... "`map` VARCHAR(64) DEFAULT '',"
            ... "`gamemode` VARCHAR(64) DEFAULT 'Unknown',"
            ... "`started_at` INTEGER DEFAULT 0,"
            ... "`ended_at` INTEGER DEFAULT 0,"
            ... "`duration` INTEGER DEFAULT 0,"
            ... "`player_count` INTEGER DEFAULT 0,"
            ... "`created_at` INTEGER DEFAULT 0,"
            ... "`updated_at` INTEGER DEFAULT 0"
            ... ")");
        g_hDatabase.Query(WhaleTracker_CreateLogsTable, query);

        Format(query, sizeof(query),
            "CREATE TABLE IF NOT EXISTS `whaletracker_log_players` ("
            ... "`log_id` VARCHAR(64) NOT NULL,"
            ... "`steamid` VARCHAR(32) NOT NULL,"
            ... "`personaname` VARCHAR(128) DEFAULT '',"
            ... "`kills` INTEGER DEFAULT 0,"
            ... "`deaths` INTEGER DEFAULT 0,"
            ... "`assists` INTEGER DEFAULT 0,"
            ... "`damage` INTEGER DEFAULT 0,"
            ... "`damage_taken` INTEGER DEFAULT 0,"
            ... "`healing` INTEGER DEFAULT 0,"
            ... "`headshots` INTEGER DEFAULT 0,"
            ... "`backstabs` INTEGER DEFAULT 0,"
            ... "`total_ubers` INTEGER DEFAULT 0,"
            ... "`playtime` INTEGER DEFAULT 0,"
            ... "`medic_drops` INTEGER DEFAULT 0,"
            ... "`uber_drops` INTEGER DEFAULT 0,"
            ... "`airshots` INTEGER DEFAULT 0,"
            ... "`shots_shotguns` INTEGER DEFAULT 0,"
            ... "`hits_shotguns` INTEGER DEFAULT 0,"
            ... "`shots_scatterguns` INTEGER DEFAULT 0,"
            ... "`hits_scatterguns` INTEGER DEFAULT 0,"
            ... "`shots_pistols` INTEGER DEFAULT 0,"
            ... "`hits_pistols` INTEGER DEFAULT 0,"
            ... "`shots_rocketlaunchers` INTEGER DEFAULT 0,"
            ... "`hits_rocketlaunchers` INTEGER DEFAULT 0,"
            ... "`shots_grenadelaunchers` INTEGER DEFAULT 0,"
            ... "`hits_grenadelaunchers` INTEGER DEFAULT 0,"
            ... "`shots_stickylaunchers` INTEGER DEFAULT 0,"
            ... "`hits_stickylaunchers` INTEGER DEFAULT 0,"
            ... "`shots_snipers` INTEGER DEFAULT 0,"
            ... "`hits_snipers` INTEGER DEFAULT 0,"
            ... "`shots_revolvers` INTEGER DEFAULT 0,"
            ... "`hits_revolvers` INTEGER DEFAULT 0,"
            ... "`best_streak` INTEGER DEFAULT 0,"
            ... "`best_ubers_life` INTEGER DEFAULT 0,"
            ... "`last_updated` INTEGER DEFAULT 0,"
            ... "PRIMARY KEY (`log_id`, `steamid`)"
            ... ")");
        g_hDatabase.Query(WhaleTracker_CreateLogPlayersTable, query);
    }

    Format(query, sizeof(query),
        "CREATE TABLE IF NOT EXISTS `whaletracker_points_cache` ("
        ... "`steamid` VARCHAR(32) PRIMARY KEY,"
        ... "`points` INTEGER DEFAULT 0,"
        ... "`rank` INTEGER DEFAULT 0,"
        ... "`name` VARCHAR(128) DEFAULT '',"
        ... "`name_color` VARCHAR(32) DEFAULT '',"
        ... "`prename` VARCHAR(64) DEFAULT '',"
        ... "`updated_at` INTEGER DEFAULT 0"
        ... ")");
    g_hDatabase.Query(WhaleTracker_CreatePointsCacheTable, query);

    SQL_FastQuery(g_hDatabase, "DROP TABLE IF EXISTS `whaletracker_mapstats`");
}

public void WhaleTracker_CreateTable(Database db, DBResultSet results, const char[] error, any data)
{
    if (error[0] != '\0')
    {
        LogError("[WhaleTracker] Failed to create table: %s", error);
    }

    PumpSaveQueue();

    static const char alterQueries[][256] =
    {
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS damage_dealt INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS damage_taken INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS uber_drops INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS last_seen INTEGER DEFAULT 0",

        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS shots_shotguns INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS hits_shotguns INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS shots_scatterguns INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS hits_scatterguns INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS shots_pistols INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS hits_pistols INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS shots_rocketlaunchers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS hits_rocketlaunchers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS shots_grenadelaunchers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS hits_grenadelaunchers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS shots_stickylaunchers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS hits_stickylaunchers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS shots_snipers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS hits_snipers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS shots_revolvers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS hits_revolvers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker ADD COLUMN IF NOT EXISTS sort_weight DOUBLE AS (CASE WHEN playtime >= 14400 THEN (kills + (0.5 * assists)) / GREATEST(deaths, 1) ELSE -1 END) STORED",
        "CREATE INDEX IF NOT EXISTS idx_sort_weight ON whaletracker (sort_weight DESC, kills DESC)"
    };

    for (int i = 0; i < sizeof(alterQueries); i++)
    {
        g_hDatabase.Query(WhaleTracker_AlterCallback, alterQueries[i]);
    }

    static const char alterOnlineQueries[][160] =
    {
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS personaname VARCHAR(128) DEFAULT ''",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS class TINYINT DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS team TINYINT DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS alive TINYINT DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS is_spectator TINYINT DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS kills INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS deaths INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS assists INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS damage INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS damage_taken INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS healing INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS headshots INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS backstabs INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS playtime INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS total_ubers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS best_streak INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS visible_max INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS time_connected INTEGER DEFAULT 0",

        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS shots_shotguns INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS hits_shotguns INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS shots_scatterguns INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS hits_scatterguns INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS shots_pistols INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS hits_pistols INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS shots_rocketlaunchers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS hits_rocketlaunchers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS shots_grenadelaunchers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS hits_grenadelaunchers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS shots_stickylaunchers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS hits_stickylaunchers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS shots_snipers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS hits_snipers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS shots_revolvers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS hits_revolvers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS host_ip VARCHAR(64) DEFAULT ''",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS host_port INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS playercount INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS map_name VARCHAR(128) DEFAULT ''",
        "ALTER TABLE whaletracker_online ADD COLUMN IF NOT EXISTS last_update INTEGER DEFAULT 0"
    };

    for (int i = 0; i < sizeof(alterOnlineQueries); i++)
    {
        g_hDatabase.Query(WhaleTracker_AlterCallback, alterOnlineQueries[i]);
    }

    static const char alterOnlineMetaQueries[][160] =
    {
        "ALTER TABLE whaletracker_online_meta ADD COLUMN IF NOT EXISTS host_ip VARCHAR(64) DEFAULT ''",
        "ALTER TABLE whaletracker_online_meta ADD COLUMN IF NOT EXISTS host_port INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online_meta ADD COLUMN IF NOT EXISTS map_name VARCHAR(128) DEFAULT ''",
        "ALTER TABLE whaletracker_online_meta ADD COLUMN IF NOT EXISTS playercount INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online_meta ADD COLUMN IF NOT EXISTS visible_max INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_online_meta ADD COLUMN IF NOT EXISTS updated_at INTEGER DEFAULT 0"
    };

    for (int i = 0; i < sizeof(alterOnlineMetaQueries); i++)
    {
        g_hDatabase.Query(WhaleTracker_AlterCallback, alterOnlineMetaQueries[i]);
    }

    static const char alterLogsQueries[][160] =
    {
        "ALTER TABLE whaletracker_logs ADD COLUMN IF NOT EXISTS map VARCHAR(64) DEFAULT ''",
        "ALTER TABLE whaletracker_logs ADD COLUMN IF NOT EXISTS gamemode VARCHAR(64) DEFAULT 'Unknown'",
        "ALTER TABLE whaletracker_logs ADD COLUMN IF NOT EXISTS started_at INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_logs ADD COLUMN IF NOT EXISTS ended_at INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_logs ADD COLUMN IF NOT EXISTS duration INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_logs ADD COLUMN IF NOT EXISTS player_count INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_logs ADD COLUMN IF NOT EXISTS created_at INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_logs ADD COLUMN IF NOT EXISTS updated_at INTEGER DEFAULT 0"
    };

    if (WhaleTracker_ShouldUseMatchLogs())
    {
        for (int i = 0; i < sizeof(alterLogsQueries); i++)
        {
            g_hDatabase.Query(WhaleTracker_AlterCallback, alterLogsQueries[i]);
        }
    }

    static const char alterLogPlayersQueries[][192] =
    {
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS personaname VARCHAR(128) DEFAULT ''",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS kills INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS deaths INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS assists INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS damage INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS damage_taken INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS healing INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS headshots INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS backstabs INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS total_ubers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS playtime INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS medic_drops INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS uber_drops INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS airshots INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS hits INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS shots_shotguns INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS hits_shotguns INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS shots_scatterguns INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS hits_scatterguns INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS shots_pistols INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS hits_pistols INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS shots_rocketlaunchers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS hits_rocketlaunchers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS shots_grenadelaunchers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS hits_grenadelaunchers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS shots_stickylaunchers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS hits_stickylaunchers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS shots_snipers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS hits_snipers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS shots_revolvers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS hits_revolvers INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS best_streak INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS best_ubers_life INTEGER DEFAULT 0",
        "ALTER TABLE whaletracker_log_players ADD COLUMN IF NOT EXISTS last_updated INTEGER DEFAULT 0"
    };

    if (WhaleTracker_ShouldUseMatchLogs())
    {
        for (int i = 0; i < sizeof(alterLogPlayersQueries); i++)
        {
            g_hDatabase.Query(WhaleTracker_AlterCallback, alterLogPlayersQueries[i]);
        }
    }

    static const char alterServersQueries[][160] =
    {
        "ALTER TABLE whaletracker_servers ADD COLUMN IF NOT EXISTS city VARCHAR(128) DEFAULT ''",
        "ALTER TABLE whaletracker_servers ADD COLUMN IF NOT EXISTS country VARCHAR(8) DEFAULT ''",
        "ALTER TABLE whaletracker_servers ADD COLUMN IF NOT EXISTS flags VARCHAR(256) DEFAULT ''"
    };

    for (int i = 0; i < sizeof(alterServersQueries); i++)
    {
        g_hDatabase.Query(WhaleTracker_AlterCallback, alterServersQueries[i]);
    }

    for (int i = 1; i <= MaxClients; i++)
    {
        if (!IsClientInGame(i) || IsFakeClient(i))
            continue;

        if (AreClientCookiesCached(i))
        {
            LoadClientStats(i);
        }
    }
}

public void WhaleTracker_CreateOnlineTable(Database db, DBResultSet results, const char[] error, any data)
{
    if (error[0] != '\0')
    {
        LogError("[WhaleTracker] Failed to create online table: %s", error);
    }
}

public void WhaleTracker_CreateLogsTable(Database db, DBResultSet results, const char[] error, any data)
{
    if (error[0] != '\0')
    {
        LogError("[WhaleTracker] Failed to create logs table: %s", error);
    }
}

public void WhaleTracker_CreateServersTable(Database db, DBResultSet results, const char[] error, any data)
{
    if (error[0] != '\0')
    {
        LogError("[WhaleTracker] Failed to create servers table: %s", error);
    }
}
public void WhaleTracker_CreateLogPlayersTable(Database db, DBResultSet results, const char[] error, any data)
{
    if (error[0] != '\0')
    {
        LogError("[WhaleTracker] Failed to create log players table: %s", error);
    }
}

public void WhaleTracker_CreatePointsCacheTable(Database db, DBResultSet results, const char[] error, any data)
{
    if (error[0] != '\0')
    {
        LogError("[WhaleTracker] Failed to create points cache table: %s", error);
        return;
    }

    g_hDatabase.Query(WhaleTracker_AlterCallback,
        "ALTER TABLE whaletracker_points_cache ADD COLUMN IF NOT EXISTS name_color VARCHAR(32) DEFAULT ''");
    g_hDatabase.Query(WhaleTracker_AlterCallback,
        "ALTER TABLE whaletracker_points_cache ADD COLUMN IF NOT EXISTS name VARCHAR(128) DEFAULT ''");
    g_hDatabase.Query(WhaleTracker_AlterCallback,
        "ALTER TABLE whaletracker_points_cache ADD COLUMN IF NOT EXISTS prename VARCHAR(64) DEFAULT ''");
    g_hDatabase.Query(WhaleTracker_AlterCallback,
        "ALTER TABLE whaletracker_points_cache ADD COLUMN IF NOT EXISTS rank INTEGER DEFAULT 0");
}

public void WhaleTracker_AlterCallback(Database db, DBResultSet results, const char[] error, any data)
{
    if (error[0] != '\0')
    {
        LogError("[WhaleTracker] Failed to alter table: %s", error);
    }
}

void LoadClientStats(int client)
{
    if (!IsClientInGame(client) || !g_bDatabaseReady || g_hDatabase == null)
    {
        return;
    }

    char steamId[STEAMID64_LEN];
    if (!GetClientAuthId(client, AuthId_SteamID64, steamId, sizeof(steamId)))
    {
        return;
    }

    strcopy(g_Stats[client].steamId, sizeof(g_Stats[client].steamId), steamId);
    strcopy(g_MapStats[client].steamId, sizeof(g_MapStats[client].steamId), steamId);

    char query[512];
    Format(query, sizeof(query),
        "SELECT first_seen, kills, deaths, healing, total_ubers, best_ubers_life, medic_drops, uber_drops, airshots, headshots, backstabs, best_killstreak, assists, playtime, damage_dealt, damage_taken, last_seen "
        ... "FROM whaletracker WHERE steamid = '%s'", steamId);

    g_hDatabase.Query(WhaleTracker_LoadCallback, query, client);
}

public void WhaleTracker_LoadCallback(Database db, DBResultSet results, const char[] error, any client)
{
    int index = client;
    if (!IsValidClient(index))
    {
        return;
    }

    if (error[0] != '\0')
    {
        LogError("[WhaleTracker] Failed to load stats for %N: %s", index, error);
        return;
    }

    if (results != null && results.FetchRow())
    {
        g_Stats[index].firstSeenTimestamp = results.FetchInt(0);
        FormatTime(g_Stats[index].firstSeen, sizeof(g_Stats[index].firstSeen), "%Y-%m-%d", g_Stats[index].firstSeenTimestamp);
        g_Stats[index].kills = results.FetchInt(1);
        g_Stats[index].deaths = results.FetchInt(2);
        g_Stats[index].totalHealing = results.FetchInt(3);
        g_Stats[index].totalUbers = results.FetchInt(4);
        g_Stats[index].bestUbersLife = results.FetchInt(5);
        g_Stats[index].totalMedicDrops = results.FetchInt(6);
        g_Stats[index].totalUberDrops = results.FetchInt(7);
        g_Stats[index].totalAirshots = results.FetchInt(8);
        g_Stats[index].totalHeadshots = results.FetchInt(9);
        g_Stats[index].totalBackstabs = results.FetchInt(10);
        g_Stats[index].bestKillstreak = results.FetchInt(11);
        g_Stats[index].totalAssists = results.FetchInt(12);
        g_Stats[index].playtime = results.FetchInt(13);
        g_Stats[index].totalDamage = results.FetchInt(14);
        g_Stats[index].totalDamageTaken = results.FetchInt(15);
        g_Stats[index].lastSeen = results.FetchInt(16);
        g_Stats[index].loaded = true;
        g_MapStats[index].loaded = true;
        g_MapStats[index].totalUberDrops = g_Stats[index].totalUberDrops;
    }
    else
    {
        g_Stats[index].firstSeenTimestamp = GetTime();
        FormatTime(g_Stats[index].firstSeen, sizeof(g_Stats[index].firstSeen), "%Y-%m-%d", g_Stats[index].firstSeenTimestamp);
        g_Stats[index].loaded = true;
        g_MapStats[index].loaded = true;
        g_MapStats[index].loaded = true;
    }

    TouchClientLastSeen(index);
}

bool HasMapActivity(WhaleStats stats)
{
    return stats.playtime > 0
        || stats.kills > 0
        || stats.deaths > 0
        || stats.totalAssists > 0
        || stats.totalHealing > 0
        || stats.totalDamage > 0
        || stats.totalDamageTaken > 0
        || stats.totalHeadshots > 0
        || stats.totalBackstabs > 0
        || stats.totalUberDrops > 0
        || stats.totalMedicDrops > 0;
}

bool SaveClientMapStats(int client)
{
    if (!IsValidClient(client))
        return false;

    if (!g_MapStats[client].loaded)
        return false;

    if (g_MapStats[client].steamId[0] == '\0')
        return false;

    if (!HasMapActivity(g_MapStats[client]))
        return false;

    int snapshot[MATCH_STAT_COUNT];
    SnapshotFromStats(g_MapStats[client], snapshot);
    AppendSnapshotToStorage(g_MapStats[client].steamId, snapshot);

    EnsureMatchStorage();

    char name[MAX_NAME_LENGTH];
    if (IsClientInGame(client))
    {
        GetClientName(client, name, sizeof(name));
    }
    else if (!GetStoredMatchPlayerName(g_MapStats[client].steamId, name, sizeof(name)))
    {
        name[0] = '\0';
    }

    if (!name[0])
    {
        strcopy(name, sizeof(name), g_MapStats[client].steamId);
    }

    RememberMatchPlayerName(g_MapStats[client].steamId, name);

    return true;
}

void QueueStatsSave(int client, int userId)
{
    char query[SAVE_QUERY_MAXLEN];
    char accuracyValueSegment[512];
    BuildWeaponAccuracySegment(g_Stats[client], accuracyValueSegment, sizeof(accuracyValueSegment));

    Format(query, sizeof(query),
        "INSERT INTO whaletracker "
        ... "(steamid, first_seen, kills, deaths, healing, total_ubers, best_ubers_life, medic_drops, uber_drops, airshots, headshots, backstabs, "
        ... "best_killstreak, assists, playtime, damage_dealt, damage_taken, last_seen, "
        ... "shots_shotguns, hits_shotguns, shots_scatterguns, hits_scatterguns, shots_pistols, hits_pistols, shots_rocketlaunchers, hits_rocketlaunchers, shots_grenadelaunchers, hits_grenadelaunchers, shots_stickylaunchers, hits_stickylaunchers, shots_snipers, hits_snipers, shots_revolvers, hits_revolvers) "
        ... "VALUES ('%s', %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, "
        ... "%d, %d, %d, %d, %d, %d, "
        ... "%s) "
        ... "ON DUPLICATE KEY UPDATE "
        ... "first_seen = LEAST(first_seen, VALUES(first_seen)), "
        ... "kills = GREATEST(kills, VALUES(kills)), "
        ... "deaths = GREATEST(deaths, VALUES(deaths)), "
        ... "healing = GREATEST(healing, VALUES(healing)), "
        ... "total_ubers = GREATEST(total_ubers, VALUES(total_ubers)), "
        ... "best_ubers_life = GREATEST(best_ubers_life, VALUES(best_ubers_life)), "
        ... "medic_drops = GREATEST(medic_drops, VALUES(medic_drops)), "
        ... "uber_drops = GREATEST(uber_drops, VALUES(uber_drops)), "
        ... "airshots = GREATEST(airshots, VALUES(airshots)), "
        ... "headshots = GREATEST(headshots, VALUES(headshots)), "
        ... "backstabs = GREATEST(backstabs, VALUES(backstabs)), "
        ... "best_killstreak = GREATEST(best_killstreak, VALUES(best_killstreak)), "
        ... "assists = GREATEST(assists, VALUES(assists)), "
        ... "playtime = GREATEST(playtime, VALUES(playtime)), "
        ... "damage_dealt = GREATEST(damage_dealt, VALUES(damage_dealt)), "
        ... "damage_taken = GREATEST(damage_taken, VALUES(damage_taken)), "
        ... "last_seen = GREATEST(last_seen, VALUES(last_seen)), "

        ... "shots_shotguns = GREATEST(shots_shotguns, VALUES(shots_shotguns)), "
        ... "hits_shotguns = GREATEST(hits_shotguns, VALUES(hits_shotguns)), "
        ... "shots_scatterguns = GREATEST(shots_scatterguns, VALUES(shots_scatterguns)), "
        ... "hits_scatterguns = GREATEST(hits_scatterguns, VALUES(hits_scatterguns)), "
        ... "shots_pistols = GREATEST(shots_pistols, VALUES(shots_pistols)), "
        ... "hits_pistols = GREATEST(hits_pistols, VALUES(hits_pistols)), "
        ... "shots_rocketlaunchers = GREATEST(shots_rocketlaunchers, VALUES(shots_rocketlaunchers)), "
        ... "hits_rocketlaunchers = GREATEST(hits_rocketlaunchers, VALUES(hits_rocketlaunchers)), "
        ... "shots_grenadelaunchers = GREATEST(shots_grenadelaunchers, VALUES(shots_grenadelaunchers)), "
        ... "hits_grenadelaunchers = GREATEST(hits_grenadelaunchers, VALUES(hits_grenadelaunchers)), "
        ... "shots_stickylaunchers = GREATEST(shots_stickylaunchers, VALUES(shots_stickylaunchers)), "
        ... "hits_stickylaunchers = GREATEST(hits_stickylaunchers, VALUES(hits_stickylaunchers)), "
        ... "shots_snipers = GREATEST(shots_snipers, VALUES(shots_snipers)), "
        ... "hits_snipers = GREATEST(hits_snipers, VALUES(hits_snipers)), "
        ... "shots_revolvers = GREATEST(shots_revolvers, VALUES(shots_revolvers)), "
        ... "hits_revolvers = GREATEST(hits_revolvers, VALUES(hits_revolvers))",
        g_Stats[client].steamId,
        g_Stats[client].firstSeenTimestamp,
        g_Stats[client].kills,
        g_Stats[client].deaths,
        g_Stats[client].totalHealing,
        g_Stats[client].totalUbers,
        g_Stats[client].bestUbersLife,
        g_Stats[client].totalMedicDrops,
        g_Stats[client].totalUberDrops,
        g_Stats[client].totalAirshots,
        g_Stats[client].totalHeadshots,
        g_Stats[client].totalBackstabs,
        g_Stats[client].bestKillstreak,
        g_Stats[client].totalAssists,
        g_Stats[client].playtime,
        g_Stats[client].totalDamage,
        g_Stats[client].totalDamageTaken,
        g_Stats[client].lastSeen,

        accuracyValueSegment);

    QueueSaveQuery(query, userId, false);
}

bool SaveClientStats(int client, bool includeMapStats, bool forceSave)
{
    if (!IsValidClient(client))
        return false;

    EnsureClientSteamId(client);

    if (g_Stats[client].steamId[0] == '\0')
        return false;

    bool playtimeChanged = AccumulatePlaytime(client);

    if (!g_Stats[client].loaded)
    {
        if (g_Stats[client].firstSeenTimestamp == 0)
        {
            g_Stats[client].firstSeenTimestamp = GetTime();
            FormatTime(g_Stats[client].firstSeen, sizeof(g_Stats[client].firstSeen), "%Y-%m-%d", g_Stats[client].firstSeenTimestamp);
        }
        g_Stats[client].loaded = true;
    }

    g_MapStats[client].loaded = true;

    g_KillSaveCounter[client] = 0;

    TouchClientLastSeen(client);

    if (!forceSave && !g_bStatsDirty[client] && !playtimeChanged)
    {
        return false;
    }

    int userId = GetClientUserId(client);
    QueueStatsSave(client, userId);

    if (includeMapStats)
    {
        SaveClientMapStats(client);
    }

    g_bStatsDirty[client] = false;

    return true;
}

public void WhaleTracker_SaveCallback(Database db, DBResultSet results, const char[] error, any data)
{
    int slot = data;
    int userId = 0;

    if (slot >= 0 && slot < MAX_CONCURRENT_SAVE_QUERIES)
    {
        userId = g_SaveQueryUserIds[slot];
    }

    if (error[0] != '\0')
    {
        int client = (userId > 0) ? GetClientOfUserId(userId) : 0;
        char queryPreview[256];
        queryPreview[0] = '\0';

        if (slot >= 0 && slot < MAX_CONCURRENT_SAVE_QUERIES)
        {
            if (g_SaveQueryBuffers[slot][0] != '\0')
            {
                strcopy(queryPreview, sizeof(queryPreview), g_SaveQueryBuffers[slot]);
            }
        }

        if (client > 0 && IsValidClient(client))
        {
            if (queryPreview[0])
            {
                LogError("[WhaleTracker] Failed to save stats for %N: %s | Query: %s", client, error, queryPreview);
            }
            else
            {
                LogError("[WhaleTracker] Failed to save stats for %N: %s", client, error);
            }
        }
        else if (userId > 0)
        {
            if (queryPreview[0])
            {
                LogError("[WhaleTracker] Failed to save stats (userid %d): %s | Query: %s", userId, error, queryPreview);
            }
            else
            {
                LogError("[WhaleTracker] Failed to save stats (userid %d): %s", userId, error);
            }
        }
        else
        {
            if (queryPreview[0])
            {
                LogError("[WhaleTracker] Failed to save stats: %s | Query: %s", error, queryPreview);
            }
            else
            {
                LogError("[WhaleTracker] Failed to save stats: %s", error);
            }
        }

        if (WhaleTracker_IsConnectionLostError(error))
        {
            WhaleTracker_ScheduleReconnect(2.0);
        }
    }

    if (slot >= 0 && slot < MAX_CONCURRENT_SAVE_QUERIES)
    {
        g_SaveQuerySlotUsed[slot] = false;
        g_SaveQueryUserIds[slot] = 0;
        g_SaveQueryBuffers[slot][0] = '\0';
    }

    if (g_PendingSaveQueries > 0)
    {
        g_PendingSaveQueries--;
    }

    RequestPumpSaveQueue();
}
