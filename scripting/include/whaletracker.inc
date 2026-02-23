#if defined _whaletracker_helpers_included
  #endinput
#endif
#define _whaletracker_helpers_included

public void OnConfigsExecuted()
{
    RefreshHostAddress();
    RefreshServerFlags();
}



void ResetRuntimeCounters(WhaleStats stats)
{
    stats.currentKillstreak = 0;
    stats.currentUbersLife = 0;
}

void ResetStatsStruct(WhaleStats stats, bool resetIdentity)
{
    if (resetIdentity)
    {
        stats.loaded = false;
        stats.steamId[0] = '\0';
        stats.firstSeen[0] = '\0';
        stats.firstSeenTimestamp = 0;
    }

    stats.kills = 0;
    stats.deaths = 0;

    stats.totalHealing = 0;
    stats.totalUbers = 0;
    stats.totalMedicDrops = 0;
    stats.totalAirshots = 0;
    stats.totalHeadshots = 0;
    stats.totalBackstabs = 0;
    stats.totalAssists = 0;
    stats.totalDamage = 0;
    stats.totalDamageTaken = 0;
    stats.totalUberDrops = 0;
    for (int i = 0; i <= WEAPON_CATEGORY_COUNT; i++)
    {
        stats.weaponShots[i] = 0;
        stats.weaponHits[i] = 0;
    }
    stats.lastSeen = 0;
    stats.bestKillstreak = 0;
    stats.bestUbersLife = 0;
    stats.playtime = 0;
    stats.connectTime = 0.0;

    ResetRuntimeCounters(stats);
}

void ResetAllStats(int client)
{
    ResetStatsStruct(g_Stats[client], true);
    ResetStatsStruct(g_MapStats[client], true);
    g_bStatsDirty[client] = false;
    g_bTrackEligible[client] = false;
    g_iDamageGate[client] = 0;
}

void ResetMapStats(int client)
{
    ResetStatsStruct(g_MapStats[client], false);
    g_MapStats[client].loaded = g_Stats[client].loaded;
    if (g_Stats[client].steamId[0] != '\0')
    {
        strcopy(g_MapStats[client].steamId, sizeof(g_MapStats[client].steamId), g_Stats[client].steamId);
    }
    g_MapStats[client].connectTime = GetEngineTime();
    g_MapStats[client].lastSeen = g_Stats[client].lastSeen;
}

void ResetRuntimeStats(int client)
{
    ResetRuntimeCounters(g_Stats[client]);
    ResetRuntimeCounters(g_MapStats[client]);
}

void TouchClientLastSeen(int client)
{
    if (!IsClientInGame(client))
    {
        return;
    }

    int now = GetTime();
    g_Stats[client].lastSeen = now;
    g_MapStats[client].lastSeen = now;
    g_bTrackEligible[client] = (GetClientTeam(client) > 1);
}

void ClearOnlineStats()
{
    char deleteOnline[128];
    Format(deleteOnline, sizeof(deleteOnline), "DELETE FROM whaletracker_online WHERE host_port = %d", g_iHostPort);
    QueueSaveQuery(deleteOnline, 0, false);

    char deleteServer[128];
    Format(deleteServer, sizeof(deleteServer), "DELETE FROM whaletracker_servers WHERE port = %d", g_iHostPort);
    QueueSaveQuery(deleteServer, 0, false);
}

void RemoveOnlineStats(int client)
{
    char steamId[STEAMID64_LEN];
    if (!GetClientAuthId(client, AuthId_SteamID64, steamId, sizeof(steamId)))
        return;

    char query[192];
    Format(query, sizeof(query), "DELETE FROM whaletracker_online WHERE steamid = '%s' AND host_port = %d", steamId, g_iHostPort);
    QueueSaveQuery(query, 0, false);
}

public Action Timer_UpdateOnlineStats(Handle timer, any data)
{
    if (!g_bDatabaseReady || g_hDatabase == null)
        return Plugin_Continue;

    int now = GetTime();
    float engineNow = GetEngineTime();
    int playerCount = GetClientCount(true);

    int visibleMax = GetMaxHumanPlayers();
    if (g_hVisibleMaxPlayers != null)
    {
        int conVarValue = GetConVarInt(g_hVisibleMaxPlayers);
        if (conVarValue > 0 && visibleMax > conVarValue)
        {
            visibleMax = conVarValue;
        }
    }

    char steamId[STEAMID64_LEN];
    char name[MAX_NAME_LENGTH];
    char escapedName[MAX_NAME_LENGTH * 2];
    char query[SAVE_QUERY_MAXLEN];
    char escapedMapName[256];
    char mapName[128];
    if (g_sOnlineMapName[0])
    {
        strcopy(mapName, sizeof(mapName), g_sOnlineMapName);
    }
    else
    {
        strcopy(mapName, sizeof(mapName), "unknown");
    }
    SQL_EscapeString(g_hDatabase, mapName, escapedMapName, sizeof(escapedMapName));

    char escapedHostIp[64];
    char hostIp[64];
    if (g_sPublicHostIp[0])
    {
        strcopy(hostIp, sizeof(hostIp), g_sPublicHostIp);
    }
    else if (g_sHostIp[0])
    {
        strcopy(hostIp, sizeof(hostIp), g_sHostIp);
    }
    else
    {
        strcopy(hostIp, sizeof(hostIp), "0.0.0.0");
    }
    SQL_EscapeString(g_hDatabase, hostIp, escapedHostIp, sizeof(escapedHostIp));

    char hostCity[64];
    if (g_sHostCity[0])
    {
        strcopy(hostCity, sizeof(hostCity), g_sHostCity);
    }
    else
    {
        strcopy(hostCity, sizeof(hostCity), "Unknown");
    }
    char escapedHostCity[128];
    SQL_EscapeString(g_hDatabase, hostCity, escapedHostCity, sizeof(escapedHostCity));

    char hostCountry[3];
    if (g_sHostCountryLower[0])
    {
        strcopy(hostCountry, sizeof(hostCountry), g_sHostCountryLower);
    }
    else
    {
        strcopy(hostCountry, sizeof(hostCountry), "");
    }
    char escapedHostCountry[16];
    SQL_EscapeString(g_hDatabase, hostCountry, escapedHostCountry, sizeof(escapedHostCountry));

    char escapedServerFlags[512];
    SQL_EscapeString(g_hDatabase, g_sServerFlags, escapedServerFlags, sizeof(escapedServerFlags));

    for (int client = 1; client <= MaxClients; client++)
    {
        if (!IsClientInGame(client) || IsFakeClient(client))
            continue;

        if (!GetClientAuthId(client, AuthId_SteamID64, steamId, sizeof(steamId)))
            continue;

        GetClientName(client, name, sizeof(name));
        RememberMatchPlayerName(steamId, name);
        SQL_EscapeString(g_hDatabase, name, escapedName, sizeof(escapedName));

        TouchClientLastSeen(client);

        TFClassType tfClassType = TF2_GetPlayerClass(client);
        int tfClass = view_as<int>(tfClassType);
        int team = GetClientTeam(client);
        bool alive = IsPlayerAlive(client);
        bool spectator = (team != 2 && team != 3) || tfClassType == TFClass_Unknown;

        int playtime = g_MapStats[client].playtime;
        if (g_MapStats[client].connectTime > 0.0)
        {
            playtime += RoundToFloor(engineNow - g_MapStats[client].connectTime);
        }

        char weaponValueSegment[512];
        BuildWeaponAccuracySegment(g_MapStats[client], weaponValueSegment, sizeof(weaponValueSegment));

        Format(query, sizeof(query),
            "REPLACE INTO whaletracker_online "
            ... "(steamid, personaname, class, team, alive, is_spectator, kills, deaths, assists, damage, damage_taken, healing, headshots, backstabs, playtime, total_ubers, best_streak, visible_max, time_connected, shots_shotguns, hits_shotguns, shots_scatterguns, hits_scatterguns, shots_pistols, hits_pistols, shots_rocketlaunchers, hits_rocketlaunchers, shots_grenadelaunchers, hits_grenadelaunchers, shots_stickylaunchers, hits_stickylaunchers, shots_snipers, hits_snipers, shots_revolvers, hits_revolvers, host_ip, host_port, playercount, map_name, last_update) "
            ... "VALUES ('%s', '%s', %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %s, '%s', %d, %d, '%s', %d)",
            steamId,
            escapedName,
            tfClass,
            team,
            alive ? 1 : 0,
            spectator ? 1 : 0,
            g_MapStats[client].kills,
            g_MapStats[client].deaths,
            g_MapStats[client].totalAssists,
            g_MapStats[client].totalDamage,
            g_MapStats[client].totalDamageTaken,
            g_MapStats[client].totalHealing,
            g_MapStats[client].totalHeadshots,
            g_MapStats[client].totalBackstabs,
            playtime,
            g_MapStats[client].totalUbers,
            g_MapStats[client].bestKillstreak,
            visibleMax,
            playtime,
            weaponValueSegment,
            escapedHostIp,
            g_iHostPort,
            playerCount,
            escapedMapName,
            now);

        QueueSaveQuery(query, 0, false);

        int snapshotData[MATCH_STAT_COUNT];
        SnapshotFromStats(g_MapStats[client], snapshotData);
        InsertPlayerLogRecord(snapshotData, steamId, name, now, false);
    }

    Format(query, sizeof(query), "DELETE FROM whaletracker_online WHERE last_update < %d", now - 20);
    QueueSaveQuery(query, 0, false);

    Format(query, sizeof(query),
        "REPLACE INTO whaletracker_servers "
        ... "(ip, port, playercount, visible_max, map, city, country, flags, last_update) "
        ... "VALUES ('%s', %d, %d, %d, '%s', '%s', '%s', '%s', %d)",
        escapedHostIp,
        g_iHostPort,
        playerCount,
        visibleMax,
        escapedMapName,
        escapedHostCity,
        escapedHostCountry,
        escapedServerFlags,
        now);
    QueueSaveQuery(query, 0, false);

    int duration = (g_iMatchStartTime > 0) ? (now - g_iMatchStartTime) : 0;
    InsertMatchLogRecord(now, duration, playerCount, false);

    return Plugin_Continue;
}

public Action Timer_GlobalSave(Handle timer, any data)
{
    for (int i = 1; i <= MaxClients; i++)
    {
        if (IsClientInGame(i) && !IsFakeClient(i))
        {
            SaveClientStats(i, true, false);
        }
    }
    return Plugin_Continue;
}

void EscapeSqlString(const char[] input, char[] output, int maxlen)
{
    if (maxlen <= 0)
    {
        return;
    }

    output[0] = '\0';

    if (!input[0])
    {
        return;
    }

    if (g_hDatabase != null && g_bDatabaseReady)
    {
        SQL_EscapeString(g_hDatabase, input, output, maxlen);
        return;
    }

    int written = 0;
    for (int i = 0; input[i] != '\0' && written < maxlen - 1; i++)
    {
        char c = input[i];

        if (c == '\\' || c == '\'' || c == '\"')
        {
            if (written + 2 >= maxlen)
            {
                break;
            }
            output[written++] = '\\';
            output[written++] = c;
        }
        else if (c == '\n' || c == '\r' || c == '\t')
        {
            if (written + 2 >= maxlen)
            {
                break;
            }
            output[written++] = '\\';
            output[written++] = (c == '\n') ? 'n' : ((c == '\r') ? 'r' : 't');
        }
        else
        {
            output[written++] = c;
        }
    }

    output[written] = '\0';
}

void EnsureMatchStorage()
{
    if (g_DisconnectedStats == null)
    {
        g_DisconnectedStats = new StringMap();
    }
    if (g_MatchNames == null)
    {
        g_MatchNames = new StringMap();
    }
}

void ResetMatchStorage()
{
    EnsureMatchStorage();
    g_DisconnectedStats.Clear();
    g_MatchNames.Clear();
}

void CopyLowercase(const char[] source, char[] dest, int maxlen)
{
    if (maxlen <= 0)
        return;

    int i = 0;
    for (; i < maxlen - 1 && source[i] != '\0'; i++)
    {
        dest[i] = CharToLower(source[i]);
    }
    dest[i] = '\0';
}

bool WhaleTracker_IsConnectionLostError(const char[] error)
{
    if (!error[0])
    {
        return false;
    }

    return StrContains(error, "Lost connection to MySQL server", false) != -1
        || StrContains(error, "MySQL server has gone away", false) != -1
        || StrContains(error, "Server has gone away", false) != -1;
}

void WhaleTracker_ScheduleReconnect(float delay)
{
    if (g_bShuttingDown)
    {
        return;
    }

    if (g_hReconnectTimer != null)
    {
        return;
    }

    g_bDatabaseReady = false;

    if (g_hDatabase != null)
    {
        delete g_hDatabase;
        g_hDatabase = null;
    }

    g_hReconnectTimer = CreateTimer(delay, WhaleTracker_ReconnectTimer);
}

public Action WhaleTracker_ReconnectTimer(Handle timer, any data)
{
    if (timer == g_hReconnectTimer)
    {
        g_hReconnectTimer = null;
    }

    WhaleTracker_SQLConnect();
    return Plugin_Stop;
}

bool WhaleTracker_IsDatabaseHealthy()
{
    if (!g_bDatabaseReady || g_hDatabase == null)
    {
        return false;
    }

    if (SQL_FastQuery(g_hDatabase, "SELECT 1"))
    {
        return true;
    }

    char error[256];
    SQL_GetError(g_hDatabase, error, sizeof(error));
    LogError("[WhaleTracker] DB health check failed: %s", error);
    WhaleTracker_ScheduleReconnect(1.0);
    return false;
}

void RememberMatchPlayerName(const char[] steamId, const char[] name)
{
    if (!steamId[0] || !name[0])
        return;

    EnsureMatchStorage();
    g_MatchNames.SetString(steamId, name);
}

bool GetStoredMatchPlayerName(const char[] steamId, char[] buffer, int maxlen)
{
    if (!steamId[0] || maxlen <= 0)
    {
        return false;
    }

    EnsureMatchStorage();
    return g_MatchNames.GetString(steamId, buffer, maxlen);
}

void SnapshotFromStats(const WhaleStats stats, int data[MATCH_STAT_COUNT])
{
    data[MatchStat_Kills] = stats.kills;
    data[MatchStat_Deaths] = stats.deaths;
    data[MatchStat_Assists] = stats.totalAssists;
    data[MatchStat_Damage] = stats.totalDamage;
    data[MatchStat_DamageTaken] = stats.totalDamageTaken;
    data[MatchStat_Healing] = stats.totalHealing;
    data[MatchStat_Headshots] = stats.totalHeadshots;
    data[MatchStat_Backstabs] = stats.totalBackstabs;
    data[MatchStat_Ubers] = stats.totalUbers;
    data[MatchStat_Playtime] = stats.playtime;
    data[MatchStat_MedicDrops] = stats.totalMedicDrops;
    data[MatchStat_UberDrops] = stats.totalUberDrops;
    data[MatchStat_Airshots] = stats.totalAirshots;
    data[MatchStat_BestStreak] = stats.bestKillstreak;
    data[MatchStat_BestUbersLife] = stats.bestUbersLife;
}

void MergeSnapshotArrays(int base[MATCH_STAT_COUNT], const int delta[MATCH_STAT_COUNT])
{
    base[MatchStat_Kills] += delta[MatchStat_Kills];
    base[MatchStat_Deaths] += delta[MatchStat_Deaths];
    base[MatchStat_Assists] += delta[MatchStat_Assists];
    base[MatchStat_Damage] += delta[MatchStat_Damage];
    base[MatchStat_DamageTaken] += delta[MatchStat_DamageTaken];
    base[MatchStat_Healing] += delta[MatchStat_Healing];
    base[MatchStat_Headshots] += delta[MatchStat_Headshots];
    base[MatchStat_Backstabs] += delta[MatchStat_Backstabs];
    base[MatchStat_Ubers] += delta[MatchStat_Ubers];
    base[MatchStat_Playtime] += delta[MatchStat_Playtime];
    base[MatchStat_MedicDrops] += delta[MatchStat_MedicDrops];
    base[MatchStat_UberDrops] += delta[MatchStat_UberDrops];
    base[MatchStat_Airshots] += delta[MatchStat_Airshots];

    if (delta[MatchStat_BestStreak] > base[MatchStat_BestStreak])
    {
        base[MatchStat_BestStreak] = delta[MatchStat_BestStreak];
    }
    if (delta[MatchStat_BestUbersLife] > base[MatchStat_BestUbersLife])
    {
        base[MatchStat_BestUbersLife] = delta[MatchStat_BestUbersLife];
    }
}

void AppendSnapshotToStorage(const char[] steamId, const int data[MATCH_STAT_COUNT])
{
    if (!steamId[0])
        return;

    EnsureMatchStorage();

    int aggregate[MATCH_STAT_COUNT];
    bool hasExisting = g_DisconnectedStats.GetArray(steamId, aggregate, MATCH_STAT_COUNT);

    if (hasExisting)
    {
        MergeSnapshotArrays(aggregate, data);
    }
    else
    {
        for (int i = 0; i < MATCH_STAT_COUNT; i++)
        {
            aggregate[i] = data[i];
        }
    }

    g_DisconnectedStats.SetArray(steamId, aggregate, MATCH_STAT_COUNT);
}

bool ExtractSnapshotForSteamId(const char[] steamId, int data[MATCH_STAT_COUNT])
{
    if (!steamId[0])
        return false;

    EnsureMatchStorage();
    return g_DisconnectedStats.GetArray(steamId, data, MATCH_STAT_COUNT);
}

void RemoveSnapshotForSteamId(const char[] steamId)
{
    if (!steamId[0] || g_DisconnectedStats == null)
        return;

    g_DisconnectedStats.Remove(steamId);
}

void ApplySnapshotToStats(WhaleStats stats, const int data[MATCH_STAT_COUNT])
{
    stats.kills = data[MatchStat_Kills];
    stats.deaths = data[MatchStat_Deaths];
    stats.totalAssists = data[MatchStat_Assists];
    stats.totalDamage = data[MatchStat_Damage];
    stats.totalDamageTaken = data[MatchStat_DamageTaken];
    stats.totalHealing = data[MatchStat_Healing];
    stats.totalHeadshots = data[MatchStat_Headshots];
    stats.totalBackstabs = data[MatchStat_Backstabs];
    stats.totalUbers = data[MatchStat_Ubers];
    stats.playtime = data[MatchStat_Playtime];
    stats.totalMedicDrops = data[MatchStat_MedicDrops];
    stats.totalUberDrops = data[MatchStat_UberDrops];
    stats.totalAirshots = data[MatchStat_Airshots];
    stats.bestKillstreak = data[MatchStat_BestStreak];
    stats.bestUbersLife = data[MatchStat_BestUbersLife];
    stats.loaded = true;
}

void BeginMatchTracking()
{
    if (!WhaleTracker_ShouldUseMatchLogs())
    {
        g_sCurrentLogId[0] = '\0';
        g_sLastFinalizedLogId[0] = '\0';
        g_iMatchStartTime = 0;
        g_bMatchFinalized = true;
        return;
    }

    CleanupFinalizedLog();
    ResetMatchStorage();

    g_iMatchStartTime = GetTime();
    GetCurrentMap(g_sCurrentMap, sizeof(g_sCurrentMap));
    if (!g_sCurrentMap[0])
    {
        strcopy(g_sCurrentMap, sizeof(g_sCurrentMap), "unknown");
    }

    int randomPart = GetURandomInt() & 0xFFFF;
    Format(g_sCurrentLogId, sizeof(g_sCurrentLogId), "%d_%04X", g_iMatchStartTime, randomPart);

    g_bMatchFinalized = false;
}

void RefreshCurrentOnlineMapName()
{
    char rawName[128];
    g_sOnlineMapName[0] = '\0';
    GetCurrentMap(rawName, sizeof(rawName));
    if (!rawName[0])
    {
        strcopy(g_sOnlineMapName, sizeof(g_sOnlineMapName), "unknown");
        return;
    }

    ReplaceStringEx(rawName, sizeof(rawName), "workshop/", "");
    int dotIndex = FindCharInString(rawName, '.');
    if (dotIndex > 0 && dotIndex < sizeof(rawName))
    {
        rawName[dotIndex] = '\0';
    }
    strcopy(g_sOnlineMapName, sizeof(g_sOnlineMapName), rawName[0] ? rawName : "unknown");
}

void RefreshHostAddress()
{
    if (g_hHostIpCvar == null)
    {
        g_hHostIpCvar = FindConVar("ip");
        if (g_hHostIpCvar == null)
        {
            g_hHostIpCvar = FindConVar("hostip");
        }
    }

    if (g_hHostIpCvar != null)
    {
        g_hHostIpCvar.GetString(g_sHostIp, sizeof(g_sHostIp));
    }
    else
    {
        g_sHostIp[0] = '\0';
    }

    if (!g_sHostIp[0])
    {
        strcopy(g_sHostIp, sizeof(g_sHostIp), "0.0.0.0");
    }

    if (g_hHostPortCvar == null)
    {
        g_hHostPortCvar = FindConVar("hostport");
    }
    g_iHostPort = (g_hHostPortCvar != null) ? g_hHostPortCvar.IntValue : 27015;

    RefreshPublicHostIp();
}

bool WhaleTracker_IsNativeAvailable(const char[] nativeName)
{
    return GetFeatureStatus(FeatureType_Native, nativeName) == FeatureStatus_Available;
}

bool WhaleTracker_ShouldUseSdkHooks()
{
    if (g_hEnableSdkHooks == null || !g_hEnableSdkHooks.BoolValue)
    {
        return false;
    }

    return WhaleTracker_IsNativeAvailable("SDKHook") && WhaleTracker_IsNativeAvailable("SDKUnhook");
}

bool WhaleTracker_ShouldUseDamageHeadshots()
{
    return (g_hHeadshotMode == null || g_hHeadshotMode.IntValue == WT_HEADSHOT_MODE_DAMAGE);
}

bool WhaleTracker_ShouldUseMatchLogs()
{
    return (g_hEnableMatchLogs != null && g_hEnableMatchLogs.BoolValue);
}

bool WhaleTracker_UseManualPublicIp()
{
    return (g_hPublicIpMode != null && g_hPublicIpMode.IntValue == WT_PUBLIC_IP_MODE_MANUAL);
}

bool WhaleTracker_ShouldUseMedicDropScan()
{
    return (g_hMedicDropMode != null && g_hMedicDropMode.IntValue == WT_MEDICDROP_MODE_SCAN);
}

bool WhaleTracker_UseDeferredSavePump()
{
    return (g_hDeferredSavePump != null && g_hDeferredSavePump.BoolValue);
}

void RefreshPublicHostIp()
{
    g_sPublicHostIp[0] = '\0';

    if (WhaleTracker_UseManualPublicIp())
    {
        if (g_hPublicIpManual != null)
        {
            g_hPublicIpManual.GetString(g_sPublicHostIp, sizeof(g_sPublicHostIp));
            TrimString(g_sPublicHostIp);
        }
    }
    else if (WhaleTracker_IsNativeAvailable("SteamWorks_GetPublicIP"))
    {
        int addr[4];
        if (SteamWorks_GetPublicIP(addr))
        {
            Format(g_sPublicHostIp, sizeof(g_sPublicHostIp), "%d.%d.%d.%d", addr[0], addr[1], addr[2], addr[3]);
        }
    }

    RefreshHostCity();
}

void RefreshHostCity()
{
    char sourceIp[64];
    if (g_sPublicHostIp[0])
    {
        strcopy(sourceIp, sizeof(sourceIp), g_sPublicHostIp);
    }
    else if (g_sHostIp[0])
    {
        strcopy(sourceIp, sizeof(sourceIp), g_sHostIp);
    }
    else
    {
        sourceIp[0] = '\0';
    }

    if (!sourceIp[0])
    {
        strcopy(g_sHostCity, sizeof(g_sHostCity), "Unknown");
        g_sHostCountry[0] = '\0';
        g_sHostCountryLower[0] = '\0';
        return;
    }

    char city[64];
    if (!GeoipCity(sourceIp, city, sizeof(city)) || !city[0])
    {
        strcopy(g_sHostCity, sizeof(g_sHostCity), "Unknown");
    }
    else
    {
        strcopy(g_sHostCity, sizeof(g_sHostCity), city);
    }

    char countryCode[3];
    if (!GeoipCode2(sourceIp, countryCode) || !countryCode[0])
    {
        g_sHostCountry[0] = '\0';
        g_sHostCountryLower[0] = '\0';
    }
    else
    {
        strcopy(g_sHostCountry, sizeof(g_sHostCountry), countryCode);
        CopyLowercase(countryCode, g_sHostCountryLower, sizeof(g_sHostCountryLower));
    }
}

public void ConVarChanged_ServerFlags(ConVar convar, const char[] oldValue, const char[] newValue)
{
    RefreshServerFlags();
}

void RefreshServerFlags()
{
    if (g_hServerFlags == null)
    {
        g_hServerFlags = CreateConVar("sm_serverflags", "", "Additional flag codes (comma-separated) appended to the server card", FCVAR_NOTIFY);
        g_hServerFlags.AddChangeHook(ConVarChanged_ServerFlags);
    }
    g_hServerFlags.GetString(g_sServerFlags, sizeof(g_sServerFlags));
    TrimString(g_sServerFlags);
}

void InsertPlayerLogRecord(const int data[MATCH_STAT_COUNT], const char[] steamId, const char[] name, int timestamp, bool forceSync)
{
    if (!WhaleTracker_ShouldUseMatchLogs())
        return;

    if (!g_sCurrentLogId[0] || steamId[0] == '\0')
        return;

    if (GetConVarInt(g_hDebugMinimalStats) < 1)
        return;

    char escapedName[256];
    EscapeSqlString(name, escapedName, sizeof(escapedName));

    char query[SAVE_QUERY_MAXLEN];
    Format(query, sizeof(query),
        "INSERT INTO whaletracker_log_players "
        ... "(log_id, steamid, personaname, kills, deaths, assists, damage, damage_taken, healing, headshots, backstabs, total_ubers, playtime, medic_drops, uber_drops, airshots, best_streak, best_ubers_life, last_updated) "
        ... "VALUES ('%s', '%s', '%s', %d, %d, %d, %d, %d, %d, %d, %d, %d,  %d, %d, %d, %d, %d, %d, %d) "
        ... "ON DUPLICATE KEY UPDATE "
        ... "personaname = VALUES(personaname), "
        ... "kills = VALUES(kills), "
        ... "deaths = VALUES(deaths), "
        ... "assists = VALUES(assists), "
        ... "damage = VALUES(damage), "
        ... "damage_taken = VALUES(damage_taken), "
        ... "healing = VALUES(healing), "
        ... "headshots = VALUES(headshots), "
        ... "backstabs = VALUES(backstabs), "
        ... "total_ubers = VALUES(total_ubers), "
        ... "playtime = VALUES(playtime), "
        ... "medic_drops = VALUES(medic_drops), "
        ... "uber_drops = VALUES(uber_drops), "
        ... "airshots = VALUES(airshots), "
        ... "best_streak = VALUES(best_streak), "
        ... "best_ubers_life = VALUES(best_ubers_life), "
        ... "last_updated = VALUES(last_updated)",
        g_sCurrentLogId,
        steamId,
        escapedName,
        data[MatchStat_Kills],
        data[MatchStat_Deaths],
        data[MatchStat_Assists],
        data[MatchStat_Damage],
        data[MatchStat_DamageTaken],
        data[MatchStat_Healing],
        data[MatchStat_Headshots],
        data[MatchStat_Backstabs],
        data[MatchStat_Ubers],
        data[MatchStat_Playtime],
        data[MatchStat_MedicDrops],
        data[MatchStat_UberDrops],
        data[MatchStat_Airshots],
        data[MatchStat_BestStreak],
        data[MatchStat_BestUbersLife],
        timestamp);

    QueueSaveQuery(query, 0, forceSync);
}

void NormalizeGamemodeForMap(const char[] mapName, char[] gamemode, int gamemodeLen)
{
    if (!mapName[0])
    {
        return;
    }

    if (StrContains(mapName, "ctf_", false) == 0)
    {
        strcopy(gamemode, gamemodeLen, "Capture The Flag");
        return;
    }

    if (StrContains(mapName, "cp_", false) == 0)
    {
        strcopy(gamemode, gamemodeLen, "Control Point");
        return;
    }
}

void InsertMatchLogRecord(int endTime, int duration, int participantCount, bool forceSync)
{
    if (!WhaleTracker_ShouldUseMatchLogs())
        return;

    if (!g_sCurrentLogId[0])
        return;

    if (GetConVarInt(g_hDebugMinimalStats) < 1)
        return;

    char mapName[128];
    strcopy(mapName, sizeof(mapName), g_sCurrentMap);
    if (!mapName[0])
    {
        strcopy(mapName, sizeof(mapName), "unknown");
    }

    char escapedMap[128];
    EscapeSqlString(mapName, escapedMap, sizeof(escapedMap));

    char gamemode[64] = "Unknown";
    if (g_hGameModeCvar == null)
    {
        g_hGameModeCvar = FindConVar("sm_gamemode");
    }
    if (g_hGameModeCvar != null)
    {
        g_hGameModeCvar.GetString(gamemode, sizeof(gamemode));
        if (!gamemode[0])
        {
            strcopy(gamemode, sizeof(gamemode), "Unknown");
        }
    }
    NormalizeGamemodeForMap(mapName, gamemode, sizeof(gamemode));

    char escapedMode[64];
    EscapeSqlString(gamemode, escapedMode, sizeof(escapedMode));

    int started = (g_iMatchStartTime > 0) ? g_iMatchStartTime : endTime;

    char query[512];
    Format(query, sizeof(query),
        "INSERT INTO whaletracker_logs "
        ... "(log_id, map, gamemode, started_at, ended_at, duration, player_count, created_at, updated_at) "
        ... "VALUES ('%s', '%s', '%s', %d, %d, %d, %d, %d, %d) "
        ... "ON DUPLICATE KEY UPDATE "
        ... "map = VALUES(map), "
        ... "gamemode = VALUES(gamemode), "
        ... "started_at = VALUES(started_at), "
        ... "ended_at = VALUES(ended_at), "
        ... "duration = VALUES(duration), "
        ... "player_count = VALUES(player_count), "
        ... "updated_at = VALUES(updated_at)",
        g_sCurrentLogId,
        escapedMap,
        escapedMode,
        started,
        endTime,
        duration,
        participantCount,
        started,
        endTime);

    QueueSaveQuery(query, 0, forceSync);
}

void FinalizeCurrentMatch(bool shuttingDown)
{
    if (!WhaleTracker_ShouldUseMatchLogs())
    {
        ResetMatchStorage();
        g_bMatchFinalized = true;
        g_sCurrentLogId[0] = '\0';
        g_sLastFinalizedLogId[0] = '\0';
        g_iMatchStartTime = 0;
        return;
    }

    if (g_bMatchFinalized)
        return;

    EnsureMatchStorage();

    for (int i = 1; i <= MaxClients; i++)
    {
        if (!IsClientInGame(i) || IsFakeClient(i))
            continue;

        AccumulatePlaytime(i);
        EnsureClientSteamId(i);

        if (g_MapStats[i].steamId[0] == '\0')
            continue;

        int data[MATCH_STAT_COUNT];
        SnapshotFromStats(g_MapStats[i], data);
        AppendSnapshotToStorage(g_MapStats[i].steamId, data);

        char name[MAX_NAME_LENGTH];
        GetClientName(i, name, sizeof(name));
        RememberMatchPlayerName(g_MapStats[i].steamId, name);
    }

    if (!g_bDatabaseReady || g_hDatabase == null)
    {
        ResetMatchStorage();
        g_bMatchFinalized = true;
        return;
    }

    StringMapSnapshot snap = g_DisconnectedStats != null ? g_DisconnectedStats.Snapshot() : null;
    if (snap == null || snap.Length == 0)
    {
        if (snap != null)
        {
            delete snap;
        }
        ResetMatchStorage();
        g_bMatchFinalized = true;
        return;
    }

    int endTime = GetTime();
    int duration = (g_iMatchStartTime > 0) ? (endTime - g_iMatchStartTime) : 0;
    if (duration < 0)
    {
        duration = 0;
    }

    for (int i = 0; i < snap.Length; i++)
    {
        char steamId[STEAMID64_LEN];
        snap.GetKey(i, steamId, sizeof(steamId));

        int data[MATCH_STAT_COUNT];
        if (!ExtractSnapshotForSteamId(steamId, data))
            continue;

        char name[MAX_NAME_LENGTH];
        if (!GetStoredMatchPlayerName(steamId, name, sizeof(name)))
        {
            strcopy(name, sizeof(name), steamId);
        }

        InsertPlayerLogRecord(data, steamId, name, endTime, shuttingDown);
    }

    int participantCount = snap.Length;
    delete snap;

    InsertMatchLogRecord(endTime, duration, participantCount, shuttingDown);

    ResetMatchStorage();
    g_bMatchFinalized = true;
    if (g_sCurrentLogId[0])
    {
        strcopy(g_sLastFinalizedLogId, sizeof(g_sLastFinalizedLogId), g_sCurrentLogId);
    }
    g_sCurrentLogId[0] = '\0';
}

void EnsureClientSteamId(int client)
{
    if (!IsValidClient(client) || IsFakeClient(client))
        return;

    char steamId[STEAMID64_LEN];
    if (GetClientAuthId(client, AuthId_SteamID64, steamId, sizeof(steamId)))
    {
        strcopy(g_Stats[client].steamId, sizeof(g_Stats[client].steamId), steamId);
        strcopy(g_MapStats[client].steamId, sizeof(g_MapStats[client].steamId), steamId);
        return;
    }

    // Keep map copy aligned if only fallback auth is currently available.
    if (g_Stats[client].steamId[0] != '\0')
    {
        strcopy(g_MapStats[client].steamId, sizeof(g_MapStats[client].steamId), g_Stats[client].steamId);
    }
}

void CleanupFinalizedLog()
{
    if (!WhaleTracker_ShouldUseMatchLogs())
    {
        g_sLastFinalizedLogId[0] = '\0';
        return;
    }

    if (!g_sLastFinalizedLogId[0] || !g_bDatabaseReady || g_hDatabase == null)
    {
        return;
    }

    char escaped[128];
    EscapeSqlString(g_sLastFinalizedLogId, escaped, sizeof(escaped));

    char query[SAVE_QUERY_MAXLEN];
    Format(query, sizeof(query),
        "DELETE FROM whaletracker_logs WHERE log_id = '%s' AND (SELECT COALESCE(SUM(damage), 0) FROM whaletracker_log_players WHERE log_id = '%s') < 500",
        escaped, escaped);
    QueueSaveQuery(query, 0, false);

    Format(query, sizeof(query),
        "DELETE FROM whaletracker_log_players WHERE log_id = '%s' AND NOT EXISTS (SELECT 1 FROM whaletracker_logs WHERE log_id = '%s')",
        escaped, escaped);
    QueueSaveQuery(query, 0, false);

    g_sLastFinalizedLogId[0] = '\0';
}

int GetClientTfClass(int client)
{
    TFClassType tfClassType = TF2_GetPlayerClass(client);
    int classId = view_as<int>(tfClassType);
    if (classId < CLASS_MIN || classId > CLASS_MAX)
    {
        return 0;
    }
    return classId;
}

int ResolveAccuracyWeaponCategory(int weapon)
{
    if (weapon <= MaxClients || !IsValidEntity(weapon))
    {
        return 0;
    }

    if (!HasEntProp(weapon, Prop_Send, "m_iItemDefinitionIndex"))
    {
        return 0;
    }

    int defIndex = GetEntProp(weapon, Prop_Send, "m_iItemDefinitionIndex");
    if (defIndex <= 0)
    {
        return 0;
    }

    return GetWeaponCategoryFromDefIndex(defIndex);
}

bool TrackAccuracyEvent(int client, int weapon, bool isHit)
{
    if (!WhaleTracker_IsTrackingEnabled(client))
    {
        return false;
    }

    int weaponCategory = ResolveAccuracyWeaponCategory(weapon);
    if (weaponCategory <= view_as<int>(WeaponCategory_None) || weaponCategory > WEAPON_CATEGORY_COUNT)
    {
        return false;
    }

    int classId = GetClientTfClass(client);
    if (classId <= 0)
    {
        return false;
    }

    if (isHit)
    {
        g_Stats[client].weaponHits[weaponCategory]++;
        g_MapStats[client].weaponHits[weaponCategory]++;
    }
    else
    {
        g_Stats[client].weaponShots[weaponCategory]++;
        g_MapStats[client].weaponShots[weaponCategory]++;
    }

    MarkClientDirty(client);
    return true;
}

void IncrementHeadshotStats(WhaleStats stats)
{
    stats.totalHeadshots++;
}

void RecordHeadshotEvent(int client)
{
    if (!WhaleTracker_IsTrackingEnabled(client))
    {
        return;
    }

    IncrementHeadshotStats(g_Stats[client]);
    IncrementHeadshotStats(g_MapStats[client]);
    MarkClientDirty(client);
}



bool WhaleTracker_IsTrackingEnabled(int client)
{
    return (client > 0 && client <= MaxClients && !IsFakeClient(client) && GetClientTeam(client) > 1 && g_bTrackEligible[client]);
}

bool WhaleTracker_CheckDamageGate(int client, int damage)
{
    if (client <= 0 || client > MaxClients || IsFakeClient(client) || GetClientTeam(client) <= 1)
    {
        return false;
    }
    g_iDamageGate[client] += damage;
    if (g_iDamageGate[client] >= 200)
    {
        g_bTrackEligible[client] = true;
        return true;
    }
    return false;
}

void BuildWeaponAccuracySegment(const WhaleStats stats, char[] buffer, int maxlen)
{
    Format(buffer, maxlen,
        "%d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d",
        stats.weaponShots[WeaponCategory_Shotguns],
        stats.weaponHits[WeaponCategory_Shotguns],
        stats.weaponShots[WeaponCategory_Scatterguns],
        stats.weaponHits[WeaponCategory_Scatterguns],
        stats.weaponShots[WeaponCategory_Pistols],
        stats.weaponHits[WeaponCategory_Pistols],
        stats.weaponShots[WeaponCategory_RocketLaunchers],
        stats.weaponHits[WeaponCategory_RocketLaunchers],
        stats.weaponShots[WeaponCategory_GrenadeLaunchers],
        stats.weaponHits[WeaponCategory_GrenadeLaunchers],
        stats.weaponShots[WeaponCategory_StickyLaunchers],
        stats.weaponHits[WeaponCategory_StickyLaunchers],
        stats.weaponShots[WeaponCategory_Snipers],
        stats.weaponHits[WeaponCategory_Snipers],
        stats.weaponShots[WeaponCategory_Revolvers],
        stats.weaponHits[WeaponCategory_Revolvers]);
}

void ResetLifeCounters(WhaleStats stats)
{
    stats.currentKillstreak = 0;
    stats.currentUbersLife = 0;
}

void ApplyKillStats(WhaleStats stats, bool backstab, bool headshot, bool medicDrop)
{
    stats.kills++;

    stats.currentKillstreak++;


    if (stats.currentKillstreak > stats.bestKillstreak)
    {
        stats.bestKillstreak = stats.currentKillstreak;
    }
    if (backstab)
    {
        stats.totalBackstabs++;
    }
    if (headshot)
    {
        stats.totalHeadshots++;
    }
    if (medicDrop)
    {
        stats.totalMedicDrops++;
    }
}

void ApplyAssistStats(WhaleStats stats)
{
    stats.totalAssists++;

}

void ApplyDeathStats(WhaleStats stats)
{
    stats.deaths++;
    ResetLifeCounters(stats);
}

void ApplyHealingStats(WhaleStats stats, int amount)
{
    stats.totalHealing += amount;
}

void ApplyUberStats(WhaleStats stats)
{
    stats.totalUbers++;

    stats.currentUbersLife++;
    if (stats.currentUbersLife > stats.bestUbersLife)
    {
        stats.bestUbersLife = stats.currentUbersLife;
    }
}

bool AccumulatePlaytimeStruct(WhaleStats stats)
{
    if (stats.connectTime <= 0.0)
        return false;

    float session = GetEngineTime() - stats.connectTime;
    if (session >= 1.0)
    {
        int delta = RoundToFloor(session);
        stats.playtime += delta;
        stats.connectTime = GetEngineTime();
        return delta > 0;
    }
    return false;
}

bool AccumulatePlaytime(int client)
{
    if (client <= 0 || client > MaxClients)
        return false;

    bool changed = false;
    changed |= AccumulatePlaytimeStruct(g_Stats[client]);
    changed |= AccumulatePlaytimeStruct(g_MapStats[client]);
    return changed;
}

void MarkClientDirty(int client)
{
    if (client <= 0 || client > MaxClients)
        return;
    g_bStatsDirty[client] = true;
}

void RunSaveQuerySync(const char[] query, int userId)
{
    if (g_hDatabase == null)
    {
        return;
    }

    if (!SQL_FastQuery(g_hDatabase, query))
    {
        char error[256];
        SQL_GetError(g_hDatabase, error, sizeof(error));

        if (userId > 0)
        {
            LogError("[WhaleTracker] Failed to save stats synchronously (userid %d): %s", userId, error);
        }
        else
        {
            LogError("[WhaleTracker] Failed to save stats synchronously: %s", error);
        }

        if (WhaleTracker_IsConnectionLostError(error))
        {
            WhaleTracker_ScheduleReconnect(2.0);
        }
    }
}

void PumpSaveQueue()
{
    if (g_SaveQueue == null || g_hDatabase == null || !g_bDatabaseReady || g_bShuttingDown)
    {
        return;
    }

    while (g_PendingSaveQueries < MAX_CONCURRENT_SAVE_QUERIES && g_SaveQueue.Length > 0)
    {
        DataPack pack = view_as<DataPack>(g_SaveQueue.Get(0));
        g_SaveQueue.Erase(0);

        pack.Reset();
        int userId = pack.ReadCell();

        char query[SAVE_QUERY_MAXLEN];
        pack.ReadString(query, sizeof(query));

        delete pack;

        int slot = -1;
        for (int i = 0; i < MAX_CONCURRENT_SAVE_QUERIES; i++)
        {
            if (!g_SaveQuerySlotUsed[i])
            {
                slot = i;
                break;
            }
        }

        if (slot == -1)
        {
            RunSaveQuerySync(query, userId);
            continue;
        }

        strcopy(g_SaveQueryBuffers[slot], SAVE_QUERY_MAXLEN, query);
        g_SaveQueryUserIds[slot] = userId;
        g_SaveQuerySlotUsed[slot] = true;

        g_PendingSaveQueries++;
        g_hDatabase.Query(WhaleTracker_SaveCallback, g_SaveQueryBuffers[slot], slot);
    }
}

void RequestPumpSaveQueue()
{
    if (!WhaleTracker_UseDeferredSavePump())
    {
        PumpSaveQueue();
        return;
    }

    if (g_bShuttingDown || g_hSavePumpTimer != null)
    {
        return;
    }

    g_hSavePumpTimer = CreateTimer(0.0, WhaleTracker_PumpSaveQueueTimer);
}

public Action WhaleTracker_PumpSaveQueueTimer(Handle timer, any data)
{
    if (timer == g_hSavePumpTimer)
    {
        g_hSavePumpTimer = null;
    }

    PumpSaveQueue();
    return Plugin_Stop;
}

void FlushSaveQueueSync()
{
    if (g_SaveQueue == null)
    {
        return;
    }

    while (g_SaveQueue.Length > 0)
    {
        DataPack pack = view_as<DataPack>(g_SaveQueue.Get(0));
        g_SaveQueue.Erase(0);

        pack.Reset();
        int userId = pack.ReadCell();

        char query[SAVE_QUERY_MAXLEN];
        pack.ReadString(query, sizeof(query));

        delete pack;

        RunSaveQuerySync(query, userId);
    }
}

void QueueSaveQuery(const char[] query, int userId, bool forceSync)
{
    if (forceSync || g_bShuttingDown)
    {
        RunSaveQuerySync(query, userId);
        return;
    }

    if (g_SaveQueue == null)
    {
        g_SaveQueue = new ArrayList();
    }

    DataPack pack = new DataPack();
    pack.WriteCell(userId);
    pack.WriteString(query);
    g_SaveQueue.Push(pack);

    RequestPumpSaveQueue();
}
