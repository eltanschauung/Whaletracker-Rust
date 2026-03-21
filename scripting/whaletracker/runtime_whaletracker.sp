public Plugin myinfo =
{
    name = "WhaleTracker",
    author = "Hombre",
    description = "Cumulative player stats system",
    version = "1.0.3",
    url = "https://kogasa.tf"
};

public void OnPluginStart()
{
    if (g_SaveQueue != null)
    {
        delete g_SaveQueue;
    }
    g_SaveQueue = new ArrayList();
    g_PendingSaveQueries = 0;
    g_bShuttingDown = false;
    g_hReconnectTimer = null;
    g_hSavePumpTimer = null;

    g_CvarDatabase = CreateConVar("sm_whaletracker_database", DB_CONFIG_DEFAULT, "Databases.cfg entry to use for WhaleTracker");
    g_CvarDatabase.GetString(g_sDatabaseConfig, sizeof(g_sDatabaseConfig));
    g_hGameName = CreateConVar("sm_whaletracker_game", "TF2", "Game label stored in WhaleTracker server snapshots.");
    g_hGameUrl = CreateConVar("sm_whaletracker_game_url", "440", "Steam store app ID used for WhaleTracker server snapshots.");

    g_hDebugMinimalStats = CreateConVar(
        "sm_whaletracker_debug_minimal",
        "0",
        "Limit WhaleTracker stat tracking to core metrics for debugging crashes (0 = off, 1 = on)",
        FCVAR_NONE,
        true,
        0.0,
        true,
        1.0
    );
    g_hEnableSdkHooks = CreateConVar(
        "sm_whaletracker_enable_sdkhooks",
        "1",
        "Enable SDKHooks-based damage tracking (1 = enabled, 0 = disabled).",
        FCVAR_NONE,
        true,
        0.0,
        true,
        1.0
    );
    g_hHeadshotMode = CreateConVar(
        "sm_whaletracker_headshot_mode",
        "0",
        "Headshot tracking mode (0 = damage hook, 1 = player_death customkill).",
        FCVAR_NONE,
        true,
        0.0,
        true,
        1.0
    );
    g_hEnableMatchLogs = CreateConVar(
        "sm_whaletracker_enable_matchlogs",
        "1",
        "Enable match logs table writes (1 = enabled, 0 = disabled).",
        FCVAR_NONE,
        true,
        0.0,
        true,
        1.0
    );
    g_hPublicIpMode = CreateConVar(
        "sm_whaletracker_public_ip_mode",
        "0",
        "Public IP mode (0 = SteamWorks_GetPublicIP, 1 = sm_whaletracker_public_ip).",
        FCVAR_NONE,
        true,
        0.0,
        true,
        1.0
    );
    g_hPublicIpManual = CreateConVar(
        "sm_whaletracker_public_ip",
        "",
        "Manual public server IP used when sm_whaletracker_public_ip_mode is 1."
    );
    g_hMedicDropMode = CreateConVar(
        "sm_whaletracker_medicdrop_mode",
        "0",
        "Medic drop detection mode (0 = medigun slot, 1 = weapon scan by classname).",
        FCVAR_NONE,
        true,
        0.0,
        true,
        1.0
    );
    g_hDeferredSavePump = CreateConVar(
        "sm_whaletracker_deferred_save_pump",
        "1",
        "Use timer-deferred save queue pumping (1 = deferred, 0 = immediate).",
        FCVAR_NONE,
        true,
        0.0,
        true,
        1.0
    );

    if (g_hVisibleMaxPlayers == null)
    {
        g_hVisibleMaxPlayers = FindConVar("sv_visiblemaxplayers");
    }

    HookEvent("player_death", Event_PlayerDeath, EventHookMode_Post);
    HookEvent("player_spawn", Event_PlayerSpawn, EventHookMode_Post);
    HookEvent("player_healed", Event_PlayerHealed, EventHookMode_Post);
    HookEvent("player_chargedeployed", Event_UberDeployed, EventHookMode_Post);

    RegConsoleCmd("sm_whalestats", Command_ShowStats, "Show your Whale Tracker statistics.");
    RegConsoleCmd("sm_stats", Command_ShowStats, "Show your Whale Tracker statistics.");
    RegConsoleCmd("sm_points", Command_ShowPoints, "Show your WhalePoints total.");
    RegConsoleCmd("sm_pos", Command_ShowPoints, "Show your WhalePoints total.");
    RegConsoleCmd("sm_pts", Command_ShowPoints, "Show your WhalePoints total.");
    RegConsoleCmd("sm_markets", Command_ShowMarketGardens, "Show your market garden total.");
    RegConsoleCmd("sm_mg", Command_ShowMarketGardens, "Show your market garden total.");
    RegConsoleCmd("sm_gardens", Command_ShowMarketGardens, "Show your market garden total.");
    RegConsoleCmd("sm_rank", Command_ShowPoints, "Show your WhalePoints total.");
    RegConsoleCmd("sm_ps", Command_ShowPoints, "Show your WhalePoints total.");
    RegConsoleCmd("sm_ranks", Command_ShowLeaderboard, "Show WhaleTracker leaderboard page.");
    RegAdminCmd("sm_savestats", Command_SaveAllStats, ADMFLAG_GENERIC, "Manually save all WhaleTracker stats");
    WhaleTracker_InitMotdCommands();

    EnsureMatchStorage();

    if (g_hGameModeCvar == null)
    {
        g_hGameModeCvar = FindConVar("sm_gamemode");
    }

    if (GetClientCount(true) > 0 && !g_sCurrentLogId[0])
    {
        BeginMatchTracking();
    }

    RefreshCurrentOnlineMapName();
    RefreshHostAddress();
    RefreshServerFlags();
    WhaleTracker_RustInit();

    WhaleTracker_SQLConnect();

    if (g_hOnlineTimer != null)
    {
        CloseHandle(g_hOnlineTimer);
    }
    g_hOnlineTimer = CreateTimer(10.0, Timer_UpdateOnlineStats, _, TIMER_REPEAT);
    if (g_hPeriodicSaveTimer != null)
    {
        CloseHandle(g_hPeriodicSaveTimer);
    }
    g_hPeriodicSaveTimer = CreateTimer(30.0, Timer_GlobalSave, _, TIMER_REPEAT | TIMER_FLAG_NO_MAPCHANGE);
    ClearOnlineStats();

    for (int i = 1; i <= MaxClients; i++)
    {
        ResetAllStats(i);
        ResetMapStats(i);
        g_KillSaveCounter[i] = 0;
        g_bStatsDirty[i] = false;
        if (IsClientInGame(i))
        {
            OnClientPutInServer(i);
            if (AreClientCookiesCached(i))
            {
                LoadClientStats(i);
            }
        }
    }
}

public void OnMapStart()
{
    if (!WhaleTracker_IsDatabaseHealthy())
    {
        WhaleTracker_ScheduleReconnect(1.0);
    }

    FinalizeCurrentMatch(false);
    if (GetClientCount(true) > 1)
    {
        BeginMatchTracking();
    }
    RefreshCurrentOnlineMapName();
    RefreshHostAddress();
    ClearOnlineStats();
    for (int i = 1; i <= MaxClients; i++)
    {
        ResetMapStats(i);
        if (IsClientInGame(i))
        {
            g_MapStats[i].connectTime = GetEngineTime();
        }
        g_KillSaveCounter[i] = 0;
    }
}

public void OnMapEnd()
{
    for (int i = 1; i <= MaxClients; i++)
    {
        if (IsClientInGame(i) && !IsFakeClient(i))
        {
            SaveClientStats(i, true, true);
        }
    }

    FlushSaveQueueSync();
    WhaleTracker_RustFlushSqlBatch();
    FinalizeCurrentMatch(false);
}

public void OnPluginEnd()
{
    g_bShuttingDown = true;

    FinalizeCurrentMatch(true);

    FlushSaveQueueSync();
    WhaleTracker_RustShutdown();

    if (g_hOnlineTimer != null)
    {
        CloseHandle(g_hOnlineTimer);
        g_hOnlineTimer = null;
    }
    if (g_hPeriodicSaveTimer != null)
    {
        CloseHandle(g_hPeriodicSaveTimer);
        g_hPeriodicSaveTimer = null;
    }
    if (g_hReconnectTimer != null)
    {
        CloseHandle(g_hReconnectTimer);
        g_hReconnectTimer = null;
    }
    if (g_hSavePumpTimer != null)
    {
        CloseHandle(g_hSavePumpTimer);
        g_hSavePumpTimer = null;
    }

    ClearOnlineStats();

    if (g_SaveQueue != null)
    {
        delete g_SaveQueue;
        g_SaveQueue = null;
    }

    g_hDatabase = null;
    g_bDatabaseReady = false;
    g_hVisibleMaxPlayers = null;

    for (int i = 1; i <= MaxClients; i++)
    {
        if (WhaleTracker_ShouldUseSdkHooks() && IsClientInGame(i) && !IsFakeClient(i))
        {
            SDKUnhook(i, SDKHook_OnTakeDamage, OnTakeDamage);
        }

    }
}

public void OnClientPutInServer(int client)
{
    if (IsFakeClient(client))
    {
        if (WhaleTracker_ShouldUseSdkHooks())
        {
            SDKHook(client, SDKHook_OnTakeDamage, OnTakeDamage);
        }
        return;
    }

    if (GetClientCount(true) == 1 && !g_sCurrentLogId[0])
    {
        BeginMatchTracking();
    }

    ResetRuntimeStats(client);
    g_Stats[client].connectTime = GetEngineTime();
    g_MapStats[client].connectTime = GetEngineTime();
    g_KillSaveCounter[client] = 0;
    g_bStatsDirty[client] = false;

    ResetMapStats(client);
    EnsureMatchStorage();
    EnsureClientSteamId(client);

    char steamId[STEAMID64_LEN];
    strcopy(steamId, sizeof(steamId), g_MapStats[client].steamId);

    if (steamId[0])
    {
        int snapshot[MATCH_STAT_COUNT];
        if (ExtractSnapshotForSteamId(steamId, snapshot))
        {
            ApplySnapshotToStats(g_MapStats[client], snapshot);
            RemoveSnapshotForSteamId(steamId);
        }

        char name[MAX_NAME_LENGTH];
        GetClientName(client, name, sizeof(name));
        RememberMatchPlayerName(steamId, name);
    }

    if (WhaleTracker_ShouldUseSdkHooks() && IsValidClient(client) && IsClientInGame(client))
    {
        SDKHook(client, SDKHook_OnTakeDamage, OnTakeDamage);
    }
    TouchClientLastSeen(client);

    if (AreClientCookiesCached(client))
    {
        LoadClientStats(client);
    }

    g_bTrackEligible[client] = (GetClientTeam(client) > 1);
    g_iDamageGate[client] = 0;
}

public void OnClientCookiesCached(int client)
{
    if (IsFakeClient(client))
        return;

    LoadClientStats(client);
    TouchClientLastSeen(client);
}

public void OnClientDisconnect(int client)
{
    if (IsFakeClient(client))
    {
        if (WhaleTracker_ShouldUseSdkHooks())
        {
            SDKUnhook(client, SDKHook_OnTakeDamage, OnTakeDamage);
        }
        return;
    }

        if (WhaleTracker_ShouldUseSdkHooks() && IsClientInGame(client))
        {
            SDKUnhook(client, SDKHook_OnTakeDamage, OnTakeDamage);
        }

    AccumulatePlaytime(client);
    SaveClientStats(client, true, true);
    RemoveOnlineStats(client);
    ResetAllStats(client);
    g_KillSaveCounter[client] = 0;
    g_bTrackEligible[client] = false;
    g_iDamageGate[client] = 0;
}

public void OnClientAuthorized(int client, const char[] auth)
{
    if (!IsValidClient(client) || IsFakeClient(client))
        return;

    EnsureClientSteamId(client);
    if (g_Stats[client].steamId[0] != '\0')
    {
        return;
    }

    if (!auth[0])
    {
        return;
    }

    // Fallback only; EnsureClientSteamId() will overwrite with SteamID64 when available.
    strcopy(g_Stats[client].steamId, sizeof(g_Stats[client].steamId), auth);
    strcopy(g_MapStats[client].steamId, sizeof(g_MapStats[client].steamId), auth);
}

public void OnClientPostAdminCheck(int client)
{
    if (!IsValidClient(client))
        return;

    if (IsFakeClient(client))
    {
        return;
    }

    QueryPointsCacheJoinMessage(client);
}

void AnnounceDefaultJoin(int client)
{
    if (!IsValidClient(client) || !IsClientInGame(client) || IsFakeClient(client))
    {
        return;
    }

    CPrintToChatAll("%N joined the game", client);
}

void QueryPointsCacheJoinMessage(int client)
{
    if (!IsValidClient(client) || !IsClientInGame(client) || IsFakeClient(client))
    {
        return;
    }

    if (!g_bDatabaseReady || g_hDatabase == null)
    {
        AnnounceDefaultJoin(client);
        return;
    }

    EnsureClientSteamId(client);
    if (g_Stats[client].steamId[0] == '\0')
    {
        AnnounceDefaultJoin(client);
        return;
    }

    char escapedSteamId[STEAMID64_LEN * 2];
    EscapeSqlString(g_Stats[client].steamId, escapedSteamId, sizeof(escapedSteamId));

    char query[512];
    Format(query, sizeof(query),
        "SELECT points, name_color, name, prename, rank FROM whaletracker_points_cache WHERE steamid = '%s' LIMIT 1",
        escapedSteamId);
    g_hDatabase.Query(WhaleTracker_JoinMessageQueryCallback, query, GetClientUserId(client));
}

public void WhaleTracker_JoinMessageQueryCallback(Database db, DBResultSet results, const char[] error, any data)
{
    int client = GetClientOfUserId(data);
    if (!IsValidClient(client) || !IsClientInGame(client) || IsFakeClient(client))
    {
        return;
    }

    if (error[0] != '\0')
    {
        LogError("[WhaleTracker] Failed to query points cache for join message: %s", error);
        AnnounceDefaultJoin(client);
        return;
    }

    if (results == null || !results.FetchRow())
    {
        AnnounceDefaultJoin(client);
        return;
    }

    int points = results.FetchInt(0);
    int rank = results.FetchInt(4);

    if (points < 0)
    {
        points = 0;
    }

    char colorTag[32];
    results.FetchString(1, colorTag, sizeof(colorTag));
    TrimString(colorTag);

    char cachedName[128];
    char cachedPrename[128];
    results.FetchString(2, cachedName, sizeof(cachedName));
    results.FetchString(3, cachedPrename, sizeof(cachedPrename));
    TrimString(cachedName);
    TrimString(cachedPrename);

    char displayName[128];
    if (cachedPrename[0] != '\0')
    {
        strcopy(displayName, sizeof(displayName), cachedPrename);
    }
    else if (cachedName[0] != '\0')
    {
        strcopy(displayName, sizeof(displayName), cachedName);
    }
    else
    {
        GetClientName(client, displayName, sizeof(displayName));
    }

    // Always prefer live filters DB color if available.
    GetClientFiltersNameColorTag(client, colorTag, sizeof(colorTag));

    // Keep join announcement and cached values in sync with live formula/rank.
    int livePoints = GetWhalePointsForClient(client);
    int liveRank = GetWhalePointsRankForClient(client);
    if (livePoints < 0)
    {
        livePoints = 0;
    }
    if (liveRank < 0)
    {
        liveRank = 0;
    }
    if (livePoints != points || liveRank != rank)
    {
        points = livePoints;
        rank = liveRank;
        CacheWhalePointsForClient(client, points, rank, colorTag);
    }

    if (rank > 0)
    {
        CPrintToChatAll("{%s}%s{default} (%d Points, Rank #%d) joined the game", colorTag, displayName, points, rank);
        PrintToServer("[WhaleTracker] %s (%d Points, Rank #%d, color=%s) joined the game", displayName, points, rank, colorTag);
    }
    else
    {
        CPrintToChatAll("{%s}%s{default} (Unranked) joined the game", colorTag, displayName);
        PrintToServer("[WhaleTracker] %s (Unranked, color=%s) joined the game", displayName, colorTag);
    }
}
