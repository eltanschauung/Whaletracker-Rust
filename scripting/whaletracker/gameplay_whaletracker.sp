public void Event_PlayerSpawn(Event event, const char[] name, bool dontBroadcast)
{
    int client = GetClientOfUserId(event.GetInt("userid"));
    if (!IsValidClient(client))
        return;

    ResetLifeCounters(g_Stats[client]);
    ResetLifeCounters(g_MapStats[client]);
}

public void Event_PlayerDeath(Event event, const char[] name, bool dontBroadcast)
{
    int victim = GetClientOfUserId(event.GetInt("userid"));
    int attacker = GetClientOfUserId(event.GetInt("attacker"));
    int assister = GetClientOfUserId(event.GetInt("assister"));
    int deathFlags = GetUserFlagBits(victim);
    bool attackerScoredMedicDrop = false;

    if (!(deathFlags & 32))
    {
        if (IsValidClient(attacker) && attacker != victim && WhaleTracker_IsTrackingEnabled(attacker))
        {
            int custom = event.GetInt("customkill");
            bool backstab = (custom == TF_CUSTOM_BACKSTAB);
            bool headshot = (custom == TF_CUSTOM_HEADSHOT || custom == TF_CUSTOM_HEADSHOT_DECAPITATION);
            bool countHeadshotOnDeath = (!WhaleTracker_ShouldUseSdkHooks() || !WhaleTracker_ShouldUseDamageHeadshots());
            bool medicDrop = IsMedicDrop(victim);

            ApplyKillStats(g_Stats[attacker], backstab, (countHeadshotOnDeath && headshot), medicDrop);
            ApplyKillStats(g_MapStats[attacker], backstab, (countHeadshotOnDeath && headshot), medicDrop);
            attackerScoredMedicDrop = medicDrop;
            MarkClientDirty(attacker);
        }

        if (IsValidClient(assister) && assister != victim && WhaleTracker_IsTrackingEnabled(assister))
        {
            ApplyAssistStats(g_Stats[assister]);
            ApplyAssistStats(g_MapStats[assister]);
            MarkClientDirty(assister);
        }

        if (IsValidClient(victim) && WhaleTracker_IsTrackingEnabled(victim))
        {
            if (attackerScoredMedicDrop)
            {
                g_Stats[victim].totalUberDrops++;
                g_MapStats[victim].totalUberDrops++;
            }
            ApplyDeathStats(g_Stats[victim]);
            ApplyDeathStats(g_MapStats[victim]);
            MarkClientDirty(victim);
        }
    }
}

public Action OnTakeDamage(int victim, int &attacker, int &inflictor, float &damage, int &damagetype, int &weapon, float damageForce[3], float damagePosition[3], int damagecustom)
{
    if (!WhaleTracker_ShouldUseSdkHooks())
        return Plugin_Continue;

    if (attacker == victim)
        return Plugin_Continue;

    int damageInt = RoundToFloor(damage);
    if (damageInt < 0)
    {
        damageInt = 0;
    }

    // Gate expensive tracking: allow attackers to become eligible after 200 damage dealt and only if not spectator.
    if (IsValidClient(attacker) && !IsFakeClient(attacker) && GetClientTeam(attacker) > 1 && !g_bTrackEligible[attacker])
    {
        if (!WhaleTracker_CheckDamageGate(attacker, damageInt))
        {
            // Still below threshold; skip further processing for this attacker.
            return Plugin_Continue;
        }
    }
    // Victims in spectator are ignored.
    if (IsValidClient(victim) && GetClientTeam(victim) <= 1)
    {
        return Plugin_Continue;
    }

    bool isHeadshot = (damagecustom == TF_CUSTOM_HEADSHOT || damagecustom == TF_CUSTOM_HEADSHOT_DECAPITATION);
    if (WhaleTracker_ShouldUseDamageHeadshots() && isHeadshot && IsValidClient(attacker) && !IsFakeClient(attacker))
    {
        RecordHeadshotEvent(attacker);
    }

    if (CheckIfAfterburn(damagecustom) || CheckIfBleedDmg(damagetype))
        return Plugin_Continue;

    if (damage <= 0.0)
        return Plugin_Continue;

    if (IsValidClient(attacker) && !IsFakeClient(attacker) && WhaleTracker_IsTrackingEnabled(attacker))
    {
        if (IsProjectileAirshot(attacker, victim))
            g_Stats[attacker].totalAirshots += 1;

        g_Stats[attacker].totalDamage += damageInt;
        g_MapStats[attacker].totalDamage += damageInt;
        if (!TrackAccuracyEvent(attacker, weapon, true))
        {
            if (!TrackAccuracyEvent(attacker, inflictor, true))
            {
                int activeWeapon = GetEntPropEnt(attacker, Prop_Send, "m_hActiveWeapon");
                if (activeWeapon > MaxClients)
                {
                    TrackAccuracyEvent(attacker, activeWeapon, true);
                }
            }
        }
        MarkClientDirty(attacker);
    }

    if (IsValidClient(victim) && !IsFakeClient(victim) && WhaleTracker_IsTrackingEnabled(victim))
    {
        g_Stats[victim].totalDamageTaken += damageInt;
        g_MapStats[victim].totalDamageTaken += damageInt;
        MarkClientDirty(victim);
    }

    return Plugin_Continue;
}

public Action TF2_CalcIsAttackCritical(int client, int weapon, char[] weaponname, bool& result)
{
    if (!WhaleTracker_IsTrackingEnabled(client))
        return Plugin_Continue;

    if (CheckIfAfterburn(0) || CheckIfBleedDmg(0))
        return Plugin_Continue;

    TrackAccuracyEvent(client, weapon, false);
    return Plugin_Continue;
}

public void Event_PlayerHealed(Event event, const char[] name, bool dontBroadcast)
{
    int healer = GetClientOfUserId(event.GetInt("healer"));
    if (!WhaleTracker_IsTrackingEnabled(healer))
        return;

    int amount = event.GetInt("amount");
    if (amount > 0)
    {
        ApplyHealingStats(g_Stats[healer], amount);
        ApplyHealingStats(g_MapStats[healer], amount);
        MarkClientDirty(healer);
    }
}

public void Event_UberDeployed(Event event, const char[] name, bool dontBroadcast)
{
    int medic = GetClientOfUserId(event.GetInt("userid"));
    if (!WhaleTracker_IsTrackingEnabled(medic))
        return;

    ApplyUberStats(g_Stats[medic]);
    ApplyUberStats(g_MapStats[medic]);
    MarkClientDirty(medic);
}

bool IsMedicDrop(int victim)
{
    if (!IsValidClient(victim) || IsFakeClient(victim))
        return false;

    if (TF2_GetPlayerClass(victim) != TFClass_Medic)
        return false;

    int medigun = -1;
    if (WhaleTracker_ShouldUseMedicDropScan())
    {
        if (!HasEntProp(victim, Prop_Send, "m_hMyWeapons"))
            return false;

        int maxWeapons = GetEntPropArraySize(victim, Prop_Send, "m_hMyWeapons");
        char classname[64];
        for (int i = 0; i < maxWeapons; i++)
        {
            int weapon = GetEntPropEnt(victim, Prop_Send, "m_hMyWeapons", i);
            if (weapon <= MaxClients || !IsValidEntity(weapon))
                continue;

            GetEntityClassname(weapon, classname, sizeof(classname));
            if (StrContains(classname, "medigun", false) != -1)
            {
                medigun = weapon;
                break;
            }
        }
    }
    else
    {
        medigun = GetPlayerWeaponSlot(victim, 1);
    }

    if (medigun <= MaxClients || !IsValidEntity(medigun))
        return false;
    if (!HasEntProp(medigun, Prop_Send, "m_flChargeLevel"))
        return false;

    return (GetEntPropFloat(medigun, Prop_Send, "m_flChargeLevel") >= 1.0);
}

bool IsProjectileAirshot(int attacker, int victim)
{
    if (!IsValidClient(attacker) || IsFakeClient(attacker) || !IsValidClient(victim) || IsFakeClient(victim))
        return false;

    int weapon = GetPlayerWeaponSlot(attacker, 0);
    if (weapon <= MaxClients || !IsValidEntity(weapon))
        return false;

    char classname[64];
    GetEntityClassname(weapon, classname, sizeof(classname));

    bool projectileWeapon = StrContains(classname, "rocketlauncher", false) != -1
        || StrContains(classname, "grenadelauncher", false) != -1
        || StrContains(classname, "pipeline", false) != -1
        || StrContains(classname, "stickbomb", false) != -1;

    if (!projectileWeapon)
        return false;

    int flags = GetEntityFlags(victim);
    bool victimInAir = !(flags & FL_ONGROUND);
    return victimInAir;
}

void FormatMatchDuration(int seconds, char[] buffer, int maxlen)
{
    if (maxlen <= 0)
    {
        return;
    }

    if (seconds <= 0)
    {
        strcopy(buffer, maxlen, "0s");
        return;
    }

    int hours = seconds / 3600;
    int minutes = (seconds % 3600) / 60;
    int secs = seconds % 60;

    if (hours > 0)
    {
        Format(buffer, maxlen, "%dh %dm", hours, minutes);
    }
    else if (minutes > 0)
    {
        Format(buffer, maxlen, "%dm %ds", minutes, secs);
    }
    else
    {
        Format(buffer, maxlen, "%ds", secs);
    }
}

int GetWeaponCategoryFromDefIndex(int defIndex)
{
    switch (defIndex)
    {
        case 9, 10, 11, 12, 199, 425, 527, 1153:
            return WeaponCategory_Shotguns;
        case 13, 200, 15029, 669, 45, 448, 772, 1103:
            return WeaponCategory_Scatterguns;
        case 22, 23, 209, 773, 449, 160, 161:
            return WeaponCategory_Pistols;
        case 18, 205, 658, 513, 414, 441, 1104, 730, 228:
            return WeaponCategory_RocketLaunchers;
        case 19, 206, 1151, 308:
            return WeaponCategory_GrenadeLaunchers;
        case 20, 207, 661, 265, 130:
            return WeaponCategory_StickyLaunchers;
        case 14, 201, 664, 402, 230, 851, 752, 526:
            return WeaponCategory_Snipers;
        case 24, 210, 224, 61, 525, 460:
            return WeaponCategory_Revolvers;
    }

    return WeaponCategory_None;
}

stock bool CheckIfAfterburn(int damagecustom)
{
    return (damagecustom == TF_CUSTOM_BURNING || damagecustom == TF_CUSTOM_BURNING_FLARE);
}

stock bool CheckIfBleedDmg(int damageType)
{
    return (damageType & DMG_SLASH) != 0;
}

void SendMatchStatsMessage(int viewer, int target)
{
    if (!IsValidClient(viewer) || IsFakeClient(viewer))
        return;

    if (!IsValidClient(target) || IsFakeClient(target))
    {
        CPrintToChat(viewer, "{green}[WhaleTracker]{default} No valid player selected.");
        return;
    }

    bool targetInGame = IsClientInGame(target);
    if (targetInGame)
    {
        AccumulatePlaytime(target);
    }

    EnsureClientSteamId(target);
    WhaleStats matchStats;
    matchStats = g_MapStats[target];
    bool hasActivity = HasMapActivity(matchStats) || matchStats.playtime > 0;

    if (!targetInGame && !hasActivity)
    {
        CPrintToChat(viewer, "{green}[WhaleTracker]{default} %N has no current match data.", target);
        return;
    }

    char playerName[MAX_NAME_LENGTH];
    if (targetInGame)
    {
        GetClientName(target, playerName, sizeof(playerName));
        RememberMatchPlayerName(matchStats.steamId, playerName);
    }
    else if (!GetStoredMatchPlayerName(matchStats.steamId, playerName, sizeof(playerName)))
    {
        strcopy(playerName, sizeof(playerName), matchStats.steamId);
    }

    char colorTag[32];
    GetClientFiltersNameColorTag(target, colorTag, sizeof(colorTag));

    int kills = matchStats.kills;
    int deaths = matchStats.deaths;
    int assists = matchStats.totalAssists;
    int damage = matchStats.totalDamage;
    int damageTaken = matchStats.totalDamageTaken;
    int healing = matchStats.totalHealing;
    int headshots = matchStats.totalHeadshots;
    int backstabs = matchStats.totalBackstabs;
    int ubers = matchStats.totalUbers;

    int lifetimeKills = g_Stats[target].kills;
    int lifetimeDeaths = g_Stats[target].deaths;
    float lifetimeKd = (lifetimeDeaths > 0) ? float(lifetimeKills) / float(lifetimeDeaths) : float(lifetimeKills);

    float kd = (deaths > 0) ? float(kills) / float(deaths) : float(kills);
    float dpm = 0.0, dtpm = 0.0;
    float minutes = (matchStats.playtime > 0) ? float(matchStats.playtime) / 60.0 : 0.0;
    if (minutes > 1.0)
    {
        dpm = (minutes > 0.0) ? float(damage) / minutes : 0.0;
        dtpm = (minutes > 0.0) ? float(damageTaken) / minutes : 0.0;
    }

    char timeBuffer[32];
    FormatMatchDuration(matchStats.playtime, timeBuffer, sizeof(timeBuffer));

    CPrintToChat(viewer, "{green}[WhaleTracker]{default} {%s}%s{default} — Match: K %d | D %d | KD %.2f | A %d | Dmg %d | Dmg/min %.1f",
        colorTag, playerName, kills, deaths, kd, assists, damage, dpm);
    CPrintToChat(viewer, "{green}[WhaleTracker]{default} Taken %d | Taken/min %.1f | Heal %d | HS %d | BS %d | Ubers %d | Time %s",
        damageTaken, dtpm, healing, headshots, backstabs, ubers, timeBuffer);
    CPrintToChat(viewer, "{green}[WhaleTracker]{default} Lifetime Kills %d | Deaths %d | KD: %.2f", lifetimeKills, lifetimeDeaths, lifetimeKd);
    CPrintToChat(viewer, "{green}[WhaleTracker]{default} Visit kogasa.tf/stats for full");
}
