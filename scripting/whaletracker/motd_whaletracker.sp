public void WhaleTracker_InitMotdCommands()
{
    if (!WhaleTracker_ShouldUseMatchLogs())
    {
        return;
    }

    RegConsoleCmd("sm_whale", Command_WhaleMOTD, "Opens whale stats page.");
    RegConsoleCmd("sm_ss", Command_WhaleMOTD, "Opens whale stats page.");
    AddCommandListener(WhaleMOTD_ChatListener, "say");
    AddCommandListener(WhaleMOTD_ChatListener, "say_team");
}

public Action Command_WhaleMOTD(int client, int args)
{
    if (client <= 0 || !IsClientInGame(client))
    {
        ReplyToCommand(client, "[Whale] This command can only be used in-game.");
        return Plugin_Handled;
    }

    if (!WhaleTracker_ShouldUseMatchLogs())
    {
        ReplyToCommand(client, "[Whale] Match logs are disabled on this server.");
        return Plugin_Handled;
    }

    char url[256];
    Format(url, sizeof(url), "https://kogasa.tf/stats/logs/current/");

    KeyValues kv = new KeyValues("data");
    kv.SetString("title", "Whale Stats");
    kv.SetString("type", "2");
    kv.SetString("msg", url);
    kv.SetNum("customsvr", 1);
    ShowVGUIPanel(client, "info", kv);
    delete kv;

    return Plugin_Handled;
}

public Action WhaleMOTD_ChatListener(int client, const char[] command, int argc)
{
    if (client <= 0 || !IsClientInGame(client))
    {
        return Plugin_Continue;
    }

    if (!WhaleTracker_ShouldUseMatchLogs())
    {
        return Plugin_Continue;
    }

    char message[192];
    GetCmdArgString(message, sizeof(message));
    StripQuotes(message);
    TrimString(message);

    if (!message[0])
    {
        return Plugin_Continue;
    }

    if (StrEqual(message, ".ss", false) || StrEqual(message, "!ss", false))
    {
        Command_WhaleMOTD(client, 0);
        return Plugin_Handled;
    }

    return Plugin_Continue;
}
