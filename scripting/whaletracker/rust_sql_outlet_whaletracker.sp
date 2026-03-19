#include <socket>

#if !defined WT_RUST_SQL_MAX_BATCH_JSON
#define WT_RUST_SQL_MAX_BATCH_JSON 32768
#endif
#if !defined WT_RUST_SQL_MAX_RECV_BUFFER
#define WT_RUST_SQL_MAX_RECV_BUFFER 32768
#endif
#if !defined WT_RUST_SQL_MAX_LINE
#define WT_RUST_SQL_MAX_LINE 2048
#endif

ConVar g_hRustSqlOutletEnabled = null;
ConVar g_hRustSqlHost = null;
ConVar g_hRustSqlPort = null;
ConVar g_hRustSqlQueueMax = null;
ConVar g_hRustSqlBatchMax = null;
ConVar g_hRustSqlServerId = null;
ConVar g_hRustSqlDebug = null;

Socket g_hRustSqlSocket = null;
bool g_bRustSqlConnecting = false;
bool g_bRustSqlConnected = false;
bool g_bRustSqlAwaitingAck = false;
int g_iRustSqlNextBatchId = 1;
ArrayList g_hRustSqlQueue = null;
ArrayList g_hRustSqlQueueUserIds = null;
ArrayList g_hRustSqlInflight = null;
ArrayList g_hRustSqlInflightUserIds = null;
Handle g_hRustSqlFlushTimer = null;
Handle g_hRustSqlReconnectTimer = null;
char g_sRustSqlRecvBuffer[WT_RUST_SQL_MAX_RECV_BUFFER];
int g_iRustSqlRecvBufferLen = 0;
int g_iRustSqlDroppedWrites = 0;
int g_iRustSqlDropWarnCooldownUntil = 0;

bool WhaleTracker_RustSocketApiAvailable()
{
    return GetFeatureStatus(FeatureType_Native, "Socket.Connect") == FeatureStatus_Available;
}

bool WhaleTracker_UseRustSqlOutlet()
{
    return g_hRustSqlOutletEnabled != null && GetConVarBool(g_hRustSqlOutletEnabled) && WhaleTracker_RustSocketApiAvailable();
}

bool WhaleTracker_RustSqlDebugEnabled()
{
    return g_hRustSqlDebug != null && GetConVarBool(g_hRustSqlDebug);
}

bool WhaleTracker_RustSqlIsOnlineQuery(const char[] sql)
{
    return (StrContains(sql, "whaletracker_online") != -1 || StrContains(sql, "whaletracker_servers") != -1);
}

void WhaleTracker_RustJsonEscape(const char[] input, char[] output, int maxlen)
{
    int outPos = 0;
    for (int i = 0; input[i] != '\0' && outPos < maxlen - 1; i++)
    {
        char c = input[i];
        if (c == '"' || c == '\\')
        {
            if (outPos + 2 >= maxlen) break;
            output[outPos++] = '\\';
            output[outPos++] = c;
            continue;
        }
        if (c == '\n' || c == '\r' || c == '\t')
        {
            if (outPos + 2 >= maxlen) break;
            output[outPos++] = '\\';
            output[outPos++] = (c == '\n') ? 'n' : ((c == '\r') ? 'r' : 't');
            continue;
        }
        if (c < 32) continue;
        output[outPos++] = c;
    }
    output[outPos] = '\0';
}

void WhaleTracker_BuildRustServerId(char[] buffer, int maxlen)
{
    if (g_hRustSqlServerId != null)
    {
        g_hRustSqlServerId.GetString(buffer, maxlen);
        TrimString(buffer);
        if (buffer[0] != '\0') return;
    }

    char hostname[128];
    ConVar cvarHostName = FindConVar("hostname");
    if (cvarHostName != null) cvarHostName.GetString(hostname, sizeof(hostname));
    else strcopy(hostname, sizeof(hostname), "unknown");

    int port = 0;
    ConVar cvarHostPort = FindConVar("hostport");
    if (cvarHostPort != null) port = GetConVarInt(cvarHostPort);

    FormatEx(buffer, maxlen, "%s:%d", hostname, port);
}

void WhaleTracker_RustEnsureQueues()
{
    if (g_hRustSqlQueue == null) g_hRustSqlQueue = new ArrayList(ByteCountToCells(SAVE_QUERY_MAXLEN));
    if (g_hRustSqlQueueUserIds == null) g_hRustSqlQueueUserIds = new ArrayList();
    if (g_hRustSqlInflight == null) g_hRustSqlInflight = new ArrayList(ByteCountToCells(SAVE_QUERY_MAXLEN));
    if (g_hRustSqlInflightUserIds == null) g_hRustSqlInflightUserIds = new ArrayList();
}

void WhaleTracker_RustClearInflight()
{
    if (g_hRustSqlInflight != null) g_hRustSqlInflight.Clear();
    if (g_hRustSqlInflightUserIds != null) g_hRustSqlInflightUserIds.Clear();
}

void WhaleTracker_RustRequeueInflight()
{
    if (g_hRustSqlInflight == null || g_hRustSqlInflightUserIds == null || g_hRustSqlInflight.Length <= 0) return;
    WhaleTracker_RustEnsureQueues();

    char sql[SAVE_QUERY_MAXLEN];
    for (int i = 0; i < g_hRustSqlInflight.Length; i++)
    {
        g_hRustSqlInflight.GetString(i, sql, sizeof(sql));
        g_hRustSqlQueue.PushString(sql);
        g_hRustSqlQueueUserIds.Push(g_hRustSqlInflightUserIds.Get(i));
    }
    WhaleTracker_RustClearInflight();
}

void WhaleTracker_RustFlushPendingToLocal()
{
    WhaleTracker_RustRequeueInflight();
    if (g_hRustSqlQueue == null || g_hRustSqlQueueUserIds == null) return;

    char sql[SAVE_QUERY_MAXLEN];
    while (g_hRustSqlQueue.Length > 0)
    {
        g_hRustSqlQueue.GetString(0, sql, sizeof(sql));
        int userId = g_hRustSqlQueueUserIds.Get(0);
        g_hRustSqlQueue.Erase(0);
        g_hRustSqlQueueUserIds.Erase(0);
        if (!g_bDatabaseReady || g_hDatabase == null)
        {
            continue;
        }

        DBResultSet results = SQL_Query(g_hDatabase, sql);
        if (results == null)
        {
            char error[256];
            SQL_GetError(g_hDatabase, error, sizeof(error));
            LogError("[WhaleTracker] Rust SQL outlet local fallback failed (user %d): %s | Query: %s", userId, error, sql);
            continue;
        }
        delete results;
    }
}

void WhaleTracker_RustScheduleReconnect()
{
    if (!WhaleTracker_UseRustSqlOutlet() || g_hRustSqlReconnectTimer != null) return;
    g_hRustSqlReconnectTimer = CreateTimer(2.0, WhaleTracker_RustReconnectTimer);
}

void WhaleTracker_RustDisconnectSocket()
{
    g_bRustSqlConnected = false;
    g_bRustSqlConnecting = false;
    g_bRustSqlAwaitingAck = false;
    g_iRustSqlRecvBufferLen = 0;
    g_sRustSqlRecvBuffer[0] = '\0';
    if (g_hRustSqlSocket != null)
    {
        CloseHandle(g_hRustSqlSocket);
        g_hRustSqlSocket = null;
    }
}

void WhaleTracker_RustConnectSocket()
{
    if (!WhaleTracker_UseRustSqlOutlet() || g_bRustSqlConnected || g_bRustSqlConnecting) return;
    if (g_hRustSqlReconnectTimer != null)
    {
        CloseHandle(g_hRustSqlReconnectTimer);
        g_hRustSqlReconnectTimer = null;
    }

    WhaleTracker_RustEnsureQueues();
    WhaleTracker_RustDisconnectSocket();

    char host[128], portStr[16];
    g_hRustSqlHost.GetString(host, sizeof(host));
    g_hRustSqlPort.GetString(portStr, sizeof(portStr));
    int port = StringToInt(portStr);
    if (port <= 0) port = 28017;

    g_hRustSqlSocket = new Socket(SOCKET_TCP, WhaleTracker_RustOnSocketError);
    if (g_hRustSqlSocket == null)
    {
        LogError("[WhaleTracker] Failed to create Rust SQL outlet socket");
        WhaleTracker_RustScheduleReconnect();
        return;
    }

    g_hRustSqlSocket.SetOption(SocketKeepAlive, 1);
    g_hRustSqlSocket.SetOption(SocketSendBuffer, 65536);
    g_hRustSqlSocket.SetOption(SocketReceiveBuffer, 65536);
    g_bRustSqlConnecting = true;
    LogMessage("[WhaleTracker] Rust SQL outlet connecting to %s:%d", host, port);
    g_hRustSqlSocket.Connect(WhaleTracker_RustOnSocketConnected, WhaleTracker_RustOnSocketReceive, WhaleTracker_RustOnSocketDisconnected, host, port);
}

public Action WhaleTracker_RustReconnectTimer(Handle timer, any data)
{
    g_hRustSqlReconnectTimer = null;
    WhaleTracker_RustConnectSocket();
    return Plugin_Stop;
}

public Action WhaleTracker_RustFlushTimer(Handle timer, any data)
{
    WhaleTracker_RustFlushSqlBatch();
    return Plugin_Continue;
}

public void WhaleTracker_RustInit()
{
    g_hRustSqlOutletEnabled = CreateConVar("sm_whaletracker_rust_sql_outlet", "1", "Send WhaleTracker SQL writes to Rust TCP outlet (1=yes, 0=local DB writes only)");
    g_hRustSqlHost = CreateConVar("sm_whaletracker_rust_host", "127.0.0.1", "Rust SQL outlet host");
    g_hRustSqlPort = CreateConVar("sm_whaletracker_rust_port", "28017", "Rust SQL outlet TCP port");
    g_hRustSqlQueueMax = CreateConVar("sm_whaletracker_rust_queue_max", "4096", "Max queued SQL writes for Rust outlet before dropping oldest");
    g_hRustSqlBatchMax = CreateConVar("sm_whaletracker_rust_batch_max", "64", "Max SQL writes per Rust outlet batch");
    g_hRustSqlServerId = CreateConVar("sm_whaletracker_rust_server_id", "", "Optional server identifier for Rust SQL outlet hello");
    g_hRustSqlDebug = CreateConVar("sm_whaletracker_rust_sql_debug", "0", "Enable verbose Rust SQL outlet debug logging (1=yes, 0=no)");

    WhaleTracker_RustEnsureQueues();
    WhaleTracker_RustClearInflight();
    if (WhaleTracker_RustSocketApiAvailable())
    {
        view_as<Socket>(null).SetOption(CallbacksPerFrame, 8);
        view_as<Socket>(null).SetOption(ConcatenateCallbacks, 8192);
    }
    if (g_hRustSqlFlushTimer != null) CloseHandle(g_hRustSqlFlushTimer);
    g_hRustSqlFlushTimer = CreateTimer(0.10, WhaleTracker_RustFlushTimer, _, TIMER_REPEAT);
    LogMessage("[WhaleTracker] Rust SQL outlet init: enabled=%d socket_api=%d", GetConVarBool(g_hRustSqlOutletEnabled) ? 1 : 0, WhaleTracker_RustSocketApiAvailable() ? 1 : 0);
    WhaleTracker_RustConnectSocket();
}

public void WhaleTracker_RustShutdown()
{
    WhaleTracker_RustFlushSqlBatch();
    WhaleTracker_RustFlushPendingToLocal();
    WhaleTracker_RustDisconnectSocket();
    if (g_hRustSqlFlushTimer != null) { CloseHandle(g_hRustSqlFlushTimer); g_hRustSqlFlushTimer = null; }
    if (g_hRustSqlReconnectTimer != null) { CloseHandle(g_hRustSqlReconnectTimer); g_hRustSqlReconnectTimer = null; }
}

public bool WhaleTracker_RustQueueSqlWrite(const char[] query, int userId, bool forceSync)
{
    if (!WhaleTracker_UseRustSqlOutlet()) return false;
    if (forceSync || g_bShuttingDown)
    {
        if (WhaleTracker_RustSqlIsOnlineQuery(query))
        {
            if (WhaleTracker_RustSqlDebugEnabled())
                LogMessage("[WhaleTracker] Rust SQL outlet bypassed online query (forceSync=%d shuttingDown=%d): %s", forceSync ? 1 : 0, g_bShuttingDown ? 1 : 0, query);
        }
        return false;
    }
    if (!g_bRustSqlConnected || g_hRustSqlSocket == null)
    {
        return false;
    }

    WhaleTracker_RustEnsureQueues();
    int queueMax = GetConVarInt(g_hRustSqlQueueMax);
    if (queueMax < 1) queueMax = 1;
    while (g_hRustSqlQueue.Length >= queueMax && g_hRustSqlQueue.Length > 0)
    {
        g_hRustSqlQueue.Erase(0);
        g_hRustSqlQueueUserIds.Erase(0);
        g_iRustSqlDroppedWrites++;
    }

    if (g_iRustSqlDroppedWrites > 0)
    {
        int now = GetTime();
        if (now >= g_iRustSqlDropWarnCooldownUntil)
        {
            g_iRustSqlDropWarnCooldownUntil = now + 10;
            LogError("[WhaleTracker] Dropped %d Rust SQL outlet writes due to queue pressure", g_iRustSqlDroppedWrites);
            g_iRustSqlDroppedWrites = 0;
        }
    }

    g_hRustSqlQueue.PushString(query);
    g_hRustSqlQueueUserIds.Push(userId);
    if (WhaleTracker_RustSqlIsOnlineQuery(query))
    {
        if (WhaleTracker_RustSqlDebugEnabled())
            LogMessage("[WhaleTracker] Rust SQL queued online query (queue=%d user=%d connected=%d awaiting_ack=%d): %s",
                g_hRustSqlQueue.Length,
                userId,
                g_bRustSqlConnected ? 1 : 0,
                g_bRustSqlAwaitingAck ? 1 : 0,
                query);
    }
    WhaleTracker_RustFlushSqlBatch();
    return true;
}

public void WhaleTracker_RustFlushSqlBatch()
{
    if (!WhaleTracker_UseRustSqlOutlet() || !g_bRustSqlConnected || g_hRustSqlSocket == null || g_bRustSqlAwaitingAck) return;
    if (g_hRustSqlQueue == null || g_hRustSqlQueueUserIds == null || g_hRustSqlQueue.Length <= 0) return;

    char out[WT_RUST_SQL_MAX_BATCH_JSON];
    int pos = 0;
    int batchId = g_iRustSqlNextBatchId++;
    pos += FormatEx(out[pos], sizeof(out) - pos, "{\"type\":\"sql_batch\",\"batch_id\":%d,\"sent_at\":%d,\"writes\":[", batchId, GetTime());

    int batchMax = GetConVarInt(g_hRustSqlBatchMax);
    if (batchMax < 1) batchMax = 1;
    WhaleTracker_RustClearInflight();

    char sql[SAVE_QUERY_MAXLEN];
    char escaped[(SAVE_QUERY_MAXLEN * 2) + 1];
    int sentCount = 0;
    for (int i = 0; i < g_hRustSqlQueue.Length && sentCount < batchMax; i++)
    {
        g_hRustSqlQueue.GetString(i, sql, sizeof(sql));
        WhaleTracker_RustJsonEscape(sql, escaped, sizeof(escaped));
        int userId = g_hRustSqlQueueUserIds.Get(i);
        int estimated = strlen(escaped) + 64 + (sentCount > 0 ? 1 : 0);
        if (pos + estimated + 4 >= sizeof(out)) break;
        if (sentCount > 0) { out[pos++] = ','; out[pos] = '\0'; }
        pos += FormatEx(out[pos], sizeof(out) - pos, "{\"sql\":\"%s\",\"user_id\":%d}", escaped, userId);
        g_hRustSqlInflight.PushString(sql);
        g_hRustSqlInflightUserIds.Push(userId);
        sentCount++;
    }
    if (sentCount <= 0) { WhaleTracker_RustClearInflight(); return; }
    pos += FormatEx(out[pos], sizeof(out) - pos, "]}\n");

    g_hRustSqlSocket.Send(out, pos);
    g_hRustSqlSocket.SetSendqueueEmptyCallback(WhaleTracker_RustOnSocketSendqueueEmpty);
    g_bRustSqlAwaitingAck = true;
    if (WhaleTracker_RustSqlDebugEnabled())
        LogMessage("[WhaleTracker] Rust SQL sent batch id=%d writes=%d bytes=%d queue_remaining=%d", batchId, sentCount, pos, g_hRustSqlQueue.Length - sentCount);

    for (int i = 0; i < sentCount; i++)
    {
        g_hRustSqlQueue.Erase(0);
        g_hRustSqlQueueUserIds.Erase(0);
    }
}

public void WhaleTracker_RustOnSocketConnected(Socket socket, any arg)
{
    g_bRustSqlConnecting = false;
    g_bRustSqlConnected = true;
    g_bRustSqlAwaitingAck = false;
    g_iRustSqlRecvBufferLen = 0;
    g_sRustSqlRecvBuffer[0] = '\0';

    char serverId[128], escapedServerId[256], hello[512];
    WhaleTracker_BuildRustServerId(serverId, sizeof(serverId));
    WhaleTracker_RustJsonEscape(serverId, escapedServerId, sizeof(escapedServerId));
    int len = FormatEx(hello, sizeof(hello), "{\"type\":\"hello\",\"service\":\"whaletracker_sql_outlet\",\"proto\":1,\"server_id\":\"%s\",\"ts\":%d}\n", escapedServerId, GetTime());
    socket.Send(hello, len);
    socket.SetSendqueueEmptyCallback(WhaleTracker_RustOnSocketSendqueueEmpty);
    LogMessage("[WhaleTracker] Rust SQL outlet connected; hello sent (server_id=%s)", serverId);
    WhaleTracker_RustFlushSqlBatch();
}

public void WhaleTracker_RustOnSocketDisconnected(Socket socket, any arg)
{
    LogMessage("[WhaleTracker] Rust SQL outlet disconnected; falling back to local DB for inflight=%d queued=%d",
        (g_hRustSqlInflight != null) ? g_hRustSqlInflight.Length : 0,
        (g_hRustSqlQueue != null) ? g_hRustSqlQueue.Length : 0);
    WhaleTracker_RustFlushPendingToLocal();
    WhaleTracker_RustDisconnectSocket();
    WhaleTracker_RustScheduleReconnect();
}

public void WhaleTracker_RustOnSocketError(Socket socket, const int errorType, const int errorNum, any arg)
{
    LogError("[WhaleTracker] Rust SQL outlet socket error type=%d errno=%d", errorType, errorNum);
    WhaleTracker_RustFlushPendingToLocal();
    WhaleTracker_RustDisconnectSocket();
    WhaleTracker_RustScheduleReconnect();
}

public void WhaleTracker_RustOnSocketSendqueueEmpty(Socket socket, any arg) {}

public void WhaleTracker_RustOnSocketReceive(Socket socket, const char[] receiveData, const int dataSize, any arg)
{
    if (dataSize <= 0) return;
    if (WhaleTracker_RustSqlDebugEnabled())
        LogMessage("[WhaleTracker] Rust SQL outlet recv bytes=%d", dataSize);
    if (g_iRustSqlRecvBufferLen + dataSize >= sizeof(g_sRustSqlRecvBuffer))
    {
        LogError("[WhaleTracker] Rust SQL outlet receive buffer overflow");
        g_iRustSqlRecvBufferLen = 0;
        g_sRustSqlRecvBuffer[0] = '\0';
        return;
    }

    for (int i = 0; i < dataSize && g_iRustSqlRecvBufferLen < sizeof(g_sRustSqlRecvBuffer) - 1; i++)
    {
        g_sRustSqlRecvBuffer[g_iRustSqlRecvBufferLen++] = receiveData[i];
    }
    g_sRustSqlRecvBuffer[g_iRustSqlRecvBufferLen] = '\0';
    WhaleTracker_RustParseIncomingLines();
}

void WhaleTracker_RustParseIncomingLines()
{
    int start = 0;
    for (int i = 0; i < g_iRustSqlRecvBufferLen; i++)
    {
        if (g_sRustSqlRecvBuffer[i] != '\n') continue;
        int lineLen = i - start;
        if (lineLen > 0)
        {
            char line[WT_RUST_SQL_MAX_LINE];
            if (lineLen >= sizeof(line)) lineLen = sizeof(line) - 1;
            for (int j = 0; j < lineLen; j++) line[j] = g_sRustSqlRecvBuffer[start + j];
            line[lineLen] = '\0';
            WhaleTracker_RustHandleBackendLine(line);
        }
        start = i + 1;
    }
    if (start > 0)
    {
        int remaining = g_iRustSqlRecvBufferLen - start;
        for (int k = 0; k < remaining; k++) g_sRustSqlRecvBuffer[k] = g_sRustSqlRecvBuffer[start + k];
        g_iRustSqlRecvBufferLen = remaining;
        g_sRustSqlRecvBuffer[g_iRustSqlRecvBufferLen] = '\0';
    }
}

void WhaleTracker_RustHandleBackendLine(const char[] line)
{
    if (line[0] == '\0') return;
    if (WhaleTracker_RustSqlDebugEnabled())
        LogMessage("[WhaleTracker] Rust SQL outlet recv line: %s", line);
    if (StrContains(line, "\"type\":\"ack\"") != -1)
    {
        g_bRustSqlAwaitingAck = false;
        if (WhaleTracker_RustSqlDebugEnabled())
            LogMessage("[WhaleTracker] Rust SQL outlet ACK received; inflight=%d queued=%d",
                (g_hRustSqlInflight != null) ? g_hRustSqlInflight.Length : 0,
                (g_hRustSqlQueue != null) ? g_hRustSqlQueue.Length : 0);
        WhaleTracker_RustClearInflight();
        WhaleTracker_RustFlushSqlBatch();
        return;
    }
    if (StrContains(line, "\"type\":\"error\"") != -1)
    {
        g_bRustSqlAwaitingAck = false;
        LogError("[WhaleTracker] Rust SQL outlet backend error: %s", line);
        WhaleTracker_RustRequeueInflight();
        if (WhaleTracker_RustSqlDebugEnabled())
            LogMessage("[WhaleTracker] Rust SQL outlet requeued after backend error; queued=%d", (g_hRustSqlQueue != null) ? g_hRustSqlQueue.Length : 0);
    }
}
