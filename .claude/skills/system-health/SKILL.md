---
name: system-health
description: Monitor Q-TRON system health across KR and US markets — API connectivity, data freshness, state file integrity, dashboard status, and notification delivery.
user_invocable: true
command: system-health
---

# Q-TRON System Health Monitor

Comprehensive health check across the Q-TRON KR and US trading systems. Validates API connectivity, data pipeline freshness, state file integrity, dashboard availability, and notification delivery.

---

## Invocation

```
/system-health                    Full health check (all systems)
/system-health kr                 KR market systems only
/system-health us                 US market systems only
/system-health api                API connectivity check only
/system-health data               Data freshness check only
/system-health state              State file integrity only
/system-health dashboard          Dashboard status only
/system-health notify             Notification system check only
```

---

## Key File Paths

### KR Market (Gen04-REST)
- **Main entry**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/main.py`
- **Config**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/config.py`
- **REST provider**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/data/rest_provider.py`
- **REST token manager**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/data/rest_token_manager.py`
- **REST websocket**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/data/rest_websocket.py`
- **REST logger**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/data/rest_logger.py`
- **IP monitor**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/data/ip_monitor.py`
- **pykrx provider**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/data/pykrx_provider.py`
- **DB provider**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/data/db_provider.py`
- **Universe builder**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/data/universe_builder.py`
- **Portfolio manager**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/core/portfolio_manager.py`
- **State manager**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/core/state_manager.py`
- **Kakao notify**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/notify/kakao_notify.py`
- **Telegram bot**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/notify/telegram_bot.py`
- **Alert engine**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/notify/alert_engine.py`
- **Alert state**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/notify/alert_state.py`
- **Exposure guard**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/risk/exposure_guard.py`
- **Safety checks**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/risk/safety_checks.py`
- **Regime API**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/regime/api.py`

### KR State & Data
- **State dir**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/state/`
- **Live portfolio**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/state/portfolio_state_live.json`
- **Live runtime**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/state/runtime_state_live.json`
- **Paper portfolio**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/state/portfolio_state_paper.json`
- **Paper runtime**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/state/runtime_state_paper.json`
- **Backtest data**: `C:/Q-TRON-32_ARCHIVE/backtest/data_full/`

### US Market (Gen04-US)
- **Main entry**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/main.py`
- **Config**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/config.py`
- **Alpaca provider**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/data/alpaca_provider.py`
- **Alpaca data**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/data/alpaca_data.py`
- **DB provider**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/data/db_provider.py`
- **Universe builder**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/data/universe_builder.py`
- **Portfolio manager**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/core/portfolio_manager.py`
- **State manager**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/core/state_manager.py`
- **Telegram bot**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/notify/telegram_bot.py`

### US State
- **Paper portfolio**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/state/portfolio_state_us_paper.json`
- **Paper runtime**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/state/runtime_state_us_paper.json`

### Strategy Lab
- **KR Lab**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/lab/`
- **US Lab**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/lab/`

### Python Environments
- **KR Python**: `C:/Q-TRON-32_ARCHIVE/.venv/Scripts/python.exe` (3.9, 32-bit)
- **US Python**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/.venv/Scripts/python.exe` (3.12, 64-bit)

---

## Health Check Procedure

### Phase 1: API Connectivity

1. **KR Kiwoom REST API**:
   - Read `rest_token_manager.py` for token status and expiry
   - Read `rest_provider.py` for recent API call success/failure counts
   - Read `ip_monitor.py` for IP address stability (Kiwoom IP whitelist)
   - Check `rest_logger.py` for recent error patterns
   - Verify websocket connection status in `rest_websocket.py`
   - Check for REST_STALE events (stale data from REST API)

2. **US Alpaca API**:
   - Read `alpaca_provider.py` for connection status
   - Check API key validity (read config, do NOT expose keys)
   - Verify market data subscription active

3. **Database connectivity**:
   - Read `db_provider.py` (both KR and US) for PostgreSQL connection status
   - Check for connection pool exhaustion or timeout errors

4. **Report format**:
   ```
   API CONNECTIVITY
   System          Service              Status     Last Success    Notes
   KR              Kiwoom REST          [OK/DOWN]  HH:MM:SS        ...
   KR              Kiwoom WebSocket     [OK/DOWN]  HH:MM:SS        ...
   KR              pykrx               [OK/DOWN]  HH:MM:SS        ...
   KR              PostgreSQL           [OK/DOWN]  HH:MM:SS        ...
   US              Alpaca Trading       [OK/DOWN]  HH:MM:SS        ...
   US              Alpaca Data          [OK/DOWN]  HH:MM:SS        ...
   US              PostgreSQL           [OK/DOWN]  HH:MM:SS        ...
   ```

### Phase 2: Data Freshness

1. **OHLCV data**:
   - Check latest date in backtest data (`backtest/data_full/`)
   - For KR: verify pykrx_provider returns current-day data
   - For US: verify Alpaca data feed is current
   - Flag if data is more than 1 trading day stale

2. **Universe data**:
   - Check `universe_builder.py` last run timestamp
   - Verify universe size (KR: 500+ expected, US: varies)
   - Check for quality filter anomalies (sudden drop in universe size)

3. **Regime data**:
   - Check regime collector timestamps (domestic + global for KR, collector for US)
   - Verify regime prediction is current (not stale)
   - Check regime storage for gaps

4. **Intraday data** (KR only):
   - Check `intraday_collector.py` for collection status
   - Check `microstructure_collector.py` for VWAP, volume data freshness
   - Check `swing_collector.py` for swing data

5. **Report format**:
   ```
   DATA FRESHNESS
   Data Source          Market    Latest Date    Expected    Status
   OHLCV (backtest)     KR       YYYY-MM-DD     Today-1     [FRESH/STALE]
   OHLCV (live)         KR       YYYY-MM-DD     Today       [FRESH/STALE]
   Universe             KR       YYYY-MM-DD     This week   [FRESH/STALE]
   Regime prediction    KR       YYYY-MM-DD     Today       [FRESH/STALE]
   Intraday             KR       YYYY-MM-DD     Today       [FRESH/STALE]
   OHLCV (live)         US       YYYY-MM-DD     Today       [FRESH/STALE]
   Universe             US       YYYY-MM-DD     This week   [FRESH/STALE]
   Regime prediction    US       YYYY-MM-DD     Today       [FRESH/STALE]
   ```

### Phase 3: State File Integrity

1. **Parse state files** (JSON validation):
   - Load each state JSON file and verify it parses correctly
   - Check for required fields (positions, cash, last_update, etc.)
   - Verify backward compatibility (old JSON loads into new schema)

2. **State consistency**:
   - Portfolio state: all positions have required fields (ticker, qty, avg_price, peak_price)
   - Runtime state: rebalance counter, last_rebalance_date, safe_mode status
   - No negative quantities or negative cash (unless margin, which Q-TRON doesn't use)
   - Trail stop peak_price >= avg_price for all positions

3. **Backup status**:
   - Check for backup files in state/ directory
   - Verify forensic snapshots exist for recent anomalies
   - State files must NEVER be deleted (only backed up + new created)

4. **Report format**:
   ```
   STATE FILE INTEGRITY
   File                              Valid JSON    Schema OK    Anomalies
   portfolio_state_live.json         [YES/NO]      [YES/NO]     ...
   runtime_state_live.json           [YES/NO]      [YES/NO]     ...
   portfolio_state_paper.json        [YES/NO]      [YES/NO]     ...
   portfolio_state_us_paper.json     [YES/NO]      [YES/NO]     ...
   
   Positions: NN (KR live), NN (KR paper), NN (US paper)
   Backups found: NN
   Forensic snapshots: NN
   
   ANOMALIES: [list any issues]
   ```

### Phase 4: Dashboard Status

1. **KR Dashboard** (FastAPI on :8080):
   - Check if dashboard process is running
   - Verify port 8080 is accessible (localhost)
   - Check for recent errors in dashboard logs

2. **US Dashboard** (FastAPI on :8081):
   - Check if dashboard process is running
   - Verify port 8081 is accessible (localhost)
   - Check for recent errors in dashboard logs

3. **Report format**:
   ```
   DASHBOARD STATUS
   Dashboard    Port    Process    Accessible    Last Error
   KR           8080    [UP/DOWN]  [YES/NO]      ...
   US           8081    [UP/DOWN]  [YES/NO]      ...
   ```

### Phase 5: Notification System

1. **KR notifications**:
   - Read `kakao_notify.py` for Kakao alert configuration
   - Read `telegram_bot.py` for Telegram bot status
   - Read `alert_engine.py` for alert rule definitions
   - Read `alert_state.py` for recent alert history
   - Check: SAFE_MODE alerts, RECON alerts, trail stop alerts, rebalance alerts

2. **US notifications**:
   - Read `telegram_bot.py` for Telegram bot status
   - Verify bot token is configured (do NOT expose token)

3. **Alert coverage**:
   - SAFE_MODE level changes: [CONFIGURED/MISSING]
   - RECON discrepancies: [CONFIGURED/MISSING]
   - Trail stop triggers: [CONFIGURED/MISSING]
   - Rebalance completion: [CONFIGURED/MISSING]
   - EOD summary: [CONFIGURED/MISSING]
   - DD guard activation: [CONFIGURED/MISSING]

4. **Report format**:
   ```
   NOTIFICATION SYSTEM
   Channel          Market    Configured    Last Sent       Status
   Kakao            KR        [YES/NO]      YYYY-MM-DD      [ACTIVE/STALE]
   Telegram         KR        [YES/NO]      YYYY-MM-DD      [ACTIVE/STALE]
   Telegram         US        [YES/NO]      YYYY-MM-DD      [ACTIVE/STALE]
   
   ALERT COVERAGE
   Alert Type              KR           US
   SAFE_MODE change        [OK/MISS]    [OK/MISS]
   RECON discrepancy       [OK/MISS]    [OK/MISS]
   Trail stop trigger      [OK/MISS]    [OK/MISS]
   Rebalance complete      [OK/MISS]    [OK/MISS]
   EOD summary             [OK/MISS]    [OK/MISS]
   DD guard activation     [OK/MISS]    [OK/MISS]
   ```

---

## Summary Report

```
Q-TRON SYSTEM HEALTH SUMMARY
============================================================
Category              KR Status    US Status    Priority
------------------------------------------------------------
API connectivity      [OK/WARN/DOWN]  [OK/WARN/DOWN]   ...
Data freshness        [FRESH/STALE]   [FRESH/STALE]    ...
State integrity       [OK/ISSUE]      [OK/ISSUE]       ...
Dashboard             [UP/DOWN]       [UP/DOWN]        ...
Notifications         [OK/PARTIAL]    [OK/PARTIAL]     ...
============================================================
OVERALL: [HEALTHY / DEGRADED / CRITICAL]

RECOMMENDED ACTIONS:
1. [action if any issues found]
```

---

## Diagnostic Commands

These commands can be used to gather additional information during health checks:

```bash
# KR system process check
tasklist | findstr python

# Check if dashboards are listening
netstat -an | findstr "8080\|8081"

# KR Python environment check
C:/Q-TRON-32_ARCHIVE/.venv/Scripts/python.exe --version

# US Python environment check
C:/Q-TRON-32_ARCHIVE/Gen04-US/.venv/Scripts/python.exe --version

# State file sizes (detect corruption — 0 bytes = problem)
ls -la C:/Q-TRON-32_ARCHIVE/Gen04-REST/state/*.json
ls -la C:/Q-TRON-32_ARCHIVE/Gen04-US/state/*.json

# Recent log errors (KR)
# Check Gen04-REST/logs/ directory for recent error entries

# Recent log errors (US)
# Check Gen04-US/logs/ directory for recent error entries
```

---

## Safety Rules

- **Read-only monitoring** -- never modify state files, configs, or restart services
- **Do NOT expose API keys**, tokens, or credentials in reports
- **Do NOT make API calls** to broker (Kiwoom/Alpaca) -- only read local state and logs
- State files must never be deleted (per State Protection rules)
- If critical issues found (state corruption, API down during market hours), classify as P0 and escalate per execution policy
- Respect PC sharing constraint: do not run heavy diagnostics during evening hours
