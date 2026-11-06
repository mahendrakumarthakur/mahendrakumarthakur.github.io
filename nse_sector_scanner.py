"""
nse_sector_scanner.py
---------------------
NSE Sector F&O Scanner - runs once at 9:15 AM IST via Windows Task Scheduler.

Requirements satisfied:
  1. NSE DATA:
     - Uses official NSE public API endpoints.
     - Small number of requests (~23 total).
     - No continuous polling, no CAPTCHA bypass, no proxy rotation.
     - Reasonable timeouts, retries, and sleep delays between requests.
  2. SECTORS:
     - Dynamically discovers all Sectoral Indices from NSE allIndices.
     - Obtains constituent stocks, current LTP, and percentage change.
  3. F&O FILTER:
     - Keeps ONLY stocks in the NSE F&O segment via official NSE F&O list.
     - Excludes sectors with zero F&O stocks from the final result.
  4. SQLITE:
     - Creates/uses market_scanner.db.
     - Historical storage across scans (never overwrites).
     - Tables: scans, nifty, sectors, stocks with useful indexes.
  5. NIFTY 50:
     - LTP, percentage change, direction (POSITIVE/NEGATIVE).
  6. FINAL SUMMARY:
     - Top 2 Gaining Sectors with their Top 2 F&O gaining stocks.
     - Top 2 Losing Sectors with their Top 2 F&O losing stocks.
     - Uses actual NSE-supplied sector percentage change for ranking.
  7. TERMINAL OUTPUT:
     - Clean, readable summary matching exact required structure.
"""

import sqlite3
import json
import time
import random
import re
import threading
import urllib.parse
from datetime import datetime, timezone, timedelta
import sys
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

# Ensure UTF-8 output so emojis and special chars render correctly in Windows Terminal
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# Dependency check
try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print("ERROR: curl_cffi is not installed.")
    print("Run:  pip install curl_cffi")
    sys.exit(1)

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------
DB_FILE     = "market_scanner.db"
DATA_SOURCE = "NSE"
IST         = timezone(timedelta(hours=5, minutes=30))

# In allIndices, NSE categorizes sectoral indices under 'SECTORAL INDICES'.
# NIFTY BANK and NIFTY FINANCIAL SERVICES are major sectors listed under
# 'INDICES ELIGIBLE IN DERIVATIVES' in allIndices, so we include them too.
SECTOR_KEYS = {"SECTORAL INDICES"}
ADDITIONAL_SECTORS = {"NIFTY BANK", "NIFTY FINANCIAL SERVICES"}

EXCLUDE_SECTORS = {
    "NIFTY 500",
    "NIFTY TOTAL MARKET",
    "NIFTY LARGEMIDCAP 250",
}

REQUEST_TIMEOUT     = 15      # seconds per request
RETRY_ATTEMPTS      = 3       # max retries on transient errors
RETRY_DELAY         = 5.0     # base seconds between retries (multiplied by attempt)
INTER_REQUEST_DELAY = (0.8, 1.4)  # random sleep interval between requests to protect IP

# -----------------------------------------------------------------------------
# DISK CACHE (day-stable data: lot sizes, F&O symbol list — one fetch per day)
# -----------------------------------------------------------------------------

CACHE_DIR          = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
CACHE_MAX_AGE_DAYS = 7


def _cache_file(name: str, scan_date: str) -> str:
    return os.path.join(CACHE_DIR, f"{name}_{scan_date}.json")


def _prune_cache() -> None:
    """Delete cache files older than CACHE_MAX_AGE_DAYS."""
    try:
        if not os.path.isdir(CACHE_DIR):
            return
        cutoff = time.time() - CACHE_MAX_AGE_DAYS * 86400
        for fn in os.listdir(CACHE_DIR):
            fp = os.path.join(CACHE_DIR, fn)
            if os.path.isfile(fp) and os.path.getmtime(fp) < cutoff:
                os.remove(fp)
    except Exception:
        pass


def _load_cache(name: str, scan_date: str):
    """Return today's cached JSON payload, or None."""
    try:
        fp = _cache_file(name, scan_date)
        if os.path.isfile(fp):
            with open(fp, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return None


def _load_latest_cache(name: str):
    """Return (payload, filename) from the most recent cache file for name."""
    try:
        candidates = sorted(
            fn for fn in os.listdir(CACHE_DIR)
            if fn.startswith(f"{name}_") and fn.endswith(".json")
        )
    except Exception:
        return None, None
    if not candidates:
        return None, None
    fp = os.path.join(CACHE_DIR, candidates[-1])
    try:
        with open(fp, "r", encoding="utf-8") as f:
            return json.load(f), candidates[-1]
    except Exception:
        return None, None


def _save_cache(name: str, scan_date: str, payload) -> None:
    """Atomically write payload to today's cache file (best effort)."""
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        fp = _cache_file(name, scan_date)
        tmp = fp + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        os.replace(tmp, fp)
    except Exception:
        pass


# -----------------------------------------------------------------------------
# HTTP SESSION (curl_cffi impersonates Chrome 120 for TLS fingerprint match)
# -----------------------------------------------------------------------------

def create_session() -> cffi_requests.Session:
    """Create a curl_cffi session configured to mimic a standard browser."""
    session = cffi_requests.Session(impersonate="chrome120")
    session.headers.update({
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://www.nseindia.com/market-data/live-market-indices/heatmap",
        "Sec-Ch-Ua": "\"Not_A Brand\";v=\"8\", \"Chromium\";v=\"120\", \"Google Chrome\";v=\"120\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    })
    return session


def warm_up_session(session: cffi_requests.Session) -> bool:
    """
    Initial handshake with NSE homepage and heatmap page to establish cookies.
    """
    print("[*] Initialising session with NSE ...")
    try:
        r1 = session.get("https://www.nseindia.com", timeout=REQUEST_TIMEOUT)
        time.sleep(random.uniform(1.0, 2.0))
        r2 = session.get(
            "https://www.nseindia.com/market-data/live-market-indices/heatmap",
            timeout=REQUEST_TIMEOUT
        )
        if r1.status_code == 200 and r2.status_code == 200:
            print("[+] Handshake successful.")
            return True
        print(f"[!] Handshake warning: HTTP {r1.status_code} / {r2.status_code}")
    except Exception as exc:
        print(f"[!] Session handshake error: {exc}")
    return False


def safe_get(session: cffi_requests.Session, url: str):
    """
    GET request with intelligent retry and backoff handling:
    - 200: success, returns Response.
    - 404: resource not available, returns None immediately without spamming.
    - 403 / 429: cooldown for 60 seconds, re-warms session, retries.
    - Network / 5xx errors: sleeps with exponential backoff before retrying.
    """
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)

            if resp.status_code == 200:
                return resp

            if resp.status_code == 404:
                print(f"[!] HTTP 404 for {url} (not retrying)")
                return None

            if resp.status_code in (403, 429):
                print(f"[!] HTTP {resp.status_code} detected! Cooling down 60s before retry ...")
                time.sleep(60)
                warm_up_session(session)
                continue

            print(f"[!] HTTP {resp.status_code} for {url} (attempt {attempt}/{RETRY_ATTEMPTS})")

        except Exception as exc:
            print(f"[!] Network error on attempt {attempt}/{RETRY_ATTEMPTS}: {exc}")

        if attempt < RETRY_ATTEMPTS:
            sleep_time = RETRY_DELAY * attempt
            print(f"    Waiting {sleep_time:.1f}s before retry ...")
            time.sleep(sleep_time)

    print(f"[X] Request failed after {RETRY_ATTEMPTS} attempts: {url}")
    return None


# -----------------------------------------------------------------------------
# NSE DATA FETCHERS
# -----------------------------------------------------------------------------

def _f(val, default: float = 0.0) -> float:
    """Float conversion that survives NSE quirks: None, '', or '1,234.50' strings.

    One malformed number must never raise inside a fetcher - it used to crash
    the whole scan (worker re-raise), which is how the 2026-11-03 Actions run
    died after fetching and then published an empty database.
    """
    if val in (None, ""):
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        try:
            return float(str(val).replace(",", ""))
        except (TypeError, ValueError):
            return default


def fetch_all_indices(session: cffi_requests.Session) -> list:
    """
    Fetch https://www.nseindia.com/api/allIndices
    Returns list of all indices with metadata, LTP, and percentage change.
    """
    print("[*] Fetching allIndices from NSE ...")
    resp = safe_get(session, "https://www.nseindia.com/api/allIndices")
    if not resp:
        return []
    try:
        return resp.json().get("data", [])
    except Exception as exc:
        print(f"[!] JSON parsing error (allIndices): {exc}")
        return []


def fetch_fno_symbols(session: cffi_requests.Session) -> set:
    """
    Obtain the current list of stocks available in the NSE F&O segment.
    Uses official NSE endpoints:
      1. https://www.nseindia.com/api/master-quote (official 24/7 master list of F&O underlyings)
      2. https://www.nseindia.com/api/equity-stock-indices?index=SECURITIES%20IN%20F%26O (market hours)
    """
    print("[*] Fetching NSE F&O underlying securities list ...")

    # 1. Primary: official master-quote list (returns active F&O underlying symbols)
    resp = safe_get(session, "https://www.nseindia.com/api/master-quote")
    if resp:
        try:
            symbols_list = resp.json()
            if isinstance(symbols_list, list) and len(symbols_list) > 50:
                fno_set = {str(s).strip().upper() for s in symbols_list if s}
                print(f"[+] Active F&O symbols retrieved (via master-quote): {len(fno_set)}")
                return fno_set
        except Exception as exc:
            print(f"[!] Parse error on master-quote: {exc}")

    # 2. Secondary: live market equity-stock-indices index
    url_fno = "https://www.nseindia.com/api/equity-stock-indices?index=SECURITIES%20IN%20F%26O"
    resp = safe_get(session, url_fno)
    if resp:
        try:
            items = resp.json().get("data", [])
            fno_set = {item.get("symbol", "").strip().upper() for item in items if item.get("symbol")}
            if fno_set:
                print(f"[+] Active F&O symbols retrieved (via live index): {len(fno_set)}")
                return fno_set
        except Exception as exc:
            print(f"[!] Parse error on live F&O index: {exc}")

    print("[!] Could not retrieve F&O symbols list from NSE.")
    return set()


def fetch_sector_constituents(session: cffi_requests.Session, query_symbol: str) -> list:
    """
    Fetch constituent stocks for a sector index using:
      https://www.nseindia.com/api/equity-stock-indices?index={query_symbol}
    
    query_symbol corresponds to the indexSymbol field from allIndices
    (e.g., 'NIFTY AUTO', 'NIFTY CONSR DURBL', 'NIFTY FIN SERVICE', etc.)
    """
    encoded_query = urllib.parse.quote(query_symbol)
    url = f"https://www.nseindia.com/api/equity-stock-indices?index={encoded_query}"
    resp = safe_get(session, url)
    if not resp:
        return []
    try:
        rows = resp.json().get("data", [])
    except Exception as exc:
        print(f"[!] JSON parsing error for {query_symbol}: {exc}")
        return []

    stocks = []
    # Row 0 is the index summary row; rows 1+ are constituent stocks.
    # _f() per row: a malformed value degrades that one field to 0 instead of
    # raising inside the worker thread and aborting the whole scan.
    for item in rows[1:]:
        sym = str(item.get("symbol") or "").strip().upper()
        if not sym:
            continue
        meta = item.get("meta") or {}
        company_name = meta.get("companyName") or item.get("identifier") or sym

        stocks.append({
            "symbol":       sym,
            "company_name": company_name,
            "ltp":          _f(item.get("lastPrice")),
            "pchange":      _f(item.get("pChange")),
            "open_price":   _f(item.get("open")),
            "day_high":     _f(item.get("dayHigh")),
            "day_low":      _f(item.get("dayLow")),
        })
    return stocks


def fetch_most_active_futures(session: cffi_requests.Session) -> dict:
    """
    Fetch https://www.nseindia.com/api/snapshot-derivatives-equity?index=futures
    Returns dict with 'volume' and 'value' lists of top active future contracts.
    """
    print("[*] Fetching Most Active Future Contracts from NSE ...")
    url = "https://www.nseindia.com/api/snapshot-derivatives-equity?index=futures"
    resp = safe_get(session, url)
    if not resp:
        return {"volume": [], "value": []}
    try:
        res = resp.json()
        vol_data = res.get("volume", {}).get("data", [])
        val_data = res.get("value", {}).get("data", [])
        print(f"[+] Active futures retrieved: {len(vol_data)} by volume, {len(val_data)} by value")
        return {"volume": vol_data, "value": val_data}
    except Exception as exc:
        print(f"[!] JSON parsing error (active futures): {exc}")
        return {"volume": [], "value": []}


def fetch_fno_ban_list(session) -> set:
    """
    Fetch NSE F&O Ban Period list (securities in ban period cannot have new positions).
    Uses snapshot-derivatives-equity?index=security-ban endpoint.
    Returns a set of banned symbol strings (empty set if none today or unavailable).
    """
    url = "https://www.nseindia.com/api/snapshot-derivatives-equity?index=security-ban"
    resp = safe_get(session, url)
    if not resp:
        print("[!] F&O ban list unavailable - skipping ban filter")
        return set()
    try:
        data = resp.json()
        banned = set()
        for item in data.get("data", []):
            if isinstance(item, str):
                sym = item.strip().upper()
            elif isinstance(item, dict):
                sym = (item.get("symbol") or item.get("underlying") or "").strip().upper()
            else:
                continue
            if sym:
                banned.add(sym)
        if banned:
            print(f"[!] F&O Ban list: {len(banned)} stock(s) REMOVED → {', '.join(sorted(banned))}")
        else:
            print(f"[+] F&O Ban list: 0 stocks in ban period today")
        return banned
    except Exception as exc:
        print(f"[!] F&O ban list parse error: {exc}")
        return set()



def fetch_active_stock_options(session, nifty_dir: str = "POSITIVE") -> tuple:
    """
    Fetch Most Active Stock Options from NSE.
    Returns (liq_map, call_map, put_map) where:
      - liq_map: combined map based on market direction for liquidity checks: {sym: volume}
      - call_map: {symbol: {"volume": int, "contracts_count": int, "contracts": list, "top_strike": float, ...}}
      - put_map:  {symbol: {"volume": int, "contracts_count": int, "contracts": list, "top_strike": float, ...}}
    """
    url = "https://www.nseindia.com/api/snapshot-derivatives-equity?index=options"
    resp = safe_get(session, url)
    if not resp:
        return {}, {}, {}
    try:
        data = resp.json()
        vol_all = data.get("volume", {}).get("data", [])
        val_all = data.get("value", {}).get("data", [])
        all_contracts = vol_all + val_all
        call_map = {}
        put_map  = {}
        seen_identifiers = set()

        for c in all_contracts:
            if c.get("instrument") != "Stock Options":
                continue
            c_id = c.get("identifier") or f"{c.get('underlying')}_{c.get('optionType')}_{c.get('strikePrice')}_{c.get('expiryDate')}"
            if c_id in seen_identifiers:
                continue
            seen_identifiers.add(c_id)

            otype = c.get("optionType")
            sym = c.get("underlying", "").strip().upper()
            vol = int(c.get("numberOfContractsTraded", 0) or 0)
            strike = float(c.get("strikePrice") or 0) if c.get("strikePrice") is not None else None
            ltp = float(c.get("lastPrice") or 0) if c.get("lastPrice") is not None else None
            oi = int(c.get("openInterest", 0) or 0)
            expiry = c.get("expiryDate", "")
            spot = float(c.get("underlyingValue") or 0) if c.get("underlyingValue") is not None else None

            contract_info = {
                "strike": strike,
                "ltp": ltp,
                "vol": vol,
                "oi": oi,
                "expiry": expiry,
                "spot": spot
            }

            target_dict = call_map if otype == "Call" else (put_map if otype == "Put" else None)
            if target_dict is not None:
                if sym not in target_dict:
                    target_dict[sym] = {
                        "volume": 0,
                        "contracts_count": 0,
                        "contracts": [],
                        "top_strike": strike,
                        "top_ltp": ltp,
                        "top_oi": oi,
                        "top_expiry": expiry,
                        "top_vol": vol,
                        "spot": spot
                    }
                target_dict[sym]["volume"] += vol
                target_dict[sym]["contracts_count"] += 1
                target_dict[sym]["contracts"].append(contract_info)
                if vol > target_dict[sym]["top_vol"]:
                    target_dict[sym]["top_vol"] = vol
                    target_dict[sym]["top_strike"] = strike
                    target_dict[sym]["top_ltp"] = ltp
                    target_dict[sym]["top_oi"] = oi
                    target_dict[sym]["top_expiry"] = expiry

        # liq_map for directional bias ([*] marker): {symbol: volume}
        target_map = put_map if nifty_dir == "NEGATIVE" else call_map
        liq_map = {s: info["volume"] for s, info in target_map.items()}

        print(f"[+] Active options: {len(call_map)} stock(s) with Call vol, {len(put_map)} stock(s) with Put vol")
        return liq_map, call_map, put_map
    except Exception as exc:
        print(f"[!] Active options parse error: {exc}")
        return {}, {}, {}


def fetch_nse_lot_sizes(session: cffi_requests.Session) -> dict:
    """
    Fetch official NSE F&O market lot sizes from NSE MII contract file:
    https://nsearchives.nseindia.com/content/fo/NSE_FO_contract_ddmmyyyy.csv.gz
    Tries current date back up to 5 days to handle weekends/holidays.
    """
    import gzip
    now = datetime.now()
    for i in range(5):
        dt_str = (now - timedelta(days=i)).strftime("%d%m%Y")
        url = f"https://nsearchives.nseindia.com/content/fo/NSE_FO_contract_{dt_str}.csv.gz"
        try:
            r = session.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                data = gzip.decompress(r.content).decode("utf-8", errors="ignore")
                lot_map = {}
                for line in data.splitlines()[1:]:
                    parts = line.split(",")
                    if len(parts) > 9:
                        sym = parts[3].strip().upper()
                        try:
                            lot = int(parts[9].strip())
                            if sym and lot > 0 and sym not in lot_map:
                                lot_map[sym] = lot
                        except ValueError:
                            pass
                if lot_map:
                    print(f"[+] Loaded {len(lot_map)} contract lot sizes (NSE MII {dt_str})")
                    return lot_map
        except Exception:
            pass
    return {}


# -----------------------------------------------------------------------------
# SQLITE STORAGE (Append-only historical storage with indexes)
# -----------------------------------------------------------------------------

SQL_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS scans (
        scan_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        scan_date   TEXT NOT NULL,
        scan_time   TEXT NOT NULL,
        data_source TEXT NOT NULL,
        partial     INTEGER NOT NULL DEFAULT 0
    );""",
    """CREATE TABLE IF NOT EXISTS nifty (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        scan_id     INTEGER NOT NULL REFERENCES scans(scan_id),
        ltp         REAL    NOT NULL,
        change_pct  REAL    NOT NULL,
        direction   TEXT    NOT NULL,
        timestamp   TEXT    NOT NULL
    );""",
    """CREATE TABLE IF NOT EXISTS sectors (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        scan_id     INTEGER NOT NULL REFERENCES scans(scan_id),
        sector_name TEXT    NOT NULL,
        change_pct  REAL    NOT NULL
    );""",
    """CREATE TABLE IF NOT EXISTS stocks (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        scan_id     INTEGER NOT NULL REFERENCES scans(scan_id),
        sector_name TEXT    NOT NULL,
        symbol      TEXT    NOT NULL,
        stock_name  TEXT,
        ltp         REAL    NOT NULL,
        change_pct  REAL    NOT NULL,
        is_fno      INTEGER NOT NULL DEFAULT 0,
        lot_size    INTEGER,
        open_price  REAL,
        day_high    REAL,
        day_low     REAL,
        ext_from_low REAL,   -- % extension of LTP above day low (chase-risk gauge)
        fut_basis    REAL,   -- stock futures premium(+)/discount(-) vs spot, %
        timestamp   TEXT    NOT NULL
    );""",
    "CREATE INDEX IF NOT EXISTS idx_stocks_scan   ON stocks  (scan_id);",
    "CREATE INDEX IF NOT EXISTS idx_stocks_symbol ON stocks  (symbol);",
    "CREATE INDEX IF NOT EXISTS idx_sectors_scan  ON sectors (scan_id);",
    "CREATE INDEX IF NOT EXISTS idx_nifty_scan    ON nifty   (scan_id);",
    """CREATE TABLE IF NOT EXISTS active_futures (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        scan_id          INTEGER NOT NULL REFERENCES scans(scan_id),
        symbol           TEXT NOT NULL,
        instrument       TEXT,
        expiry_date      TEXT,
        ltp              REAL,
        change_pct       REAL,
        volume_contracts INTEGER,
        value_lakhs      REAL,
        open_interest    INTEGER,
        underlying_value REAL,
        sort_by          TEXT,
        lot_size         INTEGER,
        timestamp        TEXT NOT NULL
    );""",
    "CREATE INDEX IF NOT EXISTS idx_active_futures_scan   ON active_futures (scan_id);",
    "CREATE INDEX IF NOT EXISTS idx_active_futures_symbol ON active_futures (symbol);",
    """CREATE TABLE IF NOT EXISTS active_options (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        scan_id          INTEGER NOT NULL REFERENCES scans(scan_id),
        symbol           TEXT NOT NULL,
        option_type      TEXT NOT NULL,
        volume_contracts INTEGER,
        strikes_count    INTEGER DEFAULT 1,
        strike_price     REAL,
        option_ltp       REAL,
        expiry_date      TEXT,
        open_interest    INTEGER,
        underlying_value REAL,
        timestamp        TEXT NOT NULL
    );""",
    "CREATE INDEX IF NOT EXISTS idx_active_options_scan   ON active_options (scan_id);",
    "CREATE INDEX IF NOT EXISTS idx_active_options_symbol ON active_options (symbol);",
    """CREATE TABLE IF NOT EXISTS app_config (
        key         TEXT PRIMARY KEY,
        value       TEXT NOT NULL,
        updated_at  TEXT NOT NULL
    );""",
    """CREATE VIEW IF NOT EXISTS v_latest_sectors AS
       SELECT s.scan_id, sc.scan_date, sc.scan_time, s.sector_name, s.change_pct
       FROM sectors s
       JOIN scans sc ON s.scan_id = sc.scan_id
       WHERE s.scan_id = (SELECT MAX(scan_id) FROM scans)
       ORDER BY s.change_pct DESC;""",
    """CREATE VIEW IF NOT EXISTS v_latest_stocks AS
       SELECT st.scan_id, sc.scan_date, sc.scan_time, st.sector_name, st.symbol, st.stock_name, st.ltp, st.change_pct, st.lot_size, st.open_price, st.day_high, st.day_low, st.ext_from_low, st.fut_basis
       FROM stocks st
       JOIN scans sc ON st.scan_id = sc.scan_id
       WHERE st.scan_id = (SELECT MAX(scan_id) FROM scans)
       ORDER BY st.sector_name, st.change_pct DESC;""",
    """CREATE VIEW IF NOT EXISTS v_latest_nifty AS
       SELECT n.scan_id, sc.scan_date, sc.scan_time, n.ltp, n.change_pct, n.direction, n.timestamp
       FROM nifty n
       JOIN scans sc ON n.scan_id = sc.scan_id
       WHERE n.scan_id = (SELECT MAX(scan_id) FROM scans);""",
    """CREATE VIEW IF NOT EXISTS v_latest_active_futures AS
       SELECT af.scan_id, sc.scan_date, sc.scan_time, af.symbol, af.instrument,
              af.expiry_date, af.ltp, af.change_pct, af.volume_contracts,
              af.value_lakhs, af.open_interest, af.underlying_value, af.lot_size, af.sort_by
       FROM active_futures af
       JOIN scans sc ON af.scan_id = sc.scan_id
       WHERE af.scan_id = (SELECT MAX(scan_id) FROM scans)
       ORDER BY af.volume_contracts DESC;""",
]


def update_terminal_summary_view(
    conn: sqlite3.Connection,
    nifty_dir: str = "POSITIVE",
    max_lot_size: int = None,
    stocks_per_sector: int = 2,
    active_futures_count: int = 2
):
    """
    Recreates v_terminal_summary view so it exactly mirrors the terminal output:
    - Filters stocks and active futures by max_lot_size (if configured)
    - Respects stocks_per_sector and active_futures_count
    - Respects NIFTY direction ordering (Gainers first or Losers first)
    """
    lot_filter_stocks = f"AND (st.lot_size IS NOT NULL AND st.lot_size <= {max_lot_size})" if (max_lot_size and max_lot_size > 0) else ""
    lot_filter_af = f"AND (lot_size IS NOT NULL AND lot_size <= {max_lot_size})" if (max_lot_size and max_lot_size > 0) else ""

    # The view only depends on these four params - skip the DROP/CREATE when
    # they are unchanged since the last scan.
    view_sig = json.dumps([nifty_dir, max_lot_size, stocks_per_sector, active_futures_count])
    try:
        row = conn.execute(
            "SELECT value FROM app_config WHERE key = 'terminal_view_sig';"
        ).fetchone()
    except sqlite3.OperationalError:
        row = None  # app_config not created yet (fresh DB, first scan)
    if row and row[0] == view_sig:
        return

    if nifty_dir == "POSITIVE":
        s_gain, s_af_gain, s_lose, s_af_lose = 1, 2, 3, 4
    else:
        s_lose, s_af_lose, s_gain, s_af_gain = 1, 2, 3, 4

    view_sql = f"""
    CREATE VIEW IF NOT EXISTS v_terminal_summary AS
       WITH latest_scan AS (
           SELECT MAX(scan_id) AS scan_id FROM scans
       ),
       ranked_sectors AS (
           SELECT 
               s.scan_id,
               s.sector_name,
               s.change_pct AS sector_change_pct,
               ROW_NUMBER() OVER (ORDER BY s.change_pct DESC) AS gainer_rank,
               ROW_NUMBER() OVER (ORDER BY s.change_pct ASC)  AS loser_rank
           FROM sectors s
           JOIN latest_scan ls ON s.scan_id = ls.scan_id
       ),
       top_sectors AS (
           SELECT 'TOP GAINING SECTOR' AS category, gainer_rank AS sector_rank, sector_name, sector_change_pct
           FROM ranked_sectors WHERE gainer_rank <= 2
           UNION ALL
           SELECT 'TOP LOSING SECTOR'  AS category, loser_rank  AS sector_rank, sector_name, sector_change_pct
           FROM ranked_sectors WHERE loser_rank <= 2
       ),
       ranked_stocks AS (
           SELECT 
               ts.category,
               ts.sector_rank,
               ts.sector_name,
               ts.sector_change_pct,
               st.symbol,
               st.ltp,
               st.change_pct AS stock_change_pct,
               st.lot_size,
               CASE 
                   WHEN ts.category = 'TOP GAINING SECTOR' THEN 
                       ROW_NUMBER() OVER (PARTITION BY ts.category, ts.sector_name ORDER BY st.change_pct DESC)
                   ELSE 
                       ROW_NUMBER() OVER (PARTITION BY ts.category, ts.sector_name ORDER BY st.change_pct ASC)
               END AS stock_rank,
               sc.scan_date,
               sc.scan_time,
               CASE WHEN ts.category = 'TOP GAINING SECTOR' THEN {s_gain} ELSE {s_lose} END AS sort_group
           FROM top_sectors ts
           JOIN latest_scan ls ON 1=1
           JOIN scans sc ON sc.scan_id = ls.scan_id
           JOIN stocks st ON st.scan_id = ls.scan_id AND st.sector_name = ts.sector_name
           WHERE 1=1 {lot_filter_stocks}
       ),
       distinct_active_futures AS (
           SELECT 
               symbol,
               change_pct,
               ltp,
               lot_size,
               ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY volume_contracts DESC) as rn
           FROM active_futures
           JOIN latest_scan ls ON active_futures.scan_id = ls.scan_id
           WHERE instrument = 'Stock Futures' {lot_filter_af}
       ),
       unique_stock_futures AS (
           SELECT symbol, change_pct, ltp, lot_size FROM distinct_active_futures WHERE rn = 1
       ),
       ranked_af_gainers AS (
           SELECT 
               'ACTIVE FUTURES (TOP GAINER)' AS category,
               0 AS sector_rank,
               'ACTIVE FUTURES' AS sector_name,
               0.0 AS sector_change_pct,
               st.symbol,
               st.ltp,
               st.change_pct AS stock_change_pct,
               st.lot_size,
               ROW_NUMBER() OVER (ORDER BY st.change_pct DESC) AS stock_rank,
               sc.scan_date,
               sc.scan_time,
               {s_af_gain} AS sort_group
           FROM unique_stock_futures st
           JOIN latest_scan ls ON 1=1
           JOIN scans sc ON sc.scan_id = ls.scan_id
       ),
       ranked_af_losers AS (
           SELECT 
               'ACTIVE FUTURES (TOP LOSER)' AS category,
               0 AS sector_rank,
               'ACTIVE FUTURES' AS sector_name,
               0.0 AS sector_change_pct,
               st.symbol,
               st.ltp,
               st.change_pct AS stock_change_pct,
               st.lot_size,
               ROW_NUMBER() OVER (ORDER BY st.change_pct ASC) AS stock_rank,
               sc.scan_date,
               sc.scan_time,
               {s_af_lose} AS sort_group
           FROM unique_stock_futures st
           JOIN latest_scan ls ON 1=1
           JOIN scans sc ON sc.scan_id = ls.scan_id
       ),
       combined AS (
           SELECT category, sector_rank, sector_name, sector_change_pct, stock_rank, symbol, ltp, stock_change_pct, lot_size, scan_date, scan_time, sort_group
           FROM ranked_stocks
           WHERE stock_rank <= {stocks_per_sector}
           UNION ALL
           SELECT category, sector_rank, sector_name, sector_change_pct, stock_rank, symbol, ltp, stock_change_pct, lot_size, scan_date, scan_time, sort_group
           FROM ranked_af_gainers
           WHERE stock_rank <= {active_futures_count}
           UNION ALL
           SELECT category, sector_rank, sector_name, sector_change_pct, stock_rank, symbol, ltp, stock_change_pct, lot_size, scan_date, scan_time, sort_group
           FROM ranked_af_losers
           WHERE stock_rank <= {active_futures_count}
       )
       SELECT category, sector_rank, sector_name, sector_change_pct, stock_rank, symbol, ltp, stock_change_pct, lot_size, scan_date, scan_time
       FROM combined
       ORDER BY sort_group, sector_rank, stock_rank;
    """
    conn.execute("DROP VIEW IF EXISTS v_terminal_summary;")
    conn.execute(view_sql)
    conn.execute(
        "INSERT OR REPLACE INTO app_config (key, value, updated_at) VALUES (?, ?, ?);",
        ("terminal_view_sig", view_sig, datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S%z"))
    )
    conn.commit()


SCHEMA_VERSION = 3  # bump when SQL_SCHEMA or migrations below change
                      # v2: stocks gains ext_from_low + fut_basis columns
                      # v3: scans gains partial flag (NSE open-window truncation)


def _backfill_partial_flags(cur) -> None:
    """Mark historical scans PARTIAL where the data says so (v3 backfill).

    A scan is partial when (a) some sector row has zero constituent stocks, or
    (b) its distinct-symbol count fell below 95% of the best of the previous 5
    scans (catches under-filled blocks like scan #282's PSU BANK at 6 of 8).
    """
    cur.execute("""UPDATE scans SET partial = 1 WHERE scan_id IN (
                       SELECT DISTINCT s.scan_id FROM scans s
                       JOIN sectors se ON se.scan_id = s.scan_id
                       LEFT JOIN stocks st
                              ON st.scan_id = s.scan_id
                             AND st.sector_name = se.sector_name
                       GROUP BY s.scan_id, se.sector_name
                       HAVING COUNT(st.id) = 0);""")
    rows = cur.execute(
        "SELECT scan_id, COUNT(DISTINCT symbol) FROM stocks GROUP BY scan_id ORDER BY scan_id;"
    ).fetchall()
    baseline: list = []
    for scan_id, distinct in rows:
        if baseline:
            best = max(baseline)
            if best >= 100 and distinct < best * 0.95:
                cur.execute("UPDATE scans SET partial = 1 WHERE scan_id = ?;", (scan_id,))
        baseline.append(distinct)
        if len(baseline) > 5:
            baseline.pop(0)


def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    # Migrations and DDL only run when the schema version differs; the ~35
    # ALTER/CREATE statements below are pure per-scan overhead otherwise.
    if conn.execute("PRAGMA user_version;").fetchone()[0] == SCHEMA_VERSION:
        return conn

    cur = conn.cursor()

    # Migration: ensure lot_size column exists in existing databases
    for table in ("stocks", "active_futures"):
        try:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN lot_size INTEGER;")
        except sqlite3.OperationalError:
            pass  # column already exists

    # Migration: ensure strikes_count and strike details exist in active_options
    for col, col_type in (
        ("strikes_count", "INTEGER DEFAULT 1"),
        ("strike_price", "REAL"),
        ("option_ltp", "REAL"),
        ("expiry_date", "TEXT"),
        ("open_interest", "INTEGER"),
        ("underlying_value", "REAL"),
    ):
        try:
            cur.execute(f"ALTER TABLE active_options ADD COLUMN {col} {col_type};")
        except sqlite3.OperationalError:
            pass  # column already exists

    # Migration: ensure app_config table exists
    cur.execute("""CREATE TABLE IF NOT EXISTS app_config (
        key         TEXT PRIMARY KEY,
        value       TEXT NOT NULL,
        updated_at  TEXT NOT NULL
    );""")

    # Migration: ensure open_price, day_high, day_low exist in stocks
    for col in ("open_price", "day_high", "day_low"):
        try:
            cur.execute(f"ALTER TABLE stocks ADD COLUMN {col} REAL;")
        except sqlite3.OperationalError:
            pass  # column already exists

    # Migration (v2): extension-from-low and futures-basis columns in stocks
    for col in ("ext_from_low", "fut_basis"):
        try:
            cur.execute(f"ALTER TABLE stocks ADD COLUMN {col} REAL;")
        except sqlite3.OperationalError:
            pass  # column already exists

    # Migration: ensure duration_seconds column exists in scans
    try:
        cur.execute("ALTER TABLE scans ADD COLUMN duration_seconds REAL;")
    except sqlite3.OperationalError:
        pass  # column already exists

    # Migration (v3): scans.partial flag (NSE open-window truncation / empty fetches)
    try:
        cur.execute("ALTER TABLE scans ADD COLUMN partial INTEGER NOT NULL DEFAULT 0;")
    except sqlite3.OperationalError:
        pass  # column already exists

    # Drop views to recreate them with updated column definitions
    cur.execute("DROP VIEW IF EXISTS v_latest_stocks;")
    cur.execute("DROP VIEW IF EXISTS v_terminal_summary;")
    cur.execute("DROP VIEW IF EXISTS v_latest_active_futures;")
    # v_terminal_summary is recreated after the scan's inserts by
    # update_terminal_summary_view(); invalidate its skip-signature so the
    # rebuild actually happens after this migration dropped the view.
    cur.execute("DELETE FROM app_config WHERE key = 'terminal_view_sig';")

    for stmt in SQL_SCHEMA:
        cur.execute(stmt)
    _backfill_partial_flags(cur)   # v3: mark historically truncated scans
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION};")
    conn.commit()

    return conn


def db_save_scan(conn: sqlite3.Connection, scan_date: str, scan_time: str, data_source: str,
                 duration_seconds: float = None, partial: int = 0) -> int:
    cur = conn.execute(
        "INSERT INTO scans (scan_date, scan_time, data_source, duration_seconds, partial) VALUES (?, ?, ?, ?, ?);",
        (scan_date, scan_time, data_source, duration_seconds, partial)
    )
    return cur.lastrowid


def db_save_nifty(conn: sqlite3.Connection, scan_id: int, ltp: float, change_pct: float,
                  direction: str, ts: str):
    conn.execute(
        "INSERT INTO nifty (scan_id, ltp, change_pct, direction, timestamp) VALUES (?, ?, ?, ?, ?);",
        (scan_id, ltp, change_pct, direction, ts)
    )


# -----------------------------------------------------------------------------
# FORMATTING HELPERS
# -----------------------------------------------------------------------------

def fmt_pct(val: float) -> str:
    """Format float percentage with explicit plus/minus sign."""
    return f"+{val:.2f}%" if val >= 0 else f"{val:.2f}%"


def is_ohlc_data_reliable(stocks: list, bad_fraction_limit: float = 0.10) -> bool:
    """Reject scan-wide OHLC signals when too many rows are flat or unusable."""
    by_symbol = {}
    for stk in stocks:
        symbol = str(stk.get("symbol", "")).strip().upper()
        if symbol:
            by_symbol.setdefault(symbol, stk)

    if not by_symbol:
        return False

    bad_count = 0
    for stk in by_symbol.values():
        op = float(stk.get("open_price") or 0)
        high = float(stk.get("day_high") or 0)
        low = float(stk.get("day_low") or 0)
        if op <= 0 or high <= 0 or low <= 0 or high < op or low > op or high < low:
            bad_count += 1
        elif high - low <= 0.01:
            # A zero-range quote is common while the NSE feed is still initializing.
            bad_count += 1

    return bad_count / len(by_symbol) < bad_fraction_limit


# -----------------------------------------------------------------------------
# Configuration (unified via scanner_config.json)
# -----------------------------------------------------------------------------
CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scanner_config.json")

def load_scanner_config():
    defaults = {
        "max_lot_size": 1500,
        "sectors_to_show": 2,
        "stocks_per_sector": 10,
        "active_futures_count": 10,
        "min_option_volume": 500,
        "max_percentage": 0.0,
        "top_n_candidates": 3,
        "weights": {
            "price": 3.0,
            "sector": 2.0,
            "options": 2.0,
            "futures": 1.5,
            "ohlc": 1.0,
            "breadth": 1.0
        },
        "ext_penalty_start": 0.02,
        "ext_penalty_end": 0.08,
        "db_file": "market_scanner.db"
    }
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                cfg = json.load(f)
                defaults.update(cfg)
                if "weights" in cfg and isinstance(cfg["weights"], dict):
                    defaults["weights"].update(cfg["weights"])
        except Exception:
            pass
    return defaults

_CFG = load_scanner_config()

# Default settings (loaded from scanner_config.json)
DEFAULT_MAX_LOT_SIZE         = _CFG.get("max_lot_size", 1500)
DEFAULT_STOCKS_PER_SECTOR    = _CFG.get("stocks_per_sector", 10)
DEFAULT_SECTORS_TO_SHOW      = _CFG.get("sectors_to_show", 2)
DEFAULT_ACTIVE_FUTURES_COUNT = _CFG.get("active_futures_count", 10)
DEFAULT_MIN_OPTION_VOLUME    = _CFG.get("min_option_volume", 500)
DEFAULT_MAX_PERCENTAGE       = float(_CFG.get("max_percentage", 0.0))


# -----------------------------------------------------------------------------
# MAIN WORKFLOW
# -----------------------------------------------------------------------------

def run_scanner(
    max_lot_size: int = DEFAULT_MAX_LOT_SIZE,
    stocks_per_sector: int = DEFAULT_STOCKS_PER_SECTOR,
    sectors_to_show: int = DEFAULT_SECTORS_TO_SHOW,
    active_futures_count: int = DEFAULT_ACTIVE_FUTURES_COUNT,
    min_option_volume: int = DEFAULT_MIN_OPTION_VOLUME,
    max_percentage: float = DEFAULT_MAX_PERCENTAGE
):
    start_time = time.time()
    now_ist   = datetime.now(IST)
    scan_date = now_ist.strftime("%Y-%m-%d")
    scan_time = now_ist.strftime("%H:%M:%S")
    timestamp = now_ist.strftime("%Y-%m-%dT%H:%M:%S%z")

    _prune_cache()

    # 1. Establish session
    session = create_session()
    if not warm_up_session(session):
        print("[X] Session warmup failed. Aborting.")
        sys.exit(1)

    time.sleep(1.0)

    # Lot sizes change at most once per day - reuse today's disk cache if present
    lot_sizes = _load_cache("lot_sizes", scan_date)
    if lot_sizes:
        print(f"[+] Loaded {len(lot_sizes)} contract lot sizes (disk cache)")
    else:
        lot_sizes = fetch_nse_lot_sizes(session)
        if lot_sizes:
            _save_cache("lot_sizes", scan_date, lot_sizes)
        else:
            print("[!] Lot sizes unavailable - the max-lot-size filter will drop every stock from sector lists")

    # 1.1 Fetch F&O Ban list (hard-remove banned stocks from display)
    fno_banned = fetch_fno_ban_list(session)

    # 2. Fetch all indices
    all_indices = fetch_all_indices(session)
    if not all_indices:
        print("[X] allIndices returned empty data. Aborting.")
        sys.exit(1)

    # 3. Extract NIFTY 50 data
    nifty_item = next(
        (item for item in all_indices if (item.get("index") or "").strip().upper() == "NIFTY 50"),
        None
    )
    if not nifty_item:
        print("[!] NIFTY 50 not found in allIndices. Using first available index.")
        nifty_item = all_indices[0]

    nifty_ltp    = float(nifty_item.get("last", 0) or 0)
    nifty_change = float(nifty_item.get("percentChange", 0) or 0)
    nifty_dir    = "POSITIVE" if nifty_change >= 0 else "NEGATIVE"

    # 4. Dynamically identify Sectoral Indices from allIndices
    sector_entries = []
    seen_names = set()

    for item in all_indices:
        idx_key    = (item.get("key") or "").strip()
        idx_name   = (item.get("index") or "").strip().upper()
        query_sym  = (item.get("indexSymbol") or idx_name).strip()
        change_pct = float(item.get("percentChange", 0) or 0)

        is_sector = (idx_key in SECTOR_KEYS) or (idx_name in ADDITIONAL_SECTORS)

        if is_sector and idx_name not in EXCLUDE_SECTORS and idx_name not in seen_names:
            seen_names.add(idx_name)
            sector_entries.append({
                "name":         idx_name,
                "query_symbol": query_sym,
                "change":       change_pct,   # NSE-supplied percentage change
            })

    total_sectors_found = len(sector_entries)
    print(f"[+] Sectoral indices identified: {total_sectors_found}")

    time.sleep(1.0)

    # 5. Fetch F&O securities set (day-stable - reuse today's disk cache if present)
    fno_symbols = _load_cache("fno_symbols", scan_date)
    if fno_symbols is not None:
        fno_symbols = set(fno_symbols)
        print(f"[+] Active F&O symbols loaded from disk cache: {len(fno_symbols)}")
    else:
        fno_symbols = fetch_fno_symbols(session)
        if fno_symbols:
            _save_cache("fno_symbols", scan_date, sorted(fno_symbols))

    if not fno_symbols:
        # Live fetch failed and nothing cached for today - fall back to the most
        # recent cached list rather than aborting the whole scan.
        fno_symbols, cache_fn = _load_latest_cache("fno_symbols")
        if fno_symbols:
            fno_symbols = set(fno_symbols)
            print(f"[!] Using cached F&O list from {cache_fn}: {len(fno_symbols)} symbols")
    if not fno_symbols:
        print("[X] Unable to obtain F&O stocks list. Aborting.")
        sys.exit(1)

    # 6. Fetch constituents for each sector in parallel (4 workers)
    #    Each worker still sleeps a polite random delay before its request.
    #    4 concurrent threads reduce ~25s sequential wait to ~6-8s.
    SECTOR_WORKERS = 4
    sectors_data   = {}
    empty_sectors  = []           # sectors whose fetch returned zero F&O rows
    total_sectors  = len(sector_entries)
    completed      = [0]          # mutable counter shared across threads
    lock           = threading.Lock()

    def _fetch_one(entry):
        """Fetch one sector's constituents with a polite delay, return result."""
        time.sleep(random.uniform(*INTER_REQUEST_DELAY))   # be polite per thread
        s_name   = entry["name"]
        q_sym    = entry["query_symbol"]
        s_change = entry["change"]
        stocks   = fetch_sector_constituents(session, q_sym)
        fno_stocks = [stk for stk in stocks
                      if stk["symbol"] in fno_symbols
                      and stk["symbol"] not in fno_banned]
        return s_name, s_change, fno_stocks

    print(f"[*] Fetching constituents for {total_sectors} sectors (parallel, {SECTOR_WORKERS} workers) ...")
    with ThreadPoolExecutor(max_workers=SECTOR_WORKERS) as pool:
        futures = {pool.submit(_fetch_one, e): e for e in sector_entries}
        for fut in as_completed(futures):
            s_name, s_change, fno_stocks = fut.result()
            with lock:
                completed[0] += 1
                done = completed[0]
                pct = int((done / total_sectors) * 20)
                bar = "#" * pct + "-" * (20 - pct)
                print(f"\r    [{bar}] {done:>2}/{total_sectors}  {s_name:<40}", end="", flush=True)
            # Keep every sector, even one whose fetch came back empty: dropping it
            # silently erased sectors from the DB (scans #282/#283 lost METAL,
            # PHARMA and FINANCIAL SERVICES with no trace) and hid NSE open-window
            # truncation from every viewer.
            sectors_data[s_name] = {"change": s_change, "stocks": fno_stocks}
            if not fno_stocks:
                empty_sectors.append(s_name)

    print()  # newline after progress bar

    if empty_sectors:
        print(f"\n[!] WARNING: {len(empty_sectors)} sector(s) returned no F&O constituents: {', '.join(empty_sectors)}")
        print("    Typical during the ~09:15-09:20 NSE feed rebuild - this scan will be marked PARTIAL.")

    total_fno_sectors = len(sectors_data)

    # 7. Fetch Most Active Future Contracts + Active Stock Options (needed for display)
    active_futures = fetch_most_active_futures(session)
    opt_liq_map, call_opt_map, put_opt_map = fetch_active_stock_options(session, nifty_dir)
    session.close()  # no more network I/O - release the TLS session

    # 8. Sort sectors and compute top gainers/losers for display.
    #    Empty sectors (saved to the DB for the partial flag) stay out of the
    #    terminal panels - a header with no stock rows would just displace a
    #    real sector from the top slots.
    sorted_sectors = sorted(
        ((name, info) for name, info in sectors_data.items() if info["stocks"]),
        key=lambda x: x[1]["change"],
        reverse=True
    )

    top_gainers = sorted_sectors[:sectors_to_show]
    top_losers  = list(reversed(sorted_sectors[-sectors_to_show:])) if len(sorted_sectors) >= sectors_to_show else []

    # Ensure no overlap between top gainers and top losers if total sectors is small
    if len(sorted_sectors) < sectors_to_show * 2:
        gainer_names = {name for name, _ in top_gainers}
        top_losers = [(name, data) for name, data in top_losers if name not in gainer_names]

    # 8.1 Extract Top Gaining and Losing Stock Futures from Active Futures
    # Quick lookup for stock pchange (the active-futures part is filled by the
    # merged pass below)
    sym_pct_lookup = {}
    for sec_name, sec_info in sectors_data.items():
        for stk in sec_info.get("stocks", []):
            if stk.get("symbol"):
                sym_pct_lookup[stk["symbol"].upper()] = stk.get("pchange", 0.0)

    stock_lookup = {stk["symbol"]: stk for s_info in sectors_data.values() for stk in s_info.get("stocks", [])}

    stock_futures = []
    seen_af_syms = set()     # dedupe by underlying symbol (display list)
    active_fut_symbols = set()
    fut_basis_map = {}       # symbol -> futures premium(+)/discount(-) vs spot, %
    all_af = active_futures.get("volume", []) + active_futures.get("value", [])
    for c in all_af:
        inst = c.get("instrument", "")
        sym = str(c.get("underlying") or "").strip().upper()
        if not sym:
            continue
        if inst == "Stock Futures":
            active_fut_symbols.add(sym)
            fut_ltp = _f(c.get("lastPrice"))
            und_val = _f(c.get("underlyingValue"))
            if fut_ltp > 0 and und_val > 0 and sym not in fut_basis_map:
                fut_basis_map[sym] = (fut_ltp - und_val) / und_val * 100.0
        if sym not in sym_pct_lookup:
            sym_pct_lookup[sym] = _f(c.get("pChange"))
        if inst == "Stock Futures" and sym not in seen_af_syms and sym not in fno_banned:
            seen_af_syms.add(sym)
            lot = lot_sizes.get(sym)
            if max_lot_size and max_lot_size > 0:
                if not lot or lot > max_lot_size:
                    continue
            stock_futures.append({
                "symbol": sym,
                "pchange": _f(c.get("pChange")),
                "ltp": _f(c.get("lastPrice")),
                "volume": int(_f(c.get("numberOfContractsTraded"))),
            })

    top_active_gainers = sorted(stock_futures, key=lambda x: x["pchange"], reverse=True)[:active_futures_count]
    top_active_losers = sorted(stock_futures, key=lambda x: x["pchange"])[:active_futures_count]

    # 8.2 Build Top Active Call & Put Options lists capped at stocks_per_sector, sorted by % change
    # First select top active options by volume:
    raw_active_calls = sorted(
        [{"symbol": sym, "volume": data["volume"], "contracts_count": data.get("contracts_count", 1), "pchange": sym_pct_lookup.get(sym, 0.0)} for sym, data in call_opt_map.items()],
        key=lambda x: x["volume"],
        reverse=True
    )[:stocks_per_sector]

    raw_active_puts = sorted(
        [{"symbol": sym, "volume": data["volume"], "contracts_count": data.get("contracts_count", 1), "pchange": sym_pct_lookup.get(sym, 0.0)} for sym, data in put_opt_map.items()],
        key=lambda x: x["volume"],
        reverse=True
    )[:stocks_per_sector]

    # Sort calls descending (highest % change first) and puts ascending (lowest/most negative % change first)
    top_active_calls = sorted(raw_active_calls, key=lambda x: x["pchange"], reverse=True)
    top_active_puts  = sorted(raw_active_puts, key=lambda x: x["pchange"])

    COL_WIDTH = 58          # widened for the L (ext-from-low) and B (futures basis) columns
    GRID_WIDTH = COL_WIDTH * 2 + 3
    SEP_DOUBLE = "=" * GRID_WIDTH
    SEP_MID    = "-" * COL_WIDTH + "-+-" + "-" * COL_WIDTH
    SEP_LINE   = "-" * GRID_WIDTH

    _GRN = "\033[92m"   # bright green  (positive % / Top Active Options)
    _RED = "\033[91m"   # bright red    (negative %)
    _YEL = "\033[93m"   # bright yellow (Standard F&O Options)
    _CYN = "\033[96m"   # bright cyan   (Active Futures)
    _DIM = "\033[90m"   # bright black  (no option activity)
    _RST = "\033[0m"    # reset

    os.system('cls')

    print()
    print(SEP_DOUBLE)
    print("NSE SECTOR F&O SCANNER")
    print(SEP_DOUBLE)
    print()
    print(f"Date       : {scan_date}")
    print(f"Time       : {scan_time}")
    print(f"Data Source: {DATA_SOURCE}")
    print(SEP_DOUBLE)

    def _cpct(val: float) -> str:
        """Return a colour-coded percentage string: green if >=0, red if <0."""
        txt = fmt_pct(val)
        return f"{_GRN}{txt}{_RST}" if val >= 0 else f"{_RED}{txt}{_RST}"

    def _cpts(val: float) -> str:
        txt = f"{val:+.2f}"
        return f"{_GRN}{txt}{_RST}" if val >= 0 else f"{_RED}{txt}{_RST}"

    nifty_pts  = (nifty_ltp * nifty_change) / (100 + nifty_change) if (100 + nifty_change) != 0 else 0
    dir_colour = _GRN if nifty_dir == "POSITIVE" else _RED
    # print("NIFTY 50")
    print(f"NIFTY 50 --> LTP: {nifty_ltp:,.2f}   Change: {_cpts(nifty_pts)} ({_cpct(nifty_change)})   {dir_colour}{nifty_dir}{_RST}")

    ohlc_data_reliable = is_ohlc_data_reliable(
        [stk for s_info in sectors_data.values() for stk in s_info.get("stocks", [])]
    )

    def _filter_sector_stocks(stocks_list: list) -> list:
        """Apply the max-lot-size and max-percentage display filters once per sector."""
        filtered = stocks_list
        if max_lot_size and max_lot_size > 0:
            filtered = [s for s in filtered if (lot := lot_sizes.get(s["symbol"])) and lot <= max_lot_size]
        if max_percentage and max_percentage > 0:
            filtered = [s for s in filtered if abs(s["pchange"]) < max_percentage]
        return filtered

    # Rank priority:
    # Tier 3: both Active Futures (+) and Top Active Option ([*])
    # Tier 2: Active Futures (+) only
    # Tier 1: Top Active Option ([*]) only
    # Tier 0: Neither
    def _stock_rank_key(stk_dict, is_gain_side):
        s_sym = stk_dict['symbol']
        has_fut = s_sym in active_fut_symbols
        has_opt = opt_liq_map.get(s_sym, 0) >= min_option_volume
        if has_fut and has_opt:
            tier = 3
        elif has_fut:
            tier = 2
        elif has_opt:
            tier = 1
        else:
            tier = 0
        pct = stk_dict['pchange']
        pct_val = pct if is_gain_side else -pct
        return (tier, pct_val)

    def _fmt_stock_row(sym: str, pchange: float, lot: int, col_w: int, stk: dict = None) -> str:
        lot_str  = f"Lot: {lot}" if lot else ""
        in_fut   = sym in active_fut_symbols
        fut_str  = "+"  if in_fut else " "
        opt_vol  = opt_liq_map.get(sym, 0)
        if opt_vol >= min_option_volume:
            opt_str = "[*]"                      # top active option -> scalp-able
        elif opt_vol > 0:
            opt_str = "[~]"                      # some option activity, below threshold
        else:
            # no option row for this symbol in the snapshot - that is "no reading",
            # NOT proof of illiquidity (the list only carries ~40 stock-option rows)
            opt_str = "[·]"
        pct_plain = fmt_pct(pchange)

        ohl_str = "   "
        target_stk = stk or stock_lookup.get(sym)
        if target_stk:
            op = target_stk.get("open_price") or 0
            dh = target_stk.get("day_high") or 0
            dl = target_stk.get("day_low") or 0
            if ohlc_data_reliable and dh > dl and op > 0 and dl > 0 and pchange > 0 and dl <= op:
                wick = (op - dl) / op * 100
                if wick <= 0.15:
                    ohl_str = "O=L"
            elif ohlc_data_reliable and dh > dl and op > 0 and dh > 0 and pchange < 0 and dh >= op:
                wick = (dh - op) / op * 100
                if wick <= 0.15:
                    ohl_str = "O=H"

        # L = % extension above day low (chase-risk gauge), B = futures basis vs spot
        ext_str   = "L--"
        basis_str = "B--"
        if target_stk:
            m_lo = float(target_stk.get("day_low") or 0)
            m_lt = float(target_stk.get("ltp") or 0)
            if m_lo > 0 and m_lt > 0:
                ext_str = f"L{(m_lt - m_lo) / m_lo * 100.0:+.1f}"
        if sym in fut_basis_map:
            b_val = fut_basis_map[sym]
            basis_str = f"B{b_val:+.2f}" if abs(b_val) >= 0.005 else "B 0.00"

        row = f"   {sym:<10} {pct_plain:>7}  {ohl_str} {ext_str:<6} {basis_str:<7} {lot_str:<9} {fut_str} {opt_str}"
        return f"{row:<{col_w}}"

    _PCT_RE = re.compile(r'([+-]\d+\.\d+%?)')

    def _colorize_pct(s: str) -> str:
        """Wrap every +/- percentage token with green or red ANSI codes."""
        def _sub(m):
            tok = m.group(1)
            return f"{_GRN}{tok}{_RST}" if tok.startswith('+') else f"{_RED}{tok}{_RST}"
        return _PCT_RE.sub(_sub, s)

    def _print_dual_row(l_str: str, r_str: str):
        pl = f"{l_str:<{COL_WIDTH}}"
        pr = f"{r_str:<{COL_WIDTH}}"
        # Colorize percentage values
        pl = _colorize_pct(pl)
        pr = _colorize_pct(pr)
        # Colorize O=L / O=H indicators
        pl = pl.replace(" O=L ", f" {_GRN}O=L{_RST} ").replace(" O=H ", f" {_RED}O=H{_RST} ")
        pr = pr.replace(" O=L ", f" {_GRN}O=L{_RST} ").replace(" O=H ", f" {_RED}O=H{_RST} ")
        # Colorize Active Futures "+" indicator
        pl = pl.replace(" + ", f" {_CYN}+{_RST} ")
        pr = pr.replace(" + ", f" {_CYN}+{_RST} ")
        # Colorize Option liquidity indicator
        pl = pl.replace("[*]", f"{_GRN}[*]{_RST}").replace("[~]", f"{_YEL}[~]{_RST}").replace("[·]", f"{_DIM}[·]{_RST}")
        pr = pr.replace("[*]", f"{_GRN}[*]{_RST}").replace("[~]", f"{_YEL}[~]{_RST}").replace("[·]", f"{_DIM}[·]{_RST}")
        print(f"{pl} | {pr}")

    # Determine left and right column data based on NIFTY direction
    if nifty_dir == "POSITIVE":
        left_title       = f"TOP {len(top_gainers)} GAINING SECTORS"
        right_title      = f"TOP {len(top_losers)} LOSING SECTORS"
        left_sectors     = top_gainers
        right_sectors    = top_losers
        left_af_title    = f"ACTIVE FUTURES (TOP {len(top_active_gainers)} GAINERS)"
        right_af_title   = f"ACTIVE FUTURES (TOP {len(top_active_losers)} LOSERS)"
        left_af_stocks   = top_active_gainers
        right_af_stocks  = top_active_losers
        left_opt_title   = f"TOP {len(top_active_calls)} ACTIVE CALL OPTIONS"
        right_opt_title  = f"TOP {len(top_active_puts)} ACTIVE PUT OPTIONS"
        left_opt_stocks  = top_active_calls
        right_opt_stocks = top_active_puts
        left_is_gain     = True
    else:
        left_title       = f"TOP {len(top_losers)} LOSING SECTORS"
        right_title      = f"TOP {len(top_gainers)} GAINING SECTORS"
        left_sectors     = top_losers
        right_sectors    = top_gainers
        left_af_title    = f"ACTIVE FUTURES (TOP {len(top_active_losers)} LOSERS)"
        right_af_title   = f"ACTIVE FUTURES (TOP {len(top_active_gainers)} GAINERS)"
        left_af_stocks   = top_active_losers
        right_af_stocks  = top_active_gainers
        left_opt_title   = f"TOP {len(top_active_puts)} ACTIVE PUT OPTIONS"
        right_opt_title  = f"TOP {len(top_active_calls)} ACTIVE CALL OPTIONS"
        left_opt_stocks  = top_active_puts
        right_opt_stocks = top_active_calls
        left_is_gain     = False

    print(SEP_MID)
    _print_dual_row(left_title, right_title)
    print(SEP_MID)

    # --- Render Sectors Synchronized Rank-by-Rank ---
    max_sector_count = max(len(left_sectors), len(right_sectors))
    for s_idx in range(max_sector_count):
        left_block = []
        right_block = []

        # Build Left Sector block
        if s_idx < len(left_sectors):
            s_name, s_info = left_sectors[s_idx]
            rank_str  = f"{s_idx + 1}. {s_name}"
            pct_str   = fmt_pct(s_info["change"])
            sp = max(1, COL_WIDTH - len(rank_str) - len(pct_str))
            left_block.append(f"{rank_str}{' ' * sp}{pct_str}")

            s_stocks = _filter_sector_stocks(s_info["stocks"])
            top_s = sorted(s_stocks, key=lambda x: _stock_rank_key(x, left_is_gain), reverse=True)[:stocks_per_sector]
            for stk in top_s:
                sym = stk['symbol']
                left_block.append(_fmt_stock_row(sym, stk['pchange'], lot_sizes.get(sym), COL_WIDTH))

        # Build Right Sector block
        if s_idx < len(right_sectors):
            s_name, s_info = right_sectors[s_idx]
            rank_str  = f"{s_idx + 1}. {s_name}"
            pct_str   = fmt_pct(s_info["change"])
            sp = max(1, COL_WIDTH - len(rank_str) - len(pct_str))
            right_block.append(f"{rank_str}{' ' * sp}{pct_str}")

            s_stocks = _filter_sector_stocks(s_info["stocks"])
            top_s = sorted(s_stocks, key=lambda x: _stock_rank_key(x, not left_is_gain), reverse=True)[:stocks_per_sector]
            for stk in top_s:
                sym = stk['symbol']
                right_block.append(_fmt_stock_row(sym, stk['pchange'], lot_sizes.get(sym), COL_WIDTH))

        # Pad shorter block so Sector ends at the exact same row on both sides
        rows_count = max(len(left_block), len(right_block))
        left_block  += [""] * (rows_count - len(left_block))
        right_block += [""] * (rows_count - len(right_block))

        for l_line, r_line in zip(left_block, right_block):
            _print_dual_row(l_line, r_line)

        # Print inner divider between sectors
        if s_idx < max_sector_count - 1:
            print(SEP_MID)

    # --- Render Active Futures Synchronized ---
    if left_af_stocks or right_af_stocks:
        print(SEP_MID)
        _print_dual_row(left_af_title, right_af_title)
        print(SEP_MID)

        left_af_block = []
        right_af_block = []
        for af in left_af_stocks:
            sym = af['symbol']
            left_af_block.append(_fmt_stock_row(sym, af['pchange'], lot_sizes.get(sym), COL_WIDTH))
        for af in right_af_stocks:
            sym = af['symbol']
            right_af_block.append(_fmt_stock_row(sym, af['pchange'], lot_sizes.get(sym), COL_WIDTH))

        af_rows = max(len(left_af_block), len(right_af_block))
        left_af_block  += [""] * (af_rows - len(left_af_block))
        right_af_block += [""] * (af_rows - len(right_af_block))

        for l_line, r_line in zip(left_af_block, right_af_block):
            _print_dual_row(l_line, r_line)

    # --- Render Top Active Options (Call & Put side-by-side, arranged based on Nifty direction) ---
    if left_opt_stocks or right_opt_stocks:
        print(SEP_MID)
        _print_dual_row(left_opt_title, right_opt_title)
        print(SEP_MID)

        left_opt_block  = []
        right_opt_block = []
        for item in left_opt_stocks:
            sym = item["symbol"]
            cnt = item.get("contracts_count")
            cnt_str = f" ({cnt})" if cnt else ""
            vol_str = f"Vol: {item['volume']:,}{cnt_str}"
            pct_plain = fmt_pct(item["pchange"])
            in_fut  = sym in active_fut_symbols
            fut_str = "+" if in_fut else " "
            row = f"   {sym:<10} {pct_plain:>7} {vol_str:<17} {fut_str} [*]"
            left_opt_block.append(f"{row:<{COL_WIDTH}}")

        for item in right_opt_stocks:
            sym = item["symbol"]
            cnt = item.get("contracts_count")
            cnt_str = f" ({cnt})" if cnt else ""
            vol_str = f"Vol: {item['volume']:,}{cnt_str}"
            pct_plain = fmt_pct(item["pchange"])
            in_fut  = sym in active_fut_symbols
            fut_str = "+" if in_fut else " "
            row = f"   {sym:<10} {pct_plain:>7} {vol_str:<17} {fut_str} [*]"
            right_opt_block.append(f"{row:<{COL_WIDTH}}")

        max_opt = max(len(left_opt_block), len(right_opt_block))
        left_opt_block  += [""] * (max_opt - len(left_opt_block))
        right_opt_block += [""] * (max_opt - len(right_opt_block))
        for l_line, r_line in zip(left_opt_block, right_opt_block):
            _print_dual_row(l_line, r_line)

    print(SEP_MID)
    print(f"Legend: {_GRN}[*]{_RST} Top Active Option   {_YEL}[~]{_RST} Standard F&O Option   {_DIM}[·]{_RST} No option data in snapshot   {_CYN}+{_RST} Active Futures   {_GRN}O=L{_RST} Open≈Low   {_RED}O=H{_RST} Open≈High   {_YEL}L{_RST}+x.x % above day low   {_CYN}B{_RST}+x.xx futures basis vs spot (neg=discount, -- = n/a)")


    # 9. DB writes happen AFTER display so terminal output is instant
    conn = init_db(DB_FILE)

    # PARTIAL-SCAN DETECTION (P1): NSE rebuilds its index feed for a few minutes
    # after the 09:15 open; scans inside that window have truncated sectors.
    # Rule (a): any sector fetched zero F&O constituents this scan.
    # Rule (b): distinct symbols < 95% of the best of the previous 5 scans
    #           (catches under-filled blocks like #282's PSU BANK at 6 of 8,
    #           where no sector was fully empty).
    distinct_now = len({stk["symbol"] for info in sectors_data.values() for stk in info["stocks"]})
    baseline = conn.execute(
        """SELECT MAX(c) FROM (
               SELECT COUNT(DISTINCT symbol) AS c
               FROM stocks GROUP BY scan_id ORDER BY scan_id DESC LIMIT 5
           );"""
    ).fetchone()[0] or 0
    # Determine which empty sectors are "real" F&O sectors (have historical presence)
    real_empty_sectors = []
    if empty_sectors:
        placeholders = ",".join("?" for _ in empty_sectors)
        real_empty_rows = conn.execute(
            f"""SELECT DISTINCT sector_name FROM stocks
               WHERE sector_name IN ({placeholders})
               GROUP BY sector_name
               HAVING COUNT(*) > 0""",
            empty_sectors
        ).fetchall()
        real_empty_sectors = {r[0] for r in real_empty_rows}

    partial = 1 if (real_empty_sectors or (baseline >= 100 and distinct_now < baseline * 0.95)) else 0
    if partial:
        msg = f"[!] Scan marked PARTIAL (symbols this scan: {distinct_now}, recent baseline: {baseline}"
        if real_empty_sectors:
            msg += f"; empty F&O sectors: {', '.join(sorted(real_empty_sectors))}"
        msg += "). Treat sector composition as indicative only."
        print(msg)

    scan_id = db_save_scan(conn, scan_date, scan_time, DATA_SOURCE, partial=partial)
    db_save_nifty(conn, scan_id, nifty_ltp, nifty_change, nifty_dir, timestamp)

    total_fno_stocks_saved = 0
    total_active_futures_saved = 0

    # Build all rows first, then insert everything in one executemany batch per
    # table inside a single transaction (one fsync instead of one per row).
    sector_rows = []
    stock_rows = []
    for s_name, s_info in sectors_data.items():
        sector_rows.append((scan_id, s_name, s_info["change"]))
        for stk in s_info["stocks"]:
            dl = float(stk.get("day_low") or 0)
            ext_low = ((stk["ltp"] - dl) / dl * 100.0) if (dl > 0 and stk["ltp"]) else None
            stock_rows.append((
                scan_id, s_name, stk["symbol"], stk["company_name"],
                stk["ltp"], stk["pchange"], 1,
                lot_sizes.get(stk["symbol"]),
                stk.get("open_price"), stk.get("day_high"), stk.get("day_low"),
                ext_low, fut_basis_map.get(stk["symbol"]),
                timestamp,
            ))
    total_fno_stocks_saved = len(stock_rows)

    # Active futures: one pass over both lists; a contract appearing in both is
    # saved once, under "volume" (shared identifier dedup)
    future_rows = []
    seen_identifiers = set()
    for sort_by in ("volume", "value"):
        for contract in active_futures.get(sort_by, []):
            c_id = contract.get("identifier") or (str(contract.get("underlying") or "") + str(contract.get("expiryDate") or ""))
            if c_id in seen_identifiers:
                continue
            seen_identifiers.add(c_id)
            u_sym = str(contract.get("underlying") or "").strip().upper()
            future_rows.append((
                scan_id,
                u_sym,
                contract.get("instrument", ""),
                contract.get("expiryDate", ""),
                _f(contract.get("lastPrice")),
                _f(contract.get("pChange")),
                int(_f(contract.get("numberOfContractsTraded"))),
                _f(contract.get("totalTurnover")),
                int(_f(contract.get("openInterest"))),
                _f(contract.get("underlyingValue")),
                sort_by,
                lot_sizes.get(u_sym),
                timestamp,
            ))
    total_active_futures_saved = len(future_rows)

    # Active options (both Call and Put) with contract strike details
    option_rows = []
    for option_map, opt_type in ((call_opt_map, "Call"), (put_opt_map, "Put")):
        for opt_sym, opt_data in option_map.items():
            contracts = opt_data.get("contracts", [])
            if contracts:
                for ct in contracts:
                    option_rows.append((
                        scan_id, opt_sym, opt_type, ct["vol"],
                        opt_data["contracts_count"],
                        ct["strike"], ct["ltp"], ct["expiry"], ct["oi"], ct.get("spot"),
                        timestamp,
                    ))
            else:
                option_rows.append((
                    scan_id, opt_sym, opt_type, opt_data["volume"],
                    opt_data["contracts_count"],
                    None, None, None, None, None,
                    timestamp,
                ))

    conn.executemany("INSERT INTO sectors (scan_id, sector_name, change_pct) VALUES (?, ?, ?);", sector_rows)
    conn.executemany(
        """INSERT INTO stocks
           (scan_id, sector_name, symbol, stock_name, ltp, change_pct, is_fno, lot_size,
            open_price, day_high, day_low, ext_from_low, fut_basis, timestamp)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""",
        stock_rows,
    )
    conn.executemany(
        """INSERT INTO active_futures
           (scan_id, symbol, instrument, expiry_date, ltp, change_pct, volume_contracts,
            value_lakhs, open_interest, underlying_value, sort_by, lot_size, timestamp)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""",
        future_rows,
    )
    conn.executemany(
        """INSERT INTO active_options
           (scan_id, symbol, option_type, volume_contracts, strikes_count,
            strike_price, option_ltp, expiry_date, open_interest, underlying_value, timestamp)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""",
        option_rows,
    )
    conn.commit()  # single transaction for the whole scan's data

    update_terminal_summary_view(
        conn,
        nifty_dir=nifty_dir,
        max_lot_size=max_lot_size,
        stocks_per_sector=stocks_per_sector,
        active_futures_count=active_futures_count
    )

    # Save current config to DB so scanner.html is always in sync
    # (rewrite only keys whose value actually changed)
    cfg_to_sync = load_scanner_config()
    existing_cfg = dict(conn.execute("SELECT key, value FROM app_config;").fetchall())
    changed_cfg = []
    for ck, cv in cfg_to_sync.items():
        encoded = json.dumps(cv)
        if existing_cfg.get(ck) != encoded:
            changed_cfg.append((ck, encoded, timestamp))
    if changed_cfg:
        conn.executemany(
            "INSERT OR REPLACE INTO app_config (key, value, updated_at) VALUES (?, ?, ?);",
            changed_cfg,
        )
        conn.commit()

    # Update scan row with execution duration
    elapsed_sec = time.time() - start_time
    conn.execute("UPDATE scans SET duration_seconds = ? WHERE scan_id = ?;", (elapsed_sec, scan_id))
    conn.commit()

    conn.close()

    # Automatically keep data/market_scanner.db in sync for GitHub Pages / web viewer
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    if os.path.exists(data_dir):
        try:
            shutil.copy2(DB_FILE, os.path.join(data_dir, DB_FILE))
        except Exception:
            pass

    # print()
    # print("DATABASE")
    # print(SEP_LINE)
    # # print()
    # print(f"Sectors found       : {total_sectors_found}")
    # print(f"F&O sectors         : {total_fno_sectors}")
    # print(f"F&O stocks          : {total_fno_stocks_saved}")
    # print(f"Active futures saved: {total_active_futures_saved}")
    # print()
    elapsed_sec = time.time() - start_time
    print(f"STATUS: SUCCESS, Time Taken : {elapsed_sec:.2f} seconds")
    # print(f"Time taken          : {elapsed_sec:.2f} seconds")
    print(SEP_DOUBLE)

if __name__ == "__main__":
    # All scan tuning lives in scanner_config.json - the single master config
    # shared with best_stock.py, view_scan_history.py, and scanner.html.
    run_scanner()
    
