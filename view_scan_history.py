"""
view_scan_history.py
---------------------
Interactive Historical Scan Viewer for market_scanner.db.

Allows you to:
  1. Select from past scan dates (latest date is option 1 by default).
  2. Select from scans executed on that date (latest scan is option 1 by default).
  3. Displays the scan results in the exact same side-by-side synchronized,
     color-coded format as nse_sector_scanner.py.
"""

import sqlite3
import os
import sys

# Ensure UTF-8 output so symbols render cleanly in Windows Terminal
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Database file location (same directory)
DB_FILE = "market_scanner.db"

# ANSI Colors
_GRN = "\033[92m"   # bright green  (positive % / Top Active Options)
_RED = "\033[91m"   # bright red    (negative %)
_YEL = "\033[93m"   # bright yellow (Standard F&O Options)
_CYN = "\033[96m"   # bright cyan   (Active Futures)
_RST = "\033[0m"    # reset

# Default display filters (can be configured at bottom in if __name__ == '__main__')
DEFAULT_MAX_LOT_SIZE         = 1500
DEFAULT_STOCKS_PER_SECTOR    = 10
DEFAULT_SECTORS_TO_SHOW      = 2    # Number of top gaining/losing sectors to display
DEFAULT_ACTIVE_FUTURES_COUNT = 5
DEFAULT_MIN_OPTION_VOLUME    = 500
DEFAULT_MAX_PERCENTAGE       = 0.0   # Filter out stocks with |change%| > this (0 = no filter)


def fmt_pct(val: float) -> str:
    """Format float percentage with explicit plus/minus sign."""
    if val is None:
        return "0.00%"
    return f"+{val:.2f}%" if val >= 0 else f"{val:.2f}%"


def get_db_connection(db_path: str = DB_FILE) -> sqlite3.Connection:
    if not os.path.exists(db_path):
        print(f"[X] Database '{db_path}' not found in current directory.")
        sys.exit(1)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_scan_dates(conn: sqlite3.Connection) -> list:
    """Fetch distinct scan dates ordered latest first with scan counts."""
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT scan_date, COUNT(scan_id) as scan_count
        FROM scans
        GROUP BY scan_date
        ORDER BY scan_date DESC
    """).fetchall()
    return rows


def fetch_scans_for_date(conn: sqlite3.Connection, scan_date: str) -> list:
    """Fetch all scans for a specific date with Nifty summary, latest first."""
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT s.scan_id, s.scan_date, s.scan_time, s.data_source,
               n.ltp, n.change_pct, n.direction
        FROM scans s
        LEFT JOIN nifty n ON s.scan_id = n.scan_id
        WHERE s.scan_date = ?
        ORDER BY s.scan_id DESC
    """, (scan_date,)).fetchall()
    return rows


def render_scan(
    conn: sqlite3.Connection,
    scan_id: int,
    max_lot_size: int = DEFAULT_MAX_LOT_SIZE,
    stocks_per_sector: int = DEFAULT_STOCKS_PER_SECTOR,
    sectors_to_show: int = DEFAULT_SECTORS_TO_SHOW,
    active_futures_count: int = DEFAULT_ACTIVE_FUTURES_COUNT,
    min_option_volume: int = DEFAULT_MIN_OPTION_VOLUME,
    max_percentage: float = DEFAULT_MAX_PERCENTAGE
):
    """
    Renders a past scan using the exact same side-by-side synchronized
    dual-column layout as nse_sector_scanner.py.
    """
    cur = conn.cursor()

    # 1. Scan metadata
    scan_row = cur.execute("SELECT * FROM scans WHERE scan_id = ?", (scan_id,)).fetchone()
    if not scan_row:
        print(f"[X] Scan ID #{scan_id} not found in database.")
        return

    scan_date   = scan_row["scan_date"]
    scan_time   = scan_row["scan_time"]
    data_source = scan_row["data_source"]

    # 2. NIFTY 50
    nifty_row = cur.execute("SELECT * FROM nifty WHERE scan_id = ?", (scan_id,)).fetchone()
    if nifty_row:
        nifty_ltp    = float(nifty_row["ltp"] or 0)
        nifty_change = float(nifty_row["change_pct"] or 0)
        nifty_dir    = (nifty_row["direction"] or ("POSITIVE" if nifty_change >= 0 else "NEGATIVE")).upper()
    else:
        nifty_ltp, nifty_change, nifty_dir = 0.0, 0.0, "POSITIVE"

    # 3. Sectors
    sector_rows = cur.execute("""
        SELECT sector_name, change_pct 
        FROM sectors 
        WHERE scan_id = ?
        ORDER BY change_pct DESC
    """, (scan_id,)).fetchall()

    sorted_sectors = [(r["sector_name"], {"change": float(r["change_pct"] or 0)}) for r in sector_rows]
    top_gainers = sorted_sectors[:sectors_to_show]
    top_losers  = list(reversed(sorted_sectors[-sectors_to_show:])) if len(sorted_sectors) >= sectors_to_show else []

    if len(sorted_sectors) < sectors_to_show * 2:
        gainer_names = {name for name, _ in top_gainers}
        top_losers = [(name, data) for name, data in top_losers if name not in gainer_names]

    # 4. Stocks (grouped by sector)
    stock_rows = cur.execute("""
        SELECT sector_name, symbol, stock_name, ltp, change_pct, lot_size,
               open_price, day_high, day_low
        FROM stocks
        WHERE scan_id = ?
        ORDER BY change_pct DESC
    """, (scan_id,)).fetchall()

    sector_stocks = {}
    lot_sizes = {}
    for st in stock_rows:
        sec = st["sector_name"]
        sym = st["symbol"].strip().upper()
        lot = st["lot_size"]
        if lot:
            lot_sizes[sym] = int(lot)
        if sec not in sector_stocks:
            sector_stocks[sec] = []
        sector_stocks[sec].append({
            "symbol": sym,
            "ltp": float(st["ltp"] or 0),
            "pchange": float(st["change_pct"] or 0),
            "lot_size": lot,
            "open_price": float(st["open_price"] or 0),
            "day_high": float(st["day_high"] or 0),
            "day_low": float(st["day_low"] or 0)
        })

    # 5. Active Futures
    af_rows = cur.execute("""
        SELECT symbol, instrument, change_pct, ltp, volume_contracts, lot_size
        FROM active_futures
        WHERE scan_id = ? AND instrument = 'Stock Futures'
        ORDER BY volume_contracts DESC
    """, (scan_id,)).fetchall()

    seen_af = set()
    stock_futures = []
    active_fut_symbols = set()
    for af in af_rows:
        sym = af["symbol"].strip().upper()
        active_fut_symbols.add(sym)
        if sym not in seen_af:
            seen_af.add(sym)
            lot = af["lot_size"] or lot_sizes.get(sym)
            if lot:
                lot_sizes[sym] = int(lot)
            if max_lot_size and max_lot_size > 0:
                if lot and lot > max_lot_size:
                    continue
            stock_futures.append({
                "symbol": sym,
                "pchange": float(af["change_pct"] or 0),
                "ltp": float(af["ltp"] or 0),
                "volume": int(af["volume_contracts"] or 0),
                "lot_size": lot
            })

    top_active_gainers = sorted(stock_futures, key=lambda x: x["pchange"], reverse=True)[:active_futures_count]
    top_active_losers  = sorted(stock_futures, key=lambda x: x["pchange"])[:active_futures_count]

    # 6. Active Options (if active_options table exists)
    opt_liq_map = {}
    call_opt_map = {}
    put_opt_map  = {}
    try:
        opt_rows = cur.execute("""
            SELECT symbol, option_type, volume_contracts, strikes_count
            FROM active_options 
            WHERE scan_id = ?
        """, (scan_id,)).fetchall()
        for op in opt_rows:
            sym   = op["symbol"].strip().upper()
            otype = (op["option_type"] or "").strip().title()
            vol   = int(op["volume_contracts"] or 0)
            cnt   = int(op["strikes_count"] or 1)
            if otype == "Call":
                if sym not in call_opt_map:
                    call_opt_map[sym] = {"volume": 0, "contracts_count": 0}
                call_opt_map[sym]["volume"] += vol
                call_opt_map[sym]["contracts_count"] += cnt
            elif otype == "Put":
                if sym not in put_opt_map:
                    put_opt_map[sym] = {"volume": 0, "contracts_count": 0}
                put_opt_map[sym]["volume"] += vol
                put_opt_map[sym]["contracts_count"] += cnt
            else:
                # Fallback for older scans where option_type might be different or unspecified
                if sym not in call_opt_map:
                    call_opt_map[sym] = {"volume": 0, "contracts_count": 0}
                call_opt_map[sym]["volume"] += vol
                call_opt_map[sym]["contracts_count"] += cnt

            # Overall liquidity map for [*] marker (or matching nifty_dir)
            opt_liq_map[sym] = max(opt_liq_map.get(sym, 0), vol)
    except sqlite3.OperationalError:
        # Table doesn't exist yet for earlier scans - default to empty
        pass

    # Enrich with % change from sector_stocks and active_futures if available
    flat_stocks = {stk["symbol"]: stk["pchange"] for stocks in sector_stocks.values() for stk in stocks}
    for af in stock_futures:
        if af["symbol"] not in flat_stocks:
            flat_stocks[af["symbol"]] = af["pchange"]

    # Build Top Active Call and Put Options lists capped at stocks_per_sector, sorted by % change
    raw_active_calls = sorted(
        [{"symbol": sym, "volume": data["volume"], "contracts_count": data.get("contracts_count", 1), "pchange": flat_stocks.get(sym, 0.0)} for sym, data in call_opt_map.items()],
        key=lambda x: x["volume"],
        reverse=True
    )[:stocks_per_sector]

    raw_active_puts = sorted(
        [{"symbol": sym, "volume": data["volume"], "contracts_count": data.get("contracts_count", 1), "pchange": flat_stocks.get(sym, 0.0)} for sym, data in put_opt_map.items()],
        key=lambda x: x["volume"],
        reverse=True
    )[:stocks_per_sector]

    # Sort calls descending (highest % change first) and puts ascending (lowest/most negative % change first)
    top_active_calls = sorted(raw_active_calls, key=lambda x: x["pchange"], reverse=True)
    top_active_puts  = sorted(raw_active_puts, key=lambda x: x["pchange"])

    # --- RENDER TERMINAL GRID ---
    COL_WIDTH  = 48
    GRID_WIDTH = COL_WIDTH * 2 + 3
    SEP_DOUBLE = "=" * GRID_WIDTH
    SEP_MID    = "-" * COL_WIDTH + "-+-" + "-" * COL_WIDTH
    SEP_LINE   = "-" * GRID_WIDTH

    os.system("cls" if os.name == "nt" else "clear")

    print(SEP_DOUBLE)
    print("NSE SECTOR F&O SCANNER (HISTORICAL REPLAY)")
    print(SEP_DOUBLE)

    import re as _re
    _PCT_RE = _re.compile(r'([+-]\d+\.\d+%)')

    def _cpct(val: float) -> str:
        """Return a colour-coded percentage string: green if >=0, red if <0."""
        txt = fmt_pct(val)
        return f"{_GRN}{txt}{_RST}" if val >= 0 else f"{_RED}{txt}{_RST}"

    def _colorize_pct(s: str) -> str:
        """Wrap every +/- percentage token in a string with green or red."""
        def _sub(m):
            tok = m.group(1)
            return f"{_GRN}{tok}{_RST}" if tok.startswith('+') else f"{_RED}{tok}{_RST}"
        return _PCT_RE.sub(_sub, s)

    nifty_lot  = lot_sizes.get("NIFTY", 65)
    dir_colour = _GRN if nifty_dir == "POSITIVE" else _RED
    # print("NIFTY 50")
    print(f"NIFTY 50 --> LTP: {nifty_ltp:,.2f}   Change: {_cpct(nifty_change)}   {dir_colour}{nifty_dir}{_RST}")
    # print(f"Lot Size: {nifty_lot}")

    stock_lookup = {
        stk["symbol"]: stk
        for stocks in sector_stocks.values()
        for stk in stocks
    }

    def _fmt_stock_row(sym: str, pchange: float, lot: int, col_w: int, stk: dict = None) -> str:
        lot_str   = f"Lot: {lot}" if lot else ""
        in_fut    = sym in active_fut_symbols
        fut_str   = "+" if in_fut else " "
        is_top    = opt_liq_map.get(sym, 0) >= min_option_volume
        opt_str   = "[*]" if is_top else "[~]"
        pct_plain = fmt_pct(pchange)

        ohl_str = "   "
        target_stk = stk or stock_lookup.get(sym)
        if target_stk:
            op = target_stk.get("open_price") or 0
            dh = target_stk.get("day_high") or 0
            dl = target_stk.get("day_low") or 0
            if op > 0 and dl > 0 and pchange > 0 and dl <= op:
                wick = (op - dl) / op * 100
                if wick <= 0.15:
                    ohl_str = "O=L"
            elif op > 0 and dh > 0 and pchange < 0 and dh >= op:
                wick = (dh - op) / op * 100
                if wick <= 0.15:
                    ohl_str = "O=H"

        row = f"   {sym:<10} {pct_plain:>7}  {ohl_str}  {lot_str:<9}  {fut_str}  {opt_str}"
        return f"{row:<{col_w}}"

    def _print_dual_row(l_str: str, r_str: str):
        pl = f"{l_str:<{COL_WIDTH}}"
        pr = f"{r_str:<{COL_WIDTH}}"
        # Colorize percentage values
        pl = _colorize_pct(pl)
        pr = _colorize_pct(pr)
        pl = pl.replace("  O=L  ", f"  {_GRN}O=L{_RST}  ").replace("  O=H  ", f"  {_RED}O=H{_RST}  ")
        pr = pr.replace("  O=L  ", f"  {_GRN}O=L{_RST}  ").replace("  O=H  ", f"  {_RED}O=H{_RST}  ")
        pl = pl.replace("  +  ", f"  {_CYN}+{_RST}  ")
        pr = pr.replace("  +  ", f"  {_CYN}+{_RST}  ")
        pl = pl.replace("[*]", f"{_GRN}[*]{_RST}").replace("[~]", f"{_YEL}[~]{_RST}")
        pr = pr.replace("[*]", f"{_GRN}[*]{_RST}").replace("[~]", f"{_YEL}[~]{_RST}")
        print(f"{pl} | {pr}")

    # Determine left/right assignment by NIFTY direction
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

        # Left Sector
        if s_idx < len(left_sectors):
            s_name, s_info = left_sectors[s_idx]
            rank_str  = f"{s_idx + 1}. {s_name}"
            pct_str   = fmt_pct(s_info["change"])
            sp = max(1, COL_WIDTH - len(rank_str) - len(pct_str))
            left_block.append(f"{rank_str}{' ' * sp}{pct_str}")

            s_stocks = sector_stocks.get(s_name, [])
            if max_lot_size and max_lot_size > 0:
                s_stocks = [s for s in s_stocks if s["lot_size"] and s["lot_size"] <= max_lot_size]
            if max_percentage and max_percentage > 0:
                s_stocks = [s for s in s_stocks if abs(s["pchange"]) <= max_percentage]
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

            top_s = sorted(s_stocks, key=lambda x: _stock_rank_key(x, left_is_gain), reverse=True)[:stocks_per_sector]
            for stk in top_s:
                left_block.append(_fmt_stock_row(stk["symbol"], stk["pchange"], stk["lot_size"], COL_WIDTH, stk))

        # Right Sector
        if s_idx < len(right_sectors):
            s_name, s_info = right_sectors[s_idx]
            rank_str  = f"{s_idx + 1}. {s_name}"
            pct_str   = fmt_pct(s_info["change"])
            sp = max(1, COL_WIDTH - len(rank_str) - len(pct_str))
            right_block.append(f"{rank_str}{' ' * sp}{pct_str}")

            s_stocks = sector_stocks.get(s_name, [])
            if max_lot_size and max_lot_size > 0:
                s_stocks = [s for s in s_stocks if s["lot_size"] and s["lot_size"] <= max_lot_size]
            if max_percentage and max_percentage > 0:
                s_stocks = [s for s in s_stocks if abs(s["pchange"]) <= max_percentage]

            top_s = sorted(s_stocks, key=lambda x: _stock_rank_key(x, not left_is_gain), reverse=True)[:stocks_per_sector]
            for stk in top_s:
                right_block.append(_fmt_stock_row(stk["symbol"], stk["pchange"], stk["lot_size"], COL_WIDTH, stk))

        # Pad shorter block so both finish at the exact same row
        rows_count = max(len(left_block), len(right_block))
        left_block  += [""] * (rows_count - len(left_block))
        right_block += [""] * (rows_count - len(right_block))

        for l_line, r_line in zip(left_block, right_block):
            _print_dual_row(l_line, r_line)

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
            left_af_block.append(_fmt_stock_row(af["symbol"], af["pchange"], af["lot_size"], COL_WIDTH))
        for af in right_af_stocks:
            right_af_block.append(_fmt_stock_row(af["symbol"], af["pchange"], af["lot_size"], COL_WIDTH))

        af_rows_count = max(len(left_af_block), len(right_af_block))
        left_af_block  += [""] * (af_rows_count - len(left_af_block))
        right_af_block += [""] * (af_rows_count - len(right_af_block))

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
            in_fut    = sym in active_fut_symbols
            fut_str   = "+" if in_fut else " "
            row = f"   {sym:<10} {pct_plain:>7} {vol_str:<17} {fut_str}  [*]"
            left_opt_block.append(f"{row:<{COL_WIDTH}}")

        for item in right_opt_stocks:
            sym = item["symbol"]
            cnt = item.get("contracts_count")
            cnt_str = f" ({cnt})" if cnt else ""
            vol_str = f"Vol: {item['volume']:,}{cnt_str}"
            pct_plain = fmt_pct(item["pchange"])
            in_fut    = sym in active_fut_symbols
            fut_str   = "+" if in_fut else " "
            row = f"   {sym:<10} {pct_plain:>7} {vol_str:<17} {fut_str}  [*]"
            right_opt_block.append(f"{row:<{COL_WIDTH}}")

        max_opt = max(len(left_opt_block), len(right_opt_block))
        left_opt_block  += [""] * (max_opt - len(left_opt_block))
        right_opt_block += [""] * (max_opt - len(right_opt_block))
        for l_line, r_line in zip(left_opt_block, right_opt_block):
            _print_dual_row(l_line, r_line)

    print(SEP_MID)
    print(f"Legend: {_GRN}[*]{_RST} Top Active Option   {_YEL}[~]{_RST} Standard F&O Option   {_CYN}+{_RST} Active Futures   {_GRN}O=L{_RST} Open≈Low   {_RED}O=H{_RST} Open≈High")
    print(SEP_LINE)

    # Database Summary block
    total_sectors = len(sector_rows)
    total_stocks  = len(stock_rows)
    total_af      = len(af_rows)

    print()
    print("DATABASE METADATA")
    print(SEP_LINE)
    print()
    print(f"Scan ID             : #{scan_id}")
    print(f"Scan Timestamp      : {scan_date} {scan_time}")
    print(f"Sectors in DB       : {total_sectors}")
    print(f"F&O stocks in DB    : {total_stocks}")
    print(f"Active futures in DB: {total_af}")
    print(SEP_DOUBLE)


def main(
    max_lot_size: int = DEFAULT_MAX_LOT_SIZE,
    stocks_per_sector: int = DEFAULT_STOCKS_PER_SECTOR,
    sectors_to_show: int = DEFAULT_SECTORS_TO_SHOW,
    active_futures_count: int = DEFAULT_ACTIVE_FUTURES_COUNT,
    min_option_volume: int = DEFAULT_MIN_OPTION_VOLUME,
    max_percentage: float = DEFAULT_MAX_PERCENTAGE
):
    conn = get_db_connection()

    while True:
        # Step 1: Select Scan Date
        dates = fetch_scan_dates(conn)
        if not dates:
            print("[!] No scans found in market_scanner.db.")
            return

        print()
        print("=" * 70)
        print("NSE SECTOR F&O SCANNER - HISTORICAL SCAN VIEWER")
        print("=" * 70)
        print("\nAVAILABLE SCAN DATES:")
        print("-" * 70)
        for idx, row in enumerate(dates, start=1):
            tag = " (Latest)" if idx == 1 else ""
            print(f"  {idx:>2}. {row['scan_date']}{tag:<10} [{row['scan_count']} scan(s) recorded]")
        print("-" * 70)

        choice_date = input(f"Select date [1-{len(dates)}] (Default: 1, or 'q' to quit, suffix 'd' to delete): ").strip()
        if choice_date.lower() == 'q':
            print("Exiting.")
            break

        if not choice_date:
            chosen_date_idx = 1

        elif choice_date.lower().endswith('d'):
            # --- Delete the selected date and ALL its scans ---
            del_part = choice_date[:-1]
            try:
                del_idx = int(del_part)
                if not (1 <= del_idx <= len(dates)):
                    print(f"[!] Please enter a valid index between 1 and {len(dates)} for deletion.")
                    continue
            except ValueError:
                print("[!] Invalid delete command. Use format '<index>d' e.g., '1d'.")
                continue
            del_date = dates[del_idx - 1]["scan_date"]
            cur = conn.cursor()
            cur.execute("SELECT scan_id FROM scans WHERE scan_date = ?", (del_date,))
            scan_ids = [row["scan_id"] for row in cur.fetchall()]
            if not scan_ids:
                print(f"[!] No scans found for date {del_date}.")
                continue
            dep_tables = ["nifty", "sectors", "stocks", "active_futures", "active_options"]
            for sid in scan_ids:
                for tbl in dep_tables:
                    cur.execute(f"DELETE FROM {tbl} WHERE scan_id = ?", (sid,))
                cur.execute("DELETE FROM scans WHERE scan_id = ?", (sid,))
            conn.commit()
            print(f"[OK] Deleted date {del_date} and its {len(scan_ids)} scan(s) from the database.")
            continue

        else:
            # --- Normal date selection ---
            try:
                chosen_date_idx = int(choice_date)
                if not (1 <= chosen_date_idx <= len(dates)):
                    print(f"[!] Please enter a number between 1 and {len(dates)}.")
                    continue
            except ValueError:
                print("[!] Invalid input. Please enter a valid number.")
                continue

        selected_date = dates[chosen_date_idx - 1]["scan_date"]

        # Step 2: Select Scan for the Chosen Date
        while True:
            scans = fetch_scans_for_date(conn, selected_date)
            if not scans:
                print(f"[!] No scans found for {selected_date}.")
                break

            print()
            print("=" * 70)
            print(f"SCANS RECORDED ON {selected_date}:")
            print("-" * 70)
            for idx, s in enumerate(scans, start=1):
                tag = " [Latest]" if idx == 1 else ""
                n_ltp = s['ltp']
                n_chg = s['change_pct']
                n_dir = s['direction'] or ""
                if n_ltp is not None and n_chg is not None:
                    nifty_summary = f"NIFTY: {n_ltp:,.2f} ({fmt_pct(n_chg)} {n_dir})"
                else:
                    nifty_summary = "NIFTY: N/A"
                print(f"  {idx:>2}. Scan #{s['scan_id']} at {s['scan_time']} | {nifty_summary}{tag}")
            print("-" * 70)

            choice_scan = input(f"Select scan [1-{len(scans)}] (Default: 1, 'b' for back, 'q' to quit, suffix 'd' to delete): ").strip()

            if choice_scan.lower() == 'q':
                conn.close()
                print("Exiting.")
                return

            if choice_scan.lower() == 'b':
                break

            if not choice_scan:
                chosen_scan_idx = 1

            elif choice_scan.lower().endswith('d'):
                # --- Delete a single scan ---
                del_part = choice_scan[:-1]
                try:
                    del_idx = int(del_part)
                    if not (1 <= del_idx <= len(scans)):
                        print(f"[!] Please enter a valid index between 1 and {len(scans)} for deletion.")
                        continue
                except ValueError:
                    print("[!] Invalid delete command. Use format '<index>d', e.g., '1d'.")
                    continue
                del_scan_id = scans[del_idx - 1]["scan_id"]
                cur = conn.cursor()
                dep_tables = ["nifty", "sectors", "stocks", "active_futures", "active_options"]
                for tbl in dep_tables:
                    cur.execute(f"DELETE FROM {tbl} WHERE scan_id = ?", (del_scan_id,))
                cur.execute("DELETE FROM scans WHERE scan_id = ?", (del_scan_id,))
                conn.commit()
                print(f"[OK] Deleted scan ID #{del_scan_id} from the database.")
                continue

            else:
                # --- Normal scan selection ---
                try:
                    chosen_scan_idx = int(choice_scan)
                    if not (1 <= chosen_scan_idx <= len(scans)):
                        print(f"[!] Please enter a number between 1 and {len(scans)}.")
                        continue
                except ValueError:
                    print("[!] Invalid input. Please enter a valid number.")
                    continue

            selected_scan_id = scans[chosen_scan_idx - 1]["scan_id"]

            # Step 3: Render the Scan
            render_scan(
                conn,
                selected_scan_id,
                max_lot_size=max_lot_size,
                stocks_per_sector=stocks_per_sector,
                sectors_to_show=sectors_to_show,
                active_futures_count=active_futures_count,
                min_option_volume=min_option_volume,
                max_percentage=max_percentage
            )

            print()
            post_action = input("Press Enter to select another scan (or 'q' to quit): ").strip()
            if post_action.lower() == 'q':
                conn.close()
                print("Exiting.")
                return

    conn.close()



if __name__ == "__main__":
    # =========================================================================
    # QUICK CONTROLS (Edit here directly — press Ctrl+End to reach in 1 second)
    # =========================================================================
    MAX_LOT_SIZE         = 1500  # Show stocks having lot size <= this value (None or 0 to show all)
    STOCKS_PER_SECTOR    = 10    # Number of top constituent stocks to display for each sector
    SECTORS_TO_SHOW      = 2     # Number of top gaining/losing sectors to display (each side)
    ACTIVE_FUTURES_COUNT = 5     # Number of active stock futures to display in gainers/losers
    MIN_OPTION_VOLUME    = 500   # [*] if stock option contracts >= this, else [~]
    MAX_PERCENTAGE       = 5.0   # Filter stocks with |change%| > this from sector lists (0 to disable)

    main(
        max_lot_size=MAX_LOT_SIZE,
        stocks_per_sector=STOCKS_PER_SECTOR,
        sectors_to_show=SECTORS_TO_SHOW,
        active_futures_count=ACTIVE_FUTURES_COUNT,
        min_option_volume=MIN_OPTION_VOLUME,
        max_percentage=MAX_PERCENTAGE
    )
