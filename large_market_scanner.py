"""
Momentum Scanner - FAST
Uses Webull CLI snapshot endpoint in large batches for speed
"""
import os
import sys
import json
import time
import subprocess
import threading
import signal
import sqlite3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.panel import Panel
from rich.table import Table

# ==========================================
# CONFIG
# ==========================================
WEBULL_PATH = r"C:\Users\sophi\go\bin\webull.exe"
MIN_PRICE = 2.00
MAX_PRICE = 2000.00
MIN_CHANGE_PCT = 2.0
MIN_VOLUME = 1_000_000
MIN_FLOAT = 2_000_000
TOP_N = 20
BATCH_SIZE = 100
STOCK_DB = "stock_cache.db"
HTTP_TIMEOUT = 20

paused = threading.Event()
stop_event = threading.Event()


def signal_handler(sig, frame):
    stop_event.set()


signal.signal(signal.SIGINT, signal_handler)


def key_listener():
    import msvcrt
    while not stop_event.is_set():
        if msvcrt.kbhit():
            key = msvcrt.getch()
            if key == b" ":
                if paused.is_set():
                    paused.clear()
                else:
                    paused.set()
            elif key == b"t":
                stop_event.set()
                break
        time.sleep(0.05)


def load_symbols():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, STOCK_DB)
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("SELECT ticker FROM stocks WHERE length(ticker) >= 1 AND length(ticker) <= 5")
        symbols = [r[0].upper() for r in c.fetchall()]
        conn.close()
        if symbols:
            return symbols
    txt_path = os.path.join(script_dir, "master_list.txt")
    if os.path.exists(txt_path):
        with open(txt_path, "r") as f:
            return [line.strip().upper() for line in f if line.strip()]
    return []


def fetch_batch(symbols):
    sym_str = ",".join(symbols)
    for attempt in range(3):
        try:
            result = subprocess.run(
                [WEBULL_PATH, "data", "stock", "snapshot",
                 "--symbol", sym_str, "--extend-hour", "--overnight"],
                capture_output=True, text=True, timeout=HTTP_TIMEOUT,
                encoding="utf-8", errors="replace",
            )
            if result.returncode == 0 and result.stdout.strip():
                data = json.loads(result.stdout)
                if isinstance(data, dict) and data.get("error_code") == "TOO_MANY_REQUESTS":
                    time.sleep(5 * (attempt + 1))
                    continue
                if isinstance(data, list):
                    return data
        except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception):
            time.sleep(2)
    return []


def parse_results(raw):
    out = []
    for item in raw:
        try:
            price = float(item.get("price", 0))
            pre = float(item.get("pre_close", 0))
            vol = int(item.get("volume", 0))
            sym = item.get("symbol", "")
            pct = float(item.get("change_ratio", 0)) * 100
            fp = float(item.get("out_standing_shares", 0)) if item.get("out_standing_shares") else 0
            inst_id = item.get("instrument_id", "")
            ep = item.get("extend_hour_last_price")
            ec = item.get("extend_hour_change_ratio")
            op = item.get("ovn_price")
            oc = item.get("ovn_change_ratio")
            if price >= MIN_PRICE and price <= MAX_PRICE and pct >= MIN_CHANGE_PCT and pre > 0:
                out.append({
                    "symbol": sym, "instrument_id": inst_id,
                    "price": price, "prev_close": pre,
                    "change_pct": pct, "volume": vol, "float": fp,
                    "ext_price": float(ep) if ep else None,
                    "ext_change_pct": float(ec) * 100 if ec else None,
                    "ext_vol": None,
                    "ovn_price": float(op) if op else None,
                    "ovn_change_pct": float(oc) * 100 if oc else None,
                    "ovn_vol": None,
                })
        except (ValueError, TypeError):
            continue
    return out


class StockCache:
    def __init__(self):
        self._data = {}
        self._lock = threading.Lock()

    def update(self, results):
        with self._lock:
            for r in results:
                self._data[r["symbol"]] = r

    def top_n(self, n=TOP_N):
        with self._lock:
            items = list(self._data.values())
        return sorted(items, key=lambda x: x["change_pct"], reverse=True)[:n]

    def count(self):
        with self._lock:
            return len(self._data)


def build_table(results, scanned, total, mode="scan", tick_count=0, cache_count=0):
    paused_str = "[bold yellow]PAUSED[/bold yellow]" if paused.is_set() else "[bold green]LIVE[/bold green]"
    title = f"[{mode.upper()}] Top {TOP_N} | {MIN_CHANGE_PCT}%+ | {scanned}/{total} | Cache: {cache_count} | Ticks:{tick_count} | {paused_str}"

    t = Table(title=title, expand=True)
    t.add_column("#", style="dim", width=4, justify="center")
    t.add_column("Symbol", style="bold yellow", width=8, justify="center")
    t.add_column("Price", style="green", width=10, justify="right")
    t.add_column("Chg%", style="bold green", width=8, justify="right")
    t.add_column("Ext$", style="cyan", width=10, justify="right")
    t.add_column("Ext%", style="cyan", width=8, justify="right")
    t.add_column("Ovn$", style="magenta", width=10, justify="right")
    t.add_column("Ovn%", style="magenta", width=8, justify="right")
    t.add_column("Vol", style="dim", width=12, justify="right")
    t.add_column("Float", style="dim", width=10, justify="right")

    for i, r in enumerate(results[:TOP_N], 1):
        ep = f"${r['ext_price']:.2f}" if r.get("ext_price") else "-"
        ec = f"{r['ext_change_pct']:+.2f}%" if r.get("ext_change_pct") is not None else "-"
        op = f"${r['ovn_price']:.2f}" if r.get("ovn_price") else "-"
        oc = f"{r['ovn_change_pct']:+.2f}%" if r.get("ovn_change_pct") is not None else "-"
        fl = r.get("float", 0)
        fl_str = f"{fl/1e6:.1f}M" if fl >= 1e6 else (f"{fl/1e3:.0f}K" if fl >= 1e3 else str(fl)) if fl > 0 else "-"
        t.add_row(
            str(i), r["symbol"], f"${r['price']:.2f}", f"+{r['change_pct']:.2f}%",
            ep, ec, op, oc, f"{r['volume']:,}", fl_str,
        )
    return t


def run_scanner(symbols, total):
    console = Console()
    cache = StockCache()
    tick_count = 0
    prev_prices = {}
    batches = [symbols[i:i+BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]

    prog = Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=None),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
    )
    tid = prog.add_task("Starting...", total=total)

    def layout(mode="scan"):
        top = cache.top_n(TOP_N)
        tbl = build_table(top, cache.count(), total, mode, tick_count, cache.count())
        lo = Layout()
        lo.split_column(
            Layout(Panel(prog, title=f"[{mode.upper()}]"), size=5),
            Layout(Panel(tbl, title="Results")),
        )
        return lo

    with Live(layout(), console=console, refresh_per_second=4, screen=True) as live:
        # Phase 1: Full scan - 4 workers, 100 per batch, fast
        scanned = 0
        for i in range(0, len(batches), 4):
            if stop_event.is_set():
                break
            if paused.is_set():
                while paused.is_set():
                    live.update(layout())
                    time.sleep(0.2)

            group = batches[i:i+4]
            with ThreadPoolExecutor(max_workers=4) as ex:
                futs = {ex.submit(fetch_batch, b): b for b in group}
                for f in as_completed(futs):
                    try:
                        raw = f.result()
                        results = parse_results(raw)
                        if results:
                            for r in results:
                                sym = r["symbol"]
                                new_price = r["price"]
                                old_price = prev_prices.get(sym)
                                if old_price is not None and new_price != old_price:
                                    tick_count += 1
                                prev_prices[sym] = new_price
                            cache.update(results)
                    except Exception:
                        pass
                    scanned += len(futs[f])
                    pct = (scanned / total * 100) if total > 0 else 0
                    filled = int(20 * scanned / total) if total > 0 else 0
                    bar = "█" * filled + "░" * (20 - filled)
                    prog.update(tid, advance=0, description=f"Scanning: {scanned}/{total} {bar} {pct:.0f}%")
            live.update(layout("scan"))
            time.sleep(0.5)

        # Phase 2: Quick refresh - top stocks every 3s
        while not stop_event.is_set():
            if paused.is_set():
                while paused.is_set():
                    live.update(layout())
                    time.sleep(0.2)
                continue

            top = cache.top_n(TOP_N)
            top_syms = [r["symbol"] for r in top]
            if top_syms:
                try:
                    raw = fetch_batch(top_syms)
                    results = parse_results(raw)
                    if results:
                        for r in results:
                            sym = r["symbol"]
                            new_price = r["price"]
                            old_price = prev_prices.get(sym)
                            if old_price is not None and new_price != old_price:
                                tick_count += 1
                            prev_prices[sym] = new_price
                        cache.update(results)
                except Exception:
                    pass

            live.update(layout("tick"))
            for _ in range(30):
                if stop_event.is_set():
                    break
                time.sleep(0.1)


def main():
    console = Console()
    symbols = load_symbols()
    if not symbols:
        console.print("[red]No symbols found[/red]")
        sys.exit(1)

    console.print(f"[bold green]Momentum Scanner[/bold green] | {len(symbols)} symbols | 4 workers")
    console.print(f"[dim]Filters: ${MIN_PRICE}+ | Vol>1M | Float>2M | {MIN_CHANGE_PCT}%+[/dim]")
    console.print(f"[bold yellow]SPACE pause | T stop[/bold yellow]")

    threading.Thread(target=key_listener, daemon=True).start()

    while True:
        try:
            run_scanner(symbols, len(symbols))
        except KeyboardInterrupt:
            stop_event.set()
            console.print("\n[bold red]Stopped.[/bold red]")
            break
        except Exception as e:
            console.print(f"[red]Error: {e}. Retry 5s...[/red]")
            time.sleep(5)


if __name__ == "__main__":
    main()
