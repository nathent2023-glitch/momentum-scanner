"""
Momentum Scanner - Uses pre-built cache for fast updates
Run cache_scanner.py first to build the cache
"""
import os
import sys
import json
import time
import subprocess
import threading
import signal
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import OrderedDict

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
WORKERS = 2
CACHE_FILE = "scanner_cache.json"
HTTP_TIMEOUT = 15

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


def fetch_batch(symbols, retry=3):
    sym_str = ",".join(symbols)
    for attempt in range(retry + 1):
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
                    if attempt < retry:
                        time.sleep(5 * (attempt + 1))
                        continue
                    return []
                if isinstance(data, list):
                    return data
        except subprocess.TimeoutExpired:
            if attempt < retry:
                time.sleep(3)
                continue
        except (json.JSONDecodeError, Exception):
            pass
        break
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
            if (price >= MIN_PRICE and price <= MAX_PRICE and pct >= MIN_CHANGE_PCT 
                and pre > 0 and vol >= MIN_VOLUME and fp >= MIN_FLOAT):
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
    def __init__(self, cache_file=CACHE_FILE):
        self._data = OrderedDict()
        self._lock = threading.Lock()
        self._cache_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), cache_file)
        self._loaded = False
        self._last_scan = None

    def load(self):
        if os.path.exists(self._cache_file):
            try:
                with open(self._cache_file, "r") as f:
                    saved = json.load(f)
                self._data = OrderedDict()
                for item in saved.get("data", []):
                    self._data[item["symbol"]] = item
                self._last_scan = saved.get("timestamp")
                self._loaded = True
                return True
            except Exception:
                pass
        return False

    def save(self):
        try:
            data = {
                "timestamp": datetime.now().isoformat(),
                "count": len(self._data),
                "data": list(self._data.values()),
            }
            with open(self._cache_file, "w") as f:
                json.dump(data, f)
        except Exception:
            pass

    def update(self, results):
        with self._lock:
            for r in results:
                s = r["symbol"]
                self._data[s] = r
                self._data.move_to_end(s)

    def top_n(self, n=TOP_N):
        with self._lock:
            items = list(self._data.values())
        return sorted(items, key=lambda x: x["change_pct"], reverse=True)[:n]

    def count(self):
        with self._lock:
            return len(self._data)

    def is_loaded(self):
        return self._loaded

    def get_last_scan(self):
        return self._last_scan


def build_table(results, scanned, total, mode="scan", tick_count=0, cache_info=None):
    paused_str = "[bold yellow]PAUSED[/bold yellow]" if paused.is_set() else "[bold green]LIVE[/bold green]"
    
    cache_str = ""
    if cache_info:
        loaded, last, count = cache_info
        pct = (count / total * 100) if total > 0 else 0
        bar_len = 20
        filled = int(bar_len * count / total) if total > 0 else 0
        bar = "█" * filled + "░" * (bar_len - filled)
        if loaded and last:
            cache_str = f" | Cache: {count}/{total} {bar} {pct:.0f}%"
        else:
            cache_str = f" | Cache: {count}/{total} {bar} {pct:.0f}%"
    
    title = (
        f"[{mode.upper()}] Top {TOP_N} | {MIN_CHANGE_PCT}%+ | "
        f"{scanned}/{total} | Ticks:{tick_count} | "
        f"{paused_str}{cache_str}"
    )

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


def run_scanner(cache, total_symbols):
    console = Console()
    tick_count = 0
    prev_prices = {}

    prog = Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=None),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
    )
    tid = prog.add_task("Starting...", total=total_symbols)

    def layout(mode="scan"):
        top = cache.top_n(TOP_N)
        cache_info = (cache.is_loaded(), cache.get_last_scan(), cache.count())
        tbl = build_table(top, cache.count(), total_symbols, mode, tick_count, cache_info)
        lo = Layout()
        lo.split_column(
            Layout(Panel(prog, title=f"[{mode.upper()}]"), size=5),
            Layout(Panel(tbl, title="Results")),
        )
        return lo

    with Live(layout(), console=console, refresh_per_second=4, screen=True) as live:
        # Quick refresh: top 20 stocks every 3 seconds
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
                        if tick_count % 10 == 0:
                            cache.save()
                except Exception:
                    pass

            live.update(layout("tick"))
            
            for _ in range(30):
                if stop_event.is_set():
                    break
                time.sleep(0.1)


def main():
    console = Console()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    txt_path = os.path.join(script_dir, "master_list.txt")

    if not os.path.exists(txt_path):
        console.print(f"[red]No master_list.txt found[/red]")
        sys.exit(1)

    with open(txt_path, "r") as f:
        total_symbols = len([line for line in f if line.strip()])

    cache = StockCache()
    
    if not cache.load():
        console.print("[red]No cache found! Run cache_scanner.py first.[/red]")
        console.print("[yellow]Command: python cache_scanner.py[/yellow]")
        sys.exit(1)

    cache_count = cache.count()
    last_scan = cache.get_last_scan()
    
    console.print(f"[bold green]Momentum Scanner[/bold green] | "
                  f"{cache_count} cached | {total_symbols} total | "
                  f"Last: {last_scan[:16] if last_scan else 'Never'}")
    console.print(f"[dim]Filters: ${MIN_PRICE}+ | Vol>1M | Float>2M | {MIN_CHANGE_PCT}%+[/dim]")
    console.print(f"[bold yellow]SPACE pause | T stop[/bold yellow]")

    threading.Thread(target=key_listener, daemon=True).start()

    while True:
        try:
            run_scanner(cache, total_symbols)
        except KeyboardInterrupt:
            stop_event.set()
            console.print("\n[bold red]Stopped.[/bold red]")
            break
        except Exception as e:
            console.print(f"[red]Error: {e}. Retry 5s...[/red]")
            time.sleep(5)


if __name__ == "__main__":
    main()
