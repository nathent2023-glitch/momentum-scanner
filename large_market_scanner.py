"""
Momentum Scanner - CLI only, tick-by-tick for top movers
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
MIN_PRICE = 0.50
MAX_PRICE = 2000.00
MIN_CHANGE_PCT = 2.0
BATCH_SIZE = 100
TOP_N = 20
WORKERS = 4
REFRESH_INTERVAL = 15
TICK_INTERVAL = 2
CACHE_MAX = 1000
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


def load_symbols(file_path):
    if not os.path.exists(file_path):
        return []
    with open(file_path, "r") as f:
        return [line.strip().upper() for line in f if line.strip()]


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
                    time.sleep(2 * (attempt + 1))
                    continue
                if isinstance(data, list):
                    return data
        except subprocess.TimeoutExpired:
            if attempt == 0:
                time.sleep(1)
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
            ep = item.get("extend_hour_last_price")
            ec = item.get("extend_hour_change_ratio")
            op = item.get("ovn_price")
            oc = item.get("ovn_change_ratio")
            if price >= MIN_PRICE and price <= MAX_PRICE and pct >= MIN_CHANGE_PCT and pre > 0:
                out.append({
                    "symbol": sym, "price": price, "prev_close": pre,
                    "change_pct": pct, "volume": vol,
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
    def __init__(self, max_size=CACHE_MAX):
        self._data = OrderedDict()
        self._max = max_size
        self._lock = threading.Lock()

    def update(self, results):
        with self._lock:
            for r in results:
                s = r["symbol"]
                self._data[s] = r
                self._data.move_to_end(s)
                while len(self._data) > self._max:
                    self._data.popitem(last=False)

    def top_n(self, n=TOP_N):
        with self._lock:
            items = list(self._data.values())
        return sorted(items, key=lambda x: x["change_pct"], reverse=True)[:n]

    def count(self):
        with self._lock:
            return len(self._data)


def build_table(results, scanned, total, mode="scan", tick_count=0):
    paused_str = "[bold yellow]PAUSED[/bold yellow]" if paused.is_set() else "[bold green]LIVE[/bold green]"
    title = (
        f"[{mode.upper()}] Top {TOP_N} | {MIN_CHANGE_PCT}%+ | "
        f"{scanned}/{total} | Cache:{len(results)} | "
        f"Ticks:{tick_count} | {paused_str}"
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

    for i, r in enumerate(results[:TOP_N], 1):
        ep = f"${r['ext_price']:.2f}" if r.get("ext_price") else "-"
        ec = f"{r['ext_change_pct']:+.2f}%" if r.get("ext_change_pct") is not None else "-"
        op = f"${r['ovn_price']:.2f}" if r.get("ovn_price") else "-"
        oc = f"{r['ovn_change_pct']:+.2f}%" if r.get("ovn_change_pct") is not None else "-"
        t.add_row(
            str(i), r["symbol"], f"${r['price']:.2f}", f"+{r['change_pct']:.2f}%",
            ep, ec, op, oc, f"{r['volume']:,}",
        )
    return t


def run_scanner(symbols, cache):
    console = Console()
    scanned = 0
    batches = [symbols[i:i+BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    tick_count = 0
    prev_prices = {}

    prog = Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=None),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
    )
    tid = prog.add_task("Starting...", total=len(symbols))

    def layout(mode="scan"):
        top = cache.top_n(TOP_N)
        tbl = build_table(top, scanned, len(symbols), mode, tick_count)
        lo = Layout()
        lo.split_column(
            Layout(Panel(prog, title=f"[{mode.upper()}]"), size=5),
            Layout(Panel(tbl, title="Results")),
        )
        return lo

    with Live(layout(), console=console, refresh_per_second=4, screen=True) as live:
        # Phase 1: CLI scan
        for i in range(0, len(batches), WORKERS):
            if paused.is_set():
                while paused.is_set():
                    live.update(layout())
                    time.sleep(0.2)
                prog.update(tid, description="Resuming...")

            group = batches[i:i+WORKERS]
            with ThreadPoolExecutor(max_workers=WORKERS) as ex:
                futs = {ex.submit(fetch_batch, b): b for b in group}
                for f in as_completed(futs):
                    try:
                        raw = f.result()
                        cache.update(parse_results(raw))
                    except Exception:
                        pass
                    scanned += len(futs[f])
                    prog.update(tid, advance=len(futs[f]),
                                description=f"CLI: {scanned}/{len(symbols)}")
            time.sleep(0.2)

        prog.update(tid, description="[bold green]Scan done. Tick-by-tick...[/bold green]")

        # Phase 2: Tick-by-tick for top stocks
        while not stop_event.is_set():
            if paused.is_set():
                while paused.is_set():
                    live.update(layout("tick"))
                    time.sleep(0.2)
                continue

            top = cache.top_n(TOP_N)
            top_syms = [r["symbol"] for r in top]

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
            time.sleep(TICK_INTERVAL)


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    txt_path = os.path.join(script_dir, "master_list.txt")

    symbols = load_symbols(txt_path)
    if not symbols:
        print(f"No symbols in {txt_path}")
        sys.exit(1)

    threading.Thread(target=key_listener, daemon=True).start()

    console = Console()
    console.print(
        f"[bold green]Momentum Scanner[/bold green] | "
        f"{len(symbols)} symbols | "
        f"{MIN_CHANGE_PCT}%+ | "
        f"${MIN_PRICE}-${MAX_PRICE} | "
        f"[bold yellow]SPACE pause | T stop[/bold yellow]"
    )

    cache = StockCache()
    while True:
        try:
            run_scanner(symbols, cache)
        except KeyboardInterrupt:
            stop_event.set()
            try:
                console.print("\n[bold red]Stopped.[/bold red]")
            except Exception:
                pass
            break
        except Exception as e:
            console.print(f"[bold red]Error: {e}. Retry 5s...[/bold red]")
            time.sleep(5)


if __name__ == "__main__":
    main()
