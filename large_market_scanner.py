"""
Momentum Scanner - Webull CLI
Fast batch scanning with live top-movers display
"""
import os
import sys
import json
import time
import subprocess
import threading
import signal
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
WEBULL = r"C:\Users\sophi\go\bin\webull.exe"
TIMEOUT = 20
WORKERS = 2
BATCH = 50
TOP = 20
DB = "stock_cache.db"

# Filters (live-editable)
f_price_min = 2.0
f_price_max = 2000.0
f_change_min = 2.0
f_volume_min = 1_000_000
f_float_min = 2_000_000

# State
is_paused = threading.Event()
shutdown = threading.Event()


def signal_handler(sig, frame):
    shutdown.set()


signal.signal(signal.SIGINT, signal_handler)


def key_listener():
    import msvcrt
    while not shutdown.is_set():
        if msvcrt.kbhit():
            key = msvcrt.getch()
            if key == b" ":
                if is_paused.is_set():
                    is_paused.clear()
                else:
                    is_paused.set()
            elif key == b"t":
                shutdown.set()
                break
            elif key == b"f":
                is_paused.set()
                time.sleep(0.2)
                global f_price_min, f_price_max, f_change_min, f_volume_min, f_float_min
                print("\n--- FILTER MODE ---")
                print(f"Current: ${f_price_min}-${f_price_max} | {f_change_min}%+ | Vol {f_volume_min/1e6:.1f}M+ | Float {f_float_min/1e6:.1f}M+")
                print("Press Enter to keep current value")
                try:
                    v = input(f"Min Price [{f_price_min}]: ").strip()
                    if v: f_price_min = float(v)
                    v = input(f"Max Price [{f_price_max}]: ").strip()
                    if v: f_price_max = float(v)
                    v = input(f"Min Change% [{f_change_min}]: ").strip()
                    if v: f_change_min = float(v)
                    v = input(f"Min Volume M [{f_volume_min/1e6:.1f}]: ").strip()
                    if v: f_volume_min = float(v) * 1e6
                    v = input(f"Min Float M [{f_float_min/1e6:.1f}]: ").strip()
                    if v: f_float_min = float(v) * 1e6
                    print(f"New: ${f_price_min}-${f_price_max} | {f_change_min}%+ | Vol {f_volume_min/1e6:.1f}M+ | Float {f_float_min/1e6:.1f}M+")
                except Exception:
                    print("Invalid, keeping current")
                is_paused.clear()
        time.sleep(0.05)


def load_symbols():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "master_list.txt")
    if os.path.exists(path):
        with open(path) as f:
            return [line.strip().upper() for line in f if line.strip()]
    return []


def fetch_batch(symbols):
    sym_str = ",".join(symbols)
    for attempt in range(3):
        try:
            result = subprocess.run(
                [WEBULL, "data", "stock", "snapshot",
                 "--symbol", sym_str, "--extend-hour", "--overnight"],
                capture_output=True, text=True, timeout=TIMEOUT,
                encoding="utf-8", errors="replace",
            )
            if result.returncode == 0 and result.stdout.strip():
                data = json.loads(result.stdout)
                if isinstance(data, dict) and data.get("error_code") == "TOO_MANY_REQUESTS":
                    time.sleep(5 * (attempt + 1))
                    continue
                if isinstance(data, list):
                    return data
        except (subprocess.TimeoutExpired, json.JSONDecodeError):
            time.sleep(2)
    return []


def parse(raw):
    out = []
    for item in raw:
        try:
            price = float(item.get("price", 0))
            prev = float(item.get("pre_close", 0))
            vol = int(item.get("volume", 0))
            sym = item.get("symbol", "")
            chg = float(item.get("change_ratio", 0)) * 100
            flt = float(item.get("out_standing_shares", 0)) if item.get("out_standing_shares") else 0
            ep = item.get("extend_hour_last_price")
            ec = item.get("extend_hour_change_ratio")
            op = item.get("ovn_price")
            oc = item.get("ovn_change_ratio")
            if (price >= f_price_min and price <= f_price_max
                    and chg >= f_change_min and prev > 0):
                out.append({
                    "symbol": sym, "price": price, "prev_close": prev,
                    "change_pct": chg, "volume": vol, "float": flt,
                    "ext_price": float(ep) if ep else None,
                    "ext_change": float(ec) * 100 if ec else None,
                    "ovn_price": float(op) if op else None,
                    "ovn_change": float(oc) * 100 if oc else None,
                })
        except (ValueError, TypeError):
            continue
    return out


class Cache:
    def __init__(self):
        self._data = {}
        self._lock = threading.Lock()

    def update(self, results):
        with self._lock:
            for r in results:
                self._data[r["symbol"]] = r

    def top(self, n=TOP):
        with self._lock:
            items = list(self._data.values())
        return sorted(items, key=lambda x: x["change_pct"], reverse=True)[:n]

    def count(self):
        with self._lock:
            return len(self._data)


def fmt_float(val):
    if val >= 1e9: return f"{val/1e9:.1f}B"
    if val >= 1e6: return f"{val/1e6:.1f}M"
    if val >= 1e3: return f"{val/1e3:.0f}K"
    return str(val) if val > 0 else "-"


def build_table(results, scanned, total, mode="scan", changes=0):
    status = "[bold yellow]PAUSED[/bold yellow]" if is_paused.is_set() else "[bold green]LIVE[/bold green]"
    title = (f"[{mode.upper()}] Top {TOP} | {f_change_min}%+ | "
             f"{scanned}/{total} | Cache: {Cache().count()} | "
             f"Changes: {changes} | {status}")

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

    for i, r in enumerate(results[:TOP], 1):
        ep = f"${r['ext_price']:.2f}" if r.get("ext_price") else "-"
        ec = f"{r['ext_change']:+.2f}%" if r.get("ext_change") is not None else "-"
        op = f"${r['ovn_price']:.2f}" if r.get("ovn_price") else "-"
        oc = f"{r['ovn_change']:+.2f}%" if r.get("ovn_change") is not None else "-"
        t.add_row(
            str(i), r["symbol"], f"${r['price']:.2f}", f"+{r['change_pct']:.2f}%",
            ep, ec, op, oc, f"{r['volume']:,}", fmt_float(r.get("float", 0)),
        )
    return t


def run_scanner(symbols, total):
    console = Console()
    cache = Cache()
    price_changes = 0
    last_prices = {}
    batches = [symbols[i:i+BATCH] for i in range(0, len(symbols), BATCH)]

    progress = Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=None),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
    )
    task_id = progress.add_task("Starting...", total=total)

    def make_layout(mode="scan"):
        top = cache.top(TOP)
        table = build_table(top, cache.count(), total, mode, price_changes)
        layout = Layout()
        layout.split_column(
            Layout(Panel(progress, title=f"[{mode.upper()}]"), size=5),
            Layout(Panel(table, title="Results")),
        )
        return layout

    def apply_batch_results(raw):
        nonlocal price_changes
        results = parse(raw)
        if results:
            for r in results:
                sym = r["symbol"]
                new = r["price"]
                old = last_prices.get(sym)
                if old is not None and new != old:
                    price_changes += 1
                last_prices[sym] = new
            cache.update(results)

    with Live(make_layout(), console=console, refresh_per_second=4, screen=True) as live:
        # Phase 1: Initial full scan
        scanned = 0
        for i in range(0, len(batches), 4):
            if shutdown.is_set():
                break
            while is_paused.is_set():
                live.update(make_layout())
                time.sleep(0.2)

            group = batches[i:i+4]
            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = {pool.submit(fetch_batch, b): b for b in group}
                for future in as_completed(futures):
                    try:
                        apply_batch_results(future.result())
                    except Exception:
                        pass
                    scanned += len(futures[future])
                    pct = (scanned / total * 100) if total > 0 else 0
                    filled = int(20 * scanned / total) if total > 0 else 0
                    bar = "█" * filled + "░" * (20 - filled)
                    progress.update(task_id, advance=0,
                                   description=f"Scanning: {scanned}/{total} {bar} {pct:.0f}%")
            live.update(make_layout("scan"))
            time.sleep(1.5)

        # Phase 2: Live loop - top refresh every 2s, full rescan every 15s
        last_rescan = time.time()
        while not shutdown.is_set():
            while is_paused.is_set():
                live.update(make_layout())
                time.sleep(0.2)

            # Quick: refresh top stocks every 2s
            top = cache.top(TOP)
            top_syms = [r["symbol"] for r in top]
            if top_syms:
                try:
                    apply_batch_results(fetch_batch(top_syms))
                except Exception:
                    pass

            live.update(make_layout("tick"))

            # Slow: full rescan every 15s
            if time.time() - last_rescan >= 15:
                last_rescan = time.time()
                for i in range(0, len(batches), 2):
                    if shutdown.is_set() or is_paused.is_set():
                        break
                    group = batches[i:i+2]
                    with ThreadPoolExecutor(max_workers=2) as pool:
                        futures = {pool.submit(fetch_batch, b): b for b in group}
                        for future in as_completed(futures):
                            try:
                                apply_batch_results(future.result())
                            except Exception:
                                pass
                    live.update(make_layout("rescan"))

            for _ in range(20):
                if shutdown.is_set():
                    break
                time.sleep(0.1)


def main():
    console = Console()
    symbols = load_symbols()
    if not symbols:
        console.print("[red]No symbols found[/red]")
        sys.exit(1)

    console.print(f"[bold green]Momentum Scanner[/bold green] | {len(symbols)} symbols")
    console.print(f"[dim]Filters: ${f_price_min}+ | Vol>1M | Float>2M | {f_change_min}%+[/dim]")
    console.print("[bold yellow]SPACE pause | T stop | F filters[/bold yellow]")

    threading.Thread(target=key_listener, daemon=True).start()

    while True:
        try:
            run_scanner(symbols, len(symbols))
        except KeyboardInterrupt:
            shutdown.set()
            console.print("\n[bold red]Stopped.[/bold red]")
            break
        except Exception as e:
            console.print(f"[red]Error: {e}. Retry 5s...[/red]")
            time.sleep(5)


if __name__ == "__main__":
    main()
