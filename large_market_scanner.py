"""
Momentum Scanner - Fast stock momentum detection
Hybrid CLI + HTTP API scanning with caching
"""
import os
import sys
import json
import time
import subprocess
import threading
import re
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
WORKERS = 6
REFRESH_INTERVAL = 15
LIVE_REFRESH = 3
CACHE_MAX = 1000
HTTP_TIMEOUT = 10

paused = threading.Event()
stop_event = threading.Event()


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
        time.sleep(0.05)


def load_symbols(file_path):
    if not os.path.exists(file_path):
        return []
    with open(file_path, "r") as f:
        return [line.strip().upper() for line in f if line.strip()]


# ==========================================
# CLI FETCH
# ==========================================
def fetch_batch_cli(symbols):
    sym_str = ",".join(symbols)
    for attempt in range(2):
        try:
            result = subprocess.run(
                [WEBULL_PATH, "data", "stock", "snapshot",
                 "--symbol", sym_str, "--extend-hour", "--overnight"],
                capture_output=True, text=True, timeout=HTTP_TIMEOUT,
            )
            if result.returncode == 0 and result.stdout.strip():
                data = json.loads(result.stdout)
                if isinstance(data, list):
                    return data
        except subprocess.TimeoutExpired:
            if attempt == 0:
                time.sleep(0.5)
                continue
        except (json.JSONDecodeError, Exception):
            pass
        break
    return []


# ==========================================
# HTTP FETCHER (background thread)
# ==========================================
class HTTPFetcher:
    def __init__(self):
        self._page = None
        self._pw = None
        self._browser = None
        self._ready = False
        self._ticker_cache = {}
        self._lock = threading.Lock()

    def start(self):
        try:
            from playwright.sync_api import sync_playwright
            self._pw = sync_playwright().start()
            self._browser = self._pw.chromium.launch(headless=True)
            ctx = self._browser.new_context()
            self._page = ctx.new_page()
            self._page.goto("https://app.webull.com/", timeout=30000)
            self._page.wait_for_timeout(4000)
            self._ready = True
        except Exception:
            self._ready = False

    def stop(self):
        self._ready = False
        try:
            if self._browser:
                self._browser.close()
            if self._pw:
                self._pw.stop()
        except Exception:
            pass

    def get_ticker_id(self, symbol):
        if symbol in self._ticker_cache:
            return self._ticker_cache[symbol]
        try:
            self._page.goto(f"https://app.webull.com/stocks/{symbol}", timeout=10000)
            self._page.wait_for_timeout(1500)
            content = self._page.content()
            match = re.search(r'tickerId[=:](\d+)', content)
            if match:
                tid = match.group(1)
                with self._lock:
                    self._ticker_cache[symbol] = tid
                return tid
        except Exception:
            pass
        return None

    def fetch_batch(self, symbols):
        if not self._ready:
            return []
        ids = []
        for sym in symbols[:TOP_N]:
            tid = self.get_ticker_id(sym)
            if tid:
                ids.append((sym, tid))
        if not ids:
            return []
        csv = ",".join(t for _, t in ids)
        try:
            data = self._page.evaluate(f"""
                async () => {{
                    const r = await fetch(
                        'https://quotes-gw.webullfintech.com/api/bgw/quote/realtime?ids={csv}&includeSecu=1&delay=0&more=1'
                    );
                    if (!r.ok) return [];
                    return await r.json();
                }}
            """)
            return self._parse(data, {t: s for s, t in ids})
        except Exception:
            return []

    def _parse(self, data, id_map):
        if not isinstance(data, list):
            return []
        out = []
        for item in data:
            try:
                tid = str(item.get("tickerId", ""))
                sym = id_map.get(tid, item.get("symbol", ""))
                close = float(item.get("close", 0))
                pre = float(item.get("preClose", 0))
                cr = float(item.get("changeRatio", 0))
                vol = int(item.get("volume", 0))
                pct = cr * 100
                if close >= MIN_PRICE and close <= MAX_PRICE and pct >= MIN_CHANGE_PCT and pre > 0:
                    out.append({
                        "symbol": sym, "price": close, "prev_close": pre,
                        "change_pct": pct, "volume": vol,
                        "ext_price": None, "ext_change_pct": None, "ext_vol": None,
                        "ovn_price": None, "ovn_change_pct": None, "ovn_vol": None,
                    })
            except (ValueError, TypeError):
                continue
        return out


# ==========================================
# CACHE
# ==========================================
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


# ==========================================
# PARSE CLI
# ==========================================
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
            ev = item.get("extend_hour_volume")
            op = item.get("ovn_price")
            oc = item.get("ovn_change_ratio")
            ov = item.get("ovn_volume")
            if price >= MIN_PRICE and price <= MAX_PRICE and pct >= MIN_CHANGE_PCT and pre > 0:
                out.append({
                    "symbol": sym, "price": price, "prev_close": pre,
                    "change_pct": pct, "volume": vol,
                    "ext_price": float(ep) if ep else None,
                    "ext_change_pct": float(ec) * 100 if ec else None,
                    "ext_vol": int(ev) if ev else None,
                    "ovn_price": float(op) if op else None,
                    "ovn_change_pct": float(oc) * 100 if oc else None,
                    "ovn_vol": int(ov) if ov else None,
                })
        except (ValueError, TypeError):
            continue
    return out


# ==========================================
# TABLE
# ==========================================
def build_table(results, scanned, total, countdown=None, src="cli"):
    paused_str = "[bold yellow]PAUSED[/bold yellow]" if paused.is_set() else "[bold green]LIVE[/bold green]"
    title = (
        f"Top {TOP_N} | {MIN_CHANGE_PCT}%+ | "
        f"{scanned}/{total} | Cache:{len(results)} | "
        f"[dim]{src}[/dim] | {paused_str}"
    )
    if paused.is_set():
        title += " | SPACE"
    elif countdown:
        title += f" | {countdown}s"

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


# ==========================================
# SCANNER
# ==========================================
def run_scanner(symbols, cache):
    console = Console()
    http = HTTPFetcher()
    threading.Thread(target=http.start, daemon=True).start()

    scanned = 0
    batches = [symbols[i:i+BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]

    prog = Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=None),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
    )
    tid = prog.add_task("Starting...", total=len(symbols))

    def layout(cd=None, ts=None, src="cli"):
        top = cache.top_n(TOP_N)
        tbl = build_table(top, scanned, len(symbols), cd, src)
        lo = Layout()
        lo.split_column(
            Layout(Panel(prog, title="Scan"), size=5),
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
                futs = {ex.submit(fetch_batch_cli, b): b for b in group}
                for f in as_completed(futs):
                    try:
                        raw = f.result()
                        cache.update(parse_results(raw))
                    except Exception:
                        pass
                    scanned += len(futs[f])
                    prog.update(tid, advance=len(futs[f]),
                                description=f"CLI: {scanned}/{len(symbols)}")
            time.sleep(0.05)

        prog.update(tid, description="[bold green]Done. HTTP refresh...[/bold green]")

        # Phase 2: HTTP refresh
        for rem in range(REFRESH_INTERVAL, 0, -1):
            if paused.is_set():
                while paused.is_set():
                    live.update(layout())
                    time.sleep(0.2)
                break

            if rem % LIVE_REFRESH == 0:
                try:
                    syms = [r["symbol"] for r in cache.top_n(TOP_N)]
                    if syms and http._ready:
                        res = http.fetch_batch(syms)
                        if res:
                            cache.update(res)
                            live.update(layout(cd=rem, ts=datetime.now().strftime("%H:%M:%S"), src="http"))
                            continue
                except Exception:
                    pass
                live.update(layout(cd=rem, ts=datetime.now().strftime("%H:%M:%S"), src="cli"))
            else:
                live.update(layout(cd=rem, src="cli"))
            time.sleep(1)

    http.stop()


# ==========================================
# MAIN
# ==========================================
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
        f"[bold yellow]SPACE pause[/bold yellow]"
    )

    cache = StockCache()
    while True:
        try:
            run_scanner(symbols, cache)
        except KeyboardInterrupt:
            stop_event.set()
            console.print("\n[bold red]Stopped.[/bold red]")
            break
        except Exception as e:
            console.print(f"[bold red]Error: {e}. Retry 5s...[/bold red]")
            time.sleep(5)


if __name__ == "__main__":
    main()
