"""
Cache Scanner - Caches all stock data before main scanner runs
Run this first to build the cache
"""
import os
import sys
import json
import time
import subprocess
import threading
import signal
from datetime import datetime

from rich.console import Console
from rich.live import Live
from rich.panel import Panel

# ==========================================
# CONFIG
# ==========================================
WEBULL_PATH = r"C:\Users\sophi\go\bin\webull.exe"
MIN_PRICE = 2.00
MAX_PRICE = 2000.00
MIN_CHANGE_PCT = 2.0
MIN_VOLUME = 1_000_000
MIN_FLOAT = 2_000_000
BATCH_SIZE = 50
CACHE_FILE = "scanner_cache.json"
HTTP_TIMEOUT = 20
BATCH_DELAY = 0.5

stop_event = threading.Event()


def signal_handler(sig, frame):
    stop_event.set()


signal.signal(signal.SIGINT, signal_handler)


def load_symbols(file_path):
    if not os.path.exists(file_path):
        return []
    with open(file_path, "r") as f:
        return [line.strip().upper() for line in f if line.strip()]


def fetch_batch(symbols, retry=4):
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
                        wait = 5 * (attempt + 1)
                        time.sleep(wait)
                        continue
                    return [], True
                if isinstance(data, list):
                    return data, False
        except subprocess.TimeoutExpired:
            if attempt < retry:
                time.sleep(5)
                continue
        except (json.JSONDecodeError, Exception):
            pass
        break
    return [], True


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
        self._data = {}
        self._lock = threading.Lock()
        self._cache_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), cache_file)

    def update(self, results):
        with self._lock:
            for r in results:
                self._data[r["symbol"]] = r

    def count(self):
        with self._lock:
            return len(self._data)

    def save(self):
        try:
            data = {
                "timestamp": datetime.now().isoformat(),
                "count": len(self._data),
                "data": list(self._data.values()),
            }
            with open(self._cache_file, "w") as f:
                json.dump(data, f)
            return True
        except Exception:
            return False

    def load(self):
        if os.path.exists(self._cache_file):
            try:
                with open(self._cache_file, "r") as f:
                    saved = json.load(f)
                self._data = {item["symbol"]: item for item in saved.get("data", [])}
                return saved.get("timestamp"), len(self._data)
            except Exception:
                return None, 0
        return None, 0

    def symbols(self):
        with self._lock:
            return list(self._data.keys())


def main():
    console = Console()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    txt_path = os.path.join(script_dir, "master_list.txt")

    symbols = load_symbols(txt_path)
    if not symbols:
        console.print(f"[red]No symbols in {txt_path}[/red]")
        sys.exit(1)

    cache = StockCache()
    cache.load()
    
    console.print(f"[bold green]Caching {len(symbols)} stocks...[/bold green]")
    console.print(f"[dim]Batch size: {BATCH_SIZE} | Single worker | 0.5s delay[/dim]")

    batches = [symbols[i:i+BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    scanned = 0
    failed_batches = []

    with Live(console=console, refresh_per_second=4) as live:
        # Single worker - one batch at a time
        for i, batch in enumerate(batches):
            if stop_event.is_set():
                break

            raw, was_limited = fetch_batch(batch)
            parsed = parse_results(raw)
            if parsed:
                cache.update(parsed)
            elif was_limited:
                failed_batches.append(batch)

            scanned += len(batch)
            pct = (scanned / len(symbols) * 100) if len(symbols) > 0 else 0
            filled = int(20 * scanned / len(symbols)) if len(symbols) > 0 else 0
            bar = "█" * filled + "░" * (20 - filled)
            live.update(Panel(
                f"[blue]{scanned}/{len(symbols)} {bar} {pct:.0f}%[/blue] | "
                f"Cache: {cache.count()} stocks | Failed: {len(failed_batches)}",
                title=f"Caching batch {i+1}/{len(batches)}"
            ))
            
            if was_limited:
                time.sleep(3)
            else:
                time.sleep(BATCH_DELAY)

        # Retry failed with longer delays
        if failed_batches and not stop_event.is_set():
            live.update(Panel(f"[yellow]Retrying {len(failed_batches)} failed batches...[/yellow]", title="Caching"))
            time.sleep(15)
            for i, batch in enumerate(failed_batches):
                if stop_event.is_set():
                    break
                raw, _ = fetch_batch(batch, retry=5)
                parsed = parse_results(raw)
                if parsed:
                    cache.update(parsed)
                live.update(Panel(
                    f"[yellow]Retry {i+1}/{len(failed_batches)} | Cache: {cache.count()}[/yellow]",
                    title="Retrying"
                ))
                time.sleep(2)

    count = cache.count()
    if count > 0:
        cache.save()
        console.print(f"\n[bold green]Cache complete: {count} stocks saved to {CACHE_FILE}[/bold green]")
    else:
        console.print("[red]No stocks matched filters. Check if market is open.[/red]")


if __name__ == "__main__":
    main()
