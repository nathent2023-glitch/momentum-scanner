# Momentum Scanner

Fast stock momentum scanner using Webull data. Scans 7000+ US stocks to find top movers with 2%+ change.

## Features

- **Hybrid scanning**: CLI for bulk scan + HTTP API for fast refresh
- **Live updates**: Top 20 refreshes every 3 seconds
- **Extended hours**: Pre-market and after-hours data included
- **Caching**: Results persist across scan cycles
- **Pause/Resume**: Press SPACE to pause

## Requirements

- Python 3.8+
- [Webull CLI](https://github.com/nickvdyck/webull) installed
- Playwright (for HTTP refresh)

## Install

```bash
pip install rich playwright
playwright install chromium
```

## Usage

```bash
python large_market_scanner.py
```

## Config

Edit the top of `large_market_scanner.py`:

```python
MIN_PRICE = 0.50      # Minimum stock price
MAX_PRICE = 2000.00   # Maximum stock price
MIN_CHANGE_PCT = 2.0  # Minimum change % to show
TOP_N = 20            # Number of stocks to display
WORKERS = 6           # Parallel scan threads
```

## How It Works

1. **Phase 1**: CLI scans all symbols in parallel batches (with progress bar)
2. **Phase 2**: HTTP API refreshes Top 20 every 3 seconds
3. Cache stores results - data accumulates over time

## License

MIT
