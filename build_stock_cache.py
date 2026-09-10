import os
import json
import sqlite3
import urllib.request
import time

class StockCacheManager:
    def __init__(self, db_name="stock_cache.db"):
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        self._init_db()

    def _init_db(self):
        """Creates the local cache tables if they don't exist."""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                ticker TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                exchange TEXT,
                cik TEXT
            )
        """)
        self.conn.commit()

    def fetch_and_cache_sec_listings(self):
        """Fetches thousands of official stock mappings from the SEC Edgar API and caches them."""
        print("Fetching master stock list from SEC Edgar API...")
        url = "https://www.sec.gov/files/company_tickers.json"
        
        # SEC requires a clear User-Agent header or they block the request
        req = urllib.request.Request(
            url, 
            headers={'User-Agent': 'Mozilla/5.0 (CacheBuilder/1.0; contact@example.com)'}
        )
        
        try:
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read().decode())
                
                # Bulk preparation for SQLite
                records = []
                for item in data.values():
                    records.append((
                        item['ticker'].strip().upper(),
                        item['title'].strip(),
                        "US_MARKET",
                        str(item['cik_str'])
                    ))
                
                # Insert or replace records in local cache database
                self.cursor.executemany("""
                    INSERT OR REPLACE INTO stocks (ticker, name, exchange, cik)
                    VALUES (?, ?, ?, ?)
                """, records)
                self.conn.commit()
                print(f"Successfully cached {len(records)} stocks to local SQLite database!")
                
        except Exception as e:
            print(f"Error fetching data: {e}")

    def query_local_cache(self, search_term):
        """Queries the local high-speed cache instead of hitting the internet."""
        search_pattern = f"%{search_term}%"
        self.cursor.execute("""
            SELECT ticker, name, exchange, cik FROM stocks 
            WHERE ticker LIKE ? OR name LIKE ?
            LIMIT 10
        """, (search_pattern, search_pattern))
        return self.cursor.fetchall()

    def close(self):
        self.conn.close()

if __name__ == "__main__":
    cache = StockCacheManager()
    
    # 1. Build the local offline cache file
    cache.fetch_and_cache_sec_listings()
    
    # 2. Test lightning-fast local lookups
    print("\nTesting local database search for 'APPLE'...")
    results = cache.query_local_cache("AAPL")
    for r in results:
        print(f"Ticker: {r[0]} | Name: {r[1]} | Exchange: {r[2]} | CIK ID: {r[3]}")
        
    cache.close()
