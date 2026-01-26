#!/usr/bin/env python3
"""
Background price cache updater with Flask web server.
- Updates prices every 60 seconds in background thread
- Serves price_cache.json via HTTP at root URL
- Ready to deploy on Render, Railway, or similar platforms
"""

import json
import requests
import time
import random
import concurrent.futures
import threading
from datetime import datetime
from pathlib import Path
from flask import Flask, send_file, jsonify
from flask_cors import CORS

# Configuration - Multiple API keys for rate limit distribution
API_KEYS = [
    'cvneau1r01qq3c7eq690cvneau1r01qq3c7eq69g',
    'd5rpp4pr01qj5oildkn0d5rpp4pr01qj5oildkng',
]
CACHE_FILE = Path(__file__).parent / 'price_cache.json'

# Asset mappings
ASSET_CONFIGS = {
    'BINANCE:BTCUSDT': {'api_symbol': 'BINANCE:BTCUSDT', 'use_yahoo': False},
    'VOO': {'api_symbol': 'VOO', 'use_yahoo': False},
    'NANC': {'api_symbol': 'NANC', 'use_yahoo': False},
    'IAU': {'api_symbol': 'IAU', 'use_yahoo': False},
    'SGOV': {'api_symbol': 'SGOV', 'use_yahoo': False},
    'IWDE': {'api_symbol': 'IWDE.L', 'use_yahoo': True},
    'XUSE': {'api_symbol': 'XUSE.AS', 'use_yahoo': True},
    'URNU': {'api_symbol': 'URNU.L', 'use_yahoo': True},
    'COPX': {'api_symbol': 'COPX.L', 'use_yahoo': True},
    'PALL': {'api_symbol': 'PALL', 'use_yahoo': False},
    'SIVR': {'api_symbol': 'SIVR', 'use_yahoo': False},
}

def get_random_headers():
    """Generate random browser-like headers to avoid rate limiting."""
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    ]
    
    accept_languages = [
        'en-US,en;q=0.9',
        'en-GB,en;q=0.9',
        'en-US,en;q=0.9,fr;q=0.8',
    ]
    
    return {
        'User-Agent': random.choice(user_agents),
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': random.choice(accept_languages),
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://finance.yahoo.com/',
        'Origin': 'https://finance.yahoo.com',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
    }

def fetch_with_timeout(url, timeout=5, use_session=False, headers=None, use_browser_headers=False):
    """Fetch with timeout - use fresh session for Yahoo to avoid tracking."""
    try:
        if headers is None:
            if use_browser_headers:
                headers = get_random_headers()
            else:
                headers = {}
        
        if use_session and 'finnhub.io' in url:
            if not hasattr(fetch_with_timeout, '_finnhub_session'):
                fetch_with_timeout._finnhub_session = requests.Session()
            response = fetch_with_timeout._finnhub_session.get(url, timeout=timeout, headers=headers)
        else:
            fresh_session = requests.Session()
            response = fresh_session.get(url, timeout=timeout, headers=headers)
        
        response.raise_for_status()
        return response.json()
    except Exception as e:
        raise Exception(f"Failed to fetch {url}: {e}")

def fetch_finnhub_price(symbol, api_key_index=0):
    """Fetch price from Finnhub with API key rotation."""
    api_key = API_KEYS[api_key_index % len(API_KEYS)]
    url = f'https://finnhub.io/api/v1/quote?symbol={symbol}&token={api_key}'
    data = fetch_with_timeout(url, timeout=5, use_session=True, use_browser_headers=False)
    return {
        'price': data['c'],
        'percentChange': data['dp']
    }

def encodeURIComponent(s):
    """URL encode helper."""
    import urllib.parse
    return urllib.parse.quote(s, safe='')

def fetch_yahoo_price(symbol, max_time=15):
    """Fetch price from Yahoo Finance via CORS proxy."""
    yahoo_url = f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}'
    start_time = time.time()
    
    proxy_templates = [
        'https://api.allorigins.win/raw?url={url}',
        'https://corsproxy.io/?{url}',
        'https://thingproxy.freeboard.io/fetch/{url}',
    ]
    
    attempt = 0
    while time.time() - start_time < max_time:
        attempt += 1
        proxies = [template.format(url=encodeURIComponent(yahoo_url)) for template in proxy_templates]
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(proxies)) as executor:
            futures = {
                executor.submit(fetch_with_timeout, proxy_url, 5, False, None, True): proxy_url 
                for proxy_url in proxies
            }
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    data = future.result()
                    result = data['chart']['result'][0]
                    meta = result['meta']
                    price = meta['regularMarketPrice']
                    
                    if meta.get('regularMarketChangePercent') is not None:
                        percentChange = meta['regularMarketChangePercent'] * 100
                    elif meta.get('previousClose') and meta.get('regularMarketPrice'):
                        percentChange = ((meta['regularMarketPrice'] - meta['previousClose']) / meta['previousClose']) * 100
                    else:
                        percentChange = 0
                    
                    return {'price': price, 'percentChange': percentChange}
                except Exception:
                    continue
        
        if attempt > 1:
            time.sleep(random.uniform(0.2, 0.5))
    
    raise Exception(f"Timeout after {max_time}s for {symbol}")

def fetch_asset_price(symbol, config, max_time=15, api_key_index=0):
    """Fetch price for a single asset."""
    start_time = time.time()
    
    if config['use_yahoo']:
        return fetch_yahoo_price(config['api_symbol'], max_time=max_time)
    else:
        attempt = 0
        while time.time() - start_time < max_time:
            attempt += 1
            try:
                key_idx = (api_key_index + attempt - 1) % len(API_KEYS)
                return fetch_finnhub_price(config['api_symbol'], api_key_index=key_idx)
            except Exception:
                if attempt > 1:
                    time.sleep(0.2)
        
        raise Exception(f"Timeout after {max_time}s for {symbol}")

def update_cache():
    """Main function to update the price cache."""
    print(f"\n{'='*60}")
    print(f"PRICE CACHE UPDATER - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    start_time = time.time()
    cache_data = {
        'timestamp': datetime.now().isoformat(),
        'assets': {}
    }
    
    symbols = [
        'BINANCE:BTCUSDT',
        'VOO',
        'NANC',
        'IAU',
        'SGOV',
        'IWDE',
        'XUSE',
        'URNU',
        'COPX',
        'PALL',
        'SIVR'
    ]
    
    print(f"Fetching {len(symbols)} financial assets...\n")
    
    def fetch_asset_with_key(symbol, key_idx, delay=0):
        if delay > 0:
            time.sleep(delay)
        
        if symbol in ASSET_CONFIGS:
            try:
                return (symbol, fetch_asset_price(symbol, ASSET_CONFIGS[symbol], max_time=15, api_key_index=key_idx))
            except Exception:
                return (symbol, None)
        return (symbol, None)
    
    yahoo_symbols = [s for s in symbols if s in ASSET_CONFIGS and ASSET_CONFIGS[s]['use_yahoo']]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(symbols)) as executor:
        futures = {}
        for idx, symbol in enumerate(symbols):
            delay = random.uniform(0, 0.5) if symbol in yahoo_symbols else 0
            futures[executor.submit(fetch_asset_with_key, symbol, idx % len(API_KEYS), delay)] = symbol
        
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            symbol, price_data = future.result()
            if price_data:
                cache_data['assets'][symbol] = price_data
                completed += 1
                print(f"  ✓ {symbol}: ${price_data['price']:.2f} ({price_data['percentChange']:+.2f}%) [{completed}/{len(symbols)}]")
            else:
                print(f"  ✗ {symbol}: Failed (will retry)")
    
    # Retry missing assets
    if len(cache_data['assets']) < len(symbols):
        missing = set(symbols) - set(cache_data['assets'].keys())
        print(f"\n  ⚠ Retrying {len(missing)} missing assets...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(missing)) as executor:
            retry_futures = {
                executor.submit(fetch_asset_with_key, symbol, idx % len(API_KEYS)): symbol 
                for idx, symbol in enumerate(missing)
            }
            
            for future in concurrent.futures.as_completed(retry_futures):
                symbol, price_data = future.result()
                if price_data:
                    cache_data['assets'][symbol] = price_data
                    print(f"  ✓ {symbol}: ${price_data['price']:.2f} ({price_data['percentChange']:+.2f}%)")
                else:
                    print(f"  ✗ {symbol}: Still failed after retry")
    
    # Save cache
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump(cache_data, f, indent=2)
        
        elapsed = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"✓ Cache updated successfully in {elapsed:.2f}s")
        print(f"  Assets cached: {len(cache_data['assets'])}")
        print(f"{'='*60}\n")
        return True
    except Exception as e:
        print(f"\n✗ Failed to save cache: {e}\n")
        return False

# ============================================================
# FLASK WEB SERVER (serves the JSON via HTTP)
# ============================================================

app = Flask(__name__)
CORS(app)  # This enables CORS for all routes

def background_updater():
    """Runs in background thread - updates cache every 60 seconds"""
    print("🚀 Background updater started!")
    while True:
        try:
            update_cache()
        except Exception as e:
            print(f"❌ Error in background updater: {e}")
        
        print(f"💤 Sleeping 60 seconds until next update...\n")
        time.sleep(60)

@app.route('/')
def get_cache():
    """Serve the price cache JSON"""
    try:
        if CACHE_FILE.exists():
            return send_file(CACHE_FILE, mimetype='application/json')
        else:
            return jsonify({"error": "Cache not ready yet, please wait"}), 503
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/health')
def health():
    """Health check endpoint for Render"""
    return jsonify({
        "status": "ok",
        "cache_exists": CACHE_FILE.exists(),
        "timestamp": datetime.now().isoformat()
    })

@app.route('/status')
def status():
    """Detailed status endpoint"""
    try:
        if CACHE_FILE.exists():
            with open(CACHE_FILE, 'r') as f:
                cache = json.load(f)
            return jsonify({
                "status": "ok",
                "last_update": cache.get('timestamp'),
                "assets_cached": len(cache.get('assets', {})),
                "cache_age_seconds": (datetime.now() - datetime.fromisoformat(cache['timestamp'])).total_seconds()
            })
        else:
            return jsonify({"status": "initializing", "message": "Cache not ready"}), 503
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

if __name__ == '__main__':
    # Start background updater thread
    updater_thread = threading.Thread(target=background_updater, daemon=True)
    updater_thread.start()
    
    # Run initial update before starting server
    print("🔄 Running initial cache update...")
    update_cache()
    
    # Start Flask server
    # Render uses PORT environment variable, default to 10000 for local testing
    import os
    port = int(os.environ.get('PORT', 10000))
    print(f"\n🌐 Starting Flask server on port {port}...")
    app.run(host='0.0.0.0', port=port)
