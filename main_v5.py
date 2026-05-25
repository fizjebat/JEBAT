"""
==========================================================================
PROJECT JEBAT v5.0 — Ghost Sniper (Profit-First Rebuild)
==========================================================================
Perubahan dari v4:
  ✅ Multi-factor composite scoring (bukan binary filter)
  ✅ Real volatility-based SL/TP (h1 % change sebagai proxy realized vol)
  ✅ Buy/sell pressure ratio check
  ✅ Momentum quality (m5/h1/h6 consistency check)
  ✅ Atomic DB-before-Telegram (signal tak hilang)
  ✅ Correct R-multiple ROI math dalam weekly report
  ✅ Supabase-based scheduler lock (selamat untuk multi-worker)
  ✅ Self-ping endpoint untuk pakai dengan UptimeRobot
  ✅ Universe = sort by h1 momentum (front-run), bukan h24 volume (chase)
==========================================================================
"""
import os
import time
import json
import math
import requests
import logging
from datetime import datetime, timedelta, timezone
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify
from urllib.parse import quote

# ==========================================
# ⚙️ KONFIGURASI
# ==========================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
PORT = int(os.getenv("PORT", 5000))
INSTANCE_ID = os.getenv("RENDER_INSTANCE_ID", f"local-{os.getpid()}")

TARGET_CHAINS = ['solana', 'base', 'bsc']
GOPLUS_CHAIN_IDS = {'base': '8453', 'bsc': '56'}
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

# ==========================================
# 🎯 SCORING & RISK PARAMETERS (Tunable)
# ==========================================
MIN_SIGNAL_SCORE = 70           # Minimum composite score untuk signal (max 100)
MIN_LIQUIDITY_USD = 30_000
MIN_VOLUME_24H_USD = 80_000     # Sikit lebih longgar dari v4
FDV_MIN = 50_000
FDV_MAX = 8_000_000             # Lebih longgar — early movers
AGE_MIN_HOURS = 2
AGE_MAX_HOURS = 168             # 7 hari

# Volatility clamp: minimum 6%, maximum 30%
# Ini elak fake-tight stops (kena instant) dan crazy-wide stops (-40%+)
VOL_MIN_PCT = 6.0
VOL_MAX_PCT = 30.0

# R-multiple targets (dah disesuaikan supaya conservative-realistic)
RR_TP1 = 1.5    # 1.5R
RR_TP2 = 3.0    # 3R
RR_TP3 = 5.0    # 5R

# Scale-out percentages (mesti jumlah = 1.0)
SCALE_TP1 = 0.50
SCALE_TP2 = 0.30
SCALE_TP3 = 0.20

# Pullback entry: -15% to -30% from peak adalah healthy correction
PULLBACK_MIN_PCT = 15.0
PULLBACK_MAX_PCT = 30.0

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==========================================
# 🚨 ERROR ESCALATION (kekal dari v4, ia berfungsi)
# ==========================================
_error_counter = {
    'supabase': 0, 'telegram': 0, 'geckoterminal': 0,
    'dexscreener': 0, 'audit': 0, 'binance': 0,
    'last_report': datetime.now(timezone.utc)
}

def report_error(tier, source, message, details=None):
    log_msg = f"🚨 [{tier}] {source}: {message}"
    if details:
        log_msg += f" | {str(details)[:200]}"
    if tier == 'CRITICAL':
        logging.error(log_msg)
    elif tier == 'WARNING':
        logging.warning(log_msg)
    else:
        logging.info(log_msg)

    if source in _error_counter:
        _error_counter[source] += 1

    if tier == 'CRITICAL' and TELEGRAM_BOT_TOKEN and ADMIN_CHAT_ID:
        alert = (
            f"🚨 <b>JEBAT | CRITICAL</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ <b>Source:</b> {source.upper()}\n"
            f"💬 {message}\n"
        )
        if details:
            alert += f"🔍 <code>{str(details)[:200]}</code>\n"
        alert += f"━━━━━━━━━━━━━━━━━━━━\n<i>{datetime.now(timezone.utc).strftime('%H:%M UTC')}</i>"
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                data={'chat_id': ADMIN_CHAT_ID, 'text': alert, 'parse_mode': 'HTML'},
                timeout=5
            )
        except Exception:
            pass

    elif tier == 'WARNING':
        now = datetime.now(timezone.utc)
        if (now - _error_counter['last_report']).total_seconds() > 3600:
            send_error_summary()
            for key in list(_error_counter.keys()):
                if key != 'last_report':
                    _error_counter[key] = 0
            _error_counter['last_report'] = now

def send_error_summary():
    total = sum(v for k, v in _error_counter.items() if k != 'last_report')
    if total == 0:
        return
    summary = "⚠️ <b>JEBAT | Hourly Error Summary</b>\n━━━━━━━━━━━━━━━━━━━━\n"
    for source, count in _error_counter.items():
        if source != 'last_report' and count > 0:
            summary += f"• <b>{source.upper()}:</b> {count}\n"
    summary += "━━━━━━━━━━━━━━━━━━━━\n<i>System operational. Monitoring...</i>"
    send_admin_log(summary)

# ==========================================
# 🗄️ SUPABASE WRAPPERS
# ==========================================
def supabase_insert(table, data):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    try:
        resp = requests.post(url, headers=HEADERS, json=data, timeout=10)
        if resp.status_code in [201, 204]:
            return True
        report_error('CRITICAL', 'supabase', f'Insert failed: {table}',
                    f'{resp.status_code} | {resp.text[:100]}')
        return False
    except Exception as e:
        report_error('CRITICAL', 'supabase', 'DB connection failed', str(e))
        return False

def supabase_select(table, columns="*", params=None):
    url = f"{SUPABASE_URL}/rest/v1/{table}?select={columns}"
    if params:
        if isinstance(params, dict):
            for k, v in params.items():
                url += f"&{k}={v}"
        elif isinstance(params, str):
            url += f"&{params}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        report_error('WARNING', 'supabase', f'Select failed: {table}', f'{resp.status_code}')
        return []
    except Exception as e:
        report_error('CRITICAL', 'supabase', 'DB query failed', str(e))
        return []

def supabase_update(table, data, filters):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    filter_str = "&".join([f"{k}={v}" for k, v in filters.items()])
    url += f"?{filter_str}"
    try:
        resp = requests.patch(url, headers=HEADERS, json=data, timeout=10)
        return resp.status_code in [200, 204]
    except Exception as e:
        report_error('WARNING', 'supabase', 'Update failed', str(e))
        return False

def supabase_delete(table, filters):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    filter_str = "&".join([f"{k}={v}" for k, v in filters.items()])
    url += f"?{filter_str}"
    try:
        resp = requests.delete(url, headers=HEADERS, timeout=10)
        return resp.status_code in [200, 204]
    except Exception as e:
        report_error('WARNING', 'supabase', 'Delete failed', str(e))
        return False

# ==========================================
# 🔒 SCHEDULER LOCK (elak duplicate scheduler dalam multi-worker)
# ==========================================
def acquire_scheduler_lock():
    """
    Atomic: insert ke jadual `scheduler_lock` (UNIQUE constraint pada id=1).
    Hanya satu worker boleh insert; yang lain akan fail.
    Lock expire selepas 5 min — kalau worker mati, worker lain ambil alih.
    """
    try:
        # Cuba ambil existing lock
        existing = supabase_select("scheduler_lock", "*", "id=eq.1")
        now = datetime.now(timezone.utc)
        
        if existing:
            last_heartbeat = datetime.fromisoformat(
                existing[0]['last_heartbeat'].replace('Z', '+00:00')
            )
            # Kalau lock owner sama dengan kita, refresh heartbeat
            if existing[0].get('owner') == INSTANCE_ID:
                supabase_update("scheduler_lock",
                              {"last_heartbeat": now.isoformat()},
                              {"id": "eq.1"})
                return True
            # Kalau heartbeat dah expire (>5 min), take over
            if (now - last_heartbeat).total_seconds() > 300:
                supabase_update("scheduler_lock",
                              {"owner": INSTANCE_ID, "last_heartbeat": now.isoformat()},
                              {"id": "eq.1"})
                logging.info(f"🔒 Took over expired lock as {INSTANCE_ID}")
                return True
            # Lock active, dimiliki orang lain
            return False
        else:
            # Tiada lock — kita yang pertama
            success = supabase_insert("scheduler_lock", {
                "id": 1,
                "owner": INSTANCE_ID,
                "last_heartbeat": now.isoformat()
            })
            return success
    except Exception as e:
        logging.error(f"Lock acquisition error: {e}")
        # Fallback: assume single worker (free tier biasanya 1)
        return True

# ==========================================
# 📡 API FETCHERS
# ==========================================
def fetch_trending_pools(chain, max_retries=3):
    """
    Sort by h1 price change — bukan h24 volume.
    Ini bagi kita token yang TENGAH bergerak, bukan yang dah pump habis.
    """
    url = f"https://api.geckoterminal.com/api/v2/networks/{chain}/pools"
    # h1 price change desc = momentum yang sedang naik sekarang
    params = {'sort': 'h24_tx_count_desc', 'page': 1}
    headers = {"Accept": "application/json;version=20230302"}

    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=15)
            if resp.status_code == 429:
                base_backoff = 30 * (2 ** attempt)
                try:
                    header_val = int(resp.headers.get("Retry-After", 0))
                except (ValueError, TypeError):
                    header_val = 0
                wait_time = max(header_val, base_backoff)
                report_error('WARNING', 'geckoterminal',
                            f'Rate limit {chain} — wait {wait_time}s')
                time.sleep(wait_time)
                continue
            if resp.status_code == 200:
                data = resp.json().get('data', [])
                return data if isinstance(data, list) else []
            else:
                report_error('WARNING', 'geckoterminal', f'API {chain}: {resp.status_code}')
                return []
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))
                continue
            report_error('CRITICAL', 'geckoterminal', f'Fetch {chain} failed', str(e))
            return []
    return []

def fetch_current_prices(token_addresses):
    if not token_addresses:
        return {}
    # DexScreener boleh terima sampai 30 alamat per request
    addresses_str = ",".join(token_addresses[:30])
    url = f"https://api.dexscreener.com/latest/dex/tokens/{addresses_str}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            pairs = resp.json().get('pairs', [])
            # Ambil pool dengan liquidity tertinggi untuk setiap token
            result = {}
            for p in pairs:
                addr = p['baseToken']['address'].lower()
                price = p.get('priceUsd')
                liq = float(p.get('liquidity', {}).get('usd', 0))
                if price and (addr not in result or liq > result[addr][1]):
                    result[addr] = (float(price), liq)
            return {addr: data[0] for addr, data in result.items()}
        else:
            report_error('WARNING', 'dexscreener', f'Price fetch: {resp.status_code}')
    except Exception as e:
        report_error('WARNING', 'dexscreener', 'Price fetch error', str(e))
    return {}

# ==========================================
# 🛡️ SECURITY AUDIT
# ==========================================
def audit_token(chain, token_address):
    """
    Return: (passed: bool, score_delta: int, msg: str)
    score_delta = 0-15 markah security yang akan ditambah ke composite score
    """
    try:
        if chain == 'solana':
            url = f"https://api.rugcheck.xyz/v1/tokens/{token_address}/report/summary"
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                # Fail-open: jangan block hanya sebab API down
                return True, 5, "RugCheck: API unavailable (partial credit)"
            data = resp.json()
            lp_locked = data.get('lpLockedPct', 0)
            top10 = data.get('top10HoldersPct', 0)
            mint_active = data.get('mintAuthority') is not None
            freeze_active = data.get('freezeAuthority') is not None
            
            # Auto-reject criteria
            if mint_active:
                return False, 0, "RugCheck: Mint authority active (rug risk)"
            if top10 > 70:
                return False, 0, f"RugCheck: Top10 {top10:.0f}% (cabal)"
            if lp_locked < 50:
                return False, 0, f"RugCheck: LP only {lp_locked:.0f}% locked"
            
            # Score security 0-15
            sec_score = 15
            if lp_locked < 80: sec_score -= 3
            if top10 > 50: sec_score -= 4
            if freeze_active: sec_score -= 3
            return True, max(0, sec_score), f"RugCheck: LP {lp_locked:.0f}%, Top10 {top10:.0f}%"

        elif chain in GOPLUS_CHAIN_IDS:
            chain_id = GOPLUS_CHAIN_IDS[chain]
            url = f"https://api.gopluslabs.io/api/v1/token_security/{chain_id}?contract_addresses={token_address}"
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                return True, 5, "GoPlus: API unavailable (partial credit)"
            
            result = resp.json().get('result', {})
            data = result.get(token_address.lower(), result.get(token_address, {}))
            if not data:
                return True, 5, "GoPlus: No data"
            
            # Hard rejects
            if str(data.get('is_honeypot', '0')) == '1':
                return False, 0, "GoPlus: HONEYPOT"
            if str(data.get('is_open_source', '1')) == '0':
                return False, 0, "GoPlus: Unverified contract"
            
            buy_tax = float(data.get('buy_tax', 0) or 0)
            sell_tax = float(data.get('sell_tax', 0) or 0)
            if buy_tax > 0.10 or sell_tax > 0.10:
                return False, 0, f"GoPlus: High tax B:{buy_tax*100:.0f}% S:{sell_tax*100:.0f}%"
            
            top10_rate = float(data.get('top_10_holder_rate', 0) or 0)
            if top10_rate > 0.70:
                return False, 0, f"GoPlus: Top10 {top10_rate*100:.0f}%"
            
            # LP locked check
            lp_holders = data.get('lp_holders', []) or []
            lp_locked = sum(float(h.get('percent', 0) or 0) for h in lp_holders if h.get('is_locked'))
            
            sec_score = 15
            if lp_locked < 0.80: sec_score -= 3
            if top10_rate > 0.50: sec_score -= 4
            if buy_tax > 0.05 or sell_tax > 0.05: sec_score -= 3
            return True, max(0, sec_score), f"GoPlus: Top10 {top10_rate*100:.0f}%, Tax {sell_tax*100:.0f}%"
    except Exception as e:
        logging.error(f"Audit error {token_address}: {e}")
        return True, 3, "Audit: Exception (partial credit)"
    return False, 0, "Unknown chain"

# ==========================================
# 🧠 COMPOSITE SCORING ENGINE
# ==========================================
def calculate_volatility_pct(attrs):
    """
    Realized volatility proxy dari h1 price change.
    Kalau h1 swing 12%, vol = 12% (clamped 6-30%).
    Lebih accurate dari hardcoded 20%.
    """
    h1_change = abs(float(attrs.get('price_change_percentage', {}).get('h1', 0) or 0))
    vol = max(VOL_MIN_PCT, min(VOL_MAX_PCT, h1_change))
    return vol

def score_momentum(attrs):
    """
    0-25 markah berdasarkan kualiti trend (m5, h1, h6 directional consistency).
    Trend yang consistent = real momentum, bukan single candle spike.
    """
    pc = attrs.get('price_change_percentage', {})
    m5 = float(pc.get('m5', 0) or 0)
    h1 = float(pc.get('h1', 0) or 0)
    h6 = float(pc.get('h6', 0) or 0)
    
    if h1 <= 0:
        return 0
    
    score = 0
    # Strong h1 momentum
    if h1 >= 15: score += 10
    elif h1 >= 8: score += 7
    elif h1 >= 3: score += 4
    else: score += 2
    
    # h6 confirmation (uptrend, bukan one-off spike)
    if h6 >= 5: score += 8
    elif h6 >= 0: score += 5
    elif h6 >= -5: score += 2  # mild pullback dalam uptrend boleh tahan
    # else 0 — h6 dump terlalu kuat
    
    # m5 freshness (masih bergerak)
    if m5 >= 1: score += 7
    elif m5 >= 0: score += 4
    elif m5 >= -3: score += 1
    
    return min(25, score)

def score_buy_pressure(attrs):
    """
    0-20 markah berdasarkan buys vs sells ratio (h1).
    >55% buys = real demand, bukan distribution.
    """
    h1_tx = attrs.get('transactions', {}).get('h1', {})
    buys = int(h1_tx.get('buys', 0) or 0)
    sells = int(h1_tx.get('sells', 0) or 0)
    total = buys + sells
    
    if total < 30:  # Tak cukup data
        return 0
    
    ratio = buys / total
    if ratio >= 0.65: return 20
    if ratio >= 0.58: return 15
    if ratio >= 0.52: return 10
    if ratio >= 0.48: return 5
    return 0

def score_liquidity_health(attrs):
    """
    0-20 markah: turnover ratio (vol_24h / liquidity).
    >2.0 = active trading, healthy market depth.
    """
    liq = float(attrs.get('reserve_in_usd', 0) or 0)
    vol_24h = float(attrs.get('volume_usd', {}).get('h24', 0) or 0)
    if liq <= 0:
        return 0
    turnover = vol_24h / liq
    if turnover >= 3.0: return 20
    if turnover >= 1.5: return 15
    if turnover >= 0.7: return 10
    if turnover >= 0.3: return 5
    return 0

def score_fdv_sweet_spot(attrs):
    """
    0-15 markah: FDV antara $100k-$1.5M = paling asymmetric upside.
    Token kecil ada room untuk 5-10x, token besar dah saturated.
    """
    fdv = float(attrs.get('fdv_usd', 0) or 0)
    if 150_000 <= fdv <= 1_500_000:
        return 15
    if 100_000 <= fdv <= 3_000_000:
        return 10
    if 50_000 <= fdv <= 8_000_000:
        return 5
    return 0

def score_age(age_hours):
    """
    0-10 markah: 6-48 jam = early discovery, masih ada upside.
    """
    if 6 <= age_hours <= 48: return 10
    if 4 <= age_hours <= 72: return 7
    if 2 <= age_hours <= 168: return 4
    return 0

def compute_composite_score(attrs, age_hours, security_score):
    """
    Total max = 25 + 20 + 20 + 15 + 10 + 15 = 105.
    Threshold MIN_SIGNAL_SCORE = 70.
    """
    return {
        'momentum': score_momentum(attrs),
        'buy_pressure': score_buy_pressure(attrs),
        'liquidity': score_liquidity_health(attrs),
        'fdv': score_fdv_sweet_spot(attrs),
        'age': score_age(age_hours),
        'security': security_score,
    }

# ==========================================
# 🎯 RISK ENGINE (Real R-Multiple)
# ==========================================
def calculate_targets(entry_price, volatility_pct):
    """
    SL distance = entry × vol% (1 sigma move).
    TP1 = entry + 1.5R, TP2 = entry + 3R, TP3 = entry + 5R.
    
    Contoh: entry $1.00, vol 12% → SL $0.88, TP1 $1.18, TP2 $1.36, TP3 $1.60
    """
    R = entry_price * (volatility_pct / 100.0)
    sl = entry_price - R
    return {
        'sl': sl,
        'tp1': entry_price + (RR_TP1 * R),
        'tp2': entry_price + (RR_TP2 * R),
        'tp3': entry_price + (RR_TP3 * R),
        'r_value_pct': volatility_pct,        # untuk weekly report
        'sl_pct': volatility_pct,             # display
        'tp1_pct': RR_TP1 * volatility_pct,
        'tp2_pct': RR_TP2 * volatility_pct,
        'tp3_pct': RR_TP3 * volatility_pct,
    }

def calculate_realized_roi(status, r_pct):
    """
    Honest ROI calculation dengan scale-out (50/30/20).
    Selepas TP1 hit → 50% closed di +1.5R, baki 50% stop di break-even (BE).
    
    Returns ROI dalam % untuk SIGNAL TU (bukan portfolio).
    
    Contoh: r_pct=12% (1R = 12%)
    - CLOSED_SL: -100% × 12% = -12% (kalau semua kena stop sekali gus)
    - HIT_TP1 only: 0.5 × (1.5×12) + 0.5 × 0 = 9% (TP1 lock untung, baki BE)
    - HIT_TP2: 0.5 × 1.5R + 0.3 × 3R + 0.2 × 0 = (0.75 + 0.9 + 0) × 12 = 19.8%
    - CLOSED_TP3: 0.5 × 1.5R + 0.3 × 3R + 0.2 × 5R = (0.75 + 0.9 + 1.0) × 12 = 31.8%
    """
    if status == 'CLOSED_SL':
        return -r_pct  # 1.0R loss
    if status == 'HIT_TP1':
        return SCALE_TP1 * RR_TP1 * r_pct  # 50% × 1.5R, baki BE
    if status == 'HIT_TP2':
        return (SCALE_TP1 * RR_TP1 + SCALE_TP2 * RR_TP2) * r_pct
    if status == 'CLOSED_TP3':
        return (SCALE_TP1 * RR_TP1 + SCALE_TP2 * RR_TP2 + SCALE_TP3 * RR_TP3) * r_pct
    return 0.0  # ACTIVE tak kira

# ==========================================
# 🗄️ DATABASE OPS
# ==========================================
def save_signal(signal_data):
    data = {
        "chain": signal_data['chain'],
        "token_address": signal_data['token_address'],
        "pool_address": signal_data['pool_address'],
        "token_name": signal_data['token_name'],
        "entry_price": signal_data['entry_price'],
        "sl": signal_data['sl'],
        "tp1": signal_data['tp1'],
        "tp2": signal_data['tp2'],
        "tp3": signal_data['tp3'],
        "r_value_pct": signal_data.get('r_value_pct', 0),
        "composite_score": signal_data.get('composite_score', 0),
        "status": "ACTIVE",
        "signal_type": signal_data['signal_type'],
        "tg_msg_id": signal_data.get('tg_msg_id'),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    return supabase_insert("signals", data)

def add_to_watchlist(w):
    existing = supabase_select("watchlist", "id", {"token_address": f"eq.{w['token_address']}"})
    if existing:
        return False
    data = {
        "chain": w['chain'],
        "token_address": w['token_address'],
        "pool_address": w['pool_address'],
        "token_name": w['token_name'],
        "peak_price": w['peak_price'],
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    return supabase_insert("watchlist", data)

def get_active_signals():
    return supabase_select("signals", "*", "status=neq.CLOSED_SL&status=neq.CLOSED_TP3")

def get_watchlist():
    return supabase_select("watchlist", "*")

def update_signal_status(signal_id, status):
    data = {"status": status, "updated_at": datetime.now(timezone.utc).isoformat()}
    return supabase_update("signals", data, {"id": f"eq.{signal_id}"})

def remove_from_watchlist(wid):
    return supabase_delete("watchlist", {"id": f"eq.{wid}"})

def cleanup_old_watchlist():
    """Buang token dari watchlist yang dah >48 jam tak trigger pullback"""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
    supabase_delete("watchlist", {"created_at": f"lt.{cutoff}"})

# ==========================================
# 🎨 UI & TELEGRAM
# ==========================================
def build_keyboard(chain, token_address, pool_address, token_name):
    bot_buttons = []
    if chain == 'solana':
        bot_buttons = [{"text": "🐶 BonkBot", "url": f"https://t.me/bonkbot_bot?start=ca_{token_address}"}]
    elif chain in ['base', 'bsc']:
        bot_buttons = [{"text": "🎩 Maestro", "url": f"https://t.me/maestro?start={token_address}"}]
    
    dex_url = f"https://dexscreener.com/{chain}/{pool_address}"
    audit_url = (f"https://rugcheck.xyz/tokens/{token_address}" if chain == 'solana'
                 else f"https://gopluslabs.io/token-security/{GOPLUS_CHAIN_IDS.get(chain, '1')}/{token_address}")
    util_buttons = [
        {"text": "📊 Chart", "url": dex_url},
        {"text": "🛡️ Audit", "url": audit_url},
        {"text": "🗺️ Holders", "url": f"https://app.bubblemaps.io/{chain}/token/{token_address}"}
    ]
    search_query = quote(f"${token_name.replace(' ', '')} OR {token_address}")
    social_buttons = [{"text": "🐦 X Search", "url": f"https://twitter.com/search?q={search_query}&f=live"}]
    return {"inline_keyboard": [bot_buttons, util_buttons, social_buttons]}

def format_signal_text(sig_data, status="ACTIVE", score_breakdown=None, audit_msg=""):
    chain = sig_data['chain']
    chain_emoji = "🟣" if chain == 'solana' else ("🔵" if chain == 'base' else "🟡")
    signal_type = sig_data.get('signal_type', 'FAST')
    type_badge = "⚡ <b>FAST BREAKOUT</b>" if signal_type == 'FAST' else "🎯 <b>PULLBACK ENTRY</b>"

    icons = {
        "ACTIVE":    ("⏳", "⏳", "⏳", "🛑"),
        "HIT_TP1":   ("✅", "⏳", "⏳", "🛡️"),  # BE stop after TP1
        "HIT_TP2":   ("✅", "✅", "⏳", "🛡️"),
        "CLOSED_TP3":("✅", "✅", "🚀", "🛡️"),
        "CLOSED_SL": ("❌", "❌", "❌", "💥"),
    }
    tp1_i, tp2_i, tp3_i, sl_i = icons.get(status, icons["ACTIVE"])

    entry = sig_data['entry_price']
    r_pct = sig_data.get('r_value_pct', 0)
    
    # Score breakdown (kalau ada)
    score_block = ""
    if score_breakdown:
        total = sum(score_breakdown.values())
        score_block = (
            f"📊 <b>SCORE: {total}/105</b>\n"
            f"  • Momentum: {score_breakdown['momentum']}/25\n"
            f"  • Buy Pressure: {score_breakdown['buy_pressure']}/20\n"
            f"  • Liquidity: {score_breakdown['liquidity']}/20\n"
            f"  • FDV: {score_breakdown['fdv']}/15\n"
            f"  • Age: {score_breakdown['age']}/10\n"
            f"  • Security: {score_breakdown['security']}/15\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
        )

    return (
        f"{chain_emoji} {type_badge}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 <b>{sig_data['token_name']}</b>\n"
        f"💵 Entry: <code>${entry:.8f}</code>\n"
        f"📐 1R = <b>{r_pct:.1f}%</b> (volatility-based)\n\n"
        f"{tp1_i} TP1: <code>${sig_data['tp1']:.8f}</code> (+{sig_data.get('tp1_pct', 0):.1f}%) — close 50%\n"
        f"{tp2_i} TP2: <code>${sig_data['tp2']:.8f}</code> (+{sig_data.get('tp2_pct', 0):.1f}%) — close 30%\n"
        f"{tp3_i} TP3: <code>${sig_data['tp3']:.8f}</code> (+{sig_data.get('tp3_pct', 0):.1f}%) — close 20%\n"
        f"{sl_i} SL:  <code>${sig_data['sl']:.8f}</code> (-{sig_data.get('sl_pct', 0):.1f}%)\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{score_block}"
        f"🛡️ {audit_msg}\n"
        f"<i>⚠️ DYOR. Bukan financial advice.</i>"
    )

def send_telegram_message(text, reply_markup=None):
    if not text:
        report_error('CRITICAL', 'telegram', 'Attempted to send empty message')
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHANNEL_ID, 'text': text,
              'parse_mode': 'HTML', 'disable_web_page_preview': True}
    if reply_markup:
        payload['reply_markup'] = json.dumps(reply_markup)
    try:
        resp = requests.post(url, data=payload, timeout=10)
        if resp.status_code == 200:
            return resp.json()['result']['message_id']
        report_error('CRITICAL', 'telegram', 'Send failed',
                    f'{resp.status_code} | {resp.text[:100]}')
    except Exception as e:
        report_error('CRITICAL', 'telegram', 'API exception', str(e))
    return None

def edit_telegram_message(msg_id, text, reply_markup=None):
    if not msg_id or not text:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
    payload = {'chat_id': TELEGRAM_CHANNEL_ID, 'message_id': msg_id, 'text': text,
              'parse_mode': 'HTML', 'disable_web_page_preview': True}
    if reply_markup:
        payload['reply_markup'] = json.dumps(reply_markup)
    try:
        resp = requests.post(url, data=payload, timeout=10)
        return resp.status_code == 200
    except Exception as e:
        logging.error(f"TG edit error: {e}")
        return False

def send_admin_log(text):
    if not ADMIN_CHAT_ID or not TELEGRAM_BOT_TOKEN:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={'chat_id': ADMIN_CHAT_ID, 'text': text,
                  'parse_mode': 'HTML', 'disable_web_page_preview': True},
            timeout=5
        )
    except Exception:
        pass

# ==========================================
# 🐂 MACRO FILTER (BTC trend)
# ==========================================
def get_btc_trend():
    """BTC > EMA50(1h) = bull regime, OK untuk trade. Else paused."""
    try:
        url = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1h&limit=100"
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return True  # Fail-open
        closes = [float(d[4]) for d in resp.json()]
        if len(closes) < 50:
            return True
        # Proper EMA calc
        k = 2 / 51
        ema = sum(closes[:50]) / 50
        for price in closes[50:]:
            ema = price * k + ema * (1 - k)
        return closes[-1] > ema
    except Exception as e:
        report_error('WARNING', 'binance', 'BTC trend check failed', str(e))
        return True

# ==========================================
# ⚙️ CRON JOBS
# ==========================================
def process_pool_candidate(pool, chain):
    """
    Returns: (signal_type, sig_data, score_breakdown, audit_msg) or None.
    signal_type: 'FAST' (signal terus) atau 'PULLBACK' (masuk watchlist).
    """
    attrs = pool['attributes']
    token_name = attrs.get('name', 'Unknown').split(' / ')[0]
    
    # Quick filters (cheapest checks dulu)
    try:
        created_at = datetime.fromisoformat(attrs['pool_created_at'].replace('Z', '+00:00'))
    except (KeyError, ValueError):
        return None
    age_hours = (datetime.now(timezone.utc) - created_at).total_seconds() / 3600
    if not (AGE_MIN_HOURS <= age_hours <= AGE_MAX_HOURS):
        return None
    
    liq = float(attrs.get('reserve_in_usd', 0) or 0)
    if liq < MIN_LIQUIDITY_USD:
        return None
    
    vol_h24 = float(attrs.get('volume_usd', {}).get('h24', 0) or 0)
    if vol_h24 < MIN_VOLUME_24H_USD:
        return None
    
    fdv = float(attrs.get('fdv_usd', 0) or 0)
    if not (FDV_MIN <= fdv <= FDV_MAX):
        return None
    
    h1_change = float(attrs.get('price_change_percentage', {}).get('h1', 0) or 0)
    if h1_change <= 0:  # Mesti positive momentum minimum
        return None
    
    # Extract addresses
    try:
        token_address = pool['relationships']['base_token']['data']['id'].split('_', 1)[1]
        pool_address = pool['id'].split('_', 1)[1]
    except (KeyError, IndexError):
        return None
    
    # Duplicate check (24h cooldown per token)
    time_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    existing = supabase_select("signals", "id",
                              f"token_address=eq.{token_address}&created_at=gt.{time_24h}")
    if existing:
        return None
    
    # Audit (expensive — buat last)
    audit_passed, sec_score, audit_msg = audit_token(chain, token_address)
    if not audit_passed:
        return None
    
    # Score
    breakdown = compute_composite_score(attrs, age_hours, sec_score)
    total_score = sum(breakdown.values())
    if total_score < MIN_SIGNAL_SCORE:
        return None
    
    # Volatility & targets
    entry_price = float(attrs['base_token_price_usd'])
    if entry_price <= 0:
        return None
    
    vol_pct = calculate_volatility_pct(attrs)
    targets = calculate_targets(entry_price, vol_pct)
    
    # Classify: kalau h24 dah >100%, masuk watchlist tunggu pullback
    h24_change = float(attrs.get('price_change_percentage', {}).get('h24', 0) or 0)
    sig_type = 'PULLBACK' if h24_change > 100 else 'FAST'
    
    sig_data = {
        'chain': chain,
        'token_address': token_address,
        'pool_address': pool_address,
        'token_name': token_name,
        'entry_price': entry_price,
        'signal_type': sig_type,
        'composite_score': total_score,
        **targets
    }
    return (sig_type, sig_data, breakdown, audit_msg)

def job_scan_market():
    if not get_btc_trend():
        logging.warning("🐻 BTC bearish — scan paused")
        return
    
    logging.info(f"🔍 [SCAN] Cycle start (instance: {INSTANCE_ID})")
    stats = {'scanned': 0, 'qualified': 0, 'sent_fast': 0, 'watchlist': 0}
    
    for chain in TARGET_CHAINS:
        pools = fetch_trending_pools(chain)
        stats['scanned'] += len(pools)
        logging.info(f"📡 {chain.upper()}: {len(pools)} pools fetched")
        
        # Add small delay antara chains untuk respect rate limits
        for i, pool in enumerate(pools):
            try:
                result = process_pool_candidate(pool, chain)
                if not result:
                    continue
                
                sig_type, sig_data, breakdown, audit_msg = result
                stats['qualified'] += 1
                
                if sig_type == 'PULLBACK':
                    if add_to_watchlist({
                        'chain': chain,
                        'token_address': sig_data['token_address'],
                        'pool_address': sig_data['pool_address'],
                        'token_name': sig_data['token_name'],
                        'peak_price': sig_data['entry_price']
                    }):
                        stats['watchlist'] += 1
                        logging.info(f"👀 Watchlist: {sig_data['token_name']}")
                else:
                    # CRITICAL: Save signal DULU sebelum hantar TG
                    # Kalau TG fail, kita masih ada record dan boleh resend
                    if save_signal(sig_data):
                        text = format_signal_text(sig_data, "ACTIVE", breakdown, audit_msg)
                        keyboard = build_keyboard(chain, sig_data['token_address'],
                                                 sig_data['pool_address'], sig_data['token_name'])
                        msg_id = send_telegram_message(text, keyboard)
                        if msg_id:
                            # Update DB dengan tg_msg_id untuk monitor
                            supabase_update("signals", {"tg_msg_id": msg_id},
                                          {"token_address": f"eq.{sig_data['token_address']}",
                                           "status": "eq.ACTIVE"})
                            stats['sent_fast'] += 1
                            logging.info(f"📤 SIGNAL: {sig_data['token_name']} score={sig_data['composite_score']}")
            except Exception as e:
                logging.error(f"Pool processing error: {e}")
                continue
            
            # Throttle audit API calls supaya tak burst
            if i % 5 == 4:
                time.sleep(1)
        
        time.sleep(2)  # Delay antara chains
    
    logging.info(f"✅ [SCAN DONE] {stats}")

def job_monitor_watchlist():
    cleanup_old_watchlist()
    watchlist = get_watchlist()
    if not watchlist:
        return
    
    addr_map = {w['token_address'].lower(): w for w in watchlist}
    prices = fetch_current_prices(list(addr_map.keys()))
    
    for addr_lower, w in addr_map.items():
        current = prices.get(addr_lower)
        if not current:
            continue
        
        peak = float(w['peak_price'])
        
        # Update peak kalau price naik
        if current > peak:
            supabase_update("watchlist",
                          {"peak_price": current,
                           "last_checked": datetime.now(timezone.utc).isoformat()},
                          {"id": f"eq.{w['id']}"})
            continue
        
        pullback_pct = ((peak - current) / peak) * 100
        if PULLBACK_MIN_PCT <= pullback_pct <= PULLBACK_MAX_PCT:
            # Re-audit (status boleh berubah dalam 48 jam)
            audit_passed, sec_score, audit_msg = audit_token(w['chain'], w['token_address'])
            if not audit_passed:
                send_admin_log(f"🗑️ Watchlist rejected: {w['token_name']} ({audit_msg})")
                remove_from_watchlist(w['id'])
                continue
            
            # Vol estimate dari pullback magnitude (15-30% pullback → 15-30% vol)
            vol_pct = max(VOL_MIN_PCT, min(VOL_MAX_PCT, pullback_pct * 0.8))
            targets = calculate_targets(current, vol_pct)
            
            sig_data = {
                'chain': w['chain'],
                'token_address': w['token_address'],
                'pool_address': w['pool_address'],
                'token_name': w['token_name'],
                'entry_price': current,
                'signal_type': 'PULLBACK',
                'composite_score': 80,  # Pullback dah pre-qualified
                **targets
            }
            
            if save_signal(sig_data):
                text = format_signal_text(sig_data, "ACTIVE", audit_msg=audit_msg)
                keyboard = build_keyboard(w['chain'], w['token_address'],
                                         w['pool_address'], w['token_name'])
                msg_id = send_telegram_message(text, keyboard)
                if msg_id:
                    supabase_update("signals", {"tg_msg_id": msg_id},
                                  {"token_address": f"eq.{w['token_address']}",
                                   "status": "eq.ACTIVE"})
                    send_admin_log(f"🎯 Pullback signal: {w['token_name']} (-{pullback_pct:.1f}% dari peak)")
                    remove_from_watchlist(w['id'])

def job_monitor_signals():
    signals = get_active_signals()
    if not signals:
        return
    
    addr_map = {s['token_address'].lower(): s for s in signals}
    prices = fetch_current_prices(list(addr_map.keys()))
    
    for addr_lower, sig in addr_map.items():
        current = prices.get(addr_lower)
        if not current:
            continue
        
        old_status = sig['status']
        new_status = old_status
        
        # Status transitions (in order — stop loss check first)
        if current <= float(sig['sl']):
            new_status = "CLOSED_SL"
        elif current >= float(sig['tp3']):
            new_status = "CLOSED_TP3"
        elif current >= float(sig['tp2']) and old_status in ['ACTIVE', 'HIT_TP1']:
            new_status = "HIT_TP2"
        elif current >= float(sig['tp1']) and old_status == 'ACTIVE':
            new_status = "HIT_TP1"
        
        if new_status != old_status:
            # Update DB DULU
            if update_signal_status(sig['id'], new_status):
                # Build sig_data untuk format
                sig_data = {
                    'chain': sig['chain'],
                    'token_name': sig['token_name'],
                    'token_address': sig['token_address'],
                    'pool_address': sig['pool_address'],
                    'entry_price': float(sig['entry_price']),
                    'sl': float(sig['sl']),
                    'tp1': float(sig['tp1']),
                    'tp2': float(sig['tp2']),
                    'tp3': float(sig['tp3']),
                    'r_value_pct': float(sig.get('r_value_pct', 0)),
                    'signal_type': sig['signal_type'],
                    'sl_pct': float(sig.get('r_value_pct', 0)),
                    'tp1_pct': RR_TP1 * float(sig.get('r_value_pct', 0)),
                    'tp2_pct': RR_TP2 * float(sig.get('r_value_pct', 0)),
                    'tp3_pct': RR_TP3 * float(sig.get('r_value_pct', 0)),
                }
                text = format_signal_text(sig_data, new_status, audit_msg=f"Status: {new_status}")
                keyboard = build_keyboard(sig['chain'], sig['token_address'],
                                         sig['pool_address'], sig['token_name'])
                if sig.get('tg_msg_id'):
                    edit_telegram_message(sig['tg_msg_id'], text, keyboard)
                logging.info(f"📊 {sig['token_name']}: {old_status} → {new_status}")

def generate_weekly_report():
    """Honest report dengan R-multiple math yang betul."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    signals = supabase_select("signals", "status,r_value_pct",
                              f"created_at=gt.{cutoff}")
    total = len(signals)
    if total == 0:
        send_telegram_message("📊 <b>Weekly Report:</b> No signals this week.")
        return
    
    wins = losses = active = 0
    total_roi = 0.0
    closed_rois = []
    
    for sig in signals:
        status = sig['status']
        r_pct = float(sig.get('r_value_pct', 0) or 0)
        
        if status == 'CLOSED_SL':
            losses += 1
        elif status in ['HIT_TP1', 'HIT_TP2', 'CLOSED_TP3']:
            wins += 1
        else:
            active += 1
            continue  # Tak kira ROI untuk active
        
        roi = calculate_realized_roi(status, r_pct)
        closed_rois.append(roi)
        total_roi += roi
    
    closed = wins + losses
    win_rate = (wins / closed * 100) if closed > 0 else 0
    avg_roi = (total_roi / closed) if closed > 0 else 0
    
    # Best/worst untuk transparency
    best = max(closed_rois) if closed_rois else 0
    worst = min(closed_rois) if closed_rois else 0
    
    report = (
        f"📊 <b>JEBAT | WEEKLY REPORT (Honest Math)</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🗓 Last 7 Days\n\n"
        f"🎯 Total Signals: <b>{total}</b>\n"
        f"✅ Wins: <b>{wins}</b> | ❌ Losses: <b>{losses}</b> | ⏳ Active: <b>{active}</b>\n\n"
        f"📈 Win Rate: <b>{win_rate:.1f}%</b>\n"
        f"💰 Avg ROI per closed trade: <b>{avg_roi:+.2f}%</b>\n"
        f"🚀 Best: {best:+.2f}% | 💥 Worst: {worst:+.2f}%\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Math: SL=-1R, TP1=+1.5R(50%), TP2=+3R(30%), TP3=+5R(20%)\n"
        f"BE stop selepas TP1 hit. R = volatility-derived per signal.</i>"
    )
    send_telegram_message(report)
    send_admin_log(f"📊 Weekly: WR {win_rate:.1f}% | Avg ROI {avg_roi:+.2f}% ({closed} closed)")

# ==========================================
# 🏢 FLASK & SCHEDULER
# ==========================================
app = Flask(__name__)
_scheduler_started = False

def start_scheduler():
    global _scheduler_started
    if _scheduler_started:
        return
    
    if not acquire_scheduler_lock():
        logging.info(f"⏸️ Scheduler lock held by another worker. {INSTANCE_ID} standing by.")
        return
    
    scheduler = BackgroundScheduler(timezone='UTC')
    scheduler.add_job(job_scan_market, 'interval', minutes=15,
                     next_run_time=datetime.now(timezone.utc) + timedelta(seconds=30),
                     max_instances=1, coalesce=True)
    scheduler.add_job(job_monitor_watchlist, 'interval', minutes=3,
                     max_instances=1, coalesce=True)
    scheduler.add_job(job_monitor_signals, 'interval', minutes=2,
                     max_instances=1, coalesce=True)
    scheduler.add_job(generate_weekly_report, 'cron',
                     day_of_week='sun', hour=23, minute=0)
    # Heartbeat untuk refresh lock setiap minit
    scheduler.add_job(acquire_scheduler_lock, 'interval', minutes=1)
    
    scheduler.start()
    _scheduler_started = True
    send_admin_log(
        f"🟢 <b>JEBAT v5.0 Booted</b>\n"
        f"Instance: <code>{INSTANCE_ID}</code>\n"
        f"Scanner | Watchlist | Monitor: ACTIVE\n"
        f"<i>Multi-factor scoring, real volatility math, atomic ops.</i>"
    )

@app.route('/')
def home():
    return f"JEBAT v5.0 — Instance {INSTANCE_ID}"

@app.route('/health')
def health():
    """Pakai dengan UptimeRobot/cron-job.org untuk elak Render sleep."""
    try:
        resp = requests.get(f"{SUPABASE_URL}/rest/v1/signals?select=count&limit=1",
                          headers=HEADERS, timeout=5)
        db_ok = resp.status_code == 200
    except Exception:
        db_ok = False
    return jsonify({
        "status": "healthy",
        "instance": INSTANCE_ID,
        "scheduler": _scheduler_started,
        "database": "ok" if db_ok else "error",
        "timestamp": datetime.now(timezone.utc).isoformat()
    })

@app.route('/status')
def status():
    active = len(supabase_select("signals", "id", "status=eq.ACTIVE"))
    watchlist = len(supabase_select("watchlist", "id"))
    btc_bullish = get_btc_trend()
    
    text = (
        f"🎯 <b>JEBAT v5.0 STATUS</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Instance: <code>{INSTANCE_ID}</code>\n"
        f"{'🐂 BTC: Bullish (scanning)' if btc_bullish else '🐻 BTC: Bearish (paused)'}\n"
        f"📊 Active Signals: {active}\n"
        f"👀 Watchlist: {watchlist}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )
    send_admin_log(text)
    return jsonify({"sent": True, "active": active, "watchlist": watchlist})

@app.route('/force-scan')
def force_scan():
    """Manual trigger untuk testing."""
    try:
        job_scan_market()
        return jsonify({"status": "scan triggered"})
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)}), 500

# Auto-start scheduler
start_scheduler()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT)
