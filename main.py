import os
import time
import json
import requests
import logging
from datetime import datetime, timedelta, timezone
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify
from urllib.parse import quote

# ==========================================
# ⚙️ KONFIGURASI MARKAS
# ==========================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
SUPABASE_URL = os.getenv("SUPABASE_URL")  # Contoh: https://xyzabc.supabase.co
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # anon/public key
PORT = int(os.getenv("PORT", 5000))

TARGET_CHAINS = ['solana', 'base', 'bsc']
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"  # For inserts, don't return full row
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==========================================
# 🗄️ SUPABASE REST API WRAPPERS
# ==========================================
def supabase_insert(table, data):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    try:
        resp = requests.post(url, headers=HEADERS, json=data, timeout=10)
        return resp.status_code in [201, 204]
    except Exception as e:
        logging.error(f"Supabase insert error: {e}")
        return False

def supabase_select(table, columns="*", params=None):
    url = f"{SUPABASE_URL}/rest/v1/{table}?select={columns}"
    if params:
        # Handle both dict and string params
        if isinstance(params, dict):
            for k, v in params.items():
                url += f"&{k}={v}"
        elif isinstance(params, str):
            # If params is already a query string, append directly
            url += f"&{params}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        return []
    except Exception as e:
        logging.error(f"Supabase select error: {e}")
        return []

def supabase_update(table, data, filters):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    filter_str = "&".join([f"{k}=eq.{v}" for k, v in filters.items()])
    url += f"?{filter_str}"
    try:
        resp = requests.patch(url, headers=HEADERS, json=data, timeout=10)
        return resp.status_code in [200, 204]
    except Exception as e:
        logging.error(f"Supabase update error: {e}")
        return False

def supabase_delete(table, filters):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    filter_str = "&".join([f"{k}=eq.{v}" for k, v in filters.items()])
    url += f"?{filter_str}"
    try:
        resp = requests.delete(url, headers=HEADERS, timeout=10)
        return resp.status_code in [200, 204]
    except Exception as e:
        logging.error(f"Supabase delete error: {e}")
        return False

# ==========================================
# 📡 PERISIKAN (API CALLS)
# ==========================================
def fetch_new_pools(chain):
    url = f"https://api.geckoterminal.com/api/v2/networks/{chain}/new_pools"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            return resp.json().get('data', [])
    except Exception as e:
        send_admin_log(f"⚠️ <b>API Error:</b> Failed to fetch {chain}. {e}")
    return []

def fetch_current_prices(token_addresses):
    if not token_addresses: return {}
    addresses_str = ",".join(token_addresses[:30])
    url = f"https://api.dexscreener.com/latest/dex/tokens/{addresses_str}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            pairs = resp.json().get('pairs', [])
            return {p['baseToken']['address']: float(p['priceUsd']) for p in pairs if p.get('priceUsd')}
    except Exception as e:
        logging.error(f"Error fetching prices: {e}")
    return {}

def auto_audit_token(chain, token_address):
    """Auto-check RugCheck (SOL) or TokenSniffer (Base/BSC)"""
    try:
        if chain == 'solana':
            url = f"https://api.rugcheck.xyz/v1/tokens/{token_address}/report"
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                risks = data.get('risks', [])
                critical_risks = [r for r in risks if r.get('level') in ['danger', 'warn']]
                if len(critical_risks) > 2:
                    return False, f"RugCheck: {len(critical_risks)} risks"
                return True, "RugCheck: PASSED"
        else:
            url = f"https://tokensniffer.com/api/v2/tokens/{chain}/{token_address}"
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                score = data.get('score', 0)
                if score < 70:
                    return False, f"TokenSniffer: Score {score}/100"
                return True, f"TokenSniffer: Score {score}/100"
    except Exception as e:
        logging.error(f"Audit error for {token_address}: {e}")
        return False, f"Audit API Error: {e}"
    return False, "Audit: Unknown chain"

# ==========================================
# 🧠 ENJIN TAPIOKAN (FILTER & LOGIC)
# ==========================================
def is_valid_basic_filter(pool, chain):
    attrs = pool['attributes']
    created_at = datetime.fromisoformat(attrs['pool_created_at'].replace('Z', '+00:00'))
    age_hours = (datetime.now(timezone.utc) - created_at).total_seconds() / 3600
    if not (2 <= age_hours <= 168): return False
    liq = float(attrs.get('reserve_in_usd', 0))
    vol_h24 = float(attrs.get('volume_usd', {}).get('h24', 0))
    if liq < 30000 or vol_h24 < 100000: return False
    fdv = float(attrs.get('fdv_usd', 0))
    if not (50000 <= fdv <= 5000000): return False
    h1_change = float(attrs.get('price_change_percentage', {}).get('h1', 0))
    if h1_change <= 0: return False
    h1_buys = int(attrs.get('transactions', {}).get('h1', {}).get('buys', 0))
    if h1_buys < 50: return False
    return True

def classify_signal_type(pool):
    attrs = pool['attributes']
    h24_change = float(attrs.get('price_change_percentage', {}).get('h24', 0))
    return 'FAST' if h24_change < 80 else 'PULLBACK'

def calculate_targets(entry_price):
    return {
        'sl': entry_price * 0.85,
        'tp1': entry_price * 1.30,
        'tp2': entry_price * 1.80,
        'tp3': entry_price * 3.00
    }

# ==========================================
# 🗄️ DATABASE OPERATIONS (Via REST API)
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
        "status": "ACTIVE",
        "signal_type": signal_data['signal_type'],
        "tg_msg_id": signal_data['tg_msg_id'],
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    return supabase_insert("signals", data)

def add_to_watchlist(watchlist_data):
    # Check if already exists
    existing = supabase_select("watchlist", "id", {"token_address": f"eq.{watchlist_data['token_address']}"})
    if existing:
        return False
    data = {
        "chain": watchlist_data['chain'],
        "token_address": watchlist_data['token_address'],
        "pool_address": watchlist_data['pool_address'],
        "token_name": watchlist_data['token_name'],
        "peak_price": watchlist_data['peak_price'],
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

def remove_from_watchlist(watchlist_id):
    return supabase_delete("watchlist", {"id": f"eq.{watchlist_id}"})

# ==========================================
# 🎨 PREMIUM UI (INLINE KEYBOARDS & TEXT)
# ==========================================
def build_keyboard(chain, token_address, pool_address, token_name, audit_status):
    bot_buttons = []
    if chain == 'solana':
        bot_buttons = [{"text": "🐶 BonkBot", "url": f"https://t.me/bonkbot_bot?start=ca_{token_address}"}]
    elif chain in ['base', 'bsc']:
        bot_buttons = [{"text": "🎩 Maestro", "url": f"https://t.me/maestro?start={token_address}"}]
    dex_url = f"https://dexscreener.com/{chain}/{pool_address}"
    audit_url = f"https://rugcheck.xyz/tokens/{token_address}" if chain == 'solana' else f"https://tokensniffer.com/token/{chain}/{token_address}"
    util_buttons = [
        {"text": "📊 Chart", "url": dex_url},
        {"text": "🛡️ Full Audit", "url": audit_url},
        {"text": "🗺️ Bubblemaps", "url": f"https://app.bubblemaps.io/{chain}/token/{token_address}"}
    ]
    search_query = quote(f"${token_name.replace(' ', '')} OR {token_address}")
    twitter_search_url = f"https://twitter.com/search?q={search_query}&src=typed_query&f=live"
    social_buttons = [{"text": "🐦 X-Ray Twitter", "url": twitter_search_url}]
    return {"inline_keyboard": [bot_buttons, util_buttons, social_buttons]}

def format_signal_text(sig_data, status="ACTIVE", audit_status=""):
    chain = sig_data['chain']
    chain_emoji = "🟣" if chain == 'solana' else ("🔵" if chain == 'base' else "🟡")
    signal_type = sig_data.get('signal_type', 'FAST')
    type_badge = "⚡ <b>FAST BREAKOUT</b>" if signal_type == 'FAST' else "🎯 <b>PULLBACK ENTRY</b>"
    
    if status == "ACTIVE": tp1_icon, tp2_icon, tp3_icon, sl_icon = "⏳", "⏳", "⏳", "🛑"
    elif status == "HIT_TP1": tp1_icon, tp2_icon, tp3_icon, sl_icon = "✅", "⏳", "⏳", "🛑"
    elif status == "HIT_TP2": tp1_icon, tp2_icon, tp3_icon, sl_icon = "✅", "✅", "⏳", "🛑"
    elif status == "CLOSED_TP3": tp1_icon, tp2_icon, tp3_icon, sl_icon = "✅", "✅", "🚀", "🛑"
    elif status == "CLOSED_SL": tp1_icon, tp2_icon, tp3_icon, sl_icon = "❌", "❌", "❌", "💥"

    audit_badge = f"🛡️ <b>AUTO-AUDIT:</b> ✅ {audit_status}" if audit_status else ""
    text = (
        f"🎯 <b>GHOST SNIPER | {chain.upper()}</b> {chain_emoji}\n"
        f"{type_badge}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 <b>{sig_data['token_name']}</b>\n\n"
        f"📈 <b>ENTRY:</b> <code>${sig_data['entry_price']:.8f}</code>\n\n"
        f"{sl_icon} <b>SL:</b> ${sig_data['sl']:.8f} <i>(-15%)</i>\n"
        f"{tp1_icon} <b>TP1:</b> ${sig_data['tp1']:.8f} <i>(+30% | 50%)</i>\n"
        f"{tp2_icon} <b>TP2:</b> ${sig_data['tp2']:.8f} <i>(+80% | 30%)</i>\n"
        f"{tp3_icon} <b>TP3:</b> ${sig_data['tp3']:.8f} <i>(+200% | Moonbag)</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{audit_badge}\n"
        f"📋 <b>CA:</b> <code>{sig_data['token_address']}</code>\n"
        f"<i>💡 Tekan CA untuk copy. Cek Bubblemaps manual untuk cabal detection.</i>"
    )
    return text

# ==========================================
# 🚀 EKSEKUSI TELEGRAM
# ==========================================
def send_telegram_message(text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHANNEL_ID, 'text': text, 'parse_mode': 'HTML', 'disable_web_page_preview': True}
    if reply_markup: payload['reply_markup'] = json.dumps(reply_markup)
    try:
        resp = requests.post(url, data=payload, timeout=10)
        if resp.status_code == 200: return resp.json()['result']['message_id']
    except Exception as e: logging.error(f"TG Send Exception: {e}")
    return None

def edit_telegram_message(msg_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
    payload = {'chat_id': TELEGRAM_CHANNEL_ID, 'message_id': msg_id, 'text': text, 'parse_mode': 'HTML', 'disable_web_page_preview': True}
    if reply_markup: payload['reply_markup'] = json.dumps(reply_markup)
    try: requests.post(url, data=payload, timeout=10)
    except Exception as e: logging.error(f"TG Edit Exception: {e}")

def send_admin_log(text):
    if not ADMIN_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': ADMIN_CHAT_ID, 'text': text, 'parse_mode': 'HTML', 'disable_web_page_preview': True}
    try: requests.post(url, data=payload, timeout=5)
    except: pass

# ==========================================
# ⚙️ CRON JOBS
# ==========================================
def job_scan_market():
    send_admin_log("🔍 <b>System:</b> Starting market scan...")
    found_fast = 0
    added_watchlist = 0
    
    for chain in TARGET_CHAINS:
        pools = fetch_new_pools(chain)
        for pool in pools:
            if is_valid_basic_filter(pool, chain):
                attrs = pool['attributes']
                token_address = pool['relationships']['base_token']['data']['id'].split('_')[1]
                pool_address = pool['id'].split('_')[1]
                token_name = attrs.get('name', 'Unknown').split(' / ')[0]
                entry_price = float(attrs['base_token_price_usd'])
                
                # Check if already sent in last 24h
                existing = supabase_select("signals", "id", f"token_address=eq.{token_address}&created_at=gt.{(datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()}")
                if existing:
                    continue
                
                signal_type = classify_signal_type(pool)
                
                if signal_type == 'FAST':
                    audit_passed, audit_msg = auto_audit_token(chain, token_address)
                    if not audit_passed:
                        send_admin_log(f"🗑️ <b>Rejected:</b> {token_name} ({audit_msg})")
                        continue
                    
                    targets = calculate_targets(entry_price)
                    sig_data = {'chain': chain, 'token_address': token_address, 'pool_address': pool_address,
                                'token_name': token_name, 'entry_price': entry_price, 'signal_type': 'FAST', **targets}
                    
                    text = format_signal_text(sig_data, "ACTIVE", audit_msg)
                    keyboard = build_keyboard(chain, token_address, pool_address, token_name, audit_msg)
                    msg_id = send_telegram_message(text, keyboard)
                    
                    if msg_id:
                        sig_data['tg_msg_id'] = msg_id
                        if save_signal(sig_data):
                            found_fast += 1
                else:  # PULLBACK
                    watchlist_data = {'chain': chain, 'token_address': token_address, 'pool_address': pool_address,
                                      'token_name': token_name, 'peak_price': entry_price}
                    if add_to_watchlist(watchlist_data):
                        added_watchlist += 1
                time.sleep(1)
    
    send_admin_log(f"✅ <b>Scan Complete:</b> {found_fast} FAST signals sent | {added_watchlist} added to watchlist")

def job_monitor_watchlist():
    watchlist = get_watchlist()
    if not watchlist: return
    
    tokens_to_check = {w['token_address']: w for w in watchlist}
    prices = fetch_current_prices(list(tokens_to_check.keys()))
    
    for addr, w in tokens_to_check.items():
        current_price = prices.get(addr)
        if not current_price: continue
        
        if current_price > w['peak_price']:
            supabase_update("watchlist", {"peak_price": current_price, "last_checked": datetime.now(timezone.utc).isoformat()}, {"id": f"eq.{w['id']}"})
            continue
        
        pullback_pct = ((w['peak_price'] - current_price) / w['peak_price']) * 100
        if 15 <= pullback_pct <= 25:
            audit_passed, audit_msg = auto_audit_token(w['chain'], w['token_address'])
            if not audit_passed:
                send_admin_log(f"🗑️ <b>Watchlist Rejected:</b> {w['token_name']} ({audit_msg})")
                remove_from_watchlist(w['id'])
                continue
            
            targets = calculate_targets(current_price)
            sig_data = {'chain': w['chain'], 'token_address': w['token_address'], 'pool_address': w['pool_address'],
                        'token_name': w['token_name'], 'entry_price': current_price, 'signal_type': 'PULLBACK', **targets}
            
            text = format_signal_text(sig_data, "ACTIVE", audit_msg)
            keyboard = build_keyboard(w['chain'], w['token_address'], w['pool_address'], w['token_name'], audit_msg)
            msg_id = send_telegram_message(text, keyboard)
            
            if msg_id:
                sig_data['tg_msg_id'] = msg_id
                if save_signal(sig_data):
                    send_admin_log(f"🎯 <b>Pullback Signal:</b> {w['token_name']} (-{pullback_pct:.1f}% from peak)")
                    remove_from_watchlist(w['id'])

def job_monitor_signals():
    active_signals = get_active_signals()
    if not active_signals: return
    tokens_to_check = {sig['token_address']: sig for sig in active_signals}
    prices = fetch_current_prices(list(tokens_to_check.keys()))
    
    for addr, sig in tokens_to_check.items():
        current_price = prices.get(addr)
        if not current_price: continue
        new_status = sig['status']
        if current_price <= sig['sl']: new_status = "CLOSED_SL"
        elif current_price >= sig['tp3']: new_status = "CLOSED_TP3"
        elif current_price >= sig['tp2'] and sig['status'] not in ['HIT_TP2', 'CLOSED_TP3']: new_status = "HIT_TP2"
        elif current_price >= sig['tp1'] and sig['status'] == 'ACTIVE': new_status = "HIT_TP1"
        if new_status != sig['status']:
            updated_text = format_signal_text(sig, new_status)
            keyboard = build_keyboard(sig['chain'], sig['token_address'], sig['pool_address'], sig['token_name'], "")
            edit_telegram_message(sig['tg_msg_id'], updated_text, keyboard)
            update_signal_status(sig['id'], new_status)

def generate_weekly_report():
    signals = supabase_select("signals", "status", f"created_at=gt.{(datetime.now(timezone.utc) - timedelta(days=7)).isoformat()}")
    total = len(signals)
    if total == 0:
        send_telegram_message("📊 <b>Weekly Report:</b> No signals generated this week. Market conditions poor.")
        return
    wins = losses = active = 0
    total_roi = 0.0
    for sig in signals:
        status = sig['status']
        if status == 'CLOSED_SL':
            losses += 1
            total_roi -= 15.0
        elif status in ['HIT_TP1', 'HIT_TP2', 'CLOSED_TP3']:
            wins += 1
            roi = 15.0
            if status in ['HIT_TP2', 'CLOSED_TP3']: roi += 24.0
            if status == 'CLOSED_TP3': roi += 40.0
            total_roi += roi
        else: active += 1
    closed_trades = wins + losses
    win_rate = (wins / closed_trades) * 100 if closed_trades > 0 else 0
    avg_roi = total_roi / total
    report_text = (
        f"📊 <b>GHOST SNIPER | WEEKLY TRANSPARENCY REPORT</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🗓 <b>Period:</b> Last 7 Days\n\n"
        f"🎯 <b>Total Signals:</b> {total}\n"
        f"✅ <b>Wins (TP Hit):</b> {wins}\n"
        f"❌ <b>Losses (SL Hit):</b> {losses}\n"
        f"⏳ <b>Active (Running):</b> {active}\n\n"
        f"📈 <b>Win Rate:</b> {win_rate:.1f}%\n"
        f"💰 <b>Est. Avg ROI per Signal:</b> {avg_roi:+.2f}%\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>🤖 <b>System Integrity:</b> 100% Auto-Generated by On-Chain Data.\n"
        f"Zero manual intervention (No Front-Running).\n"
        f"ROI calculated using Scale-Out Strategy (50% TP1, 30% TP2, 20% TP3) with SL -15%.</i>"
    )
    send_telegram_message(report_text)
    send_admin_log(f"📊 <b>Weekly Report Sent:</b> WR {win_rate:.1f}% | ROI {avg_roi:+.2f}%")

# ==========================================
# 🏢 FLASK APP & SCHEDULER INITIALIZATION
# ==========================================
app = Flask(__name__)

# Global flag untuk elak duplicate scheduler dalam multiple workers
_scheduler_started = False

def start_scheduler():
    """Start APScheduler - dipanggil sekali sahaja walaupun multiple workers"""
    global _scheduler_started
    
    if _scheduler_started:
        return
    
    scheduler = BackgroundScheduler()
    
    # Scan market every 15 minutes
    scheduler.add_job(job_scan_market, 'interval', minutes=15, next_run_time=datetime.now())
    
    # Monitor watchlist every 3 minutes (for pullback detection)
    scheduler.add_job(job_monitor_watchlist, 'interval', minutes=3)
    
    # Monitor active signals every 2 minutes
    scheduler.add_job(job_monitor_signals, 'interval', minutes=2)
    
    # Weekly report every Sunday 11 PM UTC
    scheduler.add_job(generate_weekly_report, 'cron', day_of_week='sun', hour=23, minute=0)
    
    scheduler.start()
    _scheduler_started = True
    
    # Send boot message hanya sekali (first worker)
    send_admin_log("🟢 <b>Project JEBAT Booted</b>\nGhost Sniper v4.0 (REST API) Online.\nScanner | Watchlist | Monitor Active.")

@app.route('/')
def home():
    return "Project JEBAT | Ghost Sniper v4.0 (REST API) is Active"

@app.route('/health')
def health():
    try:
        resp = requests.get(f"{SUPABASE_URL}/rest/v1/signals?select=count", headers=HEADERS, timeout=5)
        db_status = "OK" if resp.status_code == 200 else "ERROR"
    except:
        db_status = "ERROR"
    
    return jsonify({
        "status": "healthy",
        "database": db_status,
        "scheduler": "running" if _scheduler_started else "not_started",
        "timestamp": datetime.now(timezone.utc).isoformat()
    })

@app.route('/test-telegram')
def test_telegram():
    """Test endpoint untuk debug Telegram connection"""
    results = {
        "bot_token_set": bool(TELEGRAM_BOT_TOKEN),
        "admin_chat_id_set": bool(ADMIN_CHAT_ID),
        "channel_id_set": bool(TELEGRAM_CHANNEL_ID),
        "admin_test": None,
        "channel_test": None,
        "scheduler_running": _scheduler_started,
        "errors": []
    }
    
    if TELEGRAM_BOT_TOKEN and ADMIN_CHAT_ID:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                'chat_id': ADMIN_CHAT_ID,
                'text': '🧪 <b>Test Message to Admin</b>\nJika kau nampak ini, Admin DM berfungsi!',
                'parse_mode': 'HTML'
            }
            resp = requests.post(url, data=payload, timeout=10)
            results["admin_test"] = "SUCCESS" if resp.status_code == 200 else "FAILED"
            if resp.status_code != 200:
                results["errors"].append(f"Admin API: {resp.text}")
        except Exception as e:
            results["admin_test"] = "ERROR"
            results["errors"].append(f"Admin Exception: {str(e)}")
    
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHANNEL_ID:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                'chat_id': TELEGRAM_CHANNEL_ID,
                'text': '🧪 <b>Test Message to Channel</b>\nJika kau nampak ini, Channel berfungsi!',
                'parse_mode': 'HTML'
            }
            resp = requests.post(url, data=payload, timeout=10)
            results["channel_test"] = "SUCCESS" if resp.status_code == 200 else "FAILED"
            if resp.status_code != 200:
                results["errors"].append(f"Channel API: {resp.text}")
        except Exception as e:
            results["channel_test"] = "ERROR"
            results["errors"].append(f"Channel Exception: {str(e)}")
    
    return jsonify(results)

# ==========================================
# 🚀 AUTO-START SCHEDULER ON IMPORT
# ==========================================
# Ini akan run bila gunicorn import module ini
# Gunicorn biasanya spawn 1 worker untuk free tier, jadi ini selamat
start_scheduler()

if __name__ == '__main__':
    # Fallback untuk local development (bukan production)
    app.run(host='0.0.0.0', port=PORT)
