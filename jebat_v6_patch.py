"""
==========================================================================
PROJECT JEBAT v6.0 — PATCH FILE
==========================================================================
CARA GUNA FAIL INI:
  Ini bukan full replacement. Ini patch — tambah & ganti section tertentu
  dalam main.py kau yang sedia ada.

  [TAMBAH BARU]   = Letak kod baru ini di lokasi yang dinyatakan
  [GANTI]         = Replace function/block yang tersebut
  [PINDAH/EDIT]   = Edit nilai atau baris tertentu

ROOT CAUSE SUMMARY (kenapa 9.7% WR):
  #1 trending_pools = masuk SELEPAS token dah pump (chasing exhaustion)
  #2 Tiada h24 cap — token dah +500% dalam 24j masih qualify
  #3 h1 bar terlalu rendah — h1 > 0% cukup untuk lulus
  #4 Tiada CEX guard — token matured/listed boleh lepas
  #5 Volume spike tidak dikesan — 1 whale buy = metrics nampak cantik
  #6 Tiada minimum tx count — 5 transaksi boleh buat ratio nampak baik

TARGET SELEPAS PATCH:
  Win rate: 9.7% → 25-35%
  Signal count: ~13/hari → ~2-5/hari (kualiti atas kuantiti)
==========================================================================
"""

# ==========================================
# [TAMBAH BARU] — Letak selepas baris AGE_MAX_HOURS = 168
# ==========================================

# --- GANTI nilai ini ---
AGE_MAX_HOURS = 72          # Turun dari 168 ke 72 — CEX guard tangani yang lama

# --- TAMBAH di bawah AGE_MAX_HOURS ---
H1_MIN_CHANGE    = 8.0      # Minimum h1 % untuk qualify — naik dari >0 ke >8%
H24_CHANGE_MAX   = 250.0    # Cap h24 — dah pump >250% = late/distribution, skip
MIN_H1_TX        = 30       # Minimum jumlah transaksi h1 — data tak reliable kalau sikit


# ==========================================
# [TAMBAH BARU] — CEX GUARD ENGINE
# Letak selepas blok SCORING & RISK PARAMETERS
# ==========================================

# CoinGecko chain ID mapping
_COINGECKO_CHAIN_MAP = {
    'solana': 'solana',
    'bsc':    'binance-smart-chain',
}

# Exchange identifiers dalam CoinGecko ticker data
_MAJOR_CEX_IDS = {
    'binance', 'okex', 'kucoin', 'bybit_spot', 'gate', 'mexc',
    'bitget', 'huobi', 'htx', 'coinbase', 'crypto_com'
}

# Cache per-address: {addr_key: {'result': bool, 'ts': datetime}}
_cex_cache: dict = {}


def is_on_cex(chain: str, token_address: str) -> bool:
    """
    Returns True jika token dah listed pada major CEX.
    Strategy: CoinGecko contract lookup — kalau ada market cap rank
    atau tickers pada CEX besar, block.

    Fail-open: kalau API error atau rate-limit, return False (jangan
    block token semata-mata sebab API down).

    Cache 1 jam per address — jimat API call.
    """
    cache_key = f"{chain}:{token_address.lower()}"
    cached = _cex_cache.get(cache_key)
    if cached:
        age_secs = (datetime.now(timezone.utc) - cached['ts']).total_seconds()
        if age_secs < 3600:
            return cached['result']

    cg_chain = _COINGECKO_CHAIN_MAP.get(chain)
    if not cg_chain:
        return False  # Chain tak dikenali → jangan block

    url = f"https://api.coingecko.com/api/v3/coins/{cg_chain}/contract/{token_address}"
    try:
        resp = requests.get(
            url, timeout=8,
            headers={'Accept': 'application/json', 'User-Agent': 'JEBAT/6.0'}
        )

        if resp.status_code == 404:
            # Token tak ada dalam CoinGecko = belum listing = OK untuk trade
            _cex_cache[cache_key] = {'result': False, 'ts': datetime.now(timezone.utc)}
            return False

        if resp.status_code == 429:
            # Rate limited — fail open
            logging.warning(f"CEX check rate-limited for {token_address[:10]}")
            return False

        if resp.status_code != 200:
            return False

        data = resp.json()

        # Signal kuat: ada market cap rank → definitely major coin
        mcap_rank = data.get('market_cap_rank')
        if mcap_rank and mcap_rank < 1500:
            _cex_cache[cache_key] = {'result': True, 'ts': datetime.now(timezone.utc)}
            logging.info(f"[CEX GUARD] {token_address[:10]} — rank #{mcap_rank}, BLOCKED")
            return True

        # Check tickers — ada pada CEX mana-mana?
        tickers = data.get('tickers', [])[:30]  # First 30 cukup
        for t in tickers:
            market_id = t.get('market', {}).get('identifier', '').lower()
            if market_id in _MAJOR_CEX_IDS:
                _cex_cache[cache_key] = {'result': True, 'ts': datetime.now(timezone.utc)}
                logging.info(
                    f"[CEX GUARD] {data.get('symbol','?').upper()} dah ada kat "
                    f"{market_id.upper()}, BLOCKED"
                )
                return True

        # Tak jumpa apa-apa — token masih DEX-only
        _cex_cache[cache_key] = {'result': False, 'ts': datetime.now(timezone.utc)}
        return False

    except Exception as e:
        logging.warning(f"CEX guard error {token_address[:10]}: {e}")
        return False  # Fail open


# ==========================================
# [TAMBAH BARU] — VOLUME SPIKE DETECTOR
# Letak selepas is_on_cex function
# ==========================================

def is_volume_spike(attrs: dict) -> bool:
    """
    Kesan corak: volume naik tiba-tiba dalam 1 jam tapi tak consistent.
    Pattern ini = whale masuk sebentar, bukan organic demand.

    Logic: Kalau 80%+ dari vol 6 jam berlaku dalam 1 jam terakhir = spike.
    Token yang trending secara organik akan ada vol distributed merata.

    Returns True = spike detected → skip token ini.
    """
    vol_h1 = float(attrs.get('volume_usd', {}).get('h1', 0) or 0)
    vol_h6 = float(attrs.get('volume_usd', {}).get('h6', 0) or 0)

    if vol_h6 <= 0 or vol_h1 <= 0:
        return False  # Tak cukup data → jangan block

    ratio = vol_h1 / vol_h6
    if ratio > 0.80:
        logging.debug(f"Volume spike detected: h1/h6={ratio:.2f}")
        return True

    return False


# ==========================================
# [TAMBAH BARU] — NEW POOLS FETCHER
# Letak selepas fetch_trending_pools function
# ==========================================

def fetch_new_pools(chain: str, max_retries: int = 3) -> list:
    """
    Ambil pool BARU dari GeckoTerminal, sorted by creation time.
    Ini adalah kunci perubahan universe:

    trending_pools = token yang DULU dah naik, viral sekarang
    new_pools      = token yang BARU mula, kita masuk AWAL

    Untuk JEBAT, new_pools + age filter (2-72h) = sweet spot discovery.
    """
    url = f"https://api.geckoterminal.com/api/v2/networks/{chain}/new_pools"
    params = {'page': 1}
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
                logging.info(f"⏳ GeckoTerminal new_pools {chain} — wait {wait_time}s")
                time.sleep(wait_time)
                continue

            if resp.status_code == 200:
                data = resp.json().get('data', [])
                return data if isinstance(data, list) else []
            else:
                report_error('WARNING', 'geckoterminal',
                             f'new_pools {chain}: {resp.status_code}')
                return []

        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))
                continue
            report_error('CRITICAL', 'geckoterminal',
                         f'fetch_new_pools {chain} failed', str(e))
            return []

    return []


# ==========================================
# [GANTI] — score_momentum function
# Replace keseluruhan function score_momentum dalam main.py
# ==========================================

def score_momentum(attrs: dict) -> int:
    """
    0–25 markah berdasarkan kualiti trend.

    PERUBAHAN v6:
    - Threshold ditighten (h1 >= 25 dapat 10, bukan h1 >= 15)
    - h24 early-mover bonus: h1 kuat tapi h24 masih rendah = awal perjalanan
    - m5 masih kritikal: token mesti MASIH bergerak, bukan dah fade
    """
    pc = attrs.get('price_change_percentage', {})
    m5  = float(pc.get('m5',  0) or 0)
    h1  = float(pc.get('h1',  0) or 0)
    h6  = float(pc.get('h6',  0) or 0)
    h24 = float(pc.get('h24', 0) or 0)

    # Hard gate (sepatutnya dah kena filter sebelum ini)
    if h1 < H1_MIN_CHANGE:
        return 0

    score = 0

    # h1 base momentum (tightened thresholds)
    if   h1 >= 25: score += 10
    elif h1 >= 15: score += 7
    elif h1 >= 10: score += 5
    # h1 8–9.99% = 0 dari tier ini tapi masih qualify overall

    # h6 trend confirmation — token mesti trending, bukan one-off h1 spike
    if   h6 >= 10: score += 8
    elif h6 >= 3:  score += 5
    elif h6 >= 0:  score += 2
    # h6 negatif = 0 (arah berlawanan)

    # m5 freshness — paling kritikal: masih bergerak SEKARANG masa scan
    if   m5 >= 3: score += 7
    elif m5 >= 1: score += 5
    elif m5 >= 0: score += 2
    # m5 negatif = 0 (momentum dah habis masa scan — jangan masuk)

    # Early-mover bonus: h1 kuat tapi h24 masih rendah = kita masuk awal
    # Token yang h24 = 500% dah exhausted, h24 = 30% bermakna perjalanan baru start
    if h1 >= 10 and h24 < 50:
        score += 5  # Bonus discovery awal

    return min(25, score)


# ==========================================
# [GANTI] — process_pool_candidate function
# Replace keseluruhan function dalam main.py
# Perubahan utama ditanda dengan # [v6 NEW] / # [v6 CHANGED]
# ==========================================

def process_pool_candidate(pool: dict, chain: str, rejected: dict):
    """
    Returns: (signal_type, sig_data, score_breakdown, audit_msg) or None.

    v6 Filter sequence (cheapest dulu, expensive last):
    1. Timestamp / Age
    2. Liquidity
    3. Volume h24
    4. FDV
    5. [v6 CHANGED] h1 >= H1_MIN_CHANGE (bukan sekadar > 0)
    6. [v6 NEW]     h24 cap <= H24_CHANGE_MAX
    7. [v6 NEW]     Min tx count >= MIN_H1_TX
    8.              Sell pressure (existing)
    9.              Buy volume (existing)
    10.[v6 NEW]     Volume spike check
    11.             Address extraction
    12.             Duplicate/cooldown check
    13.[v6 NEW]     CEX guard (CoinGecko)
    14.             Audit (RugCheck/GoPlus — most expensive)
    15.             Composite score
    """
    attrs      = pool['attributes']
    token_name = attrs.get('name', 'Unknown').split(' / ')[0]

    # ── 1. Timestamp & Age ──────────────────────────────────────────────
    try:
        created_at = datetime.fromisoformat(
            attrs['pool_created_at'].replace('Z', '+00:00')
        )
    except (KeyError, ValueError):
        rejected['no_timestamp'] = rejected.get('no_timestamp', 0) + 1
        return None

    age_hours = (datetime.now(timezone.utc) - created_at).total_seconds() / 3600
    if not (AGE_MIN_HOURS <= age_hours <= AGE_MAX_HOURS):
        rejected['age'] = rejected.get('age', 0) + 1
        return None

    # ── 2. Liquidity ────────────────────────────────────────────────────
    liq = float(attrs.get('reserve_in_usd', 0) or 0)
    if liq < MIN_LIQUIDITY_USD:
        rejected['liquidity'] = rejected.get('liquidity', 0) + 1
        return None

    # ── 3. Volume h24 ───────────────────────────────────────────────────
    vol_h24 = float(attrs.get('volume_usd', {}).get('h24', 0) or 0)
    if vol_h24 < MIN_VOLUME_24H_USD:
        rejected['volume'] = rejected.get('volume', 0) + 1
        return None

    # ── 4. FDV ──────────────────────────────────────────────────────────
    fdv = float(attrs.get('fdv_usd', 0) or 0)
    if not (FDV_MIN <= fdv <= FDV_MAX):
        rejected['fdv'] = rejected.get('fdv', 0) + 1
        return None

    # ── 5. [v6 CHANGED] h1 minimum — naik dari >0% ke >= H1_MIN_CHANGE ─
    h1_change = float(
        attrs.get('price_change_percentage', {}).get('h1', 0) or 0
    )
    if h1_change < H1_MIN_CHANGE:
        rejected['h1_weak'] = rejected.get('h1_weak', 0) + 1
        return None

    # ── 6. [v6 NEW] h24 cap — block token dah exhausted ────────────────
    h24_change = float(
        attrs.get('price_change_percentage', {}).get('h24', 0) or 0
    )
    if h24_change > H24_CHANGE_MAX:
        rejected['h24_capped'] = rejected.get('h24_capped', 0) + 1
        return None

    # ── 7. [v6 NEW] Transaction count minimum ───────────────────────────
    h1_tx    = attrs.get('transactions', {}).get('h1', {})
    h1_buys  = int(h1_tx.get('buys',  0) or 0)
    h1_sells = int(h1_tx.get('sells', 0) or 0)
    h1_total = h1_buys + h1_sells
    if h1_total < MIN_H1_TX:
        rejected['low_tx'] = rejected.get('low_tx', 0) + 1
        return None

    # ── 8. Sell pressure (existing — kekal) ─────────────────────────────
    if h1_buys > 0 and h1_sells / h1_buys > 1.8:
        rejected['sell_pressure'] = rejected.get('sell_pressure', 0) + 1
        return None

    # ── 9. Buy volume ratio (existing — kekal) ──────────────────────────
    buy_vol  = float(attrs.get('volume_usd', {}).get('h1_buy',  0) or 0)
    sell_vol = float(attrs.get('volume_usd', {}).get('h1_sell', 0) or 0)
    if buy_vol > 0 and sell_vol > 0 and sell_vol / buy_vol > 1.5:
        rejected['sell_vol'] = rejected.get('sell_vol', 0) + 1
        return None

    # ── 10. [v6 NEW] Volume spike detection ─────────────────────────────
    if is_volume_spike(attrs):
        rejected['vol_spike'] = rejected.get('vol_spike', 0) + 1
        return None

    # ── 11. Address extraction ───────────────────────────────────────────
    try:
        token_address = pool['relationships']['base_token']['data']['id'].split('_', 1)[1]
        pool_address  = pool['id'].split('_', 1)[1]
    except (KeyError, IndexError):
        rejected['no_address'] = rejected.get('no_address', 0) + 1
        return None

    # ── 12. Duplicate / cooldown check (existing — kekal) ───────────────
    now           = datetime.now(timezone.utc)
    current_price = float(attrs.get('base_token_price_usd', 0) or 0)
    recent_signals = supabase_select(
        "signals", "id,status,created_at,entry_price",
        f"token_address=eq.{token_address}"
        f"&created_at=gt.{(now - timedelta(hours=24)).isoformat().replace('+00:00','Z')}"
    )
    for prev in recent_signals:
        prev_status = prev.get('status', '')
        try:
            prev_time = datetime.fromisoformat(
                prev['created_at'].replace('Z', '+00:00')
            )
        except (KeyError, ValueError):
            continue
        elapsed_hours = (now - prev_time).total_seconds() / 3600

        if prev_status in ['ACTIVE', 'HIT_TP1', 'HIT_TP2']:
            rejected['duplicate'] = rejected.get('duplicate', 0) + 1
            return None
        elif prev_status == 'CLOSED_SL':
            if elapsed_hours < 1.0:
                rejected['duplicate'] = rejected.get('duplicate', 0) + 1
                return None
            prev_entry = float(prev.get('entry_price', 0) or 0)
            if prev_entry > 0 and current_price < prev_entry:
                rejected['sl_no_recovery'] = rejected.get('sl_no_recovery', 0) + 1
                return None
        elif prev_status == 'CLOSED_TP3' and elapsed_hours < 24.0:
            rejected['duplicate'] = rejected.get('duplicate', 0) + 1
            return None

    # ── 13. [v6 NEW] CEX guard — block token dah listed ─────────────────
    if is_on_cex(chain, token_address):
        rejected['on_cex'] = rejected.get('on_cex', 0) + 1
        return None

    # ── 14. Audit (expensive — tetap last sebelum score) ─────────────────
    audit_passed, sec_score, audit_msg = audit_token(chain, token_address)
    if not audit_passed:
        rejected['audit'] = rejected.get('audit', 0) + 1
        return None

    # ── 15. Composite score ───────────────────────────────────────────────
    breakdown   = compute_composite_score(attrs, age_hours, sec_score)
    total_score = sum(breakdown.values())
    if total_score < MIN_SIGNAL_SCORE:
        rejected['low_score'] = rejected.get('low_score', 0) + 1
        return None

    weak_momentum = breakdown['momentum'] < 15

    # ── Entry price & targets ─────────────────────────────────────────────
    entry_price = float(attrs.get('base_token_price_usd', 0) or 0)
    if entry_price <= 0:
        rejected['no_price'] = rejected.get('no_price', 0) + 1
        return None

    vol_pct = calculate_volatility_pct(attrs)
    targets = calculate_targets(entry_price, vol_pct)

    sig_type = 'PULLBACK' if (h24_change > 100 or weak_momentum) else 'FAST'

    sig_data = {
        'chain':           chain,
        'token_address':   token_address,
        'pool_address':    pool_address,
        'token_name':      token_name,
        'entry_price':     entry_price,
        'signal_type':     sig_type,
        'composite_score': total_score,
        'buy_sell_ratio':  f"{h1_buys}/{h1_sells}" if h1_buys > 0 else "N/A",
        **targets
    }
    return (sig_type, sig_data, breakdown, audit_msg)


# ==========================================
# [GANTI] — job_scan_market function
# Tambah new_pools fetching & merge logic
# ==========================================

def job_scan_market():
    if not get_btc_trend():
        print("[JEBAT] 🐻 BTC bearish — scan paused", flush=True)
        return

    print(f"[JEBAT] 🔍 SCAN CYCLE START", flush=True)
    stats    = {'scanned': 0, 'qualified': 0, 'sent_fast': 0, 'watchlist': 0}
    rejected = {}

    for chain in TARGET_CHAINS:
        # [v6] Ambil KEDUA-DUA new_pools DAN trending_pools
        # new_pools diutamakan — earlier discovery = better edge
        new_pools      = fetch_new_pools(chain)
        trending_pools = fetch_trending_pools(chain)

        # Merge + deduplicate (new_pools first)
        seen_ids: set = set()
        merged: list  = []
        for p in (new_pools + trending_pools):
            pid = p.get('id', '')
            if pid and pid not in seen_ids:
                seen_ids.add(pid)
                merged.append(p)

        stats['scanned'] += len(merged)
        print(
            f"[JEBAT] 📡 {chain.upper()}: {len(merged)} pools "
            f"({len(new_pools)} new + {len(trending_pools)} trending, "
            f"{len(merged) - len(seen_ids) + len(merged) - len(new_pools) - len(trending_pools) + len(seen_ids)} dupes removed)",
            flush=True
        )

        for i, pool in enumerate(merged):
            try:
                result = process_pool_candidate(pool, chain, rejected)
                if not result:
                    continue

                sig_type, sig_data, breakdown, audit_msg = result
                stats['qualified'] += 1

                if sig_type == 'PULLBACK':
                    if add_to_watchlist({
                        'chain':         chain,
                        'token_address': sig_data['token_address'],
                        'pool_address':  sig_data['pool_address'],
                        'token_name':    sig_data['token_name'],
                        'peak_price':    sig_data['entry_price']
                    }):
                        stats['watchlist'] += 1
                        print(f"[JEBAT] 👀 Watchlist: {sig_data['token_name']}", flush=True)
                else:
                    if save_signal(sig_data):
                        text = format_signal_text(sig_data, "ACTIVE", breakdown, audit_msg)
                        keyboard = build_keyboard(
                            chain, sig_data['token_address'],
                            sig_data['pool_address'], sig_data['token_name']
                        )
                        msg_id = send_telegram_message(text, keyboard)
                        if msg_id:
                            supabase_update(
                                "signals", {"tg_msg_id": msg_id},
                                {"token_address": f"eq.{sig_data['token_address']}",
                                 "status": "eq.ACTIVE"}
                            )
                            stats['sent_fast'] += 1
                            print(
                                f"[JEBAT] 📤 SIGNAL: {sig_data['token_name']} "
                                f"score={sig_data['composite_score']} "
                                f"h1={sig_data.get('h1_change','?')}%",
                                flush=True
                            )

            except Exception as e:
                logging.error(f"Pool processing error: {e}")
                continue

            if i % 5 == 4:
                time.sleep(1)

        time.sleep(2)

    print(f"[JEBAT] ✅ SCAN DONE: {stats}", flush=True)
    if rejected:
        print(f"[JEBAT] 🚫 REJECTIONS: {rejected}", flush=True)


# ==========================================
# NOTA TAMBAHAN — ELEMEN LAIN YANG PERLU DIPERTIMBANGKAN
# (Boleh implement dalam v6.1 selepas v6.0 stable)
# ==========================================
"""
[FUTURE v6.1 IMPROVEMENTS]

1. M5 freshness filter (hard gate):
   Tambah dalam process_pool_candidate selepas h24 cap:
   
   m5_change = float(attrs.get('price_change_percentage', {}).get('m5', 0) or 0)
   if m5_change < -5:  # Kalau 5 minit terbaru dah turun >5% = fade, skip
       rejected['m5_fading'] = ...
       return None

2. Liquidity/FDV ratio check:
   Tambah selepas FDV check:
   
   liq_fdv_ratio = (liq / fdv) if fdv > 0 else 0
   if liq_fdv_ratio < 0.02:  # Liquidity < 2% FDV = sangat thin, manipulation risk
       rejected['thin_liq'] = ...
       return None

3. Holder count check (Solana only via RugCheck):
   Dalam audit_token Solana section, tambah:
   
   holder_count = data.get('holderCount', 0)
   if holder_count < 50:  # Terlalu sikit pemegang = cabal/rug risk
       return False, 0, f"RugCheck: Only {holder_count} holders"

4. Dynamic SL floor:
   Dalam calculate_volatility_pct, tambah floor yang lebih realistic:
   
   VOL_MIN_PCT = 12.0  # Naik dari 6% ke 12%
   # Memecoin noise floor adalah ~12% — SL lebih ketat dari ni akan kena wick

5. BTC + SOL dual trend check (untuk Solana tokens):
   Dalam job_scan_market, sebelum scan Solana:
   
   if chain == 'solana' and not get_sol_trend():
       print("[JEBAT] SOL bearish — skip Solana scan", flush=True)
       continue
   
   def get_sol_trend():  # Mirror get_btc_trend tapi pakai SOLUSDT
       ...
"""
