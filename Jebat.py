"""
CRYPTO SIGNAL BOT v3.1 - ZERO-COST PRODUCTION EDITION
======================================================
v3.1 FIXES:
  [BUG1] NameError: 'level' -> 'level_price' dalam detect_retest_zone
  [BUG2] SWEEP_WICK_RATIO 2.0 -> 1.5
  [NEW]  Sweep bukan kewajipan: OR dengan engulfing / hammer / morning star
"""

import os, json, asyncio, logging, time, sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass, field
import aiohttp, websockets
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode
from telegram.helpers import escape
from flask import Flask
from threading import Thread

load_dotenv()

TELEGRAM_BOT_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_ADMIN_ID     = int(os.getenv("TELEGRAM_ADMIN_ID", "0"))
TELEGRAM_VVIP_CHANNEL = os.getenv("TELEGRAM_VVIP_CHANNEL")
PORT                  = int(os.getenv("PORT", 10000))

MIN_MARKET_CAP       = 100_000_000
MAX_MARKET_CAP       = 900_000_000
COINGECKO_POLL_SEC   = 300
MAX_COINS_TRACKED    = 30
MAX_CANDLES_PER_COIN = 200
SWING_LOOKBACK       = 5
FVG_MIN_GAP_PCT      = 0.002
OB_MIN_DISPLACEMENT  = 0.02
SWEEP_WICK_RATIO     = 1.5   # FIX BUG2: diturunkan dari 2.0
MIN_SL_PCT           = 0.015  # FIX: SL mesti minimum 1.5% bawah entry
MIN_RR_RATIO         = 2.5
SIGNAL_COOLDOWN_SEC  = 7200
FUNDAMENTAL_POLL_SEC = 300
ALLOWED_EXCHANGES    = ["binance", "bitget", "gateio"]

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ==============================================================================
# TTL CACHE
# ==============================================================================
class TTLCache:
    def __init__(self):
        self._cache: Dict[str, Any] = {}
        self._expiry: Dict[str, float] = {}
        self._lock = asyncio.Lock()
    async def get(self, key):
        async with self._lock:
            if key in self._cache and time.time() < self._expiry.get(key, 0):
                return self._cache[key]
            return None
    async def set(self, key, value, ttl):
        async with self._lock:
            self._cache[key] = value
            self._expiry[key] = time.time() + ttl
    async def cleanup(self):
        async with self._lock:
            now = time.time()
            for k in [k for k, v in self._expiry.items() if now >= v]:
                self._cache.pop(k, None); self._expiry.pop(k, None)

cache = TTLCache()

# ==============================================================================
# DATA MODELS
# ==============================================================================
@dataclass
class Candle:
    timestamp: int; open: float; high: float; low: float; close: float; volume: float

@dataclass
class SwingPoint:
    price: float; timestamp: int; index: int

@dataclass
class FVG:
    type: str; high: float; low: float; timestamp: int; mitigated: bool = False

@dataclass
class OrderBlock:
    type: str; high: float; low: float; timestamp: int; mitigated: bool = False

@dataclass
class MarketStructure:
    swing_highs: List[SwingPoint] = field(default_factory=list)
    swing_lows: List[SwingPoint]  = field(default_factory=list)
    trend: str = "RANGING"
    last_bos: Optional[dict]   = None
    last_choch: Optional[dict] = None
    active_fvgs: List[FVG]         = field(default_factory=list)
    active_obs: List[OrderBlock]   = field(default_factory=list)

@dataclass
class CoinData:
    symbol: str; binance_symbol: str
    price: float = 0.0; market_cap: float = 0.0; volume_24h: float = 0.0
    candles: List[Candle] = field(default_factory=list)
    available_exchanges: Set[str] = field(default_factory=set)
    structure: MarketStructure = field(default_factory=MarketStructure)
    retail_long_pct: float = 50.0; whale_long_pct: float = 50.0
    taker_buy_ratio: float = 1.0; circulating_supply: float = 0.0
    total_supply: float = 0.0; oi_change_pct: float = 0.0
    candles_4h: List[Candle] = field(default_factory=list)
    candles_d1: List[Candle] = field(default_factory=list)

@dataclass
class SMCSetup:
    symbol: str; direction: str; timestamp: datetime
    entry_price: float; sl_price: float
    tp1_price: float; tp2_price: float; tp3_price: float
    choch_type: str; sweep_price: float
    entry_zone_type: str; zone_high: float; zone_low: float
    rr_ratio: float; structure_score: float
    fundamental_score: float; sentiment_score: float
    signal_type: str = "FVG ZONE"
    reversal_type: str = "SWEEP"   # NEW: SWEEP | ENGULFING | HAMMER | MORNING_STAR

@dataclass
class FundamentalBias:
    symbol: str; bias: str; score: float; reasons: List[str]

# ==============================================================================
# GLOBAL STATE
# ==============================================================================
coins_data: Dict[str, CoinData] = {}
coins_lock  = asyncio.Lock()
sent_signals: Dict[str, float] = {}
active_trades: Dict[int, dict]  = {}
trades_lock = asyncio.Lock()
bot_app = None
journal_db = None
db_lock = asyncio.Lock()
daily_pnl = 0.0; daily_signals = 0; last_reset_date = None
stats_lock = asyncio.Lock()

# v3.3: Consecutive SL tracker + rolling win rate
sl_streak: Dict[str, dict] = {}
# Format: {symbol: {count, last_sl_time, last_sl_price, sweep_check}}
streak_lock  = asyncio.Lock()
rolling_wr_penalty: int = 0   # dikemas sekali per scan cycle

# ==============================================================================
# BINANCE ENGINE
# ==============================================================================
class BinanceEngine:
    REST_URL    = "https://api.binance.com/api/v3"
    FUTURES_URL = "https://fapi.binance.com"
    COMBINED_WS = "wss://stream.binance.com:9443/stream?streams="

    def __init__(self):
        self.session = None; self.ws = None; self.oi_cache = {}

    async def init(self): self.session = aiohttp.ClientSession()
    async def close(self):
        if self.session: await self.session.close()
        if self.ws:
            try: await self.ws.close()
            except: pass

    async def get_initial_klines(self, symbol, interval="5m", limit=200):
        key = f"klines_{symbol}_{interval}"
        cached = await cache.get(key)
        if cached: return cached
        try:
            async with self.session.get(f"{self.REST_URL}/klines",
                    params={"symbol": symbol, "interval": interval, "limit": limit}, timeout=15) as r:
                if r.status == 200:
                    data = await r.json()
                    candles = [Candle(int(k[0]),float(k[1]),float(k[2]),float(k[3]),float(k[4]),float(k[5])) for k in data]
                    await cache.set(key, candles, 300); return candles
        except Exception as e: logger.debug(f"Klines {symbol}: {e}")
        return []

    async def get_htf_candles(self, symbol, interval, limit):
        ttl = 14400 if interval == "4h" else 86400
        key = f"htf_{symbol}_{interval}"
        cached = await cache.get(key)
        if cached: return cached
        try:
            async with self.session.get(f"{self.REST_URL}/klines",
                    params={"symbol": symbol, "interval": interval, "limit": limit}, timeout=15) as r:
                if r.status == 200:
                    data = await r.json()
                    candles = [Candle(int(k[0]),float(k[1]),float(k[2]),float(k[3]),float(k[4]),float(k[5])) for k in data]
                    await cache.set(key, candles, ttl); return candles
        except Exception as e: logger.debug(f"HTF {symbol} {interval}: {e}")
        return []

    async def get_exchange_info(self):
        cached = await cache.get("binance_pairs")
        if cached: return cached
        try:
            async with self.session.get(f"{self.REST_URL}/exchangeInfo", timeout=30) as r:
                if r.status == 200:
                    data = await r.json()
                    pairs = {s["symbol"] for s in data.get("symbols",[]) if s["quoteAsset"]=="USDT" and s["status"]=="TRADING"}
                    await cache.set("binance_pairs", pairs, 86400); return pairs
        except Exception as e: logger.error(f"Exchange info: {e}")
        return set()

    async def get_long_short_ratio(self, symbol):
        key = f"ls_ratio_{symbol}"; cached = await cache.get(key)
        if cached: return cached
        try:
            retail_long = whale_long = 0.5
            try:
                async with self.session.get(f"{self.FUTURES_URL}/futures/data/globalLongShortAccountRatio",
                        params={"symbol":symbol,"period":"5m"}, timeout=10) as r:
                    if r.status==200:
                        d = await r.json()
                        if d and isinstance(d, list): retail_long = float(d[-1].get("longAccount", 0.5))
            except: pass
            try:
                async with self.session.get(f"{self.FUTURES_URL}/futures/data/topLongShortAccountRatio",
                        params={"symbol":symbol,"period":"5m"}, timeout=10) as r:
                    if r.status==200:
                        d = await r.json()
                        if d and isinstance(d, list): whale_long = float(d[-1].get("longAccount", 0.5))
            except: pass
            result = {"retail_long_pct": retail_long*100, "whale_long_pct": whale_long*100,
                      "divergence": (whale_long-retail_long)*100}
            await cache.set(key, result, 300); return result
        except Exception as e: logger.debug(f"LS Ratio {symbol}: {e}")
        return None

    async def get_open_interest_change(self, symbol):
        try:
            async with self.session.get(f"{self.FUTURES_URL}/fapi/v1/openInterest",
                    params={"symbol":symbol}, timeout=10) as r:
                if r.status==200:
                    data = await r.json()
                    cur = float(data.get("openInterest", 0))
                    prev = self.oi_cache.get(symbol, cur)
                    self.oi_cache[symbol] = cur
                    return ((cur-prev)/prev)*100 if prev > 0 else 0
        except Exception as e: logger.debug(f"OI {symbol}: {e}")
        return None

    async def get_taker_ratio(self, symbol):
        key = f"taker_{symbol}"; cached = await cache.get(key)
        if cached: return cached
        try:
            async with self.session.get(f"{self.FUTURES_URL}/futures/data/takerlongshortRatio",
                    params={"symbol":symbol,"period":"5m"}, timeout=10) as r:
                if r.status==200:
                    d = await r.json()
                    if d and isinstance(d, list):
                        ratio = float(d[-1].get("buySellRatio", 1.0))
                        await cache.set(key, ratio, 300); return ratio
        except Exception as e: logger.debug(f"Taker {symbol}: {e}")
        return None

    async def subscribe_websocket(self):
        while True:
            try:
                async with coins_lock:
                    active = [c.binance_symbol.lower() for c in coins_data.values() if c.available_exchanges][:MAX_COINS_TRACKED]
                if not active: await asyncio.sleep(30); continue
                streams = ["!miniTicker@arr"] + [f"{s}@kline_5m" for s in active[:20]]
                url = f"{self.COMBINED_WS}{'/'.join(streams)}"
                logger.info(f"Binance WS: {len(streams)} streams")
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    self.ws = ws
                    async for msg in ws:
                        try: await self._handle_message(json.loads(msg))
                        except Exception as e: logger.debug(f"WS parse: {e}")
            except websockets.ConnectionClosed:
                logger.warning("Binance WS: Reconnecting..."); await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Binance WS: {e}"); await asyncio.sleep(10)

    async def _handle_message(self, msg):
        stream = msg.get("stream", ""); data = msg.get("data", {})
        if stream == "!miniTicker@arr":
            if isinstance(data, list):
                for item in data:
                    sym = item.get("s", "")
                    try: price = float(item.get("c", 0))
                    except: continue
                    await self._update_price(sym, price)
        elif "@kline_5m" in stream:
            k = data.get("k", {})
            if k.get("x", False):
                sym = k.get("s", "")
                try:
                    candle = Candle(int(k.get("t",0)),float(k.get("o",0)),float(k.get("h",0)),
                                    float(k.get("l",0)),float(k.get("c",0)),float(k.get("v",0)))
                    await self._add_candle(sym, candle)
                except Exception as e: logger.debug(f"Candle parse: {e}")

    async def _update_price(self, symbol, price):
        async with coins_lock:
            for coin in coins_data.values():
                if coin.binance_symbol == symbol: coin.price = price; break

    async def _add_candle(self, symbol, candle):
        async with coins_lock:
            for coin in coins_data.values():
                if coin.binance_symbol == symbol:
                    coin.candles.append(candle)
                    if len(coin.candles) > MAX_CANDLES_PER_COIN:
                        coin.candles = coin.candles[-MAX_CANDLES_PER_COIN:]
                    coin_ref = coin; break
            else: return
        try: await analyze_coin(coin_ref)
        except Exception as e: logger.debug(f"Analyze trigger {symbol}: {e}")

# ==============================================================================
# COINGECKO ENGINE
# ==============================================================================
class CoinGeckoEngine:
    BASE_URL = "https://api.coingecko.com/api/v3"
    def __init__(self): self.session = None
    async def init(self): self.session = aiohttp.ClientSession()
    async def close(self):
        if self.session: await self.session.close()

    async def get_midcap_coins(self):
        cached = await cache.get("coingecko_midcap")
        if cached: return cached
        try:
            async with self.session.get(f"{self.BASE_URL}/coins/markets",
                    params={"vs_currency":"usd","order":"market_cap_desc","per_page":250,"page":1,"sparkline":"false"}, timeout=30) as r:
                if r.status == 200:
                    data = await r.json()
                    midcap = [{"symbol":c["symbol"].upper(),"market_cap":c.get("market_cap",0) or 0,
                               "price":c.get("current_price",0) or 0,"volume":c.get("total_volume",0) or 0,
                               "circulating_supply":c.get("circulating_supply",0) or 0,
                               "total_supply":c.get("total_supply",0) or 0}
                              for c in data
                              if MIN_MARKET_CAP <= (c.get("market_cap",0) or 0) <= MAX_MARKET_CAP
                              and c.get("symbol","").isascii() and c.get("symbol","").isalnum()
                              and 2 <= len(c.get("symbol","")) <= 10]
                    await cache.set("coingecko_midcap", midcap, 300); return midcap
                elif r.status == 429:
                    logger.warning("CoinGecko: Rate limited"); await cache.set("coingecko_midcap", [], 60); return []
        except Exception as e: logger.error(f"CoinGecko: {e}")
        return []

    async def poll_loop(self):
        while True:
            try:
                midcap = await self.get_midcap_coins()
                binance_pairs = await binance.get_exchange_info()
                async with coins_lock:
                    current_symbols = {c["symbol"] for c in midcap}
                    for c in midcap:
                        sym = c["symbol"]; b_sym = f"{sym}USDT"
                        if b_sym not in binance_pairs: continue
                        if sym not in coins_data and len(coins_data) < MAX_COINS_TRACKED:
                            coins_data[sym] = CoinData(symbol=sym, binance_symbol=b_sym,
                                market_cap=c["market_cap"], price=c["price"], volume_24h=c["volume"],
                                circulating_supply=c["circulating_supply"], total_supply=c["total_supply"])
                            asyncio.create_task(load_initial_candles(sym))
                        elif sym in coins_data:
                            coins_data[sym].market_cap = c["market_cap"]
                            coins_data[sym].volume_24h  = c["volume"]
                            coins_data[sym].circulating_supply = c["circulating_supply"]
                            coins_data[sym].total_supply = c["total_supply"]
                    for s in [s for s in coins_data if s not in current_symbols]: del coins_data[s]
            except Exception as e: logger.error(f"CoinGecko poll: {e}")
            await asyncio.sleep(COINGECKO_POLL_SEC)

async def load_initial_candles(symbol):
    try:
        async with coins_lock:
            coin = coins_data.get(symbol)
            if not coin or coin.candles: return
            b_sym = coin.binance_symbol
        candles    = await binance.get_initial_klines(b_sym)
        candles_4h = await binance.get_htf_candles(b_sym, "4h", 24)
        candles_d1 = await binance.get_htf_candles(b_sym, "1d", 14)
        if candles:
            structure = MarketStructure()
            highs, lows = SMCAnalyzer.detect_swings(candles)
            structure.swing_highs = highs; structure.swing_lows = lows
            bullish_bos = bearish_bos = 0
            for i in range(20, len(candles)):
                seg_h = [s for s in highs if s.timestamp <= candles[i].timestamp]
                seg_l = [s for s in lows  if s.timestamp <= candles[i].timestamp]
                bos = SMCAnalyzer.detect_bos(candles[:i+1][-3:], seg_h, seg_l)
                if bos:
                    if bos["type"] == "bullish":
                        bullish_bos += 1; structure.trend = "BULLISH"; structure.last_bos = bos
                        choch = SMCAnalyzer.detect_choch(bos, "BEARISH" if bearish_bos > bullish_bos else "BULLISH")
                        if choch: structure.last_choch = choch
                    else:
                        bearish_bos += 1; structure.trend = "BEARISH"; structure.last_bos = bos
                        choch = SMCAnalyzer.detect_choch(bos, "BULLISH" if bullish_bos > bearish_bos else "BEARISH")
                        if choch: structure.last_choch = choch
            async with coins_lock:
                if symbol in coins_data:
                    coins_data[symbol].candles   = candles
                    coins_data[symbol].structure = structure
                    if candles_4h: coins_data[symbol].candles_4h = candles_4h
                    if candles_d1: coins_data[symbol].candles_d1 = candles_d1
            logger.info(f"Loaded {symbol}: {len(candles)}c 4H:{len(candles_4h)} D1:{len(candles_d1)} trend={structure.trend} CHoCH={'✓' if structure.last_choch else '✗'}")
    except Exception as e: logger.debug(f"Load candles {symbol}: {e}")

# ==============================================================================
# EXCHANGE VALIDATOR
# ==============================================================================
class ExchangeValidator:
    BINANCE_URL = "https://api.binance.com/api/v3/exchangeInfo"
    BITGET_URL  = "https://api.bitget.com/api/v2/spot/public/symbols"
    GATEIO_URL  = "https://api.gateio.ws/api/v4/spot/currency_pairs"
    def __init__(self):
        self.session = None
        self.binance_pairs = set(); self.bitget_pairs = set(); self.gateio_pairs = set()
    async def init(self): self.session = aiohttp.ClientSession()
    async def close(self):
        if self.session: await self.session.close()

    async def fetch_binance(self):
        key = "validator_binance"; cached = await cache.get(key)
        if cached: self.binance_pairs = cached; return
        try:
            async with self.session.get(self.BINANCE_URL, timeout=30) as r:
                if r.status == 200:
                    data = await r.json()
                    self.binance_pairs = {s["symbol"] for s in data.get("symbols",[]) if s["quoteAsset"]=="USDT" and s["status"]=="TRADING"}
                    await cache.set(key, self.binance_pairs, 86400)
        except Exception as e: logger.error(f"Binance validator: {e}")

    async def fetch_bitget(self):
        key = "validator_bitget"; cached = await cache.get(key)
        if cached: self.bitget_pairs = cached; return
        try:
            async with self.session.get(self.BITGET_URL, timeout=30) as r:
                if r.status == 200:
                    data = await r.json()
                    self.bitget_pairs = {s["symbol"] for s in data.get("data",[]) if s.get("quoteCoin")=="USDT" and s.get("status")=="normal"}
                    await cache.set(key, self.bitget_pairs, 86400)
        except Exception as e: logger.error(f"Bitget validator: {e}")

    async def fetch_gateio(self):
        key = "validator_gateio"; cached = await cache.get(key)
        if cached: self.gateio_pairs = cached; return
        try:
            async with self.session.get(self.GATEIO_URL, timeout=30) as r:
                if r.status == 200:
                    data = await r.json()
                    self.gateio_pairs = {f"{p['base']}USDT" for p in data if p.get("quote")=="USDT"}
                    await cache.set(key, self.gateio_pairs, 86400)
        except Exception as e: logger.error(f"Gate.io validator: {e}")

    def get_available(self, symbol):
        pair = f"{symbol}USDT"; avail = set()
        if pair in self.binance_pairs: avail.add("binance")
        if pair in self.bitget_pairs:  avail.add("bitget")
        if pair in self.gateio_pairs:  avail.add("gateio")
        return avail

    async def validator_loop(self):
        while True:
            try:
                await self.fetch_binance(); await self.fetch_bitget(); await self.fetch_gateio()
                async with coins_lock:
                    removed = []
                    for sym, coin in coins_data.items():
                        coin.available_exchanges = self.get_available(sym)
                        if not coin.available_exchanges: removed.append(sym)
                    for sym in removed: del coins_data[sym]; logger.info(f"Removed {sym}: not on exchanges")
            except Exception as e: logger.error(f"Validator: {e}")
            await asyncio.sleep(1800)

# ==============================================================================
# FUNDAMENTAL ENGINE
# ==============================================================================
class BinanceFundamentalEngine:
    async def update_all_fundamentals(self):
        while True:
            try:
                async with coins_lock:
                    symbols = [(c.symbol, c.binance_symbol) for c in coins_data.values() if c.available_exchanges][:30]
                for sym, b_sym in symbols:
                    try:
                        ls = await binance.get_long_short_ratio(b_sym)
                        if ls:
                            async with coins_lock:
                                if sym in coins_data:
                                    coins_data[sym].retail_long_pct = ls["retail_long_pct"]
                                    coins_data[sym].whale_long_pct  = ls["whale_long_pct"]
                        taker = await binance.get_taker_ratio(b_sym)
                        if taker is not None:
                            async with coins_lock:
                                if sym in coins_data: coins_data[sym].taker_buy_ratio = taker
                        oi = await binance.get_open_interest_change(b_sym)
                        if oi is not None:
                            async with coins_lock:
                                if sym in coins_data: coins_data[sym].oi_change_pct = oi
                        await asyncio.sleep(1)
                    except Exception as e: logger.debug(f"Fundamental {sym}: {e}")
            except Exception as e: logger.error(f"Fundamental Update: {e}")
            await asyncio.sleep(FUNDAMENTAL_POLL_SEC)

# ==============================================================================
# SMC ANALYZER
# ==============================================================================
class SMCAnalyzer:

    @staticmethod
    def detect_swings(candles, lookback=SWING_LOOKBACK):
        highs, lows = [], []
        if len(candles) < lookback*2+1: return highs, lows
        for i in range(lookback, len(candles)-lookback):
            c = candles[i]
            wh = [candles[j].high for j in range(i-lookback, i+lookback+1)]
            if c.high == max(wh) and not any(abs(sh.price-c.high)/c.high < 0.005 for sh in highs[-5:]):
                highs.append(SwingPoint(c.high, c.timestamp, i))
            wl = [candles[j].low for j in range(i-lookback, i+lookback+1)]
            if c.low == min(wl) and not any(abs(sl.price-c.low)/c.low < 0.005 for sl in lows[-5:]):
                lows.append(SwingPoint(c.low, c.timestamp, i))
        return highs[-20:], lows[-20:]

    @staticmethod
    def detect_bos(candles, swing_highs, swing_lows):
        if len(candles) < 3 or not swing_highs or not swing_lows: return None
        cur = candles[-1]; prev = candles[-2]
        if swing_highs:
            lh = swing_highs[-1].price
            if prev.close <= lh and cur.close > lh:
                return {"type":"bullish","price":lh,"timestamp":cur.timestamp,"strength":min(1.0,(cur.close-lh)/lh*100)}
        if swing_lows:
            ll = swing_lows[-1].price
            if prev.close >= ll and cur.close < ll:
                return {"type":"bearish","price":ll,"timestamp":cur.timestamp,"strength":min(1.0,(ll-cur.close)/ll*100)}
        return None

    @staticmethod
    def detect_choch(bos, trend):
        if not bos: return None
        if trend == "BEARISH" and bos["type"] == "bullish":
            return {"type":"bullish","timestamp":bos["timestamp"],"price":bos["price"],"strength":bos["strength"]}
        if trend == "BULLISH" and bos["type"] == "bearish":
            return {"type":"bearish","timestamp":bos["timestamp"],"price":bos["price"],"strength":bos["strength"]}
        return None

    @staticmethod
    def detect_fvgs(candles):
        fvgs = []
        if len(candles) < 3: return fvgs
        for i in range(max(1, len(candles)-12), len(candles)-2):
            c1, c2, c3 = candles[i], candles[i+1], candles[i+2]
            if c3.low > c1.high:
                gap = (c3.low-c1.high)/c1.high if c1.high > 0 else 0
                if gap >= FVG_MIN_GAP_PCT: fvgs.append(FVG("bullish", c3.low, c1.high, c2.timestamp))
            if c3.high < c1.low:
                gap = (c1.low-c3.high)/c1.low if c1.low > 0 else 0
                if gap >= FVG_MIN_GAP_PCT: fvgs.append(FVG("bearish", c1.low, c3.high, c2.timestamp))
        return fvgs

    @staticmethod
    def detect_obs(candles):
        obs = []
        if len(candles) < 5: return obs
        recent = [abs(c.high-c.low) for c in candles[-20:]]
        avg_range = sum(recent)/len(recent) if recent else 0.001
        for i in range(max(1, len(candles)-20), len(candles)-3):
            c = candles[i]
            if c.close < c.open:
                bc = bull_count = 0; mx = c.open
                for j in range(i+1, min(i+5, len(candles))):
                    nc = candles[j]
                    if nc.close > nc.open: bull_count += 1; bc += nc.close-nc.open; mx = max(mx, nc.close)
                total = mx - c.low
                if bull_count >= 2 and total >= avg_range*1.5 and c.open > 0 and total/c.open >= OB_MIN_DISPLACEMENT:
                    obs.append(OrderBlock("bullish", c.high, c.low, c.timestamp))
        return obs

    @staticmethod
    def detect_liquidity_sweep(candles, swing_lows, swing_highs):
        if len(candles) < 3: return None
        if swing_lows:
            for cur in reversed(candles[-5:]):
                body = abs(cur.close-cur.open) or 0.000001
                rl = swing_lows[-1].price
                lw = min(cur.open, cur.close) - cur.low
                if cur.low < rl and cur.close > rl and lw/body >= SWEEP_WICK_RATIO:
                    return {"type":"bullish","price":rl,"timestamp":cur.timestamp,
                            "depth":(rl-cur.low)/rl if rl > 0 else 0,"wick_ratio":lw/body}
        if swing_highs:
            for cur in reversed(candles[-5:]):
                body = abs(cur.close-cur.open) or 0.000001
                rh = swing_highs[-1].price
                uw = cur.high - max(cur.open, cur.close)
                if cur.high > rh and cur.close < rh and uw/body >= SWEEP_WICK_RATIO:
                    return {"type":"bearish","price":rh,"timestamp":cur.timestamp,
                            "depth":(cur.high-rh)/rh if rh > 0 else 0,"wick_ratio":uw/body}
        return None

    # ============================================================
    # NEW v3.1: REVERSAL CANDLE DETECTOR (alternative to sweep)
    # ============================================================
    @staticmethod
    def detect_reversal_signals(candles: List["Candle"]) -> Optional[dict]:
        """
        Alternatif kepada liquidity sweep.
        Detect: Bullish Engulfing | Hammer/Pin Bar | Morning Star
        Return format sama seperti sweep dict supaya boleh guna OR logic.
        """
        if len(candles) < 3:
            return None

        last  = candles[-1]
        prev  = candles[-2]

        # ── 1. BULLISH ENGULFING ──────────────────────────────────────────────
        # Candle merah diikuti candle hijau yang fully engulf body sebelumnya
        if (prev.close < prev.open and          # sebelumnya bearish
                last.close > last.open and      # sekarang bullish
                last.open  <= prev.close and    # buka di bawah/sama close prev
                last.close >= prev.open):       # tutup di atas/sama open prev
            synthetic_wick = abs(last.close - last.open)
            body_prev      = abs(prev.open  - prev.close) or 0.000001
            return {
                "type"      : "bullish",
                "price"     : min(last.low, prev.low),
                "timestamp" : last.timestamp,
                "depth"     : synthetic_wick / body_prev,
                "wick_ratio": max(SWEEP_WICK_RATIO, synthetic_wick / body_prev),
                "signal"    : "ENGULFING",
            }

        # ── 2. HAMMER / PIN BAR ──────────────────────────────────────────────
        # Lower wick >= 2x body, upper wick <= 50% body
        body      = abs(last.close - last.open)
        if body > 0:
            lower_wick = min(last.open, last.close) - last.low
            upper_wick = last.high - max(last.open, last.close)
            if lower_wick >= 2.0 * body and upper_wick <= body * 0.5:
                return {
                    "type"      : "bullish",
                    "price"     : last.low,
                    "timestamp" : last.timestamp,
                    "depth"     : lower_wick / (last.low if last.low > 0 else 1),
                    "wick_ratio": lower_wick / body,
                    "signal"    : "HAMMER",
                }

        # ── 3. MORNING STAR (3-candle) ───────────────────────────────────────
        # C1 bearish besar | C2 badan kecil (doji/star) | C3 bullish tutup > mid C1
        if len(candles) >= 3:
            c1, c2, c3 = candles[-3], candles[-2], candles[-1]
            c1_body = abs(c1.close - c1.open)
            c2_body = abs(c2.close - c2.open)
            if (c1.close < c1.open and
                    c2_body <= c1_body * 0.5 and
                    c3.close > c3.open and
                    c3.close >= (c1.open + c1.close) / 2):
                return {
                    "type"      : "bullish",
                    "price"     : min(c2.low, c3.low),
                    "timestamp" : c3.timestamp,
                    "depth"     : c3_body / c1_body if c1_body > 0 else 0,
                    "wick_ratio": 1.8,
                    "signal"    : "MORNING_STAR",
                }

        return None

    @staticmethod
    def get_premium_discount(swing_highs, swing_lows):
        if not swing_highs or not swing_lows: return None
        highest = max(sh.price for sh in swing_highs[-5:])
        lowest  = min(sl.price for sl in swing_lows[-5:])
        if highest <= lowest: return None
        return (highest + lowest) / 2

    @staticmethod
    def check_mitigation(zones, current_price):
        for z in zones:
            if not z.mitigated and z.low <= current_price <= z.high: z.mitigated = True

    @staticmethod
    def aggregate_candles(candles, factor):
        result = []
        for i in range(0, len(candles)-factor+1, factor):
            g = candles[i:i+factor]
            result.append(Candle(g[0].timestamp, g[0].open, max(c.high for c in g),
                                 min(c.low for c in g), g[-1].close, sum(c.volume for c in g)))
        return result

    @staticmethod
    def get_mtf_sr_levels(candles):
        levels = []
        for factor in [3, 6, 12]:
            agg = SMCAnalyzer.aggregate_candles(candles, factor)
            if len(agg) < 7: continue
            highs, lows = SMCAnalyzer.detect_swings(agg, lookback=2)
            for sp in highs[-4:]:
                if not any(abs(sp.price-p)/p < 0.008 for _, p in levels):
                    levels.append(("resistance", sp.price))
            for sp in lows[-4:]:
                if not any(abs(sp.price-p)/p < 0.008 for _, p in levels):
                    levels.append(("support", sp.price))
        return levels

    @staticmethod
    def detect_pullback_zone(candles, mtf_levels, current_price):
        if len(candles) < 10: return None
        for level_type, level_price in mtf_levels:
            if level_type != "resistance": continue
            broke = any(c.close > level_price*1.002 for c in candles[-20:-3])
            if not broke: continue
            if level_price*0.997 <= current_price <= level_price*1.015:
                if all(c.close >= level_price*0.994 for c in candles[-3:]):
                    return {"zone_high":level_price*1.012,"zone_low":level_price*0.996,
                            "level":level_price,"signal_type":"PULLBACK ZONE"}
        return None

    @staticmethod
    def detect_retest_zone(candles, mtf_levels, current_price):
        if len(candles) < 6: return None
        for level_type, level_price in mtf_levels:
            if level_type != "resistance": continue
            bo_idx = None
            for i in range(max(0, len(candles)-10), len(candles)-2):
                c = candles[i]
                if c.close > level_price*1.003 and c.open <= level_price*1.005:
                    bo_idx = i; break
            if bo_idx is None: continue
            wick_tests = sum(1 for c in candles[bo_idx+1:]
                             if c.low <= level_price*1.010 and c.close > level_price*0.997)
            if wick_tests < 1: continue
            last = candles[-1]
            # FIX BUG1: guna level_price bukan level (NameError sebelum ini)
            if (last.close >= level_price * 0.988 and
                    last.low   <= level_price * 1.012 and
                    current_price >= level_price * 0.990):
                return {"zone_high": level_price * 1.013,
                        "zone_low":  level_price * 0.990,
                        "level":     level_price,
                        "signal_type": "RETEST ZONE"}
        return None

    @staticmethod
    def detect_breakout_zone(candles, mtf_levels, current_price):
        if len(candles) < 3: return None
        last = candles[-1]; prev = candles[-2]
        for level_type, level_price in mtf_levels:
            if level_type != "resistance": continue
            if prev.close <= level_price*1.005 and last.close > level_price*1.003 and current_price <= level_price*1.025:
                return {"zone_high":level_price*1.025,"zone_low":level_price*1.001,
                        "level":level_price,"signal_type":"BREAKOUT ZONE"}
        return None

    @staticmethod
    def get_htf_bias(candles_4h, candles_d1):
        bias = {"d1":"NEUTRAL","h4":"NEUTRAL","score":0,"notes":[]}
        for candles, label, pts in [(candles_d1,"D1",15),(candles_4h,"4H",10)]:
            if len(candles) < 5: bias["notes"].append(f"{label}:tiada data"); continue
            highs, lows = SMCAnalyzer.detect_swings(candles, lookback=2)
            if len(highs) >= 2 and len(lows) >= 2:
                hh = highs[-1].price > highs[-2].price; hl = lows[-1].price > lows[-2].price
                lh = highs[-1].price < highs[-2].price; ll = lows[-1].price < lows[-2].price
                key = "d1" if label == "D1" else "h4"
                if hh and hl:
                    bias[key]="BULLISH"; bias["score"]+=pts; bias["notes"].append(f"{label}:HH+HL(+{pts})")
                elif lh and ll:
                    bias[key]="BEARISH"; bias["score"]-=pts; bias["notes"].append(f"{label}:LH+LL(-{pts})")
                else: bias["notes"].append(f"{label}:NEUTRAL")
            else: bias["notes"].append(f"{label}:swing<2")
        return bias

    @staticmethod
    def get_session_bonus():
        h = datetime.utcnow().hour
        if 7 <= h <= 10 or 12 <= h <= 15: return 3
        if 1 <= h <= 5: return -3
        return 0

    @classmethod
    def analyze_coin(cls, coin: CoinData) -> Tuple[Optional[SMCSetup], str]:
        if len(coin.candles) < 30:
            return None, f"candle kurang: {len(coin.candles)}/30"

        candles = coin.candles; structure = coin.structure
        current_price = candles[-1].close

        new_h, new_l = cls.detect_swings(candles)
        if new_h: structure.swing_highs = new_h
        if new_l: structure.swing_lows  = new_l

        bos = cls.detect_bos(candles, structure.swing_highs, structure.swing_lows)
        if bos:
            structure.last_bos = bos
            choch = cls.detect_choch(bos, structure.trend)
            if choch: structure.last_choch = choch
            structure.trend = "BULLISH" if bos["type"]=="bullish" else "BEARISH"

        new_fvgs = cls.detect_fvgs(candles)
        structure.active_fvgs.extend(new_fvgs)
        structure.active_fvgs = [f for f in structure.active_fvgs if not f.mitigated][-10:]
        new_obs = cls.detect_obs(candles)
        structure.active_obs.extend(new_obs)
        structure.active_obs = [o for o in structure.active_obs if not o.mitigated][-10:]
        cls.check_mitigation(structure.active_fvgs, current_price)
        cls.check_mitigation(structure.active_obs, current_price)

        sweep       = cls.detect_liquidity_sweep(candles, structure.swing_lows, structure.swing_highs)
        equilibrium = cls.get_premium_discount(structure.swing_highs, structure.swing_lows)
        mtf_levels  = cls.get_mtf_sr_levels(candles)

        # ── NEW v3.1: sweep ATAU reversal candle ────────────────────────────
        bullish_sweep    = sweep if (sweep and sweep["type"] == "bullish") else None
        reversal_candle  = cls.detect_reversal_signals(candles) if not bullish_sweep else None
        # confirmation = sweep (lebih kuat) atau reversal candle (fallback)
        confirmation     = bullish_sweep or reversal_candle
        reversal_type    = "SWEEP" if bullish_sweep else (reversal_candle["signal"] if reversal_candle else "NONE")
        # Synthetic sweep-like dict untuk scoring jika hanya ada reversal candle
        conf_for_scoring = confirmation  # boleh None jika tiada langsung

        entry_high = entry_low = sl_price = None
        signal_type = "FVG ZONE"

        # ── METHOD 1 & 2 — SMC (FVG / OB): perlukan CHoCH + confirmation + equilibrium
        if (structure.last_choch and structure.last_choch["type"] == "bullish" and
                structure.trend == "BULLISH" and confirmation and equilibrium):

            for fvg in structure.active_fvgs:
                if (fvg.type == "bullish" and not fvg.mitigated and
                        fvg.high < equilibrium and current_price <= fvg.high*1.012):
                    entry_high = fvg.high; entry_low = fvg.low
                    sl_price   = confirmation["price"] * 0.998
                    signal_type = "FVG ZONE"; break

            if not entry_high:
                for ob in structure.active_obs:
                    if (ob.type == "bullish" and not ob.mitigated and
                            ob.high < equilibrium and current_price <= ob.high*1.012):
                        entry_high = ob.high; entry_low = ob.low
                        sl_price   = confirmation["price"] * 0.998
                        signal_type = "OB ZONE"; break

        # ── METHOD 3, 4, 5 — MTF S/R: hanya perlukan trend BULLISH
        if not entry_high and structure.trend == "BULLISH":
            retest = cls.detect_retest_zone(candles, mtf_levels, current_price)
            if retest and current_price <= retest["zone_high"]*1.012:
                entry_high = retest["zone_high"]; entry_low = retest["zone_low"]
                sl_price   = retest["level"] * 0.994; signal_type = retest["signal_type"]

            if not entry_high:
                pullback = cls.detect_pullback_zone(candles, mtf_levels, current_price)
                if pullback and current_price <= pullback["zone_high"]*1.012:
                    entry_high = pullback["zone_high"]; entry_low = pullback["zone_low"]
                    sl_price   = pullback["level"] * 0.994; signal_type = pullback["signal_type"]

            if not entry_high:
                breakout = cls.detect_breakout_zone(candles, mtf_levels, current_price)
                if breakout:
                    entry_high = breakout["zone_high"]; entry_low = breakout["zone_low"]
                    sl_price   = breakout["level"] * 0.993; signal_type = breakout["signal_type"]

        # ── METHOD 6 — WICK REJECTION: confirmation (sweep atau reversal) kuat
        if not entry_high and confirmation and confirmation["type"] == "bullish":
            sweep_candle = None
            ref_price = confirmation["price"]
            for c in reversed(candles[-5:]):
                body     = abs(c.close-c.open) or 0.000001
                low_wick = min(c.open,c.close) - c.low
                if c.low < ref_price and c.close > ref_price and low_wick/body >= SWEEP_WICK_RATIO:
                    sweep_candle = c; break
            # Untuk reversal candle, candle itu sendiri adalah sweep_candle
            if sweep_candle is None and reversal_candle:
                sweep_candle = candles[-1]
            if sweep_candle:
                rej_entry = sweep_candle.close
                rej_sl    = min(sweep_candle.low, ref_price) * 0.998
                rej_risk  = rej_entry - rej_sl
                if rej_risk > 0 and current_price <= rej_entry*1.015 and (rej_entry+rej_risk*MIN_RR_RATIO) > rej_entry:
                    entry_high = rej_entry*1.003; entry_low = ref_price
                    sl_price   = rej_sl; signal_type = "WICK REJECTION"

        if not entry_high or sl_price is None or entry_low is None:
            choch_s = "✓" if structure.last_choch else "✗"
            sweep_s = f"✓{sweep['type'][0].upper()}" if sweep else "✗"
            rev_s   = reversal_candle["signal"] if reversal_candle else "✗"
            fvg_c   = len([f for f in structure.active_fvgs if not f.mitigated])
            ob_c    = len([o for o in structure.active_obs  if not o.mitigated])
            return None, (f"tiada entry zone | trend={structure.trend} CHoCH={choch_s} "
                          f"sweep={sweep_s} rev={rev_s} eq={'✓' if equilibrium else '✗'} "
                          f"FVG={fvg_c} OB={ob_c} MTF={len(mtf_levels)}")

        # FIX SL_FLOOR: SL mesti >= 1.5% atau 1x ATR14 bawah entry — elak market noise
        _atr14   = sum(abs(c.high - c.low) for c in candles[-14:]) / 14 if len(candles) >= 14 else entry_high * 0.015
        _sl_floor = min(entry_high * (1 - MIN_SL_PCT), entry_high - _atr14)
        sl_price  = min(sl_price, _sl_floor)

        # v3.3: Progressive SL widening berdasarkan consecutive SL streak
        _stk       = sl_streak.get(coin.symbol, {})
        _stk_count = _stk.get("count", 0)
        if _stk_count >= 4:
            return None, f"streak block: {_stk_count}x SL berturut — blacklist 24j"
        elif _stk_count == 3:
            sl_price = min(sl_price, entry_high - (_atr14 * 2.0))
        elif _stk_count == 2:
            sl_price = min(sl_price, entry_high - (_atr14 * 1.5))

        entry = entry_high; risk = entry - sl_price
        if risk <= 0: return None, f"risk≤0: entry={entry:.6f} sl={sl_price:.6f}"

        tp1 = entry + risk*2; tp2 = entry + risk*3; tp3 = entry + risk*5
        if len(structure.swing_highs) >= 2:
            for sh in reversed(structure.swing_highs):
                if sh.price > entry: tp1 = sh.price; break
        # FIX TP ORDER: pastikan tp2 > tp1 dan tp3 > tp2 — elak urutan terbalik
        tp2 = max(entry + risk*3, tp1 * 1.005)
        tp3 = max(entry + risk*5, tp2 * 1.005)

        rr = (tp1-entry)/risk if risk > 0 else 0
        if rr < MIN_RR_RATIO:
            return None, f"R:R rendah: {rr:.2f}<{MIN_RR_RATIO}"

        if time.time() - sent_signals.get(coin.symbol, 0) < SIGNAL_COOLDOWN_SEC:
            sisa = SIGNAL_COOLDOWN_SEC - (time.time()-sent_signals.get(coin.symbol,0))
            return None, f"cooldown: {sisa/60:.0f}min lagi"

        if entry_low*0.997 <= current_price <= entry_high:
            signal_type = signal_type + " ⚡ FAST EXEC"

        # Scoring
        base_sig = signal_type.replace(" ⚡ FAST EXEC","").strip()
        wr_val = conf_for_scoring["wick_ratio"] if conf_for_scoring else 0
        if structure.last_choch and conf_for_scoring:
            structure_score = min(100, structure.last_choch["strength"]*50 + wr_val*10)
        elif base_sig == "RETEST ZONE":   structure_score = 45
        elif base_sig == "PULLBACK ZONE": structure_score = 40
        elif base_sig == "BREAKOUT ZONE": structure_score = 35
        elif base_sig == "WICK REJECTION":
            structure_score = min(100, 30 + wr_val*12)
        else:
            choch_part = structure.last_choch["strength"]*40 if structure.last_choch else 18
            structure_score = min(100, choch_part + wr_val*8)

        # Reversal-only setup: skor asas lebih rendah (tiada sweep kuat) — transparensi
        if reversal_type != "SWEEP":
            structure_score = max(0, structure_score - 8)

        # FIX CHoCH: penalti -20 jika structure masih bearish (catching knife risk)
        if structure.last_choch and structure.last_choch["type"] == "bearish":
            structure_score = max(0, structure_score - 20)

        # v3.3: streak penalti skor
        if _stk_count == 2:   structure_score = max(0, structure_score - 15)
        elif _stk_count == 3: structure_score = max(0, structure_score - 30)

        # v3.3: rolling win rate penalti (dikemas oleh scan_loop)
        if rolling_wr_penalty > 0:
            structure_score = max(0, structure_score - rolling_wr_penalty)

        sentiment_score = min(100, (coin.whale_long_pct - coin.retail_long_pct + 50))

        return SMCSetup(
            symbol=coin.symbol, direction="LONG", timestamp=datetime.now(),
            entry_price=entry, sl_price=sl_price,
            tp1_price=tp1, tp2_price=tp2, tp3_price=tp3,
            choch_type=structure.last_choch["type"] if structure.last_choch else "bullish",
            sweep_price=conf_for_scoring["price"] if conf_for_scoring else entry*0.99,
            entry_zone_type=signal_type, zone_high=entry_high, zone_low=entry_low,
            rr_ratio=rr, structure_score=structure_score,
            fundamental_score=0, sentiment_score=sentiment_score,
            signal_type=signal_type, reversal_type=reversal_type
        ), f"setup ditemui: {signal_type} [{reversal_type}] @ {entry:.6f} RR={rr:.1f}"


# ==============================================================================
# FUNDAMENTAL ANALYZER
# ==============================================================================
class FundamentalAnalyzer:
    @staticmethod
    def analyze(coin: CoinData) -> FundamentalBias:
        reasons = []; score = 0.0
        if coin.retail_long_pct > 60.0 and coin.whale_long_pct < 45.0:
            reasons.append(f"🔴 RETAIL TRAP: Retail {coin.retail_long_pct:.0f}% Long, Whale {coin.whale_long_pct:.0f}% Long")
            score -= 40
        elif coin.retail_long_pct < 40.0 and coin.whale_long_pct > 55.0:
            reasons.append("🟢 WHALE ACCUMULATION: Retail takut, Whale kumpul"); score += 40
        else:
            div = coin.whale_long_pct - coin.retail_long_pct
            if div > 10: score += 20
            elif div < -10: score -= 20
        if coin.taker_buy_ratio > 1.10:
            reasons.append(f"🟢 Aggressive Buying (Ratio: {coin.taker_buy_ratio:.2f})"); score += 20
        elif coin.taker_buy_ratio < 0.90:
            reasons.append(f"🔴 Aggressive Selling (Ratio: {coin.taker_buy_ratio:.2f})"); score -= 20
        if coin.total_supply > 0 and coin.circulating_supply > 0:
            fr = coin.circulating_supply / coin.total_supply
            if fr < 0.40:
                reasons.append(f"⚠️ HIGH INFLATION: {fr*100:.0f}% circulating"); score -= 20
        bias = "BEARISH" if score <= -30 else ("BULLISH" if score >= 30 else "NEUTRAL")
        return FundamentalBias(symbol=coin.symbol, bias=bias, score=score, reasons=reasons)

# ==============================================================================
# SIGNAL COORDINATOR
# ==============================================================================
async def analyze_coin(coin: CoinData) -> dict:
    try:
        fund = FundamentalAnalyzer.analyze(coin)
        capped_fund = max(fund.score, -20.0)
        setup, smc_reason = SMCAnalyzer.analyze_coin(coin)
        if not setup:
            return {"status":"tiada_setup","reason":smc_reason,"score":0,"signal_type":"-","fund":fund.bias}

        htf        = SMCAnalyzer.get_htf_bias(coin.candles_4h, coin.candles_d1)
        htf_score  = max(-20, min(25, htf["score"]))
        sesi_bonus = SMCAnalyzer.get_session_bonus()
        setup.fundamental_score = capped_fund

        total_score = setup.structure_score + setup.sentiment_score + capped_fund + htf_score + sesi_bonus
        score_detail = (f"str={setup.structure_score:.0f} sent={setup.sentiment_score:.0f} "
                        f"fund={capped_fund:.0f} htf={htf_score:+d} sesi={sesi_bonus:+d} "
                        f"rev={setup.reversal_type}")
        htf_detail = f"D1={htf['d1']} 4H={htf['h4']}"
        base_sig   = setup.signal_type.replace(" ⚡ FAST EXEC","").strip()

        if total_score < 80:
            return {"status":"skor_rendah","reason":f"skor={total_score:.0f}/80 [{score_detail}] {htf_detail}",
                    "score":total_score,"signal_type":base_sig,"fund":fund.bias,"close":total_score >= 65}

        async with stats_lock:
            if time.time() - sent_signals.get(coin.symbol,0) < SIGNAL_COOLDOWN_SEC:
                sisa = SIGNAL_COOLDOWN_SEC-(time.time()-sent_signals.get(coin.symbol,0))
                return {"status":"cooldown","reason":f"{sisa/60:.0f}min lagi","score":total_score,"signal_type":base_sig,"fund":fund.bias}
            sent_signals[coin.symbol] = time.time()

        logger.info(f"🟢 [LULUS] {coin.symbol} | {base_sig} [{setup.reversal_type}] | skor={total_score:.0f} [{score_detail}] | {htf_detail}")
        await send_signal(setup, coin, fund)
        return {"status":"lulus","reason":score_detail,"score":total_score,"signal_type":base_sig,"fund":fund.bias}
    except Exception as e:
        logger.error(f"Analyze {coin.symbol}: {e}")
        return {"status":"error","reason":str(e),"score":0,"signal_type":"-","fund":"-"}

# ==============================================================================
# JOURNAL
# ==============================================================================
class Journal:
    def __init__(self, db_path="journal.db"):
        self.db_path = db_path; self.init_db()
    def init_db(self):
        global journal_db
        try:
            journal_db = sqlite3.connect(self.db_path, check_same_thread=False)
            c = journal_db.cursor()
            c.execute("""CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INTEGER UNIQUE,
                symbol TEXT, direction TEXT, entry_price REAL, sl_price REAL,
                tp1_price REAL, tp2_price REAL, tp3_price REAL,
                timestamp_open TEXT, timestamp_close TEXT, status TEXT DEFAULT 'ACTIVE',
                structure_score REAL, fund_score REAL, sentiment_score REAL,
                tp1_hit INTEGER DEFAULT 0, tp2_hit INTEGER DEFAULT 0, tp3_hit INTEGER DEFAULT 0,
                sl_hit INTEGER DEFAULT 0, final_pnl REAL DEFAULT 0.0, duration_minutes INTEGER DEFAULT 0,
                reversal_type TEXT DEFAULT 'SWEEP')""")
            c.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON signals(symbol)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_status ON signals(status)")
            # v3.3: migrate — tambah close_type jika DB lama belum ada
            try: c.execute("ALTER TABLE signals ADD COLUMN close_type TEXT DEFAULT 'SL_FULL'")
            except Exception: pass
            journal_db.commit(); logger.info("Journal DB initialized")
        except Exception as e: logger.error(f"Journal init: {e}")
    async def log_signal(self, setup: SMCSetup, msg_id: int):
        if not journal_db: return
        try:
            async with db_lock:
                c = journal_db.cursor()
                c.execute("""INSERT OR IGNORE INTO signals
                    (message_id,symbol,direction,entry_price,sl_price,tp1_price,tp2_price,tp3_price,
                     timestamp_open,structure_score,fund_score,sentiment_score,reversal_type)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (msg_id,setup.symbol,setup.direction,setup.entry_price,setup.sl_price,
                     setup.tp1_price,setup.tp2_price,setup.tp3_price,setup.timestamp.isoformat(),
                     setup.structure_score,setup.fundamental_score,setup.sentiment_score,setup.reversal_type))
                journal_db.commit()
        except Exception as e: logger.error(f"Log signal: {e}")
    async def update_tp(self, symbol, level):
        if not journal_db: return
        try:
            async with db_lock:
                c = journal_db.cursor()
                c.execute(f"UPDATE signals SET tp{level}_hit=1, status=? WHERE symbol=? AND status NOT IN ('CLOSED','SL_HIT')",
                          (f"TP{level}_HIT", symbol))
                journal_db.commit()
        except Exception as e: logger.error(f"Update TP: {e}")
    async def close_trade(self, symbol, status, pnl, duration, close_type="SL_FULL"):
        if not journal_db: return
        try:
            async with db_lock:
                c = journal_db.cursor()
                c.execute("UPDATE signals SET status=?,timestamp_close=?,final_pnl=?,duration_minutes=?,close_type=? WHERE symbol=? AND status NOT IN ('CLOSED','SL_FULL','SL_BE','SL_TP1','TP3_HIT')",
                          (status, datetime.now().isoformat(), pnl, duration, close_type, symbol))
                journal_db.commit()
        except Exception as e: logger.error(f"Close trade: {e}")
    def get_stats(self):
        if not journal_db: return {"total":0,"wins":0,"losses":0,"sl_be":0,"sl_tp1":0,"win_rate":0,"avg_pnl":0,"best":0,"worst":0}
        try:
            c = journal_db.cursor()
            c.execute("""SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status LIKE 'TP%' OR close_type IN ('SL_BE','SL_TP1') THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN close_type='SL_FULL' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN close_type='SL_BE'   THEN 1 ELSE 0 END) as sl_be,
                SUM(CASE WHEN close_type='SL_TP1'  THEN 1 ELSE 0 END) as sl_tp1,
                AVG(final_pnl), MAX(final_pnl), MIN(final_pnl)
                FROM signals""")
            total,wins,losses,sl_be,sl_tp1,avg_pnl,best,worst = c.fetchone()
            return {"total":total or 0,"wins":wins or 0,"losses":losses or 0,
                    "sl_be":sl_be or 0,"sl_tp1":sl_tp1 or 0,
                    "win_rate":(wins/total*100) if total else 0,
                    "avg_pnl":avg_pnl or 0,"best":best or 0,"worst":worst or 0}
        except: return {"total":0,"wins":0,"losses":0,"sl_be":0,"sl_tp1":0,"win_rate":0,"avg_pnl":0,"best":0,"worst":0}

# ==============================================================================
# AUTO-UPDATER
# ==============================================================================
class AutoUpdater:
    @staticmethod
    async def check_trades():
        global daily_pnl
        while True:
            try:
                async with trades_lock: trades_copy = list(active_trades.items())
                for msg_id, trade in trades_copy:
                    async with coins_lock: coin = coins_data.get(trade["symbol"])
                    if not coin or coin.price <= 0: continue
                    price = coin.price; status = trade["status"]
                    if status == "ACTIVE" and price >= trade["tp1"]:
                        await AutoUpdater.send_update(msg_id, 1, price, trade)
                        trade["status"] = "TP1_HIT"; trade["current_sl"] = trade["entry"]
                        await journal.update_tp(trade["symbol"], 1)
                    elif status == "TP1_HIT" and price >= trade["tp2"]:
                        await AutoUpdater.send_update(msg_id, 2, price, trade)
                        trade["status"] = "TP2_HIT"; trade["current_sl"] = trade["tp1"]
                        await journal.update_tp(trade["symbol"], 2)
                    elif status in ["TP1_HIT","TP2_HIT"] and price >= trade["tp3"]:
                        dur = (datetime.now()-trade["open_time"]).seconds//60
                        await AutoUpdater.send_update(msg_id, 3, price, trade, final=True, duration=dur)
                        pnl = ((price-trade["entry"])/trade["entry"])*100 if trade["entry"] > 0 else 0
                        await journal.close_trade(trade["symbol"],"TP3_HIT",pnl,dur,"TP3_HIT")
                        async with trades_lock: active_trades.pop(msg_id, None)
                        async with stats_lock: daily_pnl += pnl
                    elif price <= trade["current_sl"]:
                        dur = (datetime.now()-trade["open_time"]).seconds//60
                        # v3.3: tentukan jenis SL berdasarkan tahap trade
                        _close_map = {"ACTIVE":"SL_FULL","TP1_HIT":"SL_BE","TP2_HIT":"SL_TP1"}
                        close_type = _close_map.get(trade["status"], "SL_FULL")
                        pnl = ((price-trade["entry"])/trade["entry"])*100 if trade["entry"] > 0 else 0
                        await AutoUpdater.send_update(msg_id, 0, price, trade, sl_hit=True, duration=dur, close_type=close_type)
                        await journal.close_trade(trade["symbol"], close_type, pnl, dur, close_type)
                        # v3.3: kemas streak hanya untuk SL_FULL (rugi sebenar)
                        if close_type == "SL_FULL":
                            async with streak_lock:
                                sym = trade["symbol"]
                                prev = sl_streak.get(sym, {"count":0})
                                sl_streak[sym] = {
                                    "count":      prev["count"] + 1,
                                    "last_sl_time":  time.time(),
                                    "last_sl_price": trade["sl"],
                                    "sweep_check":   True
                                }
                                logger.info(f"[STREAK] {sym} SL#{sl_streak[sym]['count']}")
                        async with trades_lock: active_trades.pop(msg_id, None)
                        async with stats_lock: daily_pnl += pnl

                # v3.3: sweep recovery check — harga balik atas SL dalam 30 minit = stop hunt
                async with streak_lock:
                    for sym, stk in list(sl_streak.items()):
                        if not stk.get("sweep_check"): continue
                        if time.time() - stk.get("last_sl_time", 0) > 1800: continue
                        async with coins_lock: c_ref = coins_data.get(sym)
                        if c_ref and c_ref.price > stk["last_sl_price"] * 1.003:
                            old_count = stk["count"]
                            stk["count"]      = max(0, stk["count"] - 1)
                            stk["sweep_check"] = False
                            logger.info(f"[SWEEP RECOVERY] {sym} — streak {old_count}→{stk['count']}")

            except Exception as e: logger.error(f"Updater: {e}")
            await asyncio.sleep(10)

    @staticmethod
    async def send_update(msg_id, level, price, trade, final=False, sl_hit=False, duration=0, close_type="SL_FULL"):
        if not bot_app or not TELEGRAM_VVIP_CHANNEL: return
        def fmt(p): return f"{p:.2f}" if p>=100 else (f"{p:.4f}" if p>=1 else (f"{p:.6f}" if p>=0.01 else f"{p:.8f}"))
        entry = trade["entry"]; pnl = ((price-entry)/entry)*100 if entry > 0 else 0
        if sl_hit:
            if close_type == "SL_BE":
                msg = f"⚖️ <b>SL @ BREAK-EVEN</b> @ <code>{fmt(price)}</code>\n🛡️ Modal dilindungi selepas TP1\n📊 PnL: ≈0%\n⏱️ {duration}m"
            elif close_type == "SL_TP1":
                msg = f"🔒 <b>SL @ TP1 (UNTUNG TERKUNCI)</b> @ <code>{fmt(price)}</code>\n💰 PnL: +{pnl:.2f}%\n⏱️ {duration}m"
            else:
                msg = f"❌ <b>SL HIT</b> @ <code>{fmt(price)}</code>\n📉 PnL: {pnl:.2f}%\n⏱️ {duration}m"
        elif final: msg = f"🏁 <b>TP3 HIT</b> @ <code>{fmt(price)}</code>\n💰 Total: +{pnl:.2f}%\n⏱️ {duration}m"
        elif level==1: msg = f"✅ <b>TP1 HIT</b> @ <code>{fmt(price)}</code>\n🔒 SL → BE <code>{fmt(entry)}</code>"
        elif level==2: msg = f"✅ <b>TP2 HIT</b> @ <code>{fmt(price)}</code>\n🔒 SL → TP1 <code>{fmt(trade['tp1'])}</code>"
        else: return
        try:
            await bot_app.bot.send_message(chat_id=TELEGRAM_VVIP_CHANNEL, text=msg,
                parse_mode=ParseMode.HTML, reply_to_message_id=msg_id)
        except Exception as e: logger.error(f"Update send: {e}")

# ==============================================================================
# TELEGRAM SIGNAL + HANDLERS
# ==============================================================================
journal = Journal()

async def send_admin_error(msg):
    if not TELEGRAM_ADMIN_ID or not bot_app: return
    try:
        await bot_app.bot.send_message(chat_id=TELEGRAM_ADMIN_ID,
            text=f"🚨 <b>ERROR</b>\n\n<code>{escape(str(msg)[:400])}</code>", parse_mode=ParseMode.HTML)
    except: pass

async def send_signal(setup: SMCSetup, coin: CoinData, fund: FundamentalBias):
    if not TELEGRAM_VVIP_CHANNEL or not bot_app: return
    try:
        def fmt(p): return f"{p:.2f}" if p>=100 else (f"{p:.4f}" if p>=1 else (f"{p:.6f}" if p>=0.01 else f"{p:.8f}"))
        risk    = setup.entry_price - setup.sl_price
        rr_tp1  = (setup.tp1_price-setup.entry_price)/risk if risk > 0 else 0
        rr_tp2  = (setup.tp2_price-setup.entry_price)/risk if risk > 0 else 0
        rr_tp3  = (setup.tp3_price-setup.entry_price)/risk if risk > 0 else 0
        exch_str = " • ".join([e.upper() for e in sorted(coin.available_exchanges)])
        bias_emoji = "🟢" if fund.bias=="BULLISH" else ("🔴" if fund.bias=="BEARISH" else "🟡")
        reasons = "\n".join([f"   {r}" for r in fund.reasons]) if fund.reasons else "   ✓ No red flags"
        _TYPE_ICONS = {"FVG ZONE":"📊","OB ZONE":"🧱","PULLBACK ZONE":"↩️","RETEST ZONE":"🔄","BREAKOUT ZONE":"🚀","WICK REJECTION":"🪝"}
        base_sig  = setup.signal_type.replace(" ⚡ FAST EXEC","").strip()
        sig_icon  = _TYPE_ICONS.get(base_sig, "📍")
        fast_line = "\n⚡ <b>FAST EXECUTION</b> — masuk sekarang!" if "FAST EXEC" in setup.signal_type else ""
        # NEW: tunjuk jenis reversal confirmation dalam mesej
        rev_icons = {"SWEEP":"🌊","ENGULFING":"🕯️","HAMMER":"🔨","MORNING_STAR":"⭐"}
        rev_line  = f"├─ Confirmation: {rev_icons.get(setup.reversal_type,'🔍')} {setup.reversal_type}\n"

        msg = (
            f"{sig_icon} <b>{base_sig}</b>{fast_line}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🎯 <b>{escape(setup.symbol)}/USDT</b> • {setup.direction}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📍 ENTRY <code>{fmt(setup.entry_price)}</code>\n"
            f"🛑 SL <code>{fmt(setup.sl_price)}</code> ⚠️ -{((setup.entry_price-setup.sl_price)/setup.entry_price*100):.2f}%\n\n"
            f"🎯 TP1 <code>{fmt(setup.tp1_price)}</code> → +{((setup.tp1_price-setup.entry_price)/setup.entry_price*100):.2f}% (R:R 1:{rr_tp1:.1f})\n"
            f"🎯 TP2 <code>{fmt(setup.tp2_price)}</code> → +{((setup.tp2_price-setup.entry_price)/setup.entry_price*100):.2f}% (R:R 1:{rr_tp2:.1f})\n"
            f"🎯 TP3 <code>{fmt(setup.tp3_price)}</code> → +{((setup.tp3_price-setup.entry_price)/setup.entry_price*100):.2f}% (R:R 1:{rr_tp3:.1f})\n\n"
            f"🧠 <b>SMC ANALYSIS</b> (Score: {setup.structure_score:.0f}/100)\n"
            f"├─ Kaedah: {base_sig}\n"
            f"{rev_line}"
            f"├─ CHoCH: {setup.choch_type.upper()}\n"
            f"├─ Ref @ <code>{fmt(setup.sweep_price)}</code>\n"
            f"├─ Zone [{fmt(setup.zone_low)} - {fmt(setup.zone_high)}]\n"
            f"└─ R:R: 1:{setup.rr_ratio:.1f}\n\n"
            f"📊 <b>FUNDAMENTAL</b> {bias_emoji} {fund.bias} ({fund.score:+.0f})\n"
            f"{reasons}\n\n"
            f"🐋 <b>SENTIMENT</b> (Score: {setup.sentiment_score:.0f})\n"
            f"├─ Retail Long: {coin.retail_long_pct:.1f}%\n"
            f"└─ Whale Long: {coin.whale_long_pct:.1f}%\n\n"
            f"📈 <b>METADATA</b>\n"
            f"├─ MCAP: ${coin.market_cap/1e6:.0f}M\n"
            f"└─ Exchanges: {exch_str}\n"
            f"━━━━━━━━━━━━━━━━━━"
        )
        pair = f"{setup.symbol}USDT"; keyboard = []
        row = []
        if "binance" in coin.available_exchanges: row.append(InlineKeyboardButton("🟢 Binance", url=f"https://www.binance.com/en/trade/{pair}?type=spot"))
        if "bitget"  in coin.available_exchanges: row.append(InlineKeyboardButton("🔵 Bitget",  url=f"https://www.bitget.com/spot/{pair}"))
        if "gateio"  in coin.available_exchanges: row.append(InlineKeyboardButton("🟡 Gate.io", url=f"https://www.gate.io/trade/{setup.symbol}_USDT"))
        if row: keyboard.append(row)
        keyboard.append([InlineKeyboardButton("📊 TradingView", url=f"https://www.tradingview.com/chart/?symbol=BINANCE:{pair}")])

        sent = await bot_app.bot.send_message(chat_id=TELEGRAM_VVIP_CHANNEL, text=msg,
            parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(keyboard))
        async with trades_lock:
            active_trades[sent.message_id] = {"symbol":setup.symbol,"entry":setup.entry_price,
                "sl":setup.sl_price,"current_sl":setup.sl_price,"tp1":setup.tp1_price,
                "tp2":setup.tp2_price,"tp3":setup.tp3_price,"status":"ACTIVE","open_time":datetime.now()}
        await journal.log_signal(setup, sent.message_id)
        logger.info(f"📤 Signal: {setup.symbol} @ {setup.entry_price} [{setup.reversal_type}]")
        async with stats_lock:
            global daily_signals; daily_signals += 1
    except Exception as e:
        logger.error(f"Send signal: {e}"); await send_admin_error(f"Send: {e}")

async def scan_loop():
    await asyncio.sleep(60)
    logger.info("🔍 Scan loop started")
    while True:
        try:
            async with coins_lock: coins = list(coins_data.values())
            results = {}
            for coin in coins:
                try:
                    if len(coin.candles) >= 30: results[coin.symbol] = await analyze_coin(coin)
                    else: results[coin.symbol] = {"status":"skip","reason":f"candle={len(coin.candles)}/30","score":0,"signal_type":"-","fund":"-"}
                except Exception as e: results[coin.symbol] = {"status":"error","reason":str(e),"score":0,"signal_type":"-","fund":"-"}

            by_status = {}
            for sym, r in results.items(): by_status.setdefault(r["status"],[]).append((sym,r))
            logger.info(f"━━ [SCAN] {len(coins)} coin | ✅lulus={len(by_status.get('lulus',[]))} | "
                        f"🔴skor_rendah={len(by_status.get('skor_rendah',[]))} | "
                        f"⚫tiada_setup={len(by_status.get('tiada_setup',[]))} | "
                        f"⏳cooldown={len(by_status.get('cooldown',[]))} | signal_hari_ini={daily_signals}")
            for sym, r in sorted(by_status.get("skor_rendah",[]), key=lambda x: x[1]["score"], reverse=True):
                flag = "🟡 HAMPIR" if r.get("close") else "🔴 TOLAK"
                logger.info(f"  {flag} {sym} | {r['signal_type']} | {r['reason']} | fund={r['fund']}")

            # v3.3: kemas rolling win rate penalty sekali per cycle
            global rolling_wr_penalty
            try:
                _stats = journal.get_stats()
                if _stats["total"] >= 10 and _stats["win_rate"] < 22.0:
                    rolling_wr_penalty = 15
                    logger.info(f"  ⚠️ [WR ALERT] Win rate {_stats['win_rate']:.1f}% < 22% — score penalty +15 aktif")
                else:
                    rolling_wr_penalty = 0
            except Exception: rolling_wr_penalty = 0

        except Exception as e: logger.error(f"Scan loop: {e}")
        await asyncio.sleep(60)

# ==============================================================================
# ADMIN COMMANDS
# ==============================================================================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != TELEGRAM_ADMIN_ID: await update.message.reply_text("⛔ Admin only"); return
    async with coins_lock: active = sum(1 for c in coins_data.values() if c.available_exchanges); total = len(coins_data)
    async with stats_lock: sig = len(sent_signals); trades = len(active_trades); pnl = daily_pnl
    msg = (f"👑 <b>ADMIN PANEL v3.1</b>\n\n📊 Coins: {total} ({active} active)\n"
           f"📤 Signals: {sig}\n🔄 Active: {trades}\n💰 Daily PnL: {pnl:+.2f}%\n\n"
           f"v3.1: Sweep OR Engulfing/Hammer/Morning Star\n"
           f"/status | /coins | /journal | /fundamentals | /report")
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != TELEGRAM_ADMIN_ID: return
    stats = journal.get_stats()
    wr_note = f" ⚠️ penalty aktif" if rolling_wr_penalty > 0 else ""
    msg = (f"📊 <b>STATUS v3.3</b>\n\n🕐 {datetime.now().strftime('%H:%M:%S')}\n"
           f"📈 Coins: {len(coins_data)}\n📤 Signals: {len(sent_signals)}\n"
           f"🔄 Active: {len(active_trades)}\n💰 Daily PnL: {daily_pnl:+.2f}%\n\n"
           f"<b>📓 JOURNAL</b>\nTotal: {stats['total']}\n"
           f"Win Rate: {stats['win_rate']:.1f}%{wr_note}\n"
           f"✅ Win/BE/TP1lock: {stats['wins']}\n"
           f"❌ SL Full: {stats['losses']}\n"
           f"⚖️ SL@BE: {stats['sl_be']} | 🔒 SL@TP1: {stats['sl_tp1']}\n"
           f"Avg: {stats['avg_pnl']:.2f}%\nBest: +{stats['best']:.2f}%\nWorst: {stats['worst']:.2f}%")
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def cmd_coins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != TELEGRAM_ADMIN_ID: return
    async with coins_lock: coins = list(coins_data.items())[:20]
    msg = "📋 <b>COINS</b>\n\n"
    for i,(sym,c) in enumerate(coins):
        msg += f"{i+1}. {sym} | ${c.price:.4f} | [{','.join(sorted(c.available_exchanges)) or '❌'}]\n"
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def cmd_journal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != TELEGRAM_ADMIN_ID: return
    stats = journal.get_stats()
    await update.message.reply_text(f"📓 Total: {stats['total']} | WR: {stats['win_rate']:.1f}% | Avg: {stats['avg_pnl']:.2f}%")

async def cmd_fundamentals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != TELEGRAM_ADMIN_ID: return
    async with coins_lock: coins = list(coins_data.values())[:10]
    msg = "📊 <b>FUNDAMENTAL</b>\n\n"
    for c in coins:
        f = FundamentalAnalyzer.analyze(c)
        emoji = "🟢" if f.bias=="BULLISH" else ("🔴" if f.bias=="BEARISH" else "🟡")
        msg += f"{c.symbol} {emoji} {f.bias} ({f.score:+.0f})\n   R:{c.retail_long_pct:.0f}% W:{c.whale_long_pct:.0f}% T:{c.taker_buy_ratio:.2f}\n"
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != TELEGRAM_ADMIN_ID: return
    stats = journal.get_stats()
    await update.message.reply_text(
        f"📊 <b>DAILY REPORT</b>\n📅 {datetime.now().strftime('%d %b %Y')}\n━━━━━━━━━━━━━━━━\n"
        f"🎯 Signals: {stats['total']}\n✅ Wins: {stats['wins']}\n❌ Loss: {stats['losses']}\n"
        f"📊 Win Rate: {stats['win_rate']:.1f}%\n💰 Avg PnL: {stats['avg_pnl']:.2f}%", parse_mode=ParseMode.HTML)

# ==============================================================================
# SCHEDULERS + FLASK + MAIN
# ==============================================================================
async def daily_reset_loop():
    global daily_pnl, daily_signals, last_reset_date
    while True:
        today = datetime.now().date().isoformat()
        async with stats_lock:
            if last_reset_date != today:
                daily_pnl = 0.0; daily_signals = 0; last_reset_date = today
                logger.info(f"Daily reset ({today})")
        await asyncio.sleep(60)

async def daily_report_loop():
    while True:
        now = datetime.now()
        if now.hour == 20 and now.minute == 0:
            stats = journal.get_stats()
            msg = (f"📊 <b>JURNAL HARIAN VVIP</b>\n📅 {now.strftime('%d %b %Y')}\n"
                   f"🎯 Total: {stats['total']}\n✅ Win: {stats['wins']} ({stats['win_rate']:.1f}%)\n"
                   f"❌ Loss: {stats['losses']}\n💰 Avg: {stats['avg_pnl']:.2f}%")
            if bot_app and TELEGRAM_VVIP_CHANNEL:
                try: await bot_app.bot.send_message(chat_id=TELEGRAM_VVIP_CHANNEL, text=msg, parse_mode=ParseMode.HTML)
                except: pass
            await asyncio.sleep(65)
        else: await asyncio.sleep(30)

async def cache_cleanup_loop():
    while True:
        try: await cache.cleanup()
        except: pass
        await asyncio.sleep(600)

flask_app = Flask(__name__)
@flask_app.route("/")
def health():
    return {"status":"ok","version":"3.1-zero-cost","coins":len(coins_data),
            "signals":len(sent_signals),"active":len(active_trades),
            "pnl":daily_pnl,"time":datetime.now().isoformat()}
def run_flask(): flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

binance     = BinanceEngine()
validator   = ExchangeValidator()
coingecko   = CoinGeckoEngine()
fundamental = BinanceFundamentalEngine()

async def main():
    global bot_app
    if not TELEGRAM_BOT_TOKEN: logger.error("TELEGRAM_BOT_TOKEN not set!"); return
    Thread(target=run_flask, daemon=True).start()
    logger.info(f"Flask on port {PORT}")
    bot_app = Application.builder().token(TELEGRAM_BOT_TOKEN).updater(None).build()
    for cmd, fn in [("start",cmd_start),("status",cmd_status),("coins",cmd_coins),
                    ("journal",cmd_journal),("fundamentals",cmd_fundamentals),("report",cmd_report)]:
        bot_app.add_handler(CommandHandler(cmd, fn))
    await bot_app.initialize(); await bot_app.start()
    await bot_app.bot.delete_webhook(drop_pending_updates=True)
    if TELEGRAM_ADMIN_ID:
        try:
            await bot_app.bot.send_message(chat_id=TELEGRAM_ADMIN_ID,
                text="🚀 <b>BOT v3.1 DEPLOYED</b>\n\n✅ BUG FIX: level → level_price\n"
                     "✅ SWEEP_WICK_RATIO: 2.0 → 1.5\n✅ Reversal OR: Engulfing / Hammer / Morning Star\n\nMonitoring...",
                parse_mode=ParseMode.HTML)
        except Exception as e: logger.error(f"Admin notify: {e}")

    async def startup_beacon():
        for i in range(6):
            await asyncio.sleep(10)
            async with coins_lock: cc = len(coins_data); tc = sum(len(c.candles) for c in coins_data.values())
            logger.info(f"🟢 BEACON {i+1}/6: {cc} coins, {tc} candles")
    asyncio.create_task(startup_beacon())

    await asyncio.gather(binance.init(), validator.init(), coingecko.init())

    async def _polling_task():
        offset = None
        while True:
            try:
                updates = await bot_app.bot.get_updates(offset=offset, timeout=30, allowed_updates=["message","callback_query"])
                for upd in updates: await bot_app.process_update(upd); offset = upd.update_id+1
            except asyncio.CancelledError: break
            except Exception as e: logger.debug(f"Polling: {e}"); await asyncio.sleep(5)

    try:
        await asyncio.gather(
            coingecko.poll_loop(), binance.subscribe_websocket(), validator.validator_loop(),
            fundamental.update_all_fundamentals(), scan_loop(), AutoUpdater.check_trades(),
            daily_reset_loop(), daily_report_loop(), cache_cleanup_loop(), _polling_task())
    except Exception as e:
        logger.critical(f"Main: {e}"); await send_admin_error(f"Main: {e}")
    finally:
        logger.info("Shutting down...")
        try:
            if bot_app and bot_app.running: await bot_app.stop(); await bot_app.shutdown()
        except: pass
        try: await binance.close(); await validator.close(); await coingecko.close()
        except: pass

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: logger.info("Stopped by user")
    except Exception as e: logger.critical(f"Fatal: {e}")
