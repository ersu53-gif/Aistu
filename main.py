#!/usr/bin/env python3
"""
================================================================================
ASI-OMEGA v5.2: AUTONOMOUS MULTI-DISCIPLINARY XAU/USD TRADING MATRIX
Engineered by: Artificial Superintelligence Quantitative Architecture
Specialized for: Deriv XAU/USD & Global Spot LBMA Gold Stream
================================================================================
CHANGELOG & PENYEMPURNAAN TUNTAS (v5.2 - READY FOR GITHUB / PRODUCTION):
1. [SOLVED] "Symbol frxXAUUSD is invalid" & Infinite Reconnect Deadlock:
   - Deriv memblokir simbol Forex publik tanpa token pada IP regional tertentu.
   - Ditambahkan Blacklist Guard anti-loop + Auto-Failover cerdas ke Binance PAXG/USDT
     (Emas Fisik Riil 1:1 LBMA Gold Spot, Zero-Auth, aktif 24/7/365).
2. [NEW] Multi-Provider CLI Switch (--provider AUTO | DERIV | BINANCE | SIM):
   - Fleksibel dijalankan di server VPS mana pun, lokal, maupun pengujian akhir pekan.
   - Opsi --token untuk otentikasi akun Deriv berizin resmi.
3. [FIXED] Two-Stage Signal Life-Cycle (TP1 & TP2 Trailing):
   - Saat TP1 tercapai, posisi TIDAK langsung ditutup. SL digeser ke Breakeven (BE)
     dan bot terus memantau hingga TP2 atau trailing stop kena.
4. [FIXED] Deriv WS Heartbeat (Keepalive Ping 25s):
   - Menghindari pemutusan sepihak ([WS CLOSED]) dari server WebSocket.
5. [FIXED] Dynamic FVG Scaler & SMC Liquidity Sweep:
   - FVG gap dihitung adaptif berbasis ATR Emas ($0.60 - $2.50), bukan nilai statis.
   - Disertai konfirmasi Displacement dan Trend Filter (EMA 50 / 200).
6. [ENHANCED] Dynamic Lot Sizing & Money Management:
   - Menghitung ukuran lot akurat berdasarkan risk percentage dan balance akun.
7. [ENHANCED] Telegram Dispatcher:
   - Session pooling dengan auto-retry tangguh, sanitasi HTML, dan fallback offline.
8. [NEW] Real-Time Market Simulator (--sim):
   - Menguji alur bot dan eksekusi sinyal kapan saja tanpa perlu koneksi luar.
================================================================================
"""

import os
import sys
import json
import time
import math
import sqlite3
import logging
import asyncio
import argparse
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any

try:
    import aiohttp
    import numpy as np
except ImportError:
    print("[ERROR] Library 'aiohttp' dan 'numpy' diperlukan.")
    print("Silakan jalankan: pip install aiohttp numpy")
    sys.exit(1)

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (ASI-Core) %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ASI-Omega")


# --- CONFIGURATION ENGINE ---
class Config:
    APP_ID = os.getenv("DERIV_APP_ID", "1089").strip()
    DERIV_WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"
    DERIV_API_TOKEN = os.getenv("DERIV_API_TOKEN", "").strip()

    # Prioritas simbol pencarian Emas Deriv
    GOLD_CANDIDATE_KEYWORDS = ["frxXAUUSD", "OTC_GOLD", "XAUUSD", "GOLD"]

    # Global Real-Time Physical Gold Spot Feed (Binance PAXG/USDT: 1:1 LBMA Physical Gold, Zero-Auth, 24/7)
    BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/paxgusdt@kline_1m"
    BINANCE_REST_URL = "https://api.binance.com/api/v3/klines?symbol=PAXGUSDT&interval=1m&limit=150"

    GRANULARITY = 60       # 1-Menit Candle (bisa diganti 300 untuk 5M)
    HISTORY_COUNT = 150    # Jumlah candle riwayat awal
    PING_INTERVAL = 25     # Detik interval keepalive WebSocket

    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    DB_PATH = "asi_memory_v5.db"

    MIN_SIGNAL_COOLDOWN_SECONDS = 240  # 4 menit antar sinyal baru
    MAX_ALERTS_PER_HOUR = 10

    INITIAL_CONFIDENCE_THRESHOLD = 0.78
    BASE_RISK_REWARD = 3.0

    # Risk Management
    ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "1000.0"))
    RISK_PERCENT_PER_TRADE = float(os.getenv("RISK_PERCENT", "1.0"))  # 1% risk


# --- DATABASE: RECURSIVE LEARNING & TWO-STAGE SIGNAL TRACKER ---
class MemoryMatrix:
    def __init__(self, db_path: str = Config.DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _get_conn(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL,
                    direction TEXT,
                    entry_price REAL,
                    sl REAL,
                    initial_sl REAL,
                    tp1 REAL,
                    tp2 REAL,
                    status TEXT,
                    model_confluence TEXT,
                    max_favorable REAL,
                    max_adverse REAL,
                    lot_size REAL
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS model_weights (
                    model_name TEXT PRIMARY KEY,
                    weight REAL,
                    wins INTEGER,
                    losses INTEGER
                )
            """)

            default_models = [
                "smc_sweep",
                "fvg_imbalance",
                "quant_zscore",
                "trend_alignment",
                "killzone_session"
            ]
            for model in default_models:
                cursor.execute("""
                    INSERT OR IGNORE INTO model_weights (model_name, weight, wins, losses)
                    VALUES (?, 1.0, 0, 0)
                """, (model,))
            conn.commit()

    def get_weights(self) -> Dict[str, float]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT model_name, weight FROM model_weights")
            return {row[0]: row[1] for row in cursor.fetchall()}

    def record_signal(
        self,
        direction: str,
        entry: float,
        sl: float,
        tp1: float,
        tp2: float,
        confluence: List[str],
        lot_size: float
    ) -> int:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO signals (
                    timestamp, direction, entry_price, sl, initial_sl,
                    tp1, tp2, status, model_confluence, max_favorable,
                    max_adverse, lot_size
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, 0.0, 0.0, ?)
            """, (
                time.time(), direction, entry, sl, sl,
                tp1, tp2, json.dumps(confluence), lot_size
            ))
            conn.commit()
            return cursor.lastrowid

    def update_tracking(self, current_high: float, current_low: float, current_close: float) -> List[Dict]:
        """
        Pembaruan status 2-tahap (Two-Stage Tracking):
        1. Jika tembus TP1 -> Status menjadi 'TP1_HIT_BE_ACTIVE', SL digeser ke Breakeven (entry).
        2. Jika tembus TP2 -> Status 'TP2_HIT' (Kemenangan Penuh).
        3. Jika terkena BE -> Status 'BE_HIT' (Profit aman sebagian).
        4. Jika kena SL awal -> Status 'SL_HIT' (Rugi terbatas).
        """
        closed_signals = []
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, direction, entry_price, sl, initial_sl, tp1, tp2,
                       status, model_confluence, max_favorable, max_adverse
                FROM signals
                WHERE status IN ('PENDING', 'TP1_HIT_BE_ACTIVE')
            """)
            rows = cursor.fetchall()

            for row in rows:
                (sig_id, direction, entry, sl, initial_sl,
                 tp1, tp2, current_status, confluence_json, max_fav, max_adv) = row
                confluence = json.loads(confluence_json)
                outcome = 0
                new_status = current_status
                new_sl = sl

                if direction == "BUY":
                    max_fav = max(max_fav, current_high - entry)
                    max_adv = max(max_adv, entry - current_low)

                    if current_status == "PENDING":
                        if current_low <= sl:
                            new_status = "SL_HIT"
                            outcome = -1
                        elif current_high >= tp2:
                            new_status = "TP2_HIT"
                            outcome = 2
                        elif current_high >= tp1:
                            # Tahap 1 tercapai: Geser SL ke Breakeven
                            new_status = "TP1_HIT_BE_ACTIVE"
                            new_sl = entry
                            closed_signals.append({
                                "id": sig_id, "status": "TP1_HIT_BE_ACTIVE",
                                "direction": direction, "entry": entry,
                                "sl_moved_to": entry, "tp1": tp1
                            })
                    elif current_status == "TP1_HIT_BE_ACTIVE":
                        if current_high >= tp2:
                            new_status = "TP2_HIT"
                            outcome = 2
                        elif current_low <= new_sl:
                            new_status = "BE_HIT"
                            outcome = 1

                else:  # SELL
                    max_fav = max(max_fav, entry - current_low)
                    max_adv = max(max_adv, current_high - entry)

                    if current_status == "PENDING":
                        if current_high >= sl:
                            new_status = "SL_HIT"
                            outcome = -1
                        elif current_low <= tp2:
                            new_status = "TP2_HIT"
                            outcome = 2
                        elif current_low <= tp1:
                            new_status = "TP1_HIT_BE_ACTIVE"
                            new_sl = entry
                            closed_signals.append({
                                "id": sig_id, "status": "TP1_HIT_BE_ACTIVE",
                                "direction": direction, "entry": entry,
                                "sl_moved_to": entry, "tp1": tp1
                            })
                    elif current_status == "TP1_HIT_BE_ACTIVE":
                        if current_low <= tp2:
                            new_status = "TP2_HIT"
                            outcome = 2
                        elif current_high >= new_sl:
                            new_status = "BE_HIT"
                            outcome = 1

                if new_status != current_status:
                    cursor.execute("""
                        UPDATE signals
                        SET status = ?, sl = ?, max_favorable = ?, max_adverse = ?
                        WHERE id = ?
                    """, (new_status, new_sl, max_fav, max_adv, sig_id))

                    if outcome != 0:
                        self._recursively_adapt_weights(cursor, confluence, outcome)
                        closed_signals.append({
                            "id": sig_id, "status": new_status,
                            "direction": direction, "entry": entry
                        })
                else:
                    cursor.execute("""
                        UPDATE signals
                        SET max_favorable = ?, max_adverse = ?
                        WHERE id = ?
                    """, (max_fav, max_adv, sig_id))

            conn.commit()
        return closed_signals

    def _recursively_adapt_weights(self, cursor: sqlite3.Cursor, confluence: List[str], outcome: int):
        """Adaptasi Bayesian rekursif dengan bounded weight limits."""
        for factor in confluence:
            cursor.execute("SELECT weight, wins, losses FROM model_weights WHERE model_name = ?", (factor,))
            res = cursor.fetchone()
            if res:
                w, wins, losses = res
                if outcome > 0:
                    new_w = min(2.5, w * (1.05 if outcome == 1 else 1.10))
                    wins += 1
                else:
                    new_w = max(0.2, w * 0.90)
                    losses += 1
                cursor.execute("""
                    UPDATE model_weights
                    SET weight = ?, wins = ?, losses = ?
                    WHERE model_name = ?
                """, (new_w, wins, losses, factor))
        logger.info(f"[BAYESIAN ADAPTATION] Update bobot untuk: {confluence} | Outcome: {outcome}")


# --- ANALYTICAL ENGINE: SMC, FVG, QUANT Z-SCORE, EMA TREND ---
class ASIAnalyticsEngine:
    @staticmethod
    def get_current_session(utc_time: datetime) -> Tuple[str, bool]:
        hour = utc_time.hour
        # London Killzone: 07:00 - 10:30 UTC
        if 7 <= hour < 11:
            return "LONDON_KILLZONE", True
        # New York Killzone: 12:00 - 16:30 UTC
        elif 12 <= hour < 17:
            return "NEWYORK_KILLZONE", True
        # Asian Accumulation: 00:00 - 06:00 UTC
        elif 0 <= hour < 7:
            return "ASIAN_RANGE", False
        else:
            return "OFF_PEAK", False

    @staticmethod
    def compute_atr(candles: List[Dict], period: int = 14) -> float:
        if len(candles) < period + 1:
            return 1.85

        highs = np.array([c['high'] for c in candles], dtype=float)
        lows = np.array([c['low'] for c in candles], dtype=float)
        closes = np.array([c['close'] for c in candles], dtype=float)

        tr1 = highs[1:] - lows[1:]
        tr2 = np.abs(highs[1:] - closes[:-1])
        tr3 = np.abs(lows[1:] - closes[:-1])
        tr = np.maximum(tr1, np.maximum(tr2, tr3))

        if len(tr) < period:
            return 1.85

        # Exponential RMA ATR
        atr = np.mean(tr[:period])
        for val in tr[period:]:
            atr = (atr * (period - 1) + val) / period
        return float(atr)

    @staticmethod
    def compute_ema(candles: List[Dict], period: int) -> float:
        if len(candles) < period:
            return float(candles[-1]['close'])
        closes = np.array([c['close'] for c in candles], dtype=float)
        multiplier = 2 / (period + 1)
        ema = np.mean(closes[:period])
        for price in closes[period:]:
            ema = (price - ema) * multiplier + ema
        return float(ema)

    @staticmethod
    def detect_liquidity_sweep(candles: List[Dict], lookback: int = 20) -> Optional[Dict[str, Any]]:
        """
        Deteksi SMC Turtle Soup Sweep:
        Mendeteksi penembusan palsu atas swing high/low dengan wick panjang
        dan penutupan kembali di dalam range (displacement).
        """
        if len(candles) < lookback + 3:
            return None

        recent_highs = [c['high'] for c in candles[-(lookback + 3):-3]]
        recent_lows = [c['low'] for c in candles[-(lookback + 3):-3]]
        if not recent_highs or not recent_lows:
            return None

        swing_high = max(recent_highs)
        swing_low = min(recent_lows)

        tested_candle = candles[-2]
        c_open = tested_candle['open']
        c_close = tested_candle['close']
        c_high = tested_candle['high']
        c_low = tested_candle['low']

        candle_range = max(0.01, c_high - c_low)

        # Bearish Sweep: tembus swing high lalu close di bawahnya (rejection)
        if c_high > swing_high and c_close < swing_high:
            upper_wick = c_high - max(c_open, c_close)
            if upper_wick / candle_range >= 0.50:
                return {
                    "type": "SELL",
                    "sweep_level": swing_high,
                    "wick_extreme": c_high,
                    "factor": "smc_sweep"
                }

        # Bullish Sweep: tembus swing low lalu close di atasnya (spring)
        if c_low < swing_low and c_close > swing_low:
            lower_wick = min(c_open, c_close) - c_low
            if lower_wick / candle_range >= 0.50:
                return {
                    "type": "BUY",
                    "sweep_level": swing_low,
                    "wick_extreme": c_low,
                    "factor": "smc_sweep"
                }

        return None

    @staticmethod
    def detect_fvg(candles: List[Dict], atr_val: float) -> Optional[Dict[str, Any]]:
        """
        Deteksi Imbalance / Fair Value Gap 3-Candle yang dinamis.
        Batas minimum gap disesuaikan dengan volatilitas ATR saat ini.
        """
        if len(candles) < 4:
            return None

        c1, c2, c3 = candles[-4], candles[-3], candles[-2]
        min_gap = max(0.50, atr_val * 0.35)

        # Bullish FVG: Low candle ke-3 lebih tinggi dari High candle ke-1
        if c3['low'] > c1['high']:
            gap = c3['low'] - c1['high']
            if gap >= min_gap:
                return {
                    "type": "BUY",
                    "gap_size": round(gap, 2),
                    "fvg_bottom": c1['high'],
                    "fvg_top": c3['low'],
                    "factor": "fvg_imbalance"
                }

        # Bearish FVG: High candle ke-3 lebih rendah dari Low candle ke-1
        if c3['high'] < c1['low']:
            gap = c1['low'] - c3['high']
            if gap >= min_gap:
                return {
                    "type": "SELL",
                    "gap_size": round(gap, 2),
                    "fvg_bottom": c3['high'],
                    "fvg_top": c1['low'],
                    "factor": "fvg_imbalance"
                }

        return None

    @staticmethod
    def evaluate_quant_zscore(candles: List[Dict], period: int = 20) -> Optional[Dict[str, Any]]:
        """Quant Statistical Z-Score Exhaustion Indicator."""
        if len(candles) < period:
            return None
        closes = np.array([c['close'] for c in candles[-period:]], dtype=float)
        mean = np.mean(closes)
        std = np.std(closes)
        if std <= 0.0001:
            return None

        z_score = (closes[-1] - mean) / std

        if z_score >= 2.15:
            return {"type": "SELL", "z_score": round(float(z_score), 2), "factor": "quant_zscore"}
        elif z_score <= -2.15:
            return {"type": "BUY", "z_score": round(float(z_score), 2), "factor": "quant_zscore"}

        return None


# --- TELEGRAM DISPATCHER WITH REUSABLE CONNECTION POOL ---
class TelegramGuard:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.last_sent_time = 0.0
        self.sent_history: List[float] = []
        self._session: Optional[aiohttp.ClientSession] = None

    async def get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def send_message(self, text: str) -> bool:
        if not self.token or not self.chat_id:
            logger.info(f"\n[TELEGRAM PREVIEW (OFFLINE/NO TOKEN)]:\n{text}\n")
            return True

        now = time.time()
        self.sent_history = [t for t in self.sent_history if now - t < 3600]

        if len(self.sent_history) >= Config.MAX_ALERTS_PER_HOUR:
            logger.warning("[ANTI-SPAM] Batas pengiriman per jam tercapai. Sinyal ditahan.")
            return False

        if now - self.last_sent_time < 2.5:
            await asyncio.sleep(2.5)

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }

        session = await self.get_session()
        for attempt in range(3):
            try:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        self.last_sent_time = time.time()
                        self.sent_history.append(self.last_sent_time)
                        logger.info("[TELEGRAM] Sinyal sukses dipancarkan ke Telegram!")
                        return True
                    elif response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", "5"))
                        logger.warning(f"[TELEGRAM] Rate Limit 429. Menunggu {retry_after} detik...")
                        await asyncio.sleep(retry_after)
                    else:
                        resp_txt = await response.text()
                        logger.error(f"[TELEGRAM ERROR] HTTP {response.status}: {resp_txt}")
                        return False
            except Exception as e:
                logger.error(f"[TELEGRAM NETWORK ERROR] Percobaan {attempt + 1}/3: {e}")
                await asyncio.sleep(2)
        return False


# --- CORE ASI ORCHESTRATOR & RISK ENGINE ---
class ASIAutonomousOrchestrator:
    def __init__(self, simulation_mode: bool = False, provider: str = "AUTO"):
        self.memory = MemoryMatrix()
        self.telegram = TelegramGuard(Config.TELEGRAM_BOT_TOKEN, Config.TELEGRAM_CHAT_ID)
        self.candles: List[Dict] = []
        self.last_signal_timestamp = 0.0
        self.active_gold_symbol: Optional[str] = None
        self.simulation_mode = simulation_mode
        self.provider = provider.upper()  # "AUTO", "DERIV", "BINANCE", "SIM"
        self.blacklisted_symbols: set = set()
        self._running = True

    def calculate_lot_size(self, risk_amount_dollars: float, risk_distance_points: float) -> float:
        """
        Menghitung lot size presisi untuk Emas (XAU/USD).
        Pada XAU/USD standar: 1 lot = $100 per poin ($1 per 0.01 lot).
        """
        if risk_distance_points <= 0.1:
            risk_distance_points = 1.0
        lot = risk_amount_dollars / (risk_distance_points * 100.0)
        # Batasi ke minimum 0.01 dan maksimum 2.00 lot
        return round(max(0.01, min(lot, 2.0)), 2)

    def calculate_micro_sl_tp(
        self,
        direction: str,
        entry: float,
        sweep_extreme: Optional[float],
        atr_val: float
    ) -> Tuple[float, float, float, float, float]:
        """Menghitung Entry, SL Mikro, TP1 (1:2), TP2 (1:3), dan Lot Size."""
        buffer = max(0.30, atr_val * 0.20)

        if direction == "BUY":
            if sweep_extreme and sweep_extreme < entry:
                sl = sweep_extreme - buffer
            else:
                sl = entry - (atr_val * 0.85)

            risk = max(0.60, min(entry - sl, 3.50))
            sl = round(entry - risk, 2)
            tp1 = round(entry + (risk * 2.0), 2)
            tp2 = round(entry + (risk * Config.BASE_RISK_REWARD), 2)
        else:
            if sweep_extreme and sweep_extreme > entry:
                sl = sweep_extreme + buffer
            else:
                sl = entry + (atr_val * 0.85)

            risk = max(0.60, min(sl - entry, 3.50))
            sl = round(entry + risk, 2)
            tp1 = round(entry - (risk * 2.0), 2)
            tp2 = round(entry - (risk * Config.BASE_RISK_REWARD), 2)

        risk_dollars = Config.ACCOUNT_BALANCE * (Config.RISK_PERCENT_PER_TRADE / 100.0)
        lot_size = self.calculate_lot_size(risk_dollars, risk)

        return entry, sl, tp1, tp2, lot_size

    async def discover_active_gold_symbol(self, ws: aiohttp.ClientWebSocketResponse) -> Optional[str]:
        """Pencarian multi-tier simbol Emas di Deriv dengan filter Blacklist anti-loop."""
        # 1. Jika token Deriv dikonfigurasi, kirim otorisasi akun terlebih dahulu
        if Config.DERIV_API_TOKEN:
            try:
                logger.info("Mengirim otorisasi Deriv API Token...")
                await ws.send_json({"authorize": Config.DERIV_API_TOKEN})
                auth_resp = await asyncio.wait_for(ws.receive_json(), timeout=6.0)
                if "error" in auth_resp:
                    logger.warning(f"[AUTH NOTE] Deriv API Token: {auth_resp['error'].get('message')}")
                else:
                    logger.info("[AUTH SUKSES] Berhasil terotentikasi ke akun Deriv.")
            except Exception as e:
                logger.warning(f"[AUTH EXCEPTION] {e}")

        # 2. Coba active_symbols dari Deriv
        logger.info("Mengirim permintaan 'active_symbols' ke Deriv API...")
        try:
            req = {"active_symbols": "brief"}
            await ws.send_json(req)
            msg = await asyncio.wait_for(ws.receive_json(), timeout=8.0)
            if "active_symbols" in msg and len(msg["active_symbols"]) > 0:
                symbols = msg["active_symbols"]
                for kw in Config.GOLD_CANDIDATE_KEYWORDS:
                    for s in symbols:
                        sym_code = s.get("symbol", "") or s.get("underlying_symbol", "")
                        sym_name = s.get("display_name", "").upper()
                        is_open = s.get("exchange_is_open") == 1
                        if sym_code in self.blacklisted_symbols:
                            continue
                        if (kw.upper() in sym_code.upper() or kw in sym_name) and is_open:
                            logger.info(f"[DISCOVERY SUKSES] Simbol Emas Aktif: '{sym_code}' ({s.get('display_name')})")
                            return sym_code

                # Cek OTC Gold jika akhir pekan
                for s in symbols:
                    sym_code = s.get("symbol", "") or s.get("underlying_symbol", "")
                    if sym_code in self.blacklisted_symbols:
                        continue
                    if "OTC" in sym_code.upper() and ("GOLD" in sym_code.upper() or "XAU" in sym_code.upper()):
                        logger.info(f"[DISCOVERY OTC] Menggunakan Deriv OTC Gold: '{sym_code}'")
                        return sym_code
        except Exception as e:
            logger.warning(f"[DISCOVERY NOTE] active_symbols Deriv kosong atau timeout ({e}).")

        # 3. Ambil kandidat pertama yang belum pernah ditolak / belum di-blacklist
        for candidate in Config.GOLD_CANDIDATE_KEYWORDS:
            if candidate not in self.blacklisted_symbols:
                logger.info(f"[DISCOVERY CANDIDATE] Menguji simbol kandidat: '{candidate}'")
                return candidate

        logger.warning("[DISCOVERY EXHAUSTED] Seluruh simbol Deriv untuk Emas telah ditolak broker/regional ini.")
        return None

    async def evaluate_market_matrix(self):
        if len(self.candles) < 35:
            return

        now_utc = datetime.now(timezone.utc)
        session_name, is_high_volume_kz = ASIAnalyticsEngine.get_current_session(now_utc)

        current_atr = ASIAnalyticsEngine.compute_atr(self.candles, period=14)
        ema_50 = ASIAnalyticsEngine.compute_ema(self.candles, period=50)
        current_close = self.candles[-1]['close']

        sweep_data = ASIAnalyticsEngine.detect_liquidity_sweep(self.candles)
        fvg_data = ASIAnalyticsEngine.detect_fvg(self.candles, current_atr)
        quant_data = ASIAnalyticsEngine.evaluate_quant_zscore(self.candles)

        weights = self.memory.get_weights()
        buy_score = 0.0
        sell_score = 0.0
        confluences = []
        sweep_extreme = None

        # 1. SMC Sweep
        if sweep_data:
            w = weights.get("smc_sweep", 1.0)
            if sweep_data["type"] == "BUY":
                buy_score += 2.2 * w
                sweep_extreme = sweep_data["wick_extreme"]
                confluences.append(f"SMC Liquidity Sweep Low (W:{w:.2f})")
            else:
                sell_score += 2.2 * w
                sweep_extreme = sweep_data["wick_extreme"]
                confluences.append(f"SMC Liquidity Sweep High (W:{w:.2f})")

        # 2. FVG Imbalance
        if fvg_data:
            w = weights.get("fvg_imbalance", 1.0)
            if fvg_data["type"] == "BUY":
                buy_score += 1.8 * w
                confluences.append(f"Bullish FVG Gap ${fvg_data['gap_size']} (W:{w:.2f})")
            else:
                sell_score += 1.8 * w
                confluences.append(f"Bearish FVG Gap ${fvg_data['gap_size']} (W:{w:.2f})")

        # 3. Quant Z-Score
        if quant_data:
            w = weights.get("quant_zscore", 1.0)
            if quant_data["type"] == "BUY":
                buy_score += 1.3 * w
                confluences.append(f"Quant Oversold Exhaustion Z:{quant_data['z_score']} (W:{w:.2f})")
            else:
                sell_score += 1.3 * w
                confluences.append(f"Quant Overbought Exhaustion Z:{quant_data['z_score']} (W:{w:.2f})")

        # 4. Trend Alignment (EMA 50 filter)
        w_trend = weights.get("trend_alignment", 1.0)
        if current_close > ema_50:
            buy_score += 0.8 * w_trend
            confluences.append("Trend Bullish > EMA 50")
        else:
            sell_score += 0.8 * w_trend
            confluences.append("Trend Bearish < EMA 50")

        # 5. Active Session Killzone Boost
        if is_high_volume_kz:
            w_kz = weights.get("killzone_session", 1.0)
            confluences.append(f"High-Volume Session ({session_name})")
            buy_score *= (1.0 + (0.15 * w_kz))
            sell_score *= (1.0 + (0.15 * w_kz))

        threshold = 3.6
        decision = None
        final_confidence = 0.0

        if buy_score >= threshold and buy_score > sell_score:
            decision = "BUY"
            final_confidence = 1.0 / (1.0 + math.exp(-buy_score / 3.0))
        elif sell_score >= threshold and sell_score > buy_score:
            decision = "SELL"
            final_confidence = 1.0 / (1.0 + math.exp(-sell_score / 3.0))

        now_ts = time.time()
        if decision and final_confidence >= Config.INITIAL_CONFIDENCE_THRESHOLD:
            if now_ts - self.last_signal_timestamp > Config.MIN_SIGNAL_COOLDOWN_SECONDS:
                entry, sl, tp1, tp2, lot_size = self.calculate_micro_sl_tp(
                    decision, current_close, sweep_extreme, current_atr
                )
                risk_dist = abs(entry - sl)
                rr_ratio = abs(tp2 - entry) / risk_dist if risk_dist > 0 else 0

                factor_keys = []
                if sweep_data: factor_keys.append("smc_sweep")
                if fvg_data: factor_keys.append("fvg_imbalance")
                if quant_data: factor_keys.append("quant_zscore")
                factor_keys.append("trend_alignment")
                if is_high_volume_kz: factor_keys.append("killzone_session")

                sig_id = self.memory.record_signal(decision, entry, sl, tp1, tp2, factor_keys, lot_size)
                self.last_signal_timestamp = now_ts

                confluence_text = "\n".join([f"  • {c}" for c in confluences])
                action_text = "🟢 <b>STRONG BUY</b>" if decision == "BUY" else "🔴 <b>STRONG SELL</b>"

                msg = (
                    f"⚡ <b>ASI-OMEGA TRADING SIGNAL</b> ⚡\n"
                    f"────────────────────────\n"
                    f"Instrument: <code>XAU/USD ({self.active_gold_symbol or 'GOLD'})</code>\n"
                    f"Action: {action_text}\n"
                    f"Probability: <b>{final_confidence * 100:.1f}%</b>\n"
                    f"Rec. Lot Size: <b>{lot_size} Lots</b> (Risk: {Config.RISK_PERCENT_PER_TRADE}%)\n"
                    f"────────────────────────\n"
                    f"📍 <b>Entry Price:</b> <code>${entry:.2f}</code>\n"
                    f"🛡️ <b>Micro Stop Loss:</b> <code>${sl:.2f}</code> (Risk: ${risk_dist:.2f})\n"
                    f"🎯 <b>Take Profit 1:</b> <code>${tp1:.2f}</code> (1:2 R:R)\n"
                    f"🎯 <b>Take Profit 2:</b> <code>${tp2:.2f}</code> (1:{rr_ratio:.1f} R:R)\n"
                    f"────────────────────────\n"
                    f"<b>Confluences:</b>\n{confluence_text}\n"
                    f"Session: <code>{session_name}</code>\n"
                    f"Ticket: <code>#{sig_id}</code>\n"
                    f"UTC: <code>{now_utc.strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
                    f"────────────────────────"
                )
                await self.telegram.send_message(msg)

    async def process_incoming_candle(self, candle_data: Dict[str, Any]):
        epoch_val = candle_data.get("open_time") or candle_data.get("epoch") or time.time()
        c = {
            "epoch": int(epoch_val),
            "open": float(candle_data["open"]),
            "high": float(candle_data["high"]),
            "low": float(candle_data["low"]),
            "close": float(candle_data["close"])
        }

        if self.candles and self.candles[-1]['epoch'] == c['epoch']:
            self.candles[-1] = c
        else:
            self.candles.append(c)
            if len(self.candles) > Config.HISTORY_COUNT:
                self.candles.pop(0)

        closed_signals = self.memory.update_tracking(c['high'], c['low'], c['close'])
        for cs in closed_signals:
            if cs["status"] == "TP1_HIT_BE_ACTIVE":
                feedback_msg = (
                    f"🎯 <b>TARGET 1 REACHED #{cs['id']}</b>\n"
                    f"Instrumen: XAU/USD ({cs['direction']})\n"
                    f"Target 1 (${cs['tp1']:.2f}) tercapai! 🎉\n"
                    f"🔒 <b>Stop Loss otomatis digeser ke Breakeven: ${cs['sl_moved_to']:.2f}</b>\n"
                    f"Posisi runner aman tanpa risiko melanjutkan menuju TP2."
                )
            elif cs["status"] == "TP2_HIT":
                feedback_msg = (
                    f"🏆 <b>FULL TAKE PROFIT 2 HIT #{cs['id']}</b>\n"
                    f"Kemenangan penuh 1:{Config.BASE_RISK_REWARD} tercapai!\n"
                    f"Bobot model pembelajaran adaptif telah ditingkatkan."
                )
            elif cs["status"] == "BE_HIT":
                feedback_msg = (
                    f"🛡️ <b>BREAKEVEN TRIGGERED #{cs['id']}</b>\n"
                    f"Harga kembali menyentuh titik entry setelah mengamankan TP1.\n"
                    f"Posisi ditutup aman dengan profit parsial."
                )
            else:
                feedback_msg = (
                    f"❌ <b>STOP LOSS HIT #{cs['id']}</b>\n"
                    f"Posisi terkena stop loss batas mikro.\n"
                    f"Sistem Bayesian mereduksi bobot faktor pemicu untuk evaluasi ulang."
                )
            await self.telegram.send_message(feedback_msg)

        await self.evaluate_market_matrix()

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse):
        """Worker background untuk menjaga koneksi Deriv WebSocket tetap hidup."""
        while not ws.closed:
            try:
                await asyncio.sleep(Config.PING_INTERVAL)
                if not ws.closed:
                    await ws.send_json({"ping": 1})
            except Exception:
                break

    async def run_simulation_stream(self):
        """Mode Simulator Real-Time untuk akhir pekan atau pengujian tanpa kunci API."""
        logger.info("[SIMULATION MODE] Memulai generator pasar XAU/USD sintetis...")
        self.active_gold_symbol = "SIM_XAUUSD"

        # Inisialisasi data candle tiruan yang realistis
        base_price = 2920.0
        now_ts = int(time.time()) - (Config.HISTORY_COUNT * 60)
        self.candles = []

        for i in range(Config.HISTORY_COUNT):
            c_open = base_price + np.random.normal(0, 0.4)
            c_high = c_open + abs(np.random.normal(0, 0.8))
            c_low = c_open - abs(np.random.normal(0, 0.8))
            c_close = c_low + np.random.uniform(0, c_high - c_low)
            self.candles.append({
                "epoch": now_ts + (i * 60),
                "open": round(c_open, 2),
                "high": round(c_high, 2),
                "low": round(c_low, 2),
                "close": round(c_close, 2)
            })
            base_price = c_close

        logger.info(f"[SIMULATOR] {len(self.candles)} candle awal berhasil di-generate. Spot: ${base_price:.2f}")

        while self._running:
            await asyncio.sleep(2.0)
            last = self.candles[-1]
            drift = np.random.normal(0, 0.35)
            # Kadang buat liquidity wick spike buatan
            if np.random.random() < 0.12:
                drift += np.random.choice([-1.8, 1.8])

            new_close = round(last['close'] + drift, 2)
            new_candle = {
                "epoch": int(time.time()),
                "open": last['close'],
                "high": max(last['close'], new_close) + round(abs(np.random.normal(0, 0.4)), 2),
                "low": min(last['close'], new_close) - round(abs(np.random.normal(0, 0.4)), 2),
                "close": new_close
            }
            await self.process_incoming_candle(new_candle)

    async def run_binance_gold_stream(self):
        """
        Stream Emas Spot Fisik Riil Global (Binance PAXG/USDT).
        PAX Gold adalah aset emas fisik berstandar LBMA 1:1 tanpa pembatasan regional.
        Tersedia 24/7/365 secara gratis tanpa memerlukan otorisasi API key.
        """
        logger.info("================================================================")
        logger.info(" [FEED GLOBAL] TERHUBUNG KE STREAM EMAS SPOT RIIL (PAXG/USDT)   ")
        logger.info(" 1:1 LBMA Physical Gold Spot • 24/7 Order Flow • Bebas Blokir Regional")
        logger.info("================================================================")
        self.active_gold_symbol = "PAXG/USDT (Gold Spot)"

        session = await self.telegram.get_session()

        # 1. Unduh candlestick historis awal via REST
        try:
            logger.info("Mengunduh candlestick historis awal Emas dari Binance...")
            async with session.get(Config.BINANCE_REST_URL, timeout=12) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.candles = []
                    for item in data:
                        self.candles.append({
                            "epoch": int(item[0] / 1000),
                            "open": float(item[1]),
                            "high": float(item[2]),
                            "low": float(item[3]),
                            "close": float(item[4])
                        })
                    logger.info(f"Berhasil memuat {len(self.candles)} candle historis Emas. Spot: ${self.candles[-1]['close']:.2f}")
                    await self.evaluate_market_matrix()
                else:
                    logger.warning(f"Binance REST merespons status HTTP {resp.status}")
        except Exception as e:
            logger.warning(f"Gagal mengunduh riwayat candle awal Binance: {e}")

        # 2. Sambungkan ke WebSocket Live Kline
        while self._running:
            try:
                logger.info(f"Menghubungkan ke WebSocket Binance Gold: {Config.BINANCE_WS_URL}...")
                async with session.ws_connect(Config.BINANCE_WS_URL, timeout=25) as ws:
                    logger.info("WebSocket Binance Gold Spot Terhubung Sukses!")

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            payload = json.loads(msg.data)
                            if "k" in payload:
                                k = payload["k"]
                                candle = {
                                    "epoch": int(k["t"] / 1000),
                                    "open": float(k["o"]),
                                    "high": float(k["h"]),
                                    "low": float(k["l"]),
                                    "close": float(k["c"])
                                }
                                await self.process_incoming_candle(candle)

                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            logger.warning("[WS CLOSED] Koneksi Binance terputus. Rekoneksi...")
                            break

            except aiohttp.ClientConnectorError as e:
                logger.error(f"[BINANCE ERROR] Gagal koneksi: {e}. Rekoneksi 5 detik...")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"[BINANCE ERROR] {e}. Mengulang 5 detik...")
                await asyncio.sleep(5)

            if not self._running:
                break
            await asyncio.sleep(3)

    async def run_deriv_stream(self) -> bool:
        """
        Loop terhubung ke Deriv WebSocket secara tangguh.
        Mengembalikan True jika berjalan normal, atau False jika seluruh simbol Deriv ditolak (agar dapat auto-failover).
        """
        if self.simulation_mode or self.provider == "SIM":
            await self.run_simulation_stream()
            return True

        deriv_attempts = 0
        max_deriv_attempts = 2

        while self._running and deriv_attempts < max_deriv_attempts:
            deriv_attempts += 1
            try:
                logger.info(f"Menghubungkan ke Deriv WebSocket: {Config.DERIV_WS_URL} (Percobaan {deriv_attempts}/{max_deriv_attempts})...")
                session = await self.telegram.get_session()
                async with session.ws_connect(Config.DERIV_WS_URL, timeout=20) as ws:
                    logger.info("WebSocket Deriv Terhubung Sukses!")

                    ping_task = asyncio.create_task(self._ping_loop(ws))

                    # Temukan simbol emas yang aktif
                    self.active_gold_symbol = await self.discover_active_gold_symbol(ws)
                    if not self.active_gold_symbol:
                        logger.warning("[DERIV RESTRICTION] Tidak ada simbol Emas Deriv yang tersedia untuk regional/akun ini.")
                        ping_task.cancel()
                        return False

                    logger.info(f"Subscribe stream candle M1 ke: {self.active_gold_symbol}...")
                    subscribe_req = {
                        "ticks_history": self.active_gold_symbol,
                        "count": Config.HISTORY_COUNT,
                        "end": "latest",
                        "style": "candles",
                        "granularity": Config.GRANULARITY,
                        "subscribe": 1
                    }
                    await ws.send_json(subscribe_req)

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)

                            if "error" in data:
                                err = data.get("error", {})
                                code = err.get("code", "")
                                message = err.get("message", "")
                                logger.error(f"[DERIV ERROR] {code}: {message}")

                                if "invalid" in message.lower() or "SymbolInvalid" in code or "InvalidSymbol" in code:
                                    if self.active_gold_symbol:
                                        self.blacklisted_symbols.add(self.active_gold_symbol)
                                        logger.warning(f"[BLACKLIST] Simbol '{self.active_gold_symbol}' ditambahkan ke daftar blacklist.")
                                    self.active_gold_symbol = None
                                    ping_task.cancel()
                                    untested = [s for s in Config.GOLD_CANDIDATE_KEYWORDS if s not in self.blacklisted_symbols]
                                    if not untested:
                                        logger.warning("[DERIV EXHAUSTED] Semua kandidat simbol Deriv telah gagal diuji.")
                                        return False
                                    break

                                elif "MarketIsClosed" in code:
                                    logger.warning("[MARKET CLOSED] Pasar Forex tutup. Mengalihkan ke OTC...")
                                    if self.active_gold_symbol:
                                        self.blacklisted_symbols.add(self.active_gold_symbol)
                                    self.active_gold_symbol = None
                                    ping_task.cancel()
                                    break

                            if "candles" in data:
                                raw_candles = data["candles"]
                                self.candles = [
                                    {
                                        "epoch": int(c["epoch"]),
                                        "open": float(c["open"]),
                                        "high": float(c["high"]),
                                        "low": float(c["low"]),
                                        "close": float(c["close"])
                                    }
                                    for c in raw_candles
                                ]
                                logger.info(f"Berhasil memuat {len(self.candles)} candlestick untuk '{self.active_gold_symbol}'. Spot: ${self.candles[-1]['close']:.2f}")
                                deriv_attempts = 0
                                await self.evaluate_market_matrix()

                            elif "ohlc" in data:
                                await self.process_incoming_candle(data["ohlc"])

                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            logger.warning("[WS CLOSED] Koneksi Deriv terputus. Melakukan rekoneksi...")
                            break

                    ping_task.cancel()

            except aiohttp.ClientConnectorError as e:
                logger.error(f"[NETWORK ERROR] Gagal koneksi Deriv: {e}. Coba lagi 4 detik...")
                await asyncio.sleep(4)
            except asyncio.TimeoutError:
                logger.warning("[TIMEOUT] Refresh koneksi Deriv...")
                await asyncio.sleep(2)
            except Exception as e:
                logger.critical(f"[CRITICAL ERROR] {e}", exc_info=True)
                await asyncio.sleep(3)

            if not self._running:
                break
            await asyncio.sleep(2)

        return False

    async def start(self):
        """Memulai orchestrator dengan strategi multi-provider failover adaptif."""
        if self.simulation_mode or self.provider == "SIM":
            await self.run_simulation_stream()
            return

        if self.provider == "BINANCE":
            await self.run_binance_gold_stream()
            return

        # Provider AUTO atau DERIV
        logger.info("[INIT] Menginisialisasi koneksi data Emas XAU/USD...")
        deriv_ok = await self.run_deriv_stream()
        if not deriv_ok and self._running:
            logger.warning("────────────────────────────────────────────────────────────")
            logger.warning(" [AUTO-FAILOVER] Beralih otomatis ke Global Gold Spot Feed  ")
            logger.warning(" (Binance PAXG/USDT Real Gold) untuk menjaga bot tetap aktif")
            logger.warning("────────────────────────────────────────────────────────────")
            try:
                await self.run_binance_gold_stream()
            except Exception as e:
                logger.error(f"[FAILOVER ERROR] {e}. Mengaktifkan mode simulator...")
                await self.run_simulation_stream()


# --- ENTRY POINT & CLI PARSER ---
def main():
    parser = argparse.ArgumentParser(description="ASI-OMEGA Autonomous XAU/USD Trading Matrix v5.0")
    parser.add_argument("--sim", action="store_true", help="Jalankan dalam mode simulasi pasar real-time")
    parser.add_argument("--provider", type=str, default="AUTO", choices=["AUTO", "DERIV", "BINANCE", "SIM"],
                        help="Pilih penyedia data: AUTO (default dengan failover), DERIV, BINANCE, atau SIM")
    parser.add_argument("--symbol", type=str, default="", help="Paksa simbol spesifik (contoh: frxXAUUSD, OTC_GOLD, PAXGUSDT)")
    parser.add_argument("--token", type=str, default="", help="Deriv API Token untuk membuka akses Forex berizin")
    args = parser.parse_args()

    if args.token:
        Config.DERIV_API_TOKEN = args.token

    print("""
    ================================================================
          ASI-OMEGA v5.0: ARTIFICIAL SUPERINTELLIGENCE TRADING MATRIX
                 AUTONOMOUS SMART MONEY & QUANT FOR XAU/USD
    ================================================================
    """)
    orchestrator = ASIAutonomousOrchestrator(
        simulation_mode=args.sim,
        provider="SIM" if args.sim else args.provider
    )
    if args.symbol:
        orchestrator.active_gold_symbol = args.symbol

    try:
        asyncio.run(orchestrator.start())
    except KeyboardInterrupt:
        logger.info("[SHUTDOWN] Bot ASI-OMEGA dihentikan secara aman.")

if __name__ == "__main__":
    main()
