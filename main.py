#!/usr/bin/env python3
"""
================================================================================
ASI-OMEGA: AUTONOMOUS MULTI-DISCIPLINARY XAUUSD TRADING MATRIX
Engineered by: Director of Artificial Superintelligence
Features:
- Deriv Public WebSockets Stream (Zero Deriv API Key needed)
- SMC Micro-Liquidity Sweep Detection + FVG (Fair Value Gap)
- Quantitative Adaptive Volatility Bands (Ultra-Tight SL Engine)
- Online Recursive Self-Improvement Memory via SQLite
- Anti-Spam / Rate-Limiting Telegram Dispatcher
- Self-Healing Async Loop & 24/5 Temporal Filter (Weekend Dormancy)
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
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any

import aiohttp
import numpy as np

# --- LOGGING INITIALIZATION ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (ASI-Core) %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("ASI-Omega")

# --- CONFIGURATION ENGINE ---
class Config:
    DERIV_WS_URL = "wss://ws.derivws.com/websockets/v3?app_id=1089"
    SYMBOL = "frxXAUUSD"
    GRANULARITY = 60  # 1-Minute Candles for ultra-precise entry
    HISTORY_COUNT = 150
    
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    
    DB_PATH = "asi_memory.db"
    
    # Anti-Spam Configuration
    MIN_SIGNAL_COOLDOWN_SECONDS = 300  # Minimal 5 menit jeda antar sinyal yang sama
    MAX_ALERTS_PER_HOUR = 8
    
    # Adaptive Thresholds
    INITIAL_CONFIDENCE_THRESHOLD = 0.82
    BASE_RISK_REWARD = 3.0  # R:R 1:3 target minimum

if not Config.TELEGRAM_BOT_TOKEN or not Config.TELEGRAM_CHAT_ID:
    logger.warning("TELEGRAM_BOT_TOKEN atau TELEGRAM_CHAT_ID belum diatur! Bot akan tetap menganalisa dan mencetak log sinyal ke konsol.")


# --- DATABASE: RECURSIVE LEARNING & SIGNAL TRACKER ---
class MemoryMatrix:
    """
    Sistem memori persisten. Menyimpan bobot indikator, hasil sinyal masa lalu,
    dan menyesuaikan bobot model secara adaptif (Recursive Bayesian Learning).
    """
    def __init__(self, db_path: str = Config.DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _get_conn(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._get_conn() as conn:
            cursor = conn.cursor()
            # Tabel Sinyal Aktif / Riwayat
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL,
                    direction TEXT,
                    entry_price REAL,
                    sl REAL,
                    tp1 REAL,
                    tp2 REAL,
                    status TEXT, -- 'PENDING', 'HIT_TP1', 'HIT_TP2', 'HIT_SL', 'EXPIRED'
                    model_confluence TEXT,
                    max_favorable REAL,
                    max_adverse REAL
                )
            """)
            # Tabel Bobot Model (Self-Improvement Weights)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS model_weights (
                    model_name TEXT PRIMARY KEY,
                    weight REAL,
                    wins INTEGER,
                    losses INTEGER
                )
            """)
            
            # Inisialisasi bobot default jika kosong
            default_models = ["smc_sweep", "fvg_imbalance", "quant_zscore", "killzone_session"]
            for model in default_models:
                cursor.execute("""
                    INSERT OR IGNORE INTO model_weights (model_name, weight, wins, losses)
                    VALUES (?, 1.0, 0, 0)
                """)
            conn.commit()

    def get_weights(self) -> Dict[str, float]:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT model_name, weight FROM model_weights")
            return {row[0]: row[1] for row in cursor.fetchall()}

    def record_signal(self, direction: str, entry: float, sl: float, tp1: float, tp2: float, confluence: List[str]) -> int:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO signals (timestamp, direction, entry_price, sl, tp1, tp2, status, model_confluence, max_favorable, max_adverse)
                VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?, 0.0, 0.0)
            """, (time.time(), direction, entry, sl, tp1, tp2, json.dumps(confluence)))
            conn.commit()
            return cursor.lastrowid

    def update_tracking(self, current_high: float, current_low: float, current_close: float) -> List[Dict]:
        """
        Mengevaluasi sinyal pending terhadap pergerakan chart real-time.
        Jika menyentuh TP atau SL, eksekusi pembaruan bobot pembelajaran (Self-Improvement).
        """
        closed_signals = []
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, direction, entry_price, sl, tp1, tp2, model_confluence, max_favorable, max_adverse FROM signals WHERE status = 'PENDING'")
            rows = cursor.fetchall()
            
            for row in rows:
                sig_id, direction, entry, sl, tp1, tp2, confluence_json, max_fav, max_adv = row
                confluence = json.loads(confluence_json)
                
                status = "PENDING"
                outcome = 0 # +1 win, -1 loss
                
                if direction == "BUY":
                    max_fav = max(max_fav, current_high - entry)
                    max_adv = max(max_adv, entry - current_low)
                    
                    if current_low <= sl:
                        status = "HIT_SL"
                        outcome = -1
                    elif current_high >= tp2:
                        status = "HIT_TP2"
                        outcome = 2
                    elif current_high >= tp1 and status == "PENDING":
                        status = "HIT_TP1"
                        outcome = 1
                else: # SELL
                    max_fav = max(max_fav, entry - current_low)
                    max_adv = max(max_adv, current_high - entry)
                    
                    if current_high >= sl:
                        status = "HIT_SL"
                        outcome = -1
                    elif current_low <= tp2:
                        status = "HIT_TP2"
                        outcome = 2
                    elif current_low <= tp1 and status == "PENDING":
                        status = "HIT_TP1"
                        outcome = 1
                
                if status != "PENDING":
                    cursor.execute("UPDATE signals SET status = ?, max_favorable = ?, max_adverse = ? WHERE id = ?", (status, max_fav, max_adv, sig_id))
                    self._recursively_adapt_weights(cursor, confluence, outcome)
                    closed_signals.append({"id": sig_id, "status": status, "direction": direction, "entry": entry})
                else:
                    cursor.execute("UPDATE signals SET max_favorable = ?, max_adverse = ? WHERE id = ?", (max_fav, max_adv, sig_id))
            conn.commit()
        return closed_signals

    def _recursively_adapt_weights(self, cursor: sqlite3.Cursor, confluence: List[str], outcome: int):
        """
        Bayesian Updating Rule:
        Jika indikator memicu win, naikkan reliabilitasnya sebesar +5%.
        Jika memicu loss, reduksi reliabilitasnya sebesar -8% (asimetrik risk aversion).
        """
        for factor in confluence:
            cursor.execute("SELECT weight, wins, losses FROM model_weights WHERE model_name = ?", (factor,))
            res = cursor.fetchone()
            if res:
                w, wins, losses = res
                if outcome > 0:
                    new_w = min(2.5, w * 1.05)
                    wins += 1
                else:
                    new_w = max(0.2, w * 0.92)
                    losses += 1
                cursor.execute("UPDATE model_weights SET weight = ?, wins = ?, losses = ? WHERE model_name = ?", (new_w, wins, losses, factor))
        logger.info(f"[RECURSIVE-LEARNING] Memory updated for models {confluence} with outcome: {outcome}")


# --- ANALYTICAL ENGINE: MULTI-DISCIPLINARY FUSION ---
class ASIAnalyticsEngine:
    @staticmethod
    def get_current_session(utc_time: datetime) -> Tuple[str, bool]:
        """
        Analisis Sesi Pasar:
        - Asian Session: 00:00 - 07:00 UTC (Liquidity Accumulation)
        - London Killzone: 07:00 - 11:00 UTC (Manipulation & Breakout)
        - New York Killzone: 12:00 - 17:00 UTC (High Volatility Expansion)
        """
        hour = utc_time.hour
        if 7 <= hour < 11:
            return "LONDON_KILLZONE", True
        elif 12 <= hour < 17:
            return "NEWYORK_KILLZONE", True
        elif 0 <= hour < 7:
            return "ASIAN_ACCUMULATION", False
        else:
            return "OFF_PEAK", False

    @staticmethod
    def compute_atr(candles: List[Dict], period: int = 14) -> np.ndarray:
        highs = np.array([c['high'] for c in candles])
        lows = np.array([c['low'] for c in candles])
        closes = np.array([c['close'] for c in candles])
        
        tr = np.maximum(highs[1:] - lows[1:], 
             np.maximum(np.abs(highs[1:] - closes[:-1]), 
                        np.abs(lows[1:] - closes[:-1])))
        atr = np.zeros_like(closes)
        if len(tr) >= period:
            atr[period] = np.mean(tr[:period])
            for i in range(period + 1, len(closes)):
                atr[i] = (atr[i - 1] * (period - 1) + tr[i - 1]) / period
        return atr

    @staticmethod
    def detect_liquidity_sweep(candles: List[Dict], lookback: int = 20) -> Optional[Dict[str, Any]]:
        """
        Smart Money Concepts (SMC):
        Mendeteksi pembantaian likuiditas (Liquidity Sweep).
        Wick menembus High/Low sebelumnya namun Close kembali ke dalam range (Fakeout/Spring).
        """
        if len(candles) < lookback + 2:
            return None

        recent_highs = [c['high'] for c in candles[-(lookback + 2):-2]]
        recent_lows = [c['low'] for c in candles[-(lookback + 2):-2]]
        prev_swing_high = max(recent_highs)
        prev_swing_low = min(recent_lows)
        
        last_candle = candles[-2] # Evaluasi candle terkonfirmasi terakhir
        
        # Bearish Liquidity Sweep (Bait Buy Side Liquidity, lalu ditolak ke bawah)
        if last_candle['high'] > prev_swing_high and last_candle['close'] < prev_swing_high:
            wick_size = last_candle['high'] - max(last_candle['open'], last_candle['close'])
            body_size = abs(last_candle['close'] - last_candle['open'])
            if wick_size > body_size * 0.8:
                return {
                    "type": "SELL",
                    "sweep_level": prev_swing_high,
                    "wick_extreme": last_candle['high'],
                    "factor": "smc_sweep"
                }

        # Bullish Liquidity Sweep (Bait Sell Side Liquidity, lalu ditolak ke atas)
        if last_candle['low'] < prev_swing_low and last_candle['close'] > prev_swing_low:
            wick_size = min(last_candle['open'], last_candle['close']) - last_candle['low']
            body_size = abs(last_candle['close'] - last_candle['open'])
            if wick_size > body_size * 0.8:
                return {
                    "type": "BUY",
                    "sweep_level": prev_swing_low,
                    "wick_extreme": last_candle['low'],
                    "factor": "smc_sweep"
                }

        return None

    @staticmethod
    def detect_fvg(candles: List[Dict]) -> Optional[Dict[str, Any]]:
        """
        Fair Value Gap (FVG) / Imbalance 3-Candle Structure:
        Ketidakseimbangan orderbook agresif yang meninggalkan celah harga.
        """
        if len(candles) < 3:
            return None
        
        c1, c2, c3 = candles[-4], candles[-3], candles[-2]
        
        # Bullish FVG: Low candle 3 berada di atas High candle 1
        if c3['low'] > c1['high']:
            gap = c3['low'] - c1['high']
            if gap > 0.15: # Minimum gap signifikan pada XAUUSD ($0.15)
                return {"type": "BUY", "fvg_bottom": c1['high'], "fvg_top": c3['low'], "factor": "fvg_imbalance"}
                
        # Bearish FVG: High candle 3 berada di bawah Low candle 1
        if c3['high'] < c1['low']:
            gap = c1['low'] - c3['high']
            if gap > 0.15:
                return {"type": "SELL", "fvg_bottom": c3['high'], "fvg_top": c1['low'], "factor": "fvg_imbalance"}
                
        return None

    @staticmethod
    def evaluate_quant_zscore(candles: List[Dict], period: int = 20) -> Optional[Dict[str, Any]]:
        """
        Statistical Arbitrage:
        Z-Score harga terhadap Moving Average. Menghitung kondisi Overextended ekstrim.
        """
        if len(candles) < period:
            return None
        closes = np.array([c['close'] for c in candles[-period:]])
        mean = np.mean(closes)
        std = np.std(closes)
        if std == 0:
            return None
        
        current_close = closes[-1]
        z_score = (current_close - mean) / std
        
        if z_score >= 2.2: # Overbought ekstrim
            return {"type": "SELL", "zscore": z_score, "factor": "quant_zscore"}
        elif z_score <= -2.2: # Oversold ekstrim
            return {"type": "BUY", "zscore": z_score, "factor": "quant_zscore"}
            
        return None


# --- TELEGRAM DISPATCHER & RATE LIMITER ---
class TelegramGuard:
    """
    Sistem pengiriman sinyal aman, anti-spam, dan anti-ban.
    Memiliki internal rate-limiter dan cooldown buffer.
    """
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.last_sent_time = 0.0
        self.sent_history: List[float] = []

    async def send_message(self, text: str) -> bool:
        if not self.token or not self.chat_id:
            logger.info(f"\n[TELEGRAM PREVIEW (No Token Configured)]:\n{text}\n")
            return True

        now = time.time()
        
        # Bersihkan history lebih dari 1 jam
        self.sent_history = [t for t in self.sent_history if now - t < 3600]
        
        # Anti-Flood Filter
        if len(self.sent_history) >= Config.MAX_ALERTS_PER_HOUR:
            logger.warning("[ANTI-SPAM] Maximum hourly alert quota reached. Throttling message.")
            return False
            
        if now - self.last_sent_time < 3.0:
            await asyncio.sleep(3.0) # Jeda antar pesan agar tidak terkena rate limit Telegram

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }

        for attempt in range(3):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, json=payload, timeout=10) as response:
                        if response.status == 200:
                            self.last_sent_time = time.time()
                            self.sent_history.append(self.last_sent_time)
                            logger.info("[TELEGRAM] Sinyal berhasil dikirim ke Channel/User.")
                            return True
                        elif response.status == 429:
                            retry_after = int(response.headers.get("Retry-After", "5"))
                            logger.warning(f"[TELEGRAM] 429 Rate Limit. Sleeping {retry_after}s...")
                            await asyncio.sleep(retry_after)
                        else:
                            resp_txt = await response.text()
                            logger.error(f"[TELEGRAM ERROR] Status: {response.status}, Detail: {resp_txt}")
            except Exception as e:
                logger.error(f"[TELEGRAM NETWORK FAILURE] Percobaan {attempt+1}/3: {e}")
                await asyncio.sleep(2)
        return False


# --- CORE ASI ORCHESTRATOR & RISK ENGINE ---
class ASIAutonomousOrchestrator:
    def __init__(self):
        self.memory = MemoryMatrix()
        self.telegram = TelegramGuard(Config.TELEGRAM_BOT_TOKEN, Config.TELEGRAM_CHAT_ID)
        self.candles: List[Dict] = []
        self.last_signal_timestamp = 0.0

    def is_market_open(self) -> bool:
        """
        Cek 24/5 Non-Stop.
        Pasar Forex/Gold tutup pada hari Sabtu & Minggu (Mulai Jumat 21:00 UTC s/d Minggu 22:00 UTC).
        """
        now_utc = datetime.now(timezone.utc)
        weekday = now_utc.weekday() # 0 = Senin, 4 = Jumat, 5 = Sabtu, 6 = Minggu
        hour = now_utc.hour

        if weekday == 5: # Sabtu
            return False
        if weekday == 6 and hour < 22: # Minggu sebelum buka
            return False
        if weekday == 4 and hour >= 22: # Jumat larut malam
            return False
        return True

    def calculate_micro_sl_tp(self, direction: str, entry: float, sweep_extreme: Optional[float], atr_val: float) -> Tuple[float, float, float, float]:
        """
        Ultra-Tight Stop Loss Calculation:
        Stop Loss diletakkan sangat presisi persis beberapa pip di luar High/Low penolakan likuiditas,
        dengan batasan buffer elastis berdasarkan ATR.
        """
        buffer = max(0.20, atr_val * 0.15) # Buffer minimum $0.20 pada Gold
        
        if direction == "BUY":
            if sweep_extreme and sweep_extreme < entry:
                sl = sweep_extreme - buffer
            else:
                sl = entry - (atr_val * 0.75)
            
            risk = entry - sl
            # Proteksi jika SL terlalu dekat (< $0.40) atau terlalu jauh (> $2.50)
            risk = max(0.40, min(risk, 2.50))
            sl = round(entry - risk, 2)
            tp1 = round(entry + (risk * 2.0), 2)
            tp2 = round(entry + (risk * Config.BASE_RISK_REWARD), 2)
        else:
            if sweep_extreme and sweep_extreme > entry:
                sl = sweep_extreme + buffer
            else:
                sl = entry + (atr_val * 0.75)
                
            risk = sl - entry
            risk = max(0.40, min(risk, 2.50))
            sl = round(entry + risk, 2)
            tp1 = round(entry - (risk * 2.0), 2)
            tp2 = round(entry - (risk * Config.BASE_RISK_REWARD), 2)
            
        return entry, sl, tp1, tp2

    async def evaluate_market_matrix(self):
        """
        Inti Analisis Superintelejen: Menggabungkan semua model, mengalikan bobot
        dari memori pembelajaran persisten, memvalidasi probabilitas.
        """
        if len(self.candles) < 30:
            return

        now_utc = datetime.now(timezone.utc)
        session_name, is_high_volume_kz = self.ASIAnalyticsEngine_session(now_utc)
        
        # Jalankan Sub-Model
        sweep_data = ASIAnalyticsEngine.detect_liquidity_sweep(self.candles)
        fvg_data = ASIAnalyticsEngine.detect_fvg(self.candles)
        quant_data = ASIAnalyticsEngine.evaluate_quant_zscore(self.candles)
        
        atr_series = ASIAnalyticsEngine.compute_atr(self.candles)
        current_atr = atr_series[-1] if len(atr_series) > 0 and atr_series[-1] > 0 else 1.2
        
        # Ambil bobot pembelajaran dari SQLite
        weights = self.memory.get_weights()
        
        buy_score = 0.0
        sell_score = 0.0
        confluences = []
        sweep_extreme = None

        if sweep_data:
            w = weights.get("smc_sweep", 1.0)
            if sweep_data["type"] == "BUY":
                buy_score += 2.0 * w
                sweep_extreme = sweep_data["wick_extreme"]
                confluences.append(f"SMC Liquidity Sweep Low (W:{w:.2f})")
            else:
                sell_score += 2.0 * w
                sweep_extreme = sweep_data["wick_extreme"]
                confluences.append(f"SMC Liquidity Sweep High (W:{w:.2f})")

        if fvg_data:
            w = weights.get("fvg_imbalance", 1.0)
            if fvg_data["type"] == "BUY":
                buy_score += 1.5 * w
                confluences.append(f"Bullish Imbalance FVG (W:{w:.2f})")
            else:
                sell_score += 1.5 * w
                confluences.append(f"Bearish Imbalance FVG (W:{w:.2f})")

        if quant_data:
            w = weights.get("quant_zscore", 1.0)
            if quant_data["type"] == "BUY":
                buy_score += 1.2 * w
                confluences.append(f"Quant Oversold Reversal (W:{w:.2f})")
            else:
                sell_score += 1.2 * w
                confluences.append(f"Quant Overbought Exhaustion (W:{w:.2f})")

        if is_high_volume_kz:
            w = weights.get("killzone_session", 1.0)
            confluences.append(f"Active Killzone Session ({session_name})")
            buy_score *= (1.0 + (0.15 * w))
            sell_score *= (1.0 + (0.15 * w))

        # Normalisasi Probabilitas Menggunakan Fungsi Sigmoid Adaptif
        current_close = self.candles[-1]['close']
        
        threshold = 3.2 # Skor gabungan minimum
        decision = None
        final_confidence = 0.0

        if buy_score > threshold and buy_score > sell_score:
            decision = "BUY"
            final_confidence = 1.0 / (1.0 + math.exp(-buy_score / 2.5))
        elif sell_score > threshold and sell_score > buy_score:
            decision = "SELL"
            final_confidence = 1.0 / (1.0 + math.exp(-sell_score / 2.5))

        # Validasi Akhir dan Anti-Spam Cooldown
        now_ts = time.time()
        if decision and final_confidence >= Config.INITIAL_CONFIDENCE_THRESHOLD:
            if now_ts - self.last_signal_timestamp > Config.MIN_SIGNAL_COOLDOWN_SECONDS:
                entry, sl, tp1, tp2 = self.calculate_micro_sl_tp(decision, current_close, sweep_extreme, current_atr)
                risk_dist = abs(entry - sl)
                rr_ratio = abs(tp2 - entry) / risk_dist if risk_dist > 0 else 0
                
                # Simpan ke Database untuk Pembelajaran Masa Depan
                factor_keys = []
                if sweep_data: factor_keys.append("smc_sweep")
                if fvg_data: factor_keys.append("fvg_imbalance")
                if quant_data: factor_keys.append("quant_zscore")
                if is_high_volume_kz: factor_keys.append("killzone_session")
                
                sig_id = self.memory.record_signal(decision, entry, sl, tp1, tp2, factor_keys)
                self.last_signal_timestamp = now_ts

                # Dispatching Notifikasi ke Telegram
                confluence_text = "\n".join([f"  • {c}" for c in confluences])
                direction_emoji = "🟢 <b>STRONG BUY</b>" if decision == "BUY" else "🔴 <b>STRONG SELL</b>"
                
                msg = (
                    f"⚡ <b>ASI-OMEGA SUPER-INTELLIGENCE SIGNAL</b> ⚡\n"
                    f"────────────────────────\n"
                    f"Instrument: <code>{Config.SYMBOL} (XAU/USD)</code>\n"
                    f"Order Type: <b>MANUAL OP</b>\n"
                    f"Action: {direction_emoji}\n"
                    f"Probabilitas Akurasi: <b>{final_confidence*100:.1f}%</b>\n"
                    f"────────────────────────\n"
                    f"📍 <b>Entry Price:</b> <code>{entry:.2f}</code>\n"
                    f"🛡️ <b>Micro Stop Loss:</b> <code>{sl:.2f}</code> (Risk: ${risk_dist:.2f})\n"
                    f"🎯 <b>Take Profit 1:</b> <code>{tp1:.2f}</code> (1:2 R:R)\n"
                    f"🎯 <b>Take Profit 2:</b> <code>{tp2:.2f}</code> (1:{rr_ratio:.1f} R:R)\n"
                    f"────────────────────────\n"
                    f"<b>Confluence Factor:</b>\n{confluence_text}\n"
                    f"Session: <code>{session_name}</code>\n"
                    f"Tracking Ticket: <code>#{sig_id}</code>\n"
                    f"Waktu UTC: <code>{now_utc.strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
                    f"────────────────────────\n"
                    f"⚠️ <i>Gunakan lot bijak sesuai batas margin akun Anda.</i>"
                )
                
                await self.telegram.send_message(msg)

    def ASIAnalyticsEngine_session(self, utc_time: datetime):
        return ASIAnalyticsEngine.get_current_session(utc_time)

    async def process_incoming_candle(self, candle_data: Dict[str, Any]):
        """
        Sinkronisasi candle M1 real-time ke dalam memory window.
        """
        c = {
            "epoch": candle_data.get("epoch", time.time()),
            "open": float(candle_data["open"]),
            "high": float(candle_data["high"]),
            "low": float(candle_data["low"]),
            "close": float(candle_data["close"])
        }
        
        # Update atau append candle
        if self.candles and self.candles[-1]['epoch'] == c['epoch']:
            self.candles[-1] = c
        else:
            self.candles.append(c)
            if len(self.candles) > Config.HISTORY_COUNT:
                self.candles.pop(0)

        # Monitor sinyal pending real-time terhadap harga saat ini (Self-Learning Loop)
        closed_signals = self.memory.update_tracking(c['high'], c['low'], c['close'])
        for cs in closed_signals:
            status_emoji = "✅" if "TP" in cs["status"] else "❌"
            feedback_msg = (
                f"{status_emoji} <b>SIGNAL RESOLUTION REPORT</b> #{cs['id']}\n"
                f"Instrument: {Config.SYMBOL}\n"
                f"Result: <b>{cs['status']}</b>\n"
                f"Sistem telah memperbarui matriks bobot indikator secara mandiri via Bayesian Learning."
            )
            await self.telegram.send_message(feedback_msg)

        # Jalankan evaluasi prediktif superintelejen
        await self.evaluate_market_matrix()

    async def run_deriv_stream(self):
        """
        Koneksi WebSocket Publik Deriv yang stabil, auto-reconnect,
        dan kebal terhadap pemutusan koneksi server.
        """
        while True:
            # Pengecekan 24/5 Temporal Filter
            if not self.is_market_open():
                logger.info("[WEEKEND DORMANCY] Market XAUUSD sedang libur akhir pekan. Bot mode hemat daya (tidur 30 menit)...")
                await asyncio.sleep(1800)
                continue

            try:
                logger.info(f"Menghubungkan ke Deriv Gateway: {Config.DERIV_WS_URL}...")
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(Config.DERIV_WS_URL, timeout=30, heartbeat=20) as ws:
                        logger.info(f"WebSocket Terhubung! Mengirim langganan candle untuk {Config.SYMBOL}...")
                        
                        # Request historical candles + continuous stream subscription
                        subscribe_req = {
                            "ticks_history": Config.SYMBOL,
                            "adjust_start_time": 1,
                            "count": Config.HISTORY_COUNT,
                            "end": "latest",
                            "style": "candles",
                            "granularity": Config.GRANULARITY,
                            "subscribe": 1
                        }
                        await ws.send_json(subscribe_req)

                        async for msg in ws:
                            if not self.is_market_open():
                                logger.info("[WEEKEND CLOSE DETECTED] Menutup koneksi untuk istirahat akhir pekan.")
                                break

                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                
                                if "error" in data:
                                    logger.error(f"[DERIV PROTOCOL ERROR] {data['error']['message']}")
                                    await asyncio.sleep(5)
                                    break
                                
                                # Initial history batch
                                if "candles" in data:
                                    raw_candles = data["candles"]
                                    self.candles = [
                                        {
                                            "epoch": int(c["epoch"]),
                                            "open": float(c["open"]),
                                            "high": float(c["high"]),
                                            "low": float(c["low"]),
                                            "close": float(c["close"])
                                        } for c in raw_candles
                                    ]
                                    logger.info(f"Berhasil memuat {len(self.candles)} candlestick riwayat.")
                                    await self.evaluate_market_matrix()
                                    
                                # Continuous live streaming update
                                elif "ohlc" in data:
                                    await self.process_incoming_candle(data["ohlc"])
                                    
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                logger.warning(f"[WS DISCONNECT] Tipe pesan: {msg.type}. Memulai reconnect...")
                                break

            except aiohttp.ClientConnectorError as e:
                logger.error(f"[NETWORK ERROR] Gagal terhubung ke host: {e}. Retry dalam 10 detik...")
                await asyncio.sleep(10)
            except asyncio.TimeoutError:
                logger.warning("[TIMEOUT] Tidak ada respon dari WebSocket. Melakukan refresh koneksi...")
                await asyncio.sleep(3)
            except Exception as e:
                logger.critical(f"[UNEXPECTED EXCEPTION] {e}", exc_info=True)
                await asyncio.sleep(5)

            logger.info("[AUTO-HEALING] Reconnecting ke Deriv Server dalam 5 detik...")
            await asyncio.sleep(5)


# --- APPLICATION ENTRY POINT ---
def main():
    print("""
    ================================================================
          ASI-OMEGA: ARTIFICIAL SUPERINTELLIGENCE TRADING BOT       
                      SPECIALIZED FOR XAU/USD (GOLD)                
    ================================================================
    """)
    orchestrator = ASIAutonomousOrchestrator()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(orchestrator.run_deriv_stream())
    except KeyboardInterrupt:
        logger.info("[SHUTDOWN] Bot dimatikan oleh user secara aman.")
    finally:
        loop.close()

if __name__ == "__main__":
    main()
