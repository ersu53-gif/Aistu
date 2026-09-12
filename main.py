import os, json, time, requests
import pandas as pd
from datetime import datetime, timezone

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT = os.getenv("TELEGRAM_CHAT_ID")
HEADERS = {"User-Agent": "Mozilla/5.0 ASI-V7.3-PERFECT"}
SPAM_FILE = "last_signal.json"

def send(msg):
    if not TOKEN or not CHAT:
        print(msg); return
    try:
        requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                      json={"chat_id": CHAT, "text": msg, "parse_mode": "HTML"}, timeout=15)
    except Exception as e:
        print(f"Telegram fail: {e}")

def is_news_block():
    try:
        r = requests.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json", timeout=15, headers=HEADERS)
        if r.status_code != 200: return False, "feed off"
        now = datetime.now(timezone.utc)
        for ev in r.json():
            if ev.get("country") != "USD": continue
            if ev.get("impact") != "High": continue
            ds = ev.get("date")
            if not ds: continue
            try:
                nt = datetime.fromisoformat(ds.replace("Z", "+00:00"))
                if nt.tzinfo is None: nt = nt.replace(tzinfo=timezone.utc)
                diff = (nt - now).total_seconds() / 60
                if -60 < diff < 90:
                    return True, f"{ev.get('title','USD HIGH')} {nt.strftime('%H:%M UTC')}"
            except: continue
        return False, "aman"
    except:
        return False, "fail open"

def get_session_block():
    now = datetime.now(timezone.utc)
    # Weekend block
    if now.weekday() >= 5:
        return True, f"WEEKEND {now.strftime('%A')} - Market tutup"
    h = now.hour
    if 0 <= h < 6: return True, f"ASIA {h:02d}:00 UTC sepi - SKIP"
    if 7 <= h <= 11: return False, f"LONDON KILLZONE {h:02d}:00 UTC - GAS"
    if 12 <= h <= 16: return False, f"NY KILLZONE {h:02d}:00 UTC - GAS BOOOM"
    return True, f"OFF {h:02d}:00 UTC capek - SKIP"

def get_h1_data():
    s = requests.Session()
    s.headers.update(HEADERS)
    # 1. Biquote
    try:
        r = s.get("https://biquote.io/api/XAUUSD/ohlc", params={"interval":"1h","limit":250}, timeout=20)
        j = r.json()
        bars = j.get("bars", []) if isinstance(j, dict) else []
        if len(bars) > 150:
            df = pd.DataFrame(bars)
            c = 'openTime' if 'openTime' in df.columns else 'timestamp' if 'timestamp' in df.columns else None
            if c:
                df[c] = pd.to_datetime(df[c])
                df = df.sort_values(c).reset_index(drop=True)
                df['close']=df['close'].astype(float); df['high']=df['high'].astype(float); df['low']=df['low'].astype(float)
                return df, "BIQUOTE-H1"
    except Exception as e: print(f"Biquote fail {e}")
    # 2. KuCoin PAXG
    try:
        r = s.get("https://api.kucoin.com/api/v1/market/candles?symbol=PAXG-USDT&type=1hour", timeout=20)
        raw = r.json().get("data", [])
        if len(raw) > 150:
            df = pd.DataFrame(raw, columns=['time','open','close','high','low','vol','turnover'])
            df['openTime'] = pd.to_datetime(df['time'].astype(int), unit='s')
            df = df.sort_values('openTime').reset_index(drop=True)
            df['close']=df['close'].astype(float); df['high']=df['high'].astype(float); df['low']=df['low'].astype(float)
            return df, "KUCOIN-H1"
    except Exception as e: print(f"KuCoin fail {e}")
    # 3. Kraken
    try:
        r = s.get("https://api.kraken.com/0/public/OHLC", params={"pair":"PAXGUSD","interval":60}, timeout=20)
        result = r.json().get("result", {})
        key = next((k for k in result.keys() if k!="last"), None)
        raw = result.get(key, []) if key else []
        if len(raw) > 150:
            df = pd.DataFrame(raw, columns=['time','open','high','low','close','vwap','vol','count'])
            df['openTime'] = pd.to_datetime(df['time'].astype(int), unit='s')
            df = df.sort_values('openTime').reset_index(drop=True)
            df['close']=df['close'].astype(float); df['high']=df['high'].astype(float); df['low']=df['low'].astype(float)
            return df, "KRAKEN-H1"
    except Exception as e: print(f"Kraken fail {e}")
    return None, "NONE"

def main():
    # ANTI SPAM PERSISTENT 12 JAM
    if os.path.exists(SPAM_FILE):
        try:
            last = json.load(open(SPAM_FILE))
            elapsed = time.time() - last.get('ts',0)
            if elapsed < 43200:
                print(f"ANTI SPAM {elapsed/3600:.1f} jam lalu - skip")
                return
        except: pass

    blocked_news, news_msg = is_news_block()
    if blocked_news:
        print(f"NEWS BLOCK: {news_msg}")
        send(f"⏸️ <b>PAUSE NEWS</b>\n🚫 {news_msg}\n90 menit anti zonk.")
        return

    blocked_sess, sess_msg = get_session_block()
    if blocked_sess:
        print(f"SESI BLOCK: {sess_msg}")
        return

    df, src = get_h1_data()
    if df is None:
        print("SEMUA FEED MATI"); return

    closes = df['close'].tolist()
    highs = df['high'].tolist()
    lows = df['low'].tolist()
    entry = closes[-1]
    ema50 = sum(closes[-50:]) / 50
    ema200 = sum(closes[-200:]) / 200

    h1_bull = entry > ema50 and ema50 > ema200
    h1_bear = entry < ema50 and ema50 < ema200
    if not (h1_bull or h1_bear):
        print("Sideways EMA"); return

    direction = "BUY" if h1_bull else "SELL"
    # Validasi sweep
    if direction == "BUY":
        if lows[-1] >= min(lows[-20:-1]): print("No sweep buy"); return
    else:
        if highs[-1] <= max(highs[-20:-1]): print("No sweep sell"); return

    tp1 = max(highs[-50:-10]) if direction=="BUY" else min(lows[-50:-10])
    tp2 = max(highs) if direction=="BUY" else min(lows)
    sl = min(lows[-20:-1]) - 3.0 if direction=="BUY" else max(highs[-20:-1]) + 3.0

    risk = abs(entry - sl)
    reward = abs(tp2 - entry)
    if risk < 5: print(f"Risk ecek {risk}"); return
    rr = reward / risk if risk>0 else 0
    if rr < 3.0 or reward < 15:
        print(f"RR ecek {rr:.2f} reward {reward:.1f}"); return

    msg = (f"🔥 <b>ASI V7.3 PERFECT</b> 🔥\n"
           f"━━━━━━━━━━━━━━\n"
           f"SESI: {sess_msg}\n"
           f"NEWS: ✅ {news_msg}\n"
           f"SRC: {src}\n"
           f"━━━━━━━━━━━━━━\n"
           f"{'🟢 BUY' if direction=='BUY' else '🔴 SELL'} H1\n"
           f"ENTRY: <code>${entry:.2f}</code>\n"
           f"SL: <code>${sl:.2f}</code> ({risk:.1f}$)\n"
           f"TP1: <code>${tp1:.2f}</code> (+{abs(tp1-entry):.1f}$)\n"
           f"TP2: <code>${tp2:.2f}</code> (+{reward:.1f}$)\n"
           f"RR: <b>1:{rr:.2f}</b>\n"
           f"HOLD: 8-24 JAM\n"
           f"━━━━━━━━━━━━━━\n"
           f"Anti Spam 12J | Weekend Block | News Block")

    send(msg)
    json.dump({"ts": time.time(), "dir": direction, "entry": entry, "rr": rr}, open(SPAM_FILE, "w"))
    print("SENT BOOOM")

if __name__ == "__main__":
    main()
