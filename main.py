import os, json, time, requests
import pandas as pd
from datetime import datetime, timezone

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT = os.getenv("TELEGRAM_CHAT_ID")
HEADERS = {"User-Agent": "Mozilla/5.0 ASI-V7.4-ALL"}
SPAM_FILE = "last_signal.json"

def send(msg):
    if not TOKEN or not CHAT: print(msg); return
    try: requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage", json={"chat_id": CHAT, "text": msg, "parse_mode": "HTML"}, timeout=15)
    except: pass

def is_news_block():
    try:
        r = requests.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json", timeout=15, headers=HEADERS)
        if r.status_code!= 200: return False, "aman"
        now = datetime.now(timezone.utc)
        for ev in r.json():
            if ev.get("country")!="USD" or ev.get("impact")!="High": continue
            try:
                nt = datetime.fromisoformat(ev.get("date").replace("Z","+00:00"))
                if nt.tzinfo is None: nt=nt.replace(tzinfo=timezone.utc)
                if -60 < (nt-now).total_seconds()/60 < 90:
                    return True, f"{ev.get('title')} {nt.strftime('%H:%M UTC')}"
            except: continue
        return False, "aman"
    except: return False, "fail"

def get_session_info():
    now = datetime.now(timezone.utc)
    h = now.hour
    if now.weekday()>=5: return f"WEEKEND {now.strftime('%A')} {h:02d}:00", False
    if 0<=h<6: return f"ASIA {h:02d}:00 - TP kecil", False
    if 7<=h<=11: return f"LONDON {h:02d}:00 - GAS", False
    if 12<=h<=16: return f"NY {h:02d}:00 - BOOOM", False
    return f"OFF {h:02d}:00 - GAS", False

def get_h1_data():
    s=requests.Session(); s.headers.update(HEADERS)
    try:
        r=s.get("https://biquote.io/api/XAUUSD/ohlc", params={"interval":"1h","limit":250}, timeout=20)
        bars=r.json().get("bars",[]) if isinstance(r.json(), dict) else []
        if len(bars)>150:
            df=pd.DataFrame(bars)
            c='openTime' if 'openTime' in df.columns else 'timestamp'
            df[c]=pd.to_datetime(df[c]); df=df.sort_values(c).reset_index(drop=True)
            df['close']=df['close'].astype(float); df['high']=df['high'].astype(float); df['low']=df['low'].astype(float)
            return df, "BIQUOTE-H1"
    except: pass
    try:
        r=s.get("https://api.kucoin.com/api/v1/market/candles?symbol=PAXG-USDT&type=1hour", timeout=20)
        raw=r.json().get("data",[])
        if len(raw)>150:
            df=pd.DataFrame(raw, columns=['time','open','close','high','low','vol','turnover'])
            df['openTime']=pd.to_datetime(df['time'].astype(int), unit='s')
            df=df.sort_values('openTime').reset_index(drop=True)
            df['close']=df['close'].astype(float); df['high']=df['high'].astype(float); df['low']=df['low'].astype(float)
            return df, "KUCOIN-H1"
    except: pass
    return None, "NONE"

def main():
    if os.path.exists(SPAM_FILE):
        try:
            last=json.load(open(SPAM_FILE))
            if time.time()-last.get('ts',0) < 43200:
                print(f"ANTI SPAM { (time.time()-last.get('ts',0))/3600:.1f} jam"); return
        except: pass

    blocked_news, news_msg = is_news_block()
    if blocked_news:
        print(f"NEWS BLOCK {news_msg}"); send(f"⏸️ PAUSE NEWS\n{news_msg}"); return

    sess_msg, _ = get_session_info()
    print(f"SESI: {sess_msg} - ALL SESSION GAS")

    df,src=get_h1_data()
    if df is None: return
    closes=df['close'].tolist(); highs=df['high'].tolist(); lows=df['low'].tolist()
    entry=closes[-1]; ema50=sum(closes[-50:])/50; ema200=sum(closes[-200:])/200
    h1_bull=entry>ema50 and ema50>ema200; h1_bear=entry<ema50 and ema50<ema200
    if not (h1_bull or h1_bear): return
    direction="BUY" if h1_bull else "SELL"
    if direction=="BUY":
        if lows[-1] >= min(lows[-20:-1]): return
    else:
        if highs[-1] <= max(highs[-20:-1]): return
    tp1=max(highs[-50:-10]) if direction=="BUY" else min(lows[-50:-10])
    tp2=max(highs) if direction=="BUY" else min(lows)
    sl=min(lows[-20:-1])-3.0 if direction=="BUY" else max(highs[-20:-1])+3.0
    risk=abs(entry-sl); reward=abs(tp2-entry)
    if risk<5: return
    rr=reward/risk if risk>0 else 0
    if rr<3.0 or reward<15: return

    msg=(f"🔥 <b>V7.4 ALL SESSION</b> 🔥\nSESI: {sess_msg}\nNEWS: ✅ {news_msg}\nSRC: {src}\n"
         f"{'🟢 BUY' if direction=='BUY' else '🔴 SELL'} ENTRY ${entry:.2f} SL ${sl:.2f} ({risk:.1f}$)\n"
         f"TP1 ${tp1:.2f} TP2 ${tp2:.2f} (+{reward:.1f}$) RR 1:{rr:.2f}")
    send(msg)
    json.dump({"ts":time.time(),"dir":direction}, open(SPAM_FILE,"w"))

if __name__=="__main__": main()
