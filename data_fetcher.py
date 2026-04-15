import pandas as pd
import datetime
import logging
import pytz
import time
import yfinance as yf

INDEX_MAP = {
    "NIFTY": {
        "yf": "^NSEI",
        "token": "99926000"
    },
    "BANKNIFTY": {
        "yf": "^NSEBANK",
        "token": "99926009"
    }
}

def initialize_hybrid_ema(index_name="NIFTY"):
    logging.info(f"Initializing Hybrid EMA for {index_name} using yfinance...")
    try:
        ticker = INDEX_MAP.get(index_name, INDEX_MAP["NIFTY"])["yf"]
        df = yf.download(ticker, interval="1d", period="5d")
        if df.empty:
            logging.error("Failed to download yfinance data.")
            return None
            
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
            
        df = df[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
        df.columns = ['open', 'high', 'low', 'close', 'volume']
        
        if df.index.tz is None:
            df.index = df.index.tz_localize('Asia/Kolkata')
        else:
            df.index = df.index.tz_convert('Asia/Kolkata')
            
        df['timestamp_ist'] = df.index
        
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.datetime.now(ist)
        last_time = df['timestamp_ist'].iloc[-1]
        
        # A 5-minute candle is still active until strictly 5 minutes have elapsed since its timestamp
        if now < (last_time + datetime.timedelta(minutes=5)):
            df = df.iloc[:-1]  # IMPORTANT: remove current forming candle
            
        df['EMA5'] = df['close'].ewm(span=5, adjust=False).mean()
        
        logging.info(f"EMA initialized from yfinance")
        return df
    except Exception as e:
        logging.error(f"yfinance init error: {e}")
        return None

def update_hybrid_ema(global_df, smartApi, exchange, symboltoken, interval="FIVE_MINUTE"):
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.datetime.now(ist)
    to_date = now.strftime('%Y-%m-%d %H:%M')
    from_date = (now - datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M')
    
    request_params = {
        "exchange": exchange,
        "symboltoken": symboltoken,
        "interval": interval,
        "fromdate": from_date,
        "todate": to_date
    }
    
    for i in range(3):
        try:
            res = smartApi.getCandleData(request_params)
            
            if res and res.get('status') == True and res.get('data') and len(res.get('data')) > 0:
                df_new = pd.DataFrame(res['data'], columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df_new['timestamp'] = pd.to_datetime(df_new['timestamp'])
                df_new.set_index('timestamp', inplace=True)
                df_new['timestamp_ist'] = df_new.index.tz_localize('Asia/Kolkata') if df_new.index.tz is None else df_new.index.tz_convert('Asia/Kolkata')
                
                last_time = df_new['timestamp_ist'].iloc[-1]
                # A 5-minute candle is still active until strictly 5 minutes have elapsed since its timestamp
                if now < (last_time + datetime.timedelta(minutes=5)):
                    df_new = df_new.iloc[:-1] # IMPORTANT: remove current forming candle
                    
                if df_new.empty:
                    # Trimming resulted in empty data, wait and retry.
                    logging.info(f"Attempt {i+1} failed — empty data")
                    if i < 2:
                        logging.info("Retrying in 2 seconds...")
                        time.sleep(2)
                    continue
                    
                logging.info("Candle data received successfully")    
                last_global_ts = global_df['timestamp_ist'].iloc[-1]
                new_candles = df_new[df_new['timestamp_ist'] > last_global_ts]
                
                if not new_candles.empty:
                    k = 2 / (5 + 1)
                    for idx, row in new_candles.iterrows():
                        last_ema = global_df['EMA5'].iloc[-1]
                        new_close = row['close']
                        new_ema = (new_close * k) + (last_ema * (1 - k))
                        
                        row['EMA5'] = new_ema
                        global_df.loc[idx] = row
                        
                return True, global_df
            else:
                logging.info(f"Attempt {i+1} failed — empty data")
                if i < 2:
                    logging.info("Retrying in 2 seconds...")
                    time.sleep(2)
                    
        except Exception as e:
            logging.error(f"API fetch error: {e}")
            logging.info(f"Attempt {i+1} failed — empty data")
            if i < 2:
                logging.info("Retrying in 2 seconds...")
                time.sleep(2)
                
    logging.info("All attempts failed — skipping candle")
    return False, global_df

def get_ltp(smartApi, exchange, symbol, symboltoken):
    for attempt in range(3):
        try:
            import time
            time.sleep(0.3)
            res = smartApi.ltpData(exchange, symbol, symboltoken)
            if res and res.get('status'):
                return res['data']['ltp']
        except Exception as e:
            logging.error(f"Error fetching LTP (Attempt {attempt+1}): {e}")
            import time
            time.sleep(1)
    return None
