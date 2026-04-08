import requests
import pandas as pd
import datetime
import logging

def get_instrument_list():
    url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    try:
        response = requests.get(url)
        return pd.DataFrame(response.json())
    except Exception as e:
        logging.error(f"Exception fetching instruments: {e}")
        return pd.DataFrame()

def select_atm_option(smartApi, df_inst, index_ltp, index_name="NIFTY"):
    """
    Select ONLY the exactly matching ATM PUT Option without scanning multiple range bounds.
    Real premium fetched natively.
    """
    try:
        step = 50
        atm_strike = round(index_ltp / step) * step

        # Filter for OPTIDX PEs directly
        df_opt = df_inst[(df_inst['name'] == index_name) & 
                         (df_inst['exch_seg'] == 'NFO') & 
                         (df_inst['instrumenttype'] == 'OPTIDX') & 
                         (df_inst['symbol'].str.endswith('PE'))].copy()

        # Select nearest weekly expiry (>= today)
        df_opt['expiry_date'] = pd.to_datetime(df_opt['expiry'], format='%d%b%Y')
        now = pd.to_datetime(datetime.date.today())
        
        df_future = df_opt[df_opt['expiry_date'] >= now]
        if df_future.empty:
            logging.error("No valid future expiries found.")
            return None, None, None
            
        df_future = df_future.sort_values(by='expiry_date')
        closest_expiry = df_future.iloc[0]['expiry_date']
        df_weekly = df_future[df_future['expiry_date'] == closest_expiry]

        # Select ONLY ATM PUT option
        strike_val = float(atm_strike * 100)
        match = df_weekly[df_weekly['strike'].astype(float) == strike_val]

        if not match.empty:
            opt = match.iloc[0]
            best_token = opt['token']
            best_symbol = opt['symbol']
            
            # Fetch real option LTP
            res = smartApi.ltpData("NFO", best_symbol, best_token)
            if res and res.get('status') and res.get('data'):
                option_ltp = float(res['data']['ltp'])
                logging.info(f"[ATM SELECTED] Strike {atm_strike} | Symbol: {best_symbol} | LTP: {option_ltp}")
                return best_token, best_symbol, option_ltp
            else:
                logging.error(f"Failed to fetch live premium for {best_symbol}")
                return None, None, None
                
        logging.warning(f"No specific match found for ATM Strike {atm_strike}.")
        return None, None, None
        
    except Exception as e:
        logging.error(f"Error selecting ATM option: {e}")
        return None, None, None


def place_buy_order(smartApi, symboltoken, symbol, qty):
    """Market Buy Order specifically."""
    try:
        orderparams = {
            "variety": "NORMAL",
            "tradingsymbol": symbol,
            "symboltoken": symboltoken,
            "transactiontype": "BUY",
            "exchange": "NFO",
            "ordertype": "MARKET",
            "producttype": "CARRYFORWARD",
            "duration": "DAY",
            "price": "0",
            "squareoff": "0",
            "stoploss": "0",
            "quantity": str(qty)
        }
        return smartApi.placeOrder(orderparams)
    except Exception as e:
        logging.error(f"Buy Order Failed: {e}")
        return None


def place_sl_order(smartApi, symboltoken, symbol, qty, trigger_price):
    """Immediately executed STOPLOSS_MARKET order."""
    try:
        orderparams = {
            "variety": "STOPLOSS",
            "tradingsymbol": symbol,
            "symboltoken": symboltoken,
            "transactiontype": "SELL",
            "exchange": "NFO",
            "ordertype": "STOPLOSS_MARKET",
            "producttype": "CARRYFORWARD",
            "duration": "DAY",
            "price": "0", 
            "triggerprice": str(trigger_price),
            "squareoff": "0",
            "stoploss": "0",
            "quantity": str(qty)
        }
        return smartApi.placeOrder(orderparams)
    except Exception as e:
        logging.error(f"SL Order Failed: {e}")
        return None
