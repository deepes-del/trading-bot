import requests
import pandas as pd
import datetime
import logging
import yfinance as yf

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
    order = None
    for attempt in range(3):
        try:
            import time
            time.sleep(0.3)
            orderparams = {
                "variety": "NORMAL",
                "tradingsymbol": str(symbol),
                "symboltoken": str(symboltoken),
                "transactiontype": "BUY",
                "exchange": "NFO",
                "ordertype": "MARKET",
                "producttype": "CARRYFORWARD",
                "duration": "DAY",
                "price": "0",
                "squareoff": "0",
                "stoploss": "0",
                "quantity": int(qty),
                "scripconsent": "yes"
            }
            order = smartApi.placeOrder(orderparams)
            if order:
                break
        except Exception as e:
            logging.error(f"Buy Order Failed (Attempt {attempt+1}): {e}")
            import time
            time.sleep(1)
            
    if not order:
        raise Exception("Empty response from broker")
    return order


def place_sl_order(smartApi, symboltoken, symbol, qty, trigger_price):
    """Immediately executed STOPLOSS_MARKET order."""
    order = None
    for attempt in range(3):
        try:
            import time
            time.sleep(0.3)
            orderparams = {
                "variety": "STOPLOSS",
                "tradingsymbol": str(symbol),
                "symboltoken": str(symboltoken),
                "transactiontype": "SELL",
                "exchange": "NFO",
                "ordertype": "STOPLOSS_MARKET",
                "producttype": "CARRYFORWARD",
                "duration": "DAY",
                "price": str(trigger_price), 
                "triggerprice": str(trigger_price),
                "squareoff": "0",
                "stoploss": "0",
                "quantity": int(qty),
                "scripconsent": "yes"
            }
            order = smartApi.placeOrder(orderparams)
            if order:
                break
        except Exception as e:
            logging.error(f"SL Order Failed (Attempt {attempt+1}): {e}")
            import time
            time.sleep(1)
            
    if not order:
        raise Exception("Empty response from broker")
    return order


# ─────────────────────────────────────────────────────────────
# NEW: Cancel a pending order (used before manual exit)
# ─────────────────────────────────────────────────────────────
def cancel_order(smartApi, order_id):
    """Cancel a pending/open order by its order ID.
    
    Retries up to 2 times on failure to handle transient API errors.
    Returns True if cancelled successfully, False otherwise.
    """
    for attempt in range(1, 4):
        try:
            res = smartApi.cancelOrder(order_id, "STOPLOSS")
            logging.info(f"[CANCEL ORDER] Attempt {attempt} | Order {order_id} | Response: {res}")
            # Angel One returns a message string on success
            if res:
                logging.info(f"SL Order {order_id} cancelled successfully.")
                return True
            else:
                logging.warning(f"Cancel returned falsy response on attempt {attempt}")
        except Exception as e:
            logging.error(f"[CANCEL ORDER] Attempt {attempt} failed for order {order_id}: {e}")
        
        if attempt < 3:
            import time
            time.sleep(0.5)
    
    logging.error(f"Failed to cancel order {order_id} after 3 attempts.")
    return False


# ─────────────────────────────────────────────────────────────
# NEW: Market SELL order (used for target exit / forced exit)
# ─────────────────────────────────────────────────────────────
def place_sell_order(smartApi, symboltoken, symbol, qty):
    """Market Sell Order for exiting an open position."""
    order = None
    for attempt in range(3):
        try:
            import time
            time.sleep(0.3)
            orderparams = {
                "variety": "NORMAL",
                "tradingsymbol": str(symbol),
                "symboltoken": str(symboltoken),
                "transactiontype": "SELL",
                "exchange": "NFO",
                "ordertype": "MARKET",
                "producttype": "CARRYFORWARD",
                "duration": "DAY",
                "price": "0",
                "squareoff": "0",
                "stoploss": "0",
                "quantity": int(qty),
                "scripconsent": "yes"
            }
            order = smartApi.placeOrder(orderparams)
            if order:
                logging.info(f"[SELL ORDER] Placed successfully | Order ID: {order}")
                break
        except Exception as e:
            logging.error(f"[SELL ORDER] Attempt {attempt+1} failed: {e}")
            import time
            time.sleep(1)
            
    if not order:
        raise Exception("Empty response from broker")
    return order


# ─────────────────────────────────────────────────────────────
# NEW: Check if an SL order is still active (not yet triggered)
# ─────────────────────────────────────────────────────────────
def is_sl_order_active(smartApi, order_id):
    """Check the order book to see if a specific SL order is still open/pending.
    
    Returns:
        True  — SL order is still pending (not yet triggered)
        False — SL order has been triggered/executed/cancelled, or check failed
    """
    try:
        order_book = smartApi.orderBook()
        if order_book and order_book.get('status') and order_book.get('data'):
            for order in order_book['data']:
                if str(order.get('orderid')) == str(order_id):
                    status = order.get('status', '').lower()
                    # 'open' or 'trigger pending' means SL is still active
                    if status in ['open', 'trigger pending', 'pending']:
                        return True
                    else:
                        return False
        return False
    except Exception as e:
        logging.error(f"Error checking SL order status: {e}")
        return False
