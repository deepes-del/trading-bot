import time
import logging
import datetime
import threading
import pytz
import config
import login
import data_fetcher
import strategy
import order_manager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

running_bots = {}
bot_lock = threading.Lock()

user_logs = {}

def safe_log(message):
    blocked = ["password", "api_key", "totp"]
    for word in blocked:
        if word in message.lower():
            return "[SENSITIVE DATA HIDDEN]"
    return message

def add_log(user_id, message):
    if not user_id:
        return
    if user_id not in user_logs:
        user_logs[user_id] = []
    # limit size
    if len(user_logs[user_id]) > 200:
        user_logs[user_id].pop(0)

    user_logs[user_id].append(message)
    print(f"[LOG][{user_id}] {message}")  # for debugging
def start_bot(user_id, user_config):
    print(f"[START] Bot triggered for user: {user_id}")
    with bot_lock:
        if user_id in running_bots:
            print(f"[BLOCKED] Bot already running for {user_id}")
            return False
        
        user_logs[user_id] = []
        user_config["is_running"] = True
        user_config["stop_requested"] = False
        
        thread = threading.Thread(target=run_bot, args=(user_config,), daemon=True)
        thread.start()
        
        running_bots[user_id] = {
            "thread": thread,
            "config": user_config
        }
    return True

def run_bot(user_config):
    try:
        _run_bot_logic(user_config)
    except Exception as e:
        logging.error(f"Bot execution crashed: {e}")
    finally:
        user_id = user_config.get("user_id")
        with bot_lock:
            if user_id in running_bots:
                del running_bots[user_id]

def _run_bot_logic(user_config):
    user_id = user_config.get("user_id")
    user_index = user_config.get("index", "NIFTY")
    user_lots = int(user_config.get("lots", 1))
    trade_qty = user_lots * (30 if user_index == "BANKNIFTY" else 65)
    symbol_token = "99926009" if user_index == "BANKNIFTY" else "99926000"

    print(f"[RUNNING] Bot active for user: {user_id}")
    logging.info(f"Initiating {user_index} Real-Time Breakout Bot...")
    
    add_log(user_id, "Bot started")
    add_log(user_id, "Logging into broker...")
    smartApi = login.login()
    if not smartApi:
        return
    add_log(user_id, "Broker login successful")

    add_log(user_id, "Waiting for market...")
    inst_df = order_manager.get_instrument_list()
    if inst_df.empty:
        logging.error("Failed to load instruments.")
        return

    # STEP 1: INITIAL EMA SETUP (yfinance)
    global_df = data_fetcher.initialize_hybrid_ema(user_index)
    if global_df is None:
        logging.error("Failed to initialize yfinance global framework. Terminating.")
        return

    trades_today = 0
    last_trade_candle_time = None
    last_fetch_minute = None
    
    setup_valid = False
    prev_low = None
    prev_high = None
    candle_time = None
    active_trade = None

    while True:
        if user_config.get("stop_requested"):
            add_log(user_id, "Bot stopped by user")
            break
            
        ist_now = datetime.datetime.now(config.TIMEZONE)
        
        market_start = ist_now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_end = ist_now.replace(hour=15, minute=30, second=0, microsecond=0)
        
        if ist_now < market_start or ist_now > market_end:
            if ist_now > market_end:
                add_log(user_id, "Market is closed. Waiting for market open...")
                time.sleep(60)
                continue
            time.sleep(30)
            continue
            
        if trades_today >= config.MAX_TRADES_PER_DAY:
            logging.info("Max daily trades reached. Stopping.")
            break

        # Calculate the start of the current 5m bucket
        # 1. STRICT 5-MINUTE SCHEDULER & ONE API CALL PER CANDLE
        if ist_now.minute % 5 == 0 and 5 <= ist_now.second <= 15:
            if last_fetch_minute != ist_now.minute:
                # Lock immediately to prevent ANY additional fetch calls in same candle!
                last_fetch_minute = ist_now.minute
                
                logging.info(f"New candle detected at {ist_now.strftime('%H:%M')}")
                logging.info("Fetching candle data...")
                
                fetch_success, updated_df = data_fetcher.update_hybrid_ema(global_df, smartApi, config.EXCHANGE, symbol_token)
                
                # 8. FAIL-SAFE: STRICT DATA VALIDATION
                if not fetch_success or updated_df is None:
                    # Do not retry! Skip the entire candle and wait for next cycle.
                    setup_valid = False
                    continue
                    
                global_df = updated_df
                
                # CANDLE TIME VALIDATION
                latest_time = global_df.index[-1]
                expected_minute = (ist_now.minute - 5) % 60
                expected_hour = ist_now.hour if ist_now.minute >= 5 else (ist_now.hour - 1) % 24
                
                if latest_time.minute != expected_minute or latest_time.hour != expected_hour:
                    logging.warning("Stale data detected — skipping this cycle")
                    setup_valid = False
                    continue
                    
                # 3. STORE SETUP
                setup_valid, prev_low, prev_high, setup_ema, candle_time = strategy.get_setup_levels(global_df)
                
                if setup_valid:
                    logging.info("Setup detected: Previous candle is above EMA")
                    logging.info(f"Waiting for breakdown below: {prev_low}")
                else:
                    logging.info("No setup: Candle not above EMA")

        # 4. BLOCK OLD SETUP REUSE
        if setup_valid and candle_time is not None:
            # Setup candle timestamp marks the START of the 5 min candle window.
            # Entry is strictly allowed during the immediately following 5-min window.
            # Thus, total allowed time difference from candle_time start to current time is strictly < 10 minutes.
            time_diff = (ist_now.replace(tzinfo=None) - candle_time.replace(tzinfo=None)).total_seconds()
            if time_diff >= 600:
                logging.warning("Setup expired — waiting for new candle")
                setup_valid = False
                continue

        # OPEN TRADE MONITORING PHASE
        if active_trade is not None:
            current_opt_ltp_raw = data_fetcher.get_ltp(smartApi, "NFO", active_trade["opt_sym"], active_trade["opt_tok"])
            if current_opt_ltp_raw is not None:
                current_opt_ltp = float(current_opt_ltp_raw)
                
                # Check for Target Hit
                if current_opt_ltp >= active_trade["target_price"]:
                    logging.info(f"TARGET HIT! Current: {current_opt_ltp} >= Target: {active_trade['target_price']}")
                    add_log(user_id, f"🎯 Target Hit! Sold at {current_opt_ltp}")
                    
                    order_manager.cancel_order(smartApi, active_trade["sl_order_id"])
                    order_manager.place_sell_order(smartApi, active_trade["opt_tok"], active_trade["opt_sym"], active_trade["trade_qty"])
                    
                    active_trade = None
                    time.sleep(0.5)
                    continue
                
                # Check for SL Hit (If triggered externally by broker)
                if current_opt_ltp <= active_trade["sl_price"]:
                    # Ensure broker caught it, release the bot immediately.
                    sl_is_active = order_manager.is_sl_order_active(smartApi, active_trade["sl_order_id"])
                    if not sl_is_active:
                        logging.info(f"SL Executed by Broker at {active_trade['sl_price']}")
                        add_log(user_id, f"🛑 Stoploss Hit at {current_opt_ltp}")
                        active_trade = None
                        time.sleep(0.5)
                        continue
            
            # If trade is still active, DO NOT execute new setups!
            time.sleep(0.5)
            continue
            
        # 2. REAL-TIME EXECUTION PHASE: Monitor live price specifically every 1 second
        if setup_valid and (last_trade_candle_time != candle_time):
            index_ltp_raw = data_fetcher.get_ltp(smartApi, config.EXCHANGE, user_index, symbol_token)
            
            if index_ltp_raw is not None:
                index_ltp = float(index_ltp_raw)
                
                # ENTRY CONDITION: LTP explicitly drops below previous candle low!
                if index_ltp < prev_low:
                    logging.info(f"Breakdown detected: LTP {index_ltp} < {prev_low}")
                    logging.info("Executing trade immediately")
                    
                    index_sl = prev_high - prev_low
                    
                    if index_sl <= 0:
                        logging.warning(f"Trade setup skipped: index_sl ({index_sl}) must be > 0.")
                        last_trade_candle_time = candle_time
                        setup_valid = False
                    else:
                        logging.info("Selecting ATM option...")
                        
                        opt_tok, opt_sym, option_ltp = order_manager.select_atm_option(
                            smartApi, inst_df, index_ltp, user_index
                        )
                        
                        if opt_tok and option_ltp:
                            logging.info(f"Selected option: {opt_sym} | Premium: {option_ltp}")
                            
                            if user_config.get("mode") == "custom":
                                option_sl_points = int(user_config.get("sl", 10))
                                target_points = int(user_config.get("target", 20))
                            else:
                                option_sl_points = min(max(index_sl, 10), 20)
                                target_points = 2 * option_sl_points
                            
                            logging.info(f"Option SL: {option_sl_points} | Target: {target_points} | Quantity: {trade_qty} ({user_lots} lots)")
                            logging.info("Placing BUY order...")
                            
                            buy_res = order_manager.place_buy_order(smartApi, opt_tok, opt_sym, trade_qty)
                            
                            if buy_res:
                                logging.info("BUY order placed successfully")
                                add_log(user_id, f"Trade executed: Bought {opt_sym} at {option_ltp}")
                                trades_today += 1
                                last_trade_candle_time = candle_time # Block further trades safely
                                setup_valid = False # Reset immediately to block multiple triggers
                                
                                entry_price = option_ltp
                                sl_price = round(entry_price - option_sl_points, 1)
                                target_price = round(entry_price + target_points, 1)
                                
                                logging.info(f"Placing STOPLOSS order at: {sl_price}")
                                sl_res = order_manager.place_sl_order(
                                    smartApi, opt_tok, opt_sym, trade_qty, sl_price
                                )
                                
                                sl_order_id = sl_res if sl_res else "UNKNOWN"
                                
                                # Protect the loop by storing active trade state!
                                active_trade = {
                                    "opt_tok": opt_tok,
                                    "opt_sym": opt_sym,
                                    "trade_qty": trade_qty,
                                    "entry_price": entry_price,
                                    "sl_price": sl_price,
                                    "target_price": target_price,
                                    "sl_order_id": sl_order_id
                                }
                                
                            else:
                                logging.error("BUY order failed — skipping trade")
                                setup_valid = False
                        else:
                            logging.error("Failed to fetch option - skipping trade")
                            setup_valid = False
                
        # Continually loop very fast every 0.5 seconds natively.    
        time.sleep(0.5)
