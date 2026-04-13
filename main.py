import time
import threading
import logging
import datetime
import pytz
import config
import login
import data_fetcher
import strategy
import order_manager
from data_fetcher import INDEX_MAP
from db_supabase import save_trade, close_trade

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')


# ─────────────────────────────────────────────────────────────
# POSITION TRACKER — Single source of truth for the active trade
# ─────────────────────────────────────────────────────────────
current_position = {
    "is_open": False,
    "symbol": None,
    "token": None,
    "entry_price": None,
    "sl_price": None,
    "target_price": None,
    "sl_order_id": None,
    "buy_order_id": None,
}

def reset_position():
    """Reset position tracker to empty state."""
    current_position["is_open"] = False
    current_position["symbol"] = None
    current_position["token"] = None
    current_position["entry_price"] = None
    current_position["sl_price"] = None
    current_position["target_price"] = None
    current_position["sl_order_id"] = None
    current_position["buy_order_id"] = None
    logging.info("[POSITION] Reset — no active position.")


def close_position(smartApi, user_config, reason="MANUAL"):
    """Close the current open position safely.
    
    Steps:
      1. Cancel the pending SL order (if still active)
      2. Place a market SELL order to exit
      3. Reset the position tracker
      
    Args:
        smartApi: Angel One API session
        reason: Label for logs (e.g. "TARGET_HIT", "FORCED_3:15", "SL_TRIGGERED")
    """
    if not current_position["is_open"]:
        logging.info(f"[CLOSE] No open position to close. Reason: {reason}")
        return
    
    sym = current_position["symbol"]
    tok = current_position["token"]
    sl_id = current_position["sl_order_id"]
    
    logging.info(f"{'='*60}")
    logging.info(f"[CLOSE POSITION] Reason: {reason}")
    logging.info(f"  Symbol : {sym}")
    logging.info(f"  Entry  : {current_position['entry_price']}")
    logging.info(f"  SL     : {current_position['sl_price']}")
    logging.info(f"  Target : {current_position['target_price']}")
    logging.info(f"{'='*60}")
    
    # STEP 1: Cancel the pending SL order first (to avoid double execution)
    if sl_id:
        sl_still_active = order_manager.is_sl_order_active(smartApi, sl_id)
        if sl_still_active:
            logging.info(f"[CLOSE] Cancelling pending SL order: {sl_id}")
            cancel_ok = order_manager.cancel_order(smartApi, sl_id)
            if not cancel_ok:
                logging.warning(f"[CLOSE] Could not cancel SL {sl_id} — it may have already triggered.")
                # If SL already triggered, the position is already sold. 
                # We should NOT place another sell. Check order status.
                # Conservative approach: reset and skip the sell.
                reset_position()
                return
        else:
            logging.info(f"[CLOSE] SL order {sl_id} already triggered/executed — skipping cancel.")
            # SL already closed the position at broker level. Just reset tracker.
            reset_position()
            return
    
    # STEP 2: Place market SELL to exit the position
    logging.info(f"[CLOSE] Placing MARKET SELL for {sym}...")
    sell_res = order_manager.place_sell_order(smartApi, tok, sym, config.LOT_SIZE)
    
    if sell_res:
        logging.info(f"[CLOSE] SELL order placed | Order ID: {sell_res} | Reason: {reason}")
        
        # Update Supabase
        exit_price_raw = data_fetcher.get_ltp(smartApi, "NFO", sym, tok)
        exit_price = float(exit_price_raw) if exit_price_raw is not None else 0
        
        close_trade(
            user_config["user_id"],
            current_position["symbol"],
            exit_price
        )
    else:
        logging.error(f"[CLOSE] SELL order FAILED for {sym}! Manual intervention may be needed.")
    
    # STEP 3: Reset position regardless (prevent infinite retry loops)
    reset_position()


# ─────────────────────────────────────────────────────────────
# RUNNING BOTS TRACKER — Prevents duplicate instances per user
# ─────────────────────────────────────────────────────────────
running_bots = {}
bot_lock = threading.Lock()  # Thread-safe access to running_bots


def start_bot(user_id, user_config):
    """Start a bot for a user. Prevents duplicate instances.
    
    Args:
        user_id: Unique identifier for the user
        user_config: Trading configuration dict
    
    Returns:
        True if bot started, False if already running
    """
    # Ensure user_id is present in config
    user_config["user_id"] = user_id
    
    with bot_lock:
        # CHECK: Is a bot already running for this user?
        if user_id in running_bots and running_bots[user_id]["config"]["is_running"]:
            logging.warning(f"[START] Bot already running for user: {user_id}")
            return False
        
        # Ensure flags are set correctly for a fresh start
        user_config["is_running"] = True
        user_config["stop_requested"] = False
        
        # Start bot in a separate thread
        thread = threading.Thread(target=run_bot, args=(user_config,), daemon=True)
        thread.start()
        
        # Track the running bot
        running_bots[user_id] = {
            "thread": thread,
            "config": user_config
        }
    
    logging.info(f"[START] Bot started for user: {user_id}")
    return True


def run_bot(user_config):
    index_name = user_config.get("index", "NIFTY")
    logging.info(f"[DEBUG] Selected Index: {index_name}")
    logging.info(f"Initiating {index_name} Real-Time Breakout Bot...")
    
    smartApi = login.login()
    if not smartApi:
        return

    logging.info("Fetching Master Instruments...")
    inst_df = order_manager.get_instrument_list()
    if inst_df.empty:
        logging.error("Failed to load instruments.")
        return

    # Get index mapping details
    index_details = INDEX_MAP.get(index_name, INDEX_MAP["NIFTY"])
    index_token = index_details["token"]

    # STEP 1: INITIAL EMA SETUP (yfinance)
    global_df = data_fetcher.initialize_hybrid_ema(index_name)
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
    
    # Flag to ensure 3:15 square-off happens exactly once
    squareoff_done = False

    while user_config["is_running"]:
        ist_now = datetime.datetime.now(config.TIMEZONE)
        
        market_start = ist_now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_end = ist_now.replace(hour=15, minute=30, second=0, microsecond=0)
        
        if ist_now < market_start or ist_now > market_end:
            if ist_now > market_end:
                logging.info("Market Closed.")
                user_config["is_running"] = False
                break
            time.sleep(30)
            continue
            
        if trades_today >= config.MAX_TRADES_PER_DAY:
            logging.info("Max daily trades reached. Stopping.")
            user_config["is_running"] = False
            break

        # ─────────────────────────────────────────────────────
        # SAFE STOP CHECK — only stop when no trade is active
        # ─────────────────────────────────────────────────────
        if user_config["stop_requested"]:
            if current_position["is_open"]:
                if not user_config.get("stop_logged", False):
                    logging.info("[SAFE STOP] Stop requested but trade is active — continuing...")
                    user_config["stop_logged"] = True
            else:
                logging.info("[SAFE STOP] No open position — stopping bot safely.")
                user_config["is_running"] = False
                continue

        # ─────────────────────────────────────────────────────
        # FORCED SQUARE-OFF AT 3:15 PM — runs before anything else
        # ─────────────────────────────────────────────────────
        squareoff_time = ist_now.replace(hour=15, minute=15, second=0, microsecond=0)
        
        if ist_now >= squareoff_time and not squareoff_done:
            squareoff_done = True
            logging.info("=" * 60)
            logging.info("[3:15 PM] FORCED SQUARE-OFF CHECK")
            logging.info("=" * 60)
            
            if current_position["is_open"]:
                close_position(smartApi, user_config, reason="FORCED_3:15_SQUAREOFF")
            else:
                logging.info("[3:15 PM] No open position — nothing to square off.")
            
            # Stop taking new trades after 3:15
            setup_valid = False
            logging.info("[3:15 PM] Bot will not take new trades. Waiting for market close.")
            
        # After 3:15, just wait for market close — no new trades
        if ist_now >= squareoff_time:
            time.sleep(5)
            continue

        # ─────────────────────────────────────────────────────
        # MONITOR EXISTING POSITION (Target + SL status check)
        # ─────────────────────────────────────────────────────
        if current_position["is_open"]:
            # Check if SL was already triggered by broker
            sl_id = current_position["sl_order_id"]
            if sl_id:
                sl_active = order_manager.is_sl_order_active(smartApi, sl_id)
                if not sl_active:
                    logging.info("[MONITOR] SL order has been triggered by broker — position closed.")
                    reset_position()
                    time.sleep(1)
                    continue
            
            # Check TARGET: Fetch live option LTP
            opt_ltp_raw = data_fetcher.get_ltp(
                smartApi, "NFO", current_position["symbol"], current_position["token"]
            )
            
            if opt_ltp_raw is not None:
                opt_ltp = float(opt_ltp_raw)
                target = current_position["target_price"]
                entry = current_position["entry_price"]
                sl = current_position["sl_price"]
                
                logging.info(
                    f"[MONITOR] Option LTP: {opt_ltp} | Entry: {entry} | "
                    f"SL: {sl} | Target: {target}"
                )
                
                # TARGET HIT — exit with profit
                if opt_ltp >= target:
                    logging.info(f"[TARGET HIT] Option LTP {opt_ltp} >= Target {target}")
                    close_position(smartApi, user_config, reason="TARGET_HIT")
            
            # Don't look for new setups while a position is open
            time.sleep(1)
            continue

        # ─────────────────────────────────────────────────────
        # CANDLE DATA FETCH — 5-minute scheduler
        # ─────────────────────────────────────────────────────
        # Calculate the start of the current 5m bucket
        # 1. STRICT 5-MINUTE SCHEDULER & ONE API CALL PER CANDLE
        if ist_now.minute % 5 == 0 and 5 <= ist_now.second <= 15:
            if last_fetch_minute != ist_now.minute:
                # Lock immediately to prevent ANY additional fetch calls in same candle!
                last_fetch_minute = ist_now.minute
                
                logging.info(f"New candle detected at {ist_now.strftime('%H:%M')}")
                logging.info("Fetching candle data...")
                
                fetch_success, updated_df = data_fetcher.update_hybrid_ema(global_df, smartApi, config.EXCHANGE, index_token)
                
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

        # ─────────────────────────────────────────────────────
        # TRADE ENTRY — Only if NO open position exists
        # ─────────────────────────────────────────────────────
        if setup_valid and (last_trade_candle_time != candle_time):
            # GUARD: Do not enter if a position is already open
            if current_position["is_open"]:
                time.sleep(1)
                continue
            
            index_ltp_raw = data_fetcher.get_ltp(smartApi, config.EXCHANGE, index_name, index_token)
            
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
                        logging.info("Fetching live option premiums...")
                        
                        opt_tok, opt_sym, option_ltp = order_manager.select_atm_option(
                            smartApi, inst_df, index_ltp, index_name
                        )
                        
                        if opt_tok and option_ltp:
                            logging.info(f"Selected option: {opt_sym} | Premium: {option_ltp}")
                            logging.info(f"Calculating risk (mode: {user_config['mode']})...")
                            
                            entry_price = option_ltp
                            
                            if user_config["mode"] == "default":
                                # ORIGINAL LOGIC — index-derived SL, clamped 10–20, 1:2 RR
                                option_sl_points = min(max(index_sl, 10), 20)
                                sl_price = round(entry_price - option_sl_points, 1)
                                target_price = round(entry_price + (2 * option_sl_points), 1)
                            elif user_config["mode"] == "custom":
                                # USER-DEFINED — fixed SL and target from user_config
                                option_sl_points = user_config["sl"]
                                sl_price = round(entry_price - option_sl_points, 1)
                                target_price = round(entry_price + user_config["target"], 1)
                            else:
                                logging.error(f"Unknown mode: {user_config['mode']} — skipping trade.")
                                setup_valid = False
                                continue
                            
                            logging.info(f"Index SL: {index_sl}")
                            logging.info(f"Option SL used: {option_sl_points}")
                            logging.info(f"Entry: {entry_price} | SL: {sl_price} | Target: {target_price}")
                            logging.info("Placing BUY order...")
                            
                            buy_res = order_manager.place_buy_order(smartApi, opt_tok, opt_sym, config.LOT_SIZE)
                            
                            if buy_res:
                                logging.info(f"BUY order placed successfully | Order ID: {buy_res}")
                                
                                # Save to Supabase
                                save_trade(
                                    user_config["user_id"],
                                    opt_sym,
                                    entry_price,
                                    sl_price,
                                    target_price
                                )
                                
                                trades_today += 1
                                last_trade_candle_time = candle_time
                                setup_valid = False
                                
                                logging.info(f"Placing STOPLOSS order at: {sl_price}")
                                
                                sl_res = order_manager.place_sl_order(
                                    smartApi, opt_tok, opt_sym, config.LOT_SIZE, sl_price
                                )
                                
                                # UPDATE POSITION TRACKER
                                current_position["is_open"] = True
                                current_position["symbol"] = opt_sym
                                current_position["token"] = opt_tok
                                current_position["entry_price"] = entry_price
                                current_position["sl_price"] = sl_price
                                current_position["target_price"] = target_price
                                current_position["sl_order_id"] = sl_res
                                current_position["buy_order_id"] = buy_res
                                
                                logging.info(f"[POSITION OPEN] {opt_sym}")
                                logging.info(f"  Entry  : {entry_price}")
                                logging.info(f"  SL     : {sl_price} (Order: {sl_res})")
                                logging.info(f"  Target : {target_price}")
                                logging.info(f"  Waiting for target hit, SL trigger, or 3:15 PM square-off...")
                                
                            else:
                                logging.error("BUY order failed — skipping trade")
                                setup_valid = False
                        else:
                            logging.error("Failed to fetch option - skipping trade")
                            setup_valid = False
                
        # Continually loop very fast every 1 second natively.    
        time.sleep(1)

    # ─────────────────────────────────────────────────────
    # CLEANUP — Remove user from running_bots after bot stops
    # ─────────────────────────────────────────────────────
    user_id = user_config.get("user_id")
    if user_id:
        with bot_lock:
            if user_id in running_bots:
                del running_bots[user_id]
                logging.info(f"[CLEANUP] Bot removed for user: {user_id}")
    
    user_config["is_running"] = False
    logging.info("[BOT] Shutdown complete.")

if __name__ == "__main__":
    user_config = {
        "mode": "default",      # "default" = original strategy | "custom" = user-defined SL/target
        "sl": 10,               # Custom mode: fixed SL points on option side
        "target": 20,           # Custom mode: fixed target points on option side
        "index": "NIFTY",       # Which index to trade
        "is_running": True,     # Controls the main loop
        "stop_requested": False # Set to True via API to request safe shutdown
    }

    try:
        start_bot("user_1", user_config)
        # Keep main thread alive while bot runs
        while user_config["is_running"]:
            time.sleep(1)
    except KeyboardInterrupt:
        user_config["stop_requested"] = True
        logging.info("Stop requested — waiting for safe shutdown...")
        # Wait for bot thread to finish safely
        if "user_1" in running_bots:
            running_bots["user_1"]["thread"].join(timeout=30)
        logging.info("Bot execution gracefully terminated by user.")
