import time
import logging
import datetime
import pytz
import config
import login
import data_fetcher
import strategy
import order_manager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def main():
    logging.info("Initiating NIFTY Real-Time Breakout Bot...")
    
    smartApi = login.login()
    if not smartApi:
        return

    logging.info("Fetching Master Instruments...")
    inst_df = order_manager.get_instrument_list()
    if inst_df.empty:
        logging.error("Failed to load instruments.")
        return

    # STEP 1: INITIAL EMA SETUP (yfinance)
    global_df = data_fetcher.initialize_hybrid_ema()
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

    while True:
        ist_now = datetime.datetime.now(config.TIMEZONE)
        
        market_start = ist_now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_end = ist_now.replace(hour=15, minute=30, second=0, microsecond=0)
        
        if ist_now < market_start or ist_now > market_end:
            if ist_now > market_end:
                logging.info("Market Closed.")
                break
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
                
                fetch_success, updated_df = data_fetcher.update_hybrid_ema(global_df, smartApi, config.EXCHANGE, config.SYMBOL_TOKEN)
                
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

        # 2. REAL-TIME EXECUTION PHASE: Monitor live price specifically every 1 second
        if setup_valid and (last_trade_candle_time != candle_time):
            index_ltp_raw = data_fetcher.get_ltp(smartApi, config.EXCHANGE, config.INDEX, config.SYMBOL_TOKEN)
            
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
                            smartApi, inst_df, index_ltp, config.INDEX
                        )
                        
                        if opt_tok and option_ltp:
                            logging.info(f"Selected option: {opt_sym} | Premium: {option_ltp}")
                            logging.info("Calculating risk...")
                            
                            option_sl_points = min(max(index_sl, 10), 20)
                            
                            logging.info(f"Index SL: {index_sl}")
                            logging.info(f"Option SL used: {option_sl_points}")
                            logging.info("Placing BUY order...")
                            
                            buy_res = order_manager.place_buy_order(smartApi, opt_tok, opt_sym, config.LOT_SIZE)
                            
                            if buy_res:
                                logging.info("BUY order placed successfully")
                                trades_today += 1
                                last_trade_candle_time = candle_time # Block further trades safely
                                setup_valid = False # Reset immediately to block multiple triggers
                                
                                entry_price = option_ltp
                                sl_price = round(entry_price - option_sl_points, 1)
                                target_price = round(entry_price + (2 * option_sl_points), 1)
                                
                                logging.info(f"Placing STOPLOSS order at: {sl_price}")
                                logging.info(f"Target set at: {target_price}")
                                
                                sl_res = order_manager.place_sl_order(
                                    smartApi, opt_tok, opt_sym, config.LOT_SIZE, sl_price
                                )
                                                                
                                logging.info("Trade already taken for this candle — waiting for next setup")
                                
                            else:
                                logging.error("BUY order failed — skipping trade")
                                setup_valid = False
                        else:
                            logging.error("Failed to fetch option - skipping trade")
                            setup_valid = False
                
        # Continually loop very fast every 1 second natively.    
        time.sleep(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Bot execution gracefully terminated by user.")
