from supabase_client import supabase


def save_trade(user_id, symbol, entry_price, sl, target):
    data = {
        "user_id": user_id,
        "symbol": symbol,
        "entry_price": entry_price,
        "sl": sl,
        "target": target,
        "status": "OPEN"
    }

    try:
        response = supabase.table("trades").insert(data).execute()
        return response
    except Exception as e:
        import logging
        logging.error(f"Error saving trade to Supabase: {e}")
        return None


def close_trade(user_id, symbol, exit_price):
    try:
        response = supabase.table("trades") \
            .update({
                "exit_price": exit_price,
                "status": "CLOSED"
            }) \
            .eq("user_id", user_id) \
            .eq("symbol", symbol) \
            .eq("status", "OPEN") \
            .execute()

        return response
    except Exception as e:
        import logging
        logging.error(f"Error closing trade in Supabase: {e}")
        return None
