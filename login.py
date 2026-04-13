import pyotp
import logging
from SmartApi import SmartConnect
import config

def login():
    try:
        smartApi = SmartConnect(api_key=config.API_KEY)
        totp = pyotp.TOTP(config.TOTP_SECRET).now()
        data = smartApi.generateSession(config.CLIENT_ID, config.PASSWORD, totp)
        if data['status']:
            logging.info("Broker Login Successful.")
            return smartApi
        else:
            logging.error(f"Login Failed: {data.get('message', data)}")
            return None
    except Exception as e:
        logging.error(f"Failed during login: {e}")
        return None
