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
            public_ip = "3.107.214.228"
            
            smartApi.clientLocalIp = "127.0.0.1"
            smartApi.clientPublicIp = public_ip
            smartApi.clientMacAddress = "00:00:00:00:00:00"
            
            logging.info(f"[DEBUG] Using Public IP: {public_ip}")
            
            logging.info("Broker Login Successful.")
            return smartApi
        else:
            logging.error(f"Login Failed: {data.get('message', data)}")
            return None
    except Exception as e:
        logging.error(f"Failed during login: {e}")
        return None
