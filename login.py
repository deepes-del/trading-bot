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
            public_ip = "54.253.200.200"
            
            smartApi._defaultHeaders.update({
                "X-ClientLocalIP": "127.0.0.1",
                "X-ClientPublicIP": public_ip,
                "X-MACAddress": "00:00:00:00:00:00"
            })
            print(f"[DEBUG] Using Public IP: {public_ip}")
            
            logging.info("Broker Login Successful.")
            return smartApi
        else:
            logging.error(f"Login Failed: {data.get('message', data)}")
            return None
    except Exception as e:
        logging.error(f"Failed during login: {e}")
        return None
