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
            import socket
            import uuid
            import re
            
            # Use the user-verified static AWS IP
            public_ip = "3.107.214.228"
            
            # Fetch local hardware signatures for API compliance
            local_ip = socket.gethostbyname(socket.gethostname())
            mac_addr = ':'.join(re.findall('..', '%012x' % uuid.getnode()))
            
            # Apply to both possible naming variants in the SmartAPI library
            smartApi.clientLocalIp = local_ip
            smartApi.clientLocalIP = local_ip
            smartApi.clientPublicIp = public_ip
            smartApi.clientPublicIP = public_ip
            smartApi.clientMacAddress = mac_addr
            
            logging.info(f"[DEBUG] SmartAPI Headers: IP={public_ip}, MAC={mac_addr}")
            logging.info("Broker Login Successful.")
            return smartApi
        else:
            logging.error(f"Login Failed: {data.get('message', data)}")
            return None
    except Exception as e:
        logging.error(f"Failed during login: {e}")
        return None
