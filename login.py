import pyotp
import logging
import requests
from SmartApi import SmartConnect
import config


def login():
    try:
        smartApi = SmartConnect(api_key=config.API_KEY)

        # 🔐 Generate TOTP
        totp = pyotp.TOTP(config.TOTP_SECRET).now()

        # 🔑 Login session
        data = smartApi.generateSession(config.CLIENT_ID, config.PASSWORD, totp)

        if data.get("status"):

            # ✅ SET TOKENS (VERY IMPORTANT)
            smartApi.setAccessToken(data['data']['jwtToken'])
            smartApi.setRefreshToken(data['data']['refreshToken'])

            # 🌐 GET REAL SERVER IP (DYNAMIC FIX)
            public_ip = requests.get("https://ifconfig.me").text.strip()

            # ✅ SET NETWORK HEADERS (CRITICAL)
            smartApi.setClientLocalIP("127.0.0.1")
            smartApi.setClientPublicIP(public_ip)
            smartApi.setMacAddress("00:00:00:00:00:00")

            logging.info(f"[DEBUG] Using Public IP: {public_ip}")
            logging.info("Broker Login Successful.")

            return smartApi

        else:
            logging.error(f"Login Failed: {data.get('message', data)}")
            return None

    except Exception as e:
        logging.error(f"Failed during login: {e}")
        return None