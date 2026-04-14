from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.responses import FileResponse
from auth import register_user, login_user
import os
import logging

# Hide repetitive /logs endpoint access spam
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

app = FastAPI()

# Simple session storage (Token -> User Information)
active_sessions = {}


@app.get("/")
def home():
    file_path = os.path.join(os.path.dirname(__file__), "index.html")
    return FileResponse(file_path)


@app.post("/register")
def register(username: str, password: str):
    return register_user(username, password)


@app.post("/login")
def login(username: str, password: str):
    user = login_user(username, password)

    if user:
        session_token = user["user_id"]  # simple session (using user_id as token for now)

        active_sessions[session_token] = {
            "user_id": user["user_id"],
            "username": user["username"]
        }

        return {
            "status": "success",
            "session_token": session_token
        }

    return {"error": "Invalid credentials"}


class BotConfig(BaseModel):
    mode: str = "default"
    sl: int = 10
    target: int = 20
    index: str = "NIFTY"
    lots: int = 1


@app.post("/start-bot")
def start_bot_api(session_token: str, config: BotConfig):
    from main import start_bot

    if session_token not in active_sessions:
        return {"error": "Invalid session"}

    user_id = active_sessions[session_token]["user_id"]

    user_config = {
        "user_id": user_id,
        "mode": config.mode,
        "sl": config.sl,
        "target": config.target,
        "index": config.index,
        "lots": config.lots,
        "is_running": True,
        "stop_requested": False
    }

    started = start_bot(user_id, user_config)

    if started:
        return {"status": "Bot started"}
    else:
        return {"error": "Bot already running"}


@app.post("/stop-bot")
def stop_bot_api(session_token: str):
    from main import running_bots

    if session_token not in active_sessions:
        return {"error": "Invalid session"}

    user_id = active_sessions[session_token]["user_id"]

    if user_id in running_bots:
        running_bots[user_id]["config"]["stop_requested"] = True
        return {"status": "Stop requested"}

    return {"error": "Bot not running"}


@app.get("/logs")
def get_logs(session_token: str):
    from main import user_logs

    if session_token not in active_sessions:
        return {"error": "Invalid session"}

    user_id = active_sessions[session_token]["user_id"]

    return {
        "logs": user_logs.get(user_id, [])
    }
