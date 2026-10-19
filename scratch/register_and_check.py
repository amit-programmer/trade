import os
import requests
from dotenv import load_dotenv

load_dotenv()
token = os.getenv("TELEGRAM_BOT_TOKEN")

commands = [
    {"command": "ai", "description": "Ask Groq AI trading question & analysis"},
    {"command": "ask", "description": "Institutional AI trading question"},
    {"command": "charts", "description": "Live candlestick charts with PD-OHLC levels"},
    {"command": "scan", "description": "Instant SMC Liquidity Sweep & OB scan"},
    {"command": "status", "description": "Check bot & scanner status"},
    {"command": "help", "description": "Full command list & permission hub"},
    {"command": "rules", "description": "Group trading rules & risk policy"},
    {"command": "channelinfo", "description": "Channel metadata & bot permissions"},
    {"command": "dmme", "description": "Receive private market report in DM"},
    {"command": "story", "description": "Post 24H pinned Market Story card"},
    {"command": "stories", "description": "View active market stories"},
    {"command": "livestream", "description": "Announce & pin live trading stream"},
    {"command": "endstream", "description": "Conclude live stream session"},
    {"command": "announce", "description": "Post official framed announcement"},
    {"command": "del", "description": "Delete replied message"},
    {"command": "ban", "description": "Ban user from group"},
    {"command": "unban", "description": "Unban user in group"},
    {"command": "kick", "description": "Kick user from group"},
    {"command": "mute", "description": "Mute user for specified duration"},
    {"command": "unmute", "description": "Unmute restricted user"},
]

for attempt in range(5):
    try:
        r = requests.post(f"https://api.telegram.org/bot{token}/setMyCommands", json={"commands": commands}, timeout=15)
        print("setMyCommands status:", r.status_code, r.json())
        break
    except Exception as e:
        print(f"Attempt {attempt+1} failed: {e}")

me = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=15).json()
print("getMe:", me.get("result", {}))
