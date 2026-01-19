# config.py

import os
from pytz import timezone

BOT_TOKEN = os.environ["BOT_TOKEN"]
AUTHORIZED_USER_IDS = {int(x.strip()) for x in os.environ["AUTHORIZED_USER_IDS"].split(",")}
DATABASE_PATH = "/data/scheduled_messages.db"
TIMEZONE = timezone(os.getenv("TIMEZONE", "UTC"))
WEB_API_SECRET = os.environ.get("WEB_API_SECRET")
