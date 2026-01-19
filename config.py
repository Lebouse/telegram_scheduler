# config.py

import os

BOT_TOKEN = os.environ["BOT_TOKEN"]
AUTHORIZED_USER_IDS = {int(x.strip()) for x in os.environ["AUTHORIZED_USER_IDS"].split(",")}
DATABASE_PATH = "/data/scheduled_messages.db"  # ← данные будут в volume
TIMEZONE = os.getenv("TIMEZONE", "UTC")
