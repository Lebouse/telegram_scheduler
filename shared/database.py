import sqlite3
import threading
import datetime
from contextlib import contextmanager
from config import DATABASE_PATH

_db_lock = threading.RLock()

def init_db():
    with get_db_connection() as conn:
        conn.execute('PRAGMA journal_mode=WAL;')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS scheduled_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                text TEXT,
                photo_file_id TEXT,
                document_file_id TEXT,
                caption TEXT,
                publish_at TEXT NOT NULL,
                original_publish_at TEXT NOT NULL,
                recurrence TEXT NOT NULL DEFAULT 'once',
                pin BOOLEAN NOT NULL DEFAULT 0,
                notify BOOLEAN NOT NULL DEFAULT 1,
                delete_after_days INTEGER,
                active BOOLEAN NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                max_end_date TEXT,
                task_hash TEXT
            )
        ''')
        conn.commit()

@contextmanager
def get_db_connection():
    with _db_lock:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False, timeout=20)
        conn.execute('PRAGMA busy_timeout = 20000;')
        try:
            yield conn
        finally:
            conn.close()

# ... (остальные функции: add_scheduled_message, get_all_active_messages и т.д. из предыдущих сообщений)
