# database.py

import sqlite3
from config import DATABASE_PATH

def init_db():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scheduled_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            text TEXT,
            photo_file_id TEXT,
            document_file_id TEXT,
            caption TEXT,
            publish_at TEXT NOT NULL,          -- следующая дата публикации
            original_publish_at TEXT NOT NULL, -- исходная дата (для расчёта повторов)
            recurrence TEXT NOT NULL DEFAULT 'once',
            pin BOOLEAN NOT NULL DEFAULT 0,
            notify BOOLEAN NOT NULL DEFAULT 1,
            delete_after_days INTEGER,
            active BOOLEAN NOT NULL DEFAULT 1
        )
    ''')
    conn.commit()
    conn.close()

def add_scheduled_message(data):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO scheduled_messages
        (chat_id, text, photo_file_id, document_file_id, caption, publish_at,
         original_publish_at, recurrence, pin, notify, delete_after_days, active)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        data['chat_id'], data['text'], data['photo_file_id'], data['document_file_id'],
        data['caption'], data['publish_at'], data['publish_at'], data['recurrence'],
        data['pin'], data['notify'], data['delete_after_days'], True
    ))
    msg_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return msg_id

def get_all_active_messages():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM scheduled_messages WHERE active = 1 ORDER BY publish_at")
    rows = cursor.fetchall()
    conn.close()
    return rows

def get_message_by_id(msg_id):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM scheduled_messages WHERE id = ?", (msg_id,))
    row = cursor.fetchone()
    conn.close()
    return row

def deactivate_message(msg_id):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute("UPDATE scheduled_messages SET active = 0 WHERE id = ?", (msg_id,))
    conn.commit()
    conn.close()

def update_next_publish_time(msg_id, next_time_iso):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute("UPDATE scheduled_messages SET publish_at = ? WHERE id = ?", (next_time_iso, msg_id))
    conn.commit()
    conn.close()
