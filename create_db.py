import sqlite3
import os

DB_PATH = "moder_bot.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Создаем таблицу mutes с id и peer_id
    c.execute("""
    CREATE TABLE IF NOT EXISTS mutes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        issued_by INTEGER,
        until TEXT,
        reason TEXT,
        peer_id INTEGER
    )
    """)

    # Создаем таблицу warns
    c.execute("""
    CREATE TABLE IF NOT EXISTS warns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        issued_by INTEGER NOT NULL,
        reason TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        peer_id INTEGER DEFAULT 0
    )
    """)

    # Создаем таблицу roles
    c.execute("""
    CREATE TABLE IF NOT EXISTS roles (
        user_id INTEGER,
        role TEXT,
        peer_id INTEGER DEFAULT 0
    )
    """)

    # Создаем таблицу blacklist
    c.execute("""
    CREATE TABLE IF NOT EXISTS blacklist (
        word TEXT UNIQUE
    )
    """)

    # Создаем таблицу bans
    c.execute("""
    CREATE TABLE IF NOT EXISTS bans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        issued_by INTEGER NOT NULL,
        until DATETIME,
        reason TEXT,
        peer_id INTEGER DEFAULT 0
    )
    """)

    # Создаем таблицу chats
    c.execute("""
    CREATE TABLE IF NOT EXISTS chats (
        chat_id INTEGER PRIMARY KEY
    )
    """)

    conn.commit()
    conn.close()
    print("✅ База moder_bot.db успешно создана!")

if __name__ == "__main__":
    init_db()
