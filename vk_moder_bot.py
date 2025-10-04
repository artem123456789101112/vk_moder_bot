#!/usr/bin/env python3
# coding: utf-8
"""
vk_moder_bot.py
Полный рабочий скрипт модератор-бота для ВК — расширенная версия с миграциями, автозадачами,
экспортом логов, бэкапом и множеством команд (рус/англ алиасы).
"""
import os
import sys
import time
import shutil
import sqlite3
import logging
import random
import threading
import datetime
from typing import List, Optional, Tuple

import vk_api
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
from vk_api import VkUpload
from dotenv import load_dotenv

# ----------------- Загрузка .env -----------------
load_dotenv()
GROUP_TOKEN = os.getenv("GROUP_TOKEN", "").strip()
GROUP_ID = int(os.getenv("GROUP_ID") or 0)
OWNER_ID = int(os.getenv("OWNER_ID") or 0)
DB_PATH = os.getenv("DB_PATH") or "moder_bot.db"
LOG_PATH = os.getenv("LOG_PATH") or "moder_bot.log"

if not GROUP_TOKEN:
    print("Ошибка: GROUP_TOKEN не задан в .env", file=sys.stderr)
    sys.exit(1)
if GROUP_ID == 0:
    print("Ошибка: GROUP_ID не задан в .env", file=sys.stderr)
    sys.exit(1)

# ----------------- Логирование -----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("vk_moder_bot")

# ----------------- Инициализация VK -----------------
vk_session = vk_api.VkApi(token=GROUP_TOKEN)
vk = vk_session.get_api()
longpoll = VkBotLongPoll(vk_session, GROUP_ID)
upload = VkUpload(vk_session)

# ----------------- Роли и права -----------------
ROLE_PRIORITY = {
    "owner": 100,
    "admin": 80,
    "moder": 60,
    "helper": 40,
    "user": 0
}

PERMS = {
    "owner":   {"warn","unwarn","warns","mute","unmute","kick","skick","ban","unban","sban","sunban","blacklist","add","role","removerole","wipe","gzov","ss","admins","setowner","setadmin","setmoder","sethelper","allowner","alladmin","allmoder","allhelper","report","backup","info","help","clear","exportlogs"},
    "admin":   {"warn","unwarn","warns","mute","unmute","kick","skick","ban","unban","add","role","removerole","gzov","ss","setmoder","sethelper","allmoder","allhelper","report","info","help","allremoverole"},
    "moder":   {"warn","warns","mute","unmute","kick","report","info","help","unwarn"},
    "helper":  {"warn","warns","mute","add","ss","report","info","help"},
    "user":    {"info","report","help","warns"}
}

# ----------------- База данных -----------------
def db_connect():
    return sqlite3.connect(DB_PATH)

def db_execute(query: str, params: tuple = (), fetch: bool = False):
    try:
        conn = db_connect()
        c = conn.cursor()
        c.execute(query, params)
        if fetch:
            rows = c.fetchall()
            conn.commit()
            conn.close()
            return rows
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.exception("DB error: %s | query: %s | params: %s", e, query, params)
        return None

def migrate_db_schema():
    """
    Простые миграции: если нет колонок id / peer_id, добавляем их.
    Это помогает при старых БД, где структура была иной.
    """
    try:
        conn = db_connect()
        c = conn.cursor()
        tables = ["warns","mutes","roles","blacklist","bans","chats"]
        for t in tables:
            try:
                c.execute(f"PRAGMA table_info({t})")
                cols = c.fetchall()
                colnames = [col[1] for col in cols]
                # добавить peer_id если отсутствует и таблица его подразумевает
                if t in ("warns","mutes","roles","bans") and "peer_id" not in colnames:
                    try:
                        c.execute(f"ALTER TABLE {t} ADD COLUMN peer_id INTEGER DEFAULT 0")
                        logger.info("Добавлена колонка peer_id в таблицу %s", t)
                    except Exception:
                        pass
                # добавить id если отсутствует (не идеальное PK, но делает совместимость)
                if "id" not in colnames and t != "chats":
                    try:
                        c.execute(f"ALTER TABLE {t} ADD COLUMN id INTEGER")
                        # заполнить id = rowid
                        c.execute(f"UPDATE {t} SET id = rowid")
                        logger.info("Добавлена колонка id в таблицу %s и проставлены значения", t)
                    except Exception:
                        pass
            except Exception:
                pass
        conn.commit()
        conn.close()
    except Exception as e:
        logger.exception("migrate_db_schema error: %s", e)

def init_db():
    # создаём таблицы (если уже есть — не трогаем)
    db_execute("""CREATE TABLE IF NOT EXISTS warns (
                    id INTEGER,
                    user_id INTEGER,
                    issued_by INTEGER,
                    reason TEXT,
                    timestamp TEXT,
                    peer_id INTEGER DEFAULT 0
                )""")
    db_execute("""CREATE TABLE IF NOT EXISTS mutes (
                    id INTEGER,
                    user_id INTEGER,
                    issued_by INTEGER,
                    until TEXT,
                    reason TEXT,
                    peer_id INTEGER DEFAULT 0
                )""")
    db_execute("""CREATE TABLE IF NOT EXISTS roles (
                    id INTEGER,
                    user_id INTEGER,
                    role TEXT,
                    peer_id INTEGER DEFAULT 0
                )""")
    db_execute("""CREATE TABLE IF NOT EXISTS blacklist (
                    id INTEGER,
                    word TEXT UNIQUE
                )""")
    db_execute("""CREATE TABLE IF NOT EXISTS bans (
                    id INTEGER,
                    user_id INTEGER,
                    issued_by INTEGER,
                    until TEXT,
                    reason TEXT,
                    peer_id INTEGER DEFAULT 0
                )""")
    db_execute("""CREATE TABLE IF NOT EXISTS chats (
                    peer_id INTEGER PRIMARY KEY
                )""")
    # пробуем привести старые таблицы к схеме — добавим недостающие колонки
    migrate_db_schema()
    logger.info("init_db done")

init_db()

# ----------------- Утилиты VK -----------------
def safe_send(peer_id: int, text: str):
    try:
        vk.messages.send(peer_id=int(peer_id), message=str(text), random_id=random.randint(1, 2**31-1))
    except Exception as e:
        logger.debug("safe_send failed: %s", e)

def safe_send_with_attachment(peer_id: int, attachments: List[str], text: str = ""):
    try:
        vk.messages.send(peer_id=int(peer_id), message=str(text), attachment=",".join(attachments), random_id=random.randint(1, 2**31-1))
    except Exception as e:
        logger.debug("safe_send_with_attachment failed: %s", e)

def mention(uid: int) -> str:
    try:
        res = vk.users.get(user_ids=uid)
        if isinstance(res, list) and res:
            n = res[0]
            name = f"{n.get('first_name','')} {n.get('last_name','')}".strip()
            return f"[id{uid}|{name}]"
    except Exception:
        pass
    return f"[id{uid}|{uid}]"

def add_chat(peer_id: int):
    try:
        db_execute("INSERT OR IGNORE INTO chats (peer_id) VALUES (?)", (int(peer_id),))
    except Exception:
        pass

def get_chats() -> List[int]:
    rows = db_execute("SELECT peer_id FROM chats", fetch=True) or []
    return [r[0] for r in rows]

# ----------------- Парсинг user id (reply / id / vk.com / @screenname) -----------------
def parse_user_id(event, args: List[str]) -> Optional[int]:
    """
    Возвращает user_id:
    - reply (если ответ)
    - [id123|..]
    - просто число
    - vk.com/id...
    - @screenname
    """
    msg = getattr(event, "message", None) or (event.obj.get("message") if hasattr(event, "obj") and isinstance(event.obj, dict) else None)
    if msg:
        try:
            reply = msg.get("reply_message") if isinstance(msg, dict) else getattr(msg, "reply_message", None)
            if reply:
                rid = reply.get("from_id") if isinstance(reply, dict) else getattr(reply, "from_id", None)
                if not rid:
                    rid = reply.get("user_id") if isinstance(reply, dict) else getattr(reply, "user_id", None)
                if rid:
                    return int(rid)
        except Exception:
            pass
    # args
    if not args:
        return None
    a = args[0].strip()
    if a.startswith("[id") and "|" in a:
        try:
            return int(a[3:a.index("|")])
        except Exception:
            pass
    if a.isdigit():
        return int(a)
    if "vk.com/" in a:
        try:
            screen = a.rstrip("/").split("/")[-1]
            res = vk.utils.resolveScreenName(screen_name=screen)
            if res and res.get("object_id"):
                return int(res["object_id"])
        except Exception:
            pass
    short = a[1:] if a.startswith("@") else a
    try:
        if short:
            users = vk.users.get(user_ids=short)
            if users and isinstance(users, list) and users:
                return int(users[0]["id"])
    except Exception:
        pass
    return None

# ----------------- Роли — запись, чтение, удаление -----------------
def set_role_db(user_id: int, role: str, peer_id: Optional[int] = None):
    if peer_id is None:
        peer_id = 0
    db_execute("DELETE FROM roles WHERE user_id=? AND peer_id=?", (user_id, peer_id))
    db_execute("INSERT INTO roles (user_id, role, peer_id) VALUES (?,?,?)", (user_id, role, peer_id))
    return True

def remove_roles_db(user_id: int, peer_id: Optional[int] = None):
    if peer_id is None:
        return db_execute("DELETE FROM roles WHERE user_id=?", (user_id,))
    return db_execute("DELETE FROM roles WHERE user_id=? AND peer_id=?", (user_id, peer_id))

def get_role_db(user_id: int, peer_id: Optional[int] = None) -> str:
    if OWNER_ID and int(user_id) == int(OWNER_ID):
        return "owner"
    if peer_id is None:
        peer_id = 0
    rows = db_execute("SELECT role FROM roles WHERE user_id=? AND peer_id=? ORDER BY id DESC LIMIT 1", (user_id, peer_id), fetch=True) or []
    if rows:
        return rows[0][0]
    if peer_id != 0:
        rows2 = db_execute("SELECT role FROM roles WHERE user_id=? AND peer_id=0 ORDER BY id DESC LIMIT 1", (user_id,), fetch=True) or []
        if rows2:
            return rows2[0][0]
    return "user"

# ----------------- Warns / Mutes / Bans -----------------
def add_warn_db(user_id: int, issued_by: int, reason: str, peer_id: int):
    ts = datetime.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") if False else None  # placeholder (overwritten)
def add_warn_db(user_id: int, issued_by: int, reason: str, peer_id: int):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return db_execute("INSERT INTO warns (user_id, issued_by, reason, timestamp, peer_id) VALUES (?,?,?,?,?)", (user_id, issued_by, reason, ts, peer_id))

def get_warns_db(user_id: int):
    rows = db_execute("SELECT id, issued_by, reason, timestamp, peer_id FROM warns WHERE user_id=? ORDER BY id ASC", (user_id,), fetch=True) or []
    return rows

def remove_last_warn_db(user_id: int):
    rows = db_execute("SELECT id FROM warns WHERE user_id=? ORDER BY id DESC LIMIT 1", (user_id,), fetch=True) or []
    if not rows:
        return None
    wid = rows[0][0]
    return db_execute("DELETE FROM warns WHERE id=?", (wid,))

def add_mute_db(user_id: int, issued_by: int, minutes: int, reason: str, peer_id: int):
    until = (datetime.datetime.now() + datetime.timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
    return db_execute("INSERT INTO mutes (user_id, issued_by, until, reason, peer_id) VALUES (?,?,?,?,?)", (user_id, issued_by, until, reason, peer_id))

def get_mutes_db(user_id: int):
    rows = db_execute("SELECT id, user_id, issued_by, until, reason, peer_id FROM mutes WHERE user_id=?", (user_id,), fetch=True) or []
    return rows

def delete_mute_db(mute_id: int):
    return db_execute("DELETE FROM mutes WHERE id=?", (mute_id,))

def delete_mutes_for_user_in_peer_db(user_id: int, peer_id: int):
    return db_execute("DELETE FROM mutes WHERE user_id=? AND peer_id=?", (user_id, peer_id))

def add_blacklist_db(word: str):
    return db_execute("INSERT OR IGNORE INTO blacklist (word) VALUES (?)", (word.lower(),))

def remove_blacklist_db(word: str):
    return db_execute("DELETE FROM blacklist WHERE word=?", (word.lower(),))

def get_blacklist_db() -> List[str]:
    rows = db_execute("SELECT word FROM blacklist", fetch=True) or []
    return [r[0] for r in rows]

def add_ban_db(user_id: int, issued_by: int, reason: str, peer_id: int = 0):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return db_execute("INSERT INTO bans (user_id, issued_by, until, reason, peer_id) VALUES (?,?,?,?,?)", (user_id, issued_by, ts, reason, peer_id))

def remove_bans_db(user_id: int, peer_id: Optional[int] = None):
    if peer_id is None:
        return db_execute("DELETE FROM bans WHERE user_id=?", (user_id,))
    return db_execute("DELETE FROM bans WHERE user_id=? AND peer_id=?", (user_id, peer_id))

def get_bans_db(user_id: int):
    rows = db_execute("SELECT id, issued_by, until, reason, peer_id FROM bans WHERE user_id=?", (user_id,), fetch=True) or []
    return rows

# ----------------- Утилиты чата (кик/добавление) -----------------
def kick_from_chat_peer(peer_peer_id: int, user_id: int) -> bool:
    try:
        if int(peer_peer_id) < 2000000000:
            return False
        chat_id = int(peer_peer_id) - 2000000000
        vk.messages.removeChatUser(chat_id=chat_id, user_id=user_id)
        return True
    except Exception as e:
        logger.debug("kick_from_chat_peer failed: %s", e)
        return False

def add_user_to_chat(peer_peer_id: int, user_id: int) -> bool:
    try:
        if int(peer_peer_id) < 2000000000:
            return False
        chat_id = int(peer_peer_id) - 2000000000
        try:
            vk.messages.addChatUser(chat_id=chat_id, user_id=user_id)
            return True
        except Exception:
            return False
    except Exception as e:
        logger.debug("add_user_to_chat failed: %s", e)
        return False

def global_kick_user(user_id: int) -> List[Tuple[int, bool]]:
    results = []
    chats = get_chats()
    for p in chats:
        ok = kick_from_chat_peer(p, user_id)
        results.append((p, ok))
        time.sleep(0.02)
    return results

# ----------------- Blacklist enforcement -----------------
def handle_blacklist_on_message(event):
    try:
        msg = getattr(event, "message", None) or (event.obj.get("message") if hasattr(event, "obj") and isinstance(event.obj, dict) else None)
        if not msg:
            return False
        peer_id = msg.get("peer_id") if isinstance(msg, dict) else getattr(msg, "peer_id", None)
        from_id = msg.get("from_id") if isinstance(msg, dict) else getattr(msg, "from_id", None)
        text = (msg.get("text") if isinstance(msg, dict) else getattr(msg, "text", "")) or ""
        words = get_blacklist_db()
        if not words:
            return False
        low = text.lower()
        for w in words:
            if not w:
                continue
            if w in low:
                conv_id = msg.get("conversation_message_id") if isinstance(msg, dict) else getattr(msg, "conversation_message_id", None)
                mid = msg.get("id") if isinstance(msg, dict) else getattr(msg, "id", None)
                try:
                    if conv_id:
                        vk.messages.delete(conversation_message_ids=[conv_id], peer_id=peer_id, delete_for_all=1)
                    elif mid:
                        vk.messages.delete(message_ids=[mid], delete_for_all=1)
                except Exception:
                    pass
                try:
                    remove_roles_db(from_id, None)
                except Exception:
                    pass
                res = global_kick_user(from_id)
                ok = sum(1 for _, v in res if v)
                add_ban_db(from_id, OWNER_ID or 0, f"Blacklisted word: {w}", 0)
                ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                notify = f"🚨 BLACKLIST TRIGGER\nUser: {mention(from_id)}\nWord: «{w}»\nDate: {ts}\nRoles removed and attempted kicks: {ok} successful."
                if OWNER_ID:
                    safe_send(OWNER_ID, notify)
                safe_send(peer_id, f"🚫 Сообщение удалено: запрещённое слово «{w}». Пользователь {mention(from_id)} кикнут/заблокирован.")
                return True
    except Exception as e:
        logger.exception("handle_blacklist_on_message error: %s", e)
    return False

# ----------------- Invite handling -----------------
def handle_invite_action(event):
    try:
        msg = getattr(event, "message", None) or (event.obj.get("message") if hasattr(event, "obj") and isinstance(event.obj, dict) else None)
        if not msg:
            return
        action = msg.get("action") if isinstance(msg, dict) else getattr(msg, "action", None)
        if not action:
            return
        act_type = action.get("type") if isinstance(action, dict) else getattr(action, "type", None)
        if act_type not in ("chat_invite_user", "chat_invite_user_by_link"):
            return
        invited = None
        if isinstance(action, dict):
            invited = action.get("member_id") or (action.get("users") and action.get("users")[0])
        actor = msg.get("from_id") if isinstance(msg, dict) else getattr(msg, "from_id", None)
        peer_id = msg.get("peer_id") if isinstance(msg, dict) else getattr(msg, "peer_id", None)
        if not invited:
            return
        invited = int(invited)
        if peer_id and peer_id >= 2000000000:
            add_chat(peer_id)
        bans = get_bans_db(invited) or []
        if bans:
            reason = bans[-1][3] if len(bans[-1])>3 else "Ban"
            kick_from_chat_peer(peer_id, invited)
            safe_send(peer_id, f"❌ {mention(invited)} приглашён — но он в бане. Кикнут. Причина: {reason}")
            return
        actor_role = get_role_db(actor, peer_id)
        rank = ROLE_PRIORITY.get(actor_role, 0)
        if rank < ROLE_PRIORITY.get("helper", 40):
            add_ban_db(actor, OWNER_ID or 0, "Unauthorized invite", peer_id)
            kick_from_chat_peer(peer_id, invited)
            safe_send(peer_id, f"🚨 {mention(actor)} пытался добавить {mention(invited)}. Пригласивший локально забанен, добавленный кикнут.")
            return
    except Exception as e:
        logger.exception("handle_invite_action error: %s", e)

def handle_new_member(event, vk):
    peer_id = event["peer_id"]
    inviter_id = event["action"]["member_id"]
    adder_id = event["from_id"]

    role_adder = get_role_db(adder_id, peer_id)

    if role_adder in ["helper", "moderator", "admin", "owner"]:
        safe_send(peer_id, f"✅ [id{inviter_id}|Пользователь] добавлен в чат.")
    else:
        # Кикаем того, кого добавили
        try:
            vk.messages.removeChatUser(chat_id=peer_id-2000000000, member_id=inviter_id)
            safe_send(peer_id, f"❌ [id{inviter_id}|Пользователь] не может быть добавлен.")
        except:
            pass

        # Баним того, кто добавил
        ban_user(adder_id, peer_id, reason="Нарушение: попытка добавить в чат без прав")

# ----------------- Helpers: проверки -----------------
def is_owner(uid: int) -> bool:
    return OWNER_ID and int(uid) == int(OWNER_ID)

def has_perm(uid: int, cmd_key: str, peer_id: Optional[int] = None) -> bool:
    if is_owner(uid):
        return True
    role = get_role_db(uid, peer_id)
    key_role = role.lower() if role else "user"
    key_role_map = {
        "владелец": "owner", "owner": "owner",
        "админ": "admin", "admin": "admin",
        "модер": "moder", "moder": "moder",
        "помощник": "helper", "helper": "helper",
    }
    key_role = key_role_map.get(key_role, key_role)
    if key_role not in PERMS:
        key_role = "user"
    return cmd_key in PERMS.get(key_role, set())

# ----------------- Алиасы и help описания -----------------
ALIASES = {
    "warn": ["!warn","/warn","!варн","/варн","!пред","/пред"],
    "warns": ["!warns","/warns","!варны","/варны","!предупреждения","/предупреждения"],
    "unwarn": ["!unwarn","/unwarn","!снятьварн","/снятьварн","!унварн","/унварн"],
    "mute": ["!mute","/mute","!мут","/мут","!заткнуть","/заткнуть"],
    "unmute": ["!unmute","/unmute","!анмут","/анмут","!размут","/размут"],
    "kick": ["!kick","/kick","!кик","/кик","!исключить","/исключить"],
    "skick": ["!скик","/скик"],
    "ban": ["!ban","/ban","!бан","/бан"],
    "unban": ["!unban","/unban","!унбан","/унбан"],
    "sban": ["!sban","/sban","!сбан","/сбан"],
    "sunban": ["!sunban","/sunban","!сунбан","/сунбан"],
    "info": ["!info","/info","!инфо","/инфо","/я","!я","/q","!q"],
    "blacklist": ["!blacklist","/blacklist","!чс","/чс","!блэклист","/блэклист"],
    "add": ["!add","/add","!добавить","/добавить","!добавитьвгруппу","/добавитьвгруппу"],
    "help": ["!help","/help","!помощь","/помощь"],
    "wipe": ["!wipe","/wipe","!вайп","/вайп"],
    "gzov": ["!gzov","/gzov","!гзов","/гзов"],
    "ss": ["!ss","/ss","!сс","/сс"],
    "report": ["!report","/report","!репорт","/репорт"],
    "admins": ["/админы","!админы","/admins","!admins"],
    "setowner": ["/owner","/назначитьвладельцем","!owner","!назначитьвладельцем","/setowner"],
    "allowner": ["/allowner","/всемвладельцем","!allowner"],
    "setadmin": ["/admin","/назначитьадминистратором","!admin","!назначитьадминистратором","/setadmin"],
    "setmoder": ["/moder","/назначитьмодератором","!moder","!назначитьмодератором","/setmoder"],
    "sethelper": ["/helper","/назначитьпомощником","!helper","!назначитьпомощником","/sethelper"],
    "alladmin": ["/alladmin","!alladmin"],
    "allmoder": ["/allmoder","!allmoder"],
    "allhelper": ["/allhelper","!allhelper"],
    "removerole": ["/снять","/разжаловать","/removerole","/ремувроль"],
    "allremoverole": ["/аллснять","/аллразжаловать","/allremoverole","/аллремувроль"],
    "backup": ["/backup","!backup","/бэкап","!бэкап"],
    "exportlogs": ["/exportlogs","/экспортлогов","/export_logs","/экспорт_логов"],
    "clear": ["/clear","!clear","/удалить","!удалить"]
}

HELP_TEXTS = {
    "info": "/info [id] - Показать информацию о пользователе.",
    "report": "Отправить репорт владельцу (/report <текст>)",
    "help": "Показать это сообщение",
    "warn": "Выдать предупреждение (/warn [id|reply] [причина])",
    "warns": "Показать предупреждения пользователя (/warns [id|reply])",
    "unwarn": "Снять последний варн (/unwarn [id|reply])",
    "mute": "Выдать мут на X минут (/mute [id|reply] <минуты> [причина]) — сообщения удаляются",
    "unmute": "Снять мут (/unmute [id|reply])",
    "kick": "Кикнуть из беседы (/kick [id|reply] [причина])",
    "skick": "Попытаться кикнуть пользователя из всех известных боту бесед (/skick [id|reply])",
    "ban": "Забанить в текущей беседе (/ban [id|reply] [причина])",
    "unban": "Снять бан в текущей беседе (/unban [id|reply])",
    "sban": "Глобальный бан (владелец) (/sban [id|reply] [причина])",
    "sunban": "Снять глобальный бан (владелец) (/sunban [id|reply])",
    "add": "Добавить пользователя в беседу (helper+) (/add [id|reply])",
    "blacklist": "Управление черным списком слов (владелец) (/blacklist add/remove/list слово)",
    "wipe": "Очистка таблиц (владелец) (/wipe warns/bans/roles/blacklist/chats)",
    "gzov": "Разослать сообщение по всем сохранённым чатам (admin+) (/gzov <текст>)",
    "ss": "Сообщение: @all Старший состав в игру! (/ss)",
    "admins": "Показать владельца/админов/модеров/помощников в беседе (/админы)",
    "setowner": "Назначить владельцем в текущей беседе (владелец) (/owner [id|reply])",
    "allowner": "Назначить владельцем во всех беседах (владелец) (/allowner [id|reply])",
    "backup": "Создать бэкап БД и отправить владельцу (владелец) (/backup)",
    "clear": "Удалить сообщение, на которое дан reply; модераторы+"
}

def resolve_alias(cmd_text: str) -> Optional[str]:
    ct = cmd_text.lower()
    for key, vals in ALIASES.items():
        if ct in vals:
            return key
    return None

# ----------------- Реализация команд -----------------
def cmd_help(peer_id: int, from_id: int, event, args: List[str]):
    role = get_role_db(from_id, peer_id)
    hierarchy = ["user", "helper", "moderator", "admin", "owner"]

    help_text = "📖 Доступные команды для вашей роли:\n\n"

    if role in hierarchy:
        help_text += "👤 Пользователь:\n\n"
        help_text += "/репорт [текст] (/report) - пожаловаться на игрока владельцу.\n\n"
        help_text += "/инфо [id] (/инфо или /я) - информация о пользователе.\n\n"
        help_text += "/помощь [id] - информация о командах.\n\n"
        help_text += "/варны [id] - информация о варнов у пользователя.\n\n"

    if role in ["helper", "moderator", "admin", "owner"]:
        help_text += "🤝 Помощник:\n\n"
        help_text += "/warn [id] [причина] (/варн) - выдать варн пользователю чата.\n\n"
        help_text += "/mute [id] [время (в минутах)] [причина] (/мут) - выдать мут пользователю чата.\n\n"
        help_text += "/ss (/cc) - вызвать старший состав в игру.\n\n"

    if role in ["moderator", "admin", "owner"]:
        help_text += "🔨 Модератор:\n\n"
        help_text += "/unwarn [id] (/унварн) - снять предупреждение.\n\n"
        help_text += "/unmute [id] (/унмут) - снять мут.\n\n"
        help_text += "/kick [id] [причина] (/кик) - исключить пользователя из беседы.\n\n"

    if role in ["admin", "owner"]:
        help_text += "🛡 Админ:\n\n"
        help_text += "/ban [id] [срок (в днях)] [причина] (/бан) — выдать бан пользователю.\n\n"
        help_text += "/unban [id] (/унбан) — снять бан пользователю в группе.\n\n"
        help_text += "/skick [id] [причина] (/cкик) - исключить пользователя из всех привязанных беседы.\n\n"
        help_text += "/removerole [id] (/снять) - снять роль в беседе у пользователя.\n\n"
        help_text += "/allremoverole [id] (/аллснять) - снять роль во всех беседах у пользователя.\n\n"
        help_text += "/gzov [текст] (/gzov) - разослать сообщение по всем приявязанным чатам.\n\n"
        help_text += "/sethelper [id] (/helper или /назначитьхелпером) - выдать роль хелпера (помощника) пользователю группы. (следящий)\n\n"
        help_text += "/setmoder [id] (/moder или /назначитьмодератором) - выдать роль модератора пользователю группы. (лидер)\n\n"
        help_text += "/allmoder [id] - выдать роль модератора во всех группах пользователю.\n\n"
        help_text += "/allhelper [id] - выдать роль хелпера (помощника) во всех группах пользователю.\n\n"

    if role == "owner":
        help_text += "👑 Владелец:\n\n"
        help_text += "/sban [id] [причина] (/сбан) — выдать бан пользователю во всех привязанных группах.\n\n"
        help_text += "/sunban [id] (/сунбан) — снять бан пользователю во всех привязанных группах.\n\n"
        help_text += "/setadmin [id] (/admin или /назначитьадминистратором) - выдать роль администратора пользователю группы.\n\n"
        help_text += "/setowner [id] (/owner или /назначитьвладельцем) - выдать роль владельца пользователю группы.\n\n"
        help_text += "/alladmin [id] - выдать роль администратора во всех группах пользователю.\n\n"
        help_text += "/allowner [id] - выдать роль владельца во всех группах пользователю.\n\n"
        help_text += "/blacklist remove (/чс remove) — удалить слово из списка запрещенных слов.\n\n"
        help_text += "/blacklist add (/чс add) — добавить в список запрещенное слово.\n\n"
        help_text += "/blacklist list (/чс list) — список запрещенных слов.\n\n"
        help_text += "/exportlogs (/экспортлогов) — экспорт логов.\n\n"
        help_text += "/backup (/бэкап) — сделать бэкап.\n\n"
        help_text += "/wipe chats — отчитить таблицу чатов.\n\n"
        help_text += "/wipe blacklist — отчитить таблицу запрещенных слов.\n\n"
        help_text += "/wipe roles — отчитить таблицу ролей.\n\n"
        help_text += "/wipe bans — отчитить таблицу банов.\n\n"
        help_text += "/wipe warns — отчитить таблицу варнов.\n\n"


    safe_send(peer_id, help_text.strip())

def cmd_info(peer_id: int, from_id: int, event, args: List[str]):
    target = parse_user_id(event, args) or from_id
    role = get_role_db(target, peer_id)
    warns = get_warns_db(target) or []
    mutes = get_mutes_db(target) or []
    bans = get_bans_db(target) or []
    active_mutes = []
    for m in mutes:
        try:
            until = datetime.datetime.strptime(m[3], "%Y-%m-%d %H:%M:%S")
            if until > datetime.datetime.now():
                active_mutes.append(m)
        except Exception:
            pass
    text = (f"📌 Инфо: {mention(target)}\n"
            f"Роль (локально): {role}\n"
            f"Всего варнов: {len(warns)}\nАктивных мутов: {len(active_mutes)}\nЗаписей о банах: {len(bans)}")
    safe_send(peer_id, text)

def cmd_warn(peer_id: int, from_id: int, event, args: List[str]):
    if not has_perm(from_id, "warn", peer_id):
        return safe_send(peer_id, "❌ Недостаточно прав.")
    target = parse_user_id(event, args)
    if not target:
        return safe_send(peer_id, "❌ Укажите пользователя (reply или id).")
    reason = " ".join(args[1:]) if len(args) > 1 else "Не указана"
    add_warn_db(target, from_id, reason, peer_id)
    warns = get_warns_db(target) or []
    safe_send(peer_id, (f"⚠️ Варн выдан {mention(target)}.\nПричина: {reason}\nВыдал: {mention(from_id)}\n"
                        f"Дата: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\nВсего варнов: {len(warns)}"))
    if len(warns) >= 3:
        kick_from_chat_peer(peer_id, target)
        safe_send(peer_id, f"❌ {mention(target)} исключён из беседы (3/3).")

def cmd_warns(peer_id: int, from_id: int, event, args: List[str]):
    target = parse_user_id(event, args) or from_id
    warns = get_warns_db(target) or []
    if not warns:
        return safe_send(peer_id, f"✅ У {mention(target)} нет варнов.")
    text = f"📜 Варны {mention(target)} ({len(warns)}):\n"
    for w in warns:
        text += f"- {w[3]} | от {mention(w[1])} | причина: {w[2]}\n"
    safe_send(peer_id, text)

def cmd_unwarn(peer_id: int, from_id: int, event, args: List[str]):
    if not has_perm(from_id, "unwarn", peer_id):
        return safe_send(peer_id, "❌ Недостаточно прав.")
    target = parse_user_id(event, args)
    if not target:
        return safe_send(peer_id, "❌ Укажите пользователя.")
    res = remove_last_warn_db(target)
    if res is None:
        return safe_send(peer_id, "❌ У пользователя нет варнов.")
    safe_send(peer_id, f"✅ Последний варн снят у {mention(target)}")

def cmd_mute(peer_id: int, from_id: int, event, args: List[str]):
    if not has_perm(from_id, "mute", peer_id):
        return safe_send(peer_id, "❌ Недостаточно прав.")
    target = parse_user_id(event, args)
    if not target:
        return safe_send(peer_id, "❌ Укажите пользователя.")
    minutes = 10
    reason = "Не указана"
    if len(args) >= 2 and args[1].isdigit():
        minutes = int(args[1])
        reason = " ".join(args[2:]) if len(args) > 2 else "Не указана"
    else:
        reason = " ".join(args[1:]) if len(args) > 1 else "Не указана"
    add_mute_db(target, from_id, minutes, reason, peer_id)
    until = (datetime.datetime.now() + datetime.timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
    safe_send(peer_id, f"🔇 Мут выдан {mention(target)} на {minutes} минут.\nПричина: {reason}\nДо: {until}")

def cmd_unmute(peer_id: int, from_id: int, event, args: List[str]):
    if not has_perm(from_id, "unmute", peer_id):
        return safe_send(peer_id, "❌ Недостаточно прав.")
    target = parse_user_id(event, args)
    if not target:
        return safe_send(peer_id, "❌ Укажите пользователя.")
    delete_mutes_for_user_in_peer_db(target, peer_id)
    safe_send(peer_id, f"🔔 Мут снят с {mention(target)}")

def cmd_kick(peer_id: int, from_id: int, event, args: List[str]):
    if not has_perm(from_id, "kick", peer_id):
        return safe_send(peer_id, "❌ Недостаточно прав.")
    target = parse_user_id(event, args)
    if not target:
        return safe_send(peer_id, "❌ Укажите пользователя.")
    reason = " ".join(args[1:]) if len(args) > 1 else "Не указана"
    ok = kick_from_chat_peer(peer_id, target)
    if ok:
        safe_send(peer_id, f"👢 {mention(target)} кикнут.\nПричина: {reason}\nВыдал: {mention(from_id)}")
    else:
        safe_send(peer_id, "❌ Не удалось кикнуть (возможно у бота нет прав).")

def cmd_skick(peer_id: int, from_id: int, event, args: List[str]):
    if not (has_perm(from_id, "skick", peer_id) or is_owner(from_id)):
        return safe_send(peer_id, "❌ Недостаточно прав.")
    target = parse_user_id(event, args)
    if not target:
        return safe_send(peer_id, "❌ Укажите пользователя.")
    res = global_kick_user(target)
    ok = sum(1 for _, v in res if v)
    safe_send(peer_id, f"👢 Попытка исключить {mention(target)} из всех бесед. Успешно: {ok}/{len(res)}")

def cmd_ban(peer_id: int, from_id: int, event, args: List[str]):
    if not has_perm(from_id, "ban", peer_id):
        return safe_send(peer_id, "❌ Недостаточно прав.")
    target = parse_user_id(event, args)
    if not target:
        return safe_send(peer_id, "❌ Укажите пользователя.")
    reason = " ".join(args[1:]) if len(args) > 1 else "Не указана"
    add_ban_db(target, from_id, reason, peer_id)
    remove_roles_db(target, peer_id)
    kick_from_chat_peer(peer_id, target)
    safe_send(peer_id, f"🔒 {mention(target)} забанен в этой беседе. Причина: {reason}")

def cmd_unban_local(peer_id: int, from_id: int, event, args: List[str]):
    if not has_perm(from_id, "unban", peer_id) and not has_perm(from_id, "ban", peer_id):
        return safe_send(peer_id, "❌ Недостаточно прав.")
    target = parse_user_id(event, args)
    if not target:
        return safe_send(peer_id, "❌ Укажите пользователя.")
    remove_bans_db(target, peer_id)
    safe_send(peer_id, f"🔓 Бан снят с {mention(target)} в этой беседе.")

def cmd_sban(peer_id: int, from_id: int, event, args: List[str]):
    if not is_owner(from_id):
        return safe_send(peer_id, "❌ Только владелец может выдавать глобальный бан.")
    target = parse_user_id(event, args)
    if not target:
        return safe_send(peer_id, "❌ Укажите пользователя.")
    reason = " ".join(args[1:]) if len(args) > 1 else "Не указана"
    add_ban_db(target, from_id, reason, 0)
    remove_roles_db(target, None)
    res = global_kick_user(target)
    ok = sum(1 for _, v in res if v)
    safe_send(peer_id, f"🚫 {mention(target)} глобально забанен. Удалён из {ok}/{len(res)} бесед. Причина: {reason}")

def cmd_sunban(peer_id: int, from_id: int, event, args: List[str]):
    if not is_owner(from_id):
        return safe_send(peer_id, "❌ Только владелец.")
    target = parse_user_id(event, args)
    if not target:
        return safe_send(peer_id, "❌ Укажите пользователя.")
    remove_bans_db(target, None)
    safe_send(peer_id, f"🔓 Глобальный бан снят с {mention(target)}")

def cmd_add(peer_id: int, from_id: int, event, args: List[str]):
    role = get_role_db(from_id, peer_id)

    if role not in ["helper", "moderator", "admin", "owner"]:
        safe_send(peer_id, "⛔ У вас нет прав для использования этой команды.")
        return

    target_id = None

    if args:
        target_id = parse_user_id(args[0])
    elif event.get("reply_message"):
        target_id = event["reply_message"]["from_id"]

    if not target_id:
        safe_send(peer_id, "⚠ Укажите пользователя через @id или ответом на сообщение.")
        return

    try:
        vk.messages.addChatUser(chat_id=peer_id-2000000000, user_id=target_id)
        safe_send(peer_id, f"✅ [id{target_id}|Пользователь] добавлен в чат.")
    except Exception as e:
        safe_send(peer_id, f"⚠ Ошибка: {e}")

def cmd_role_local(peer_id: int, from_id: int, event, args: List[str], role_name: str):
    if not (has_perm(from_id, "role", peer_id) or is_owner(from_id)):
        return safe_send(peer_id, "❌ Недостаточно прав.")
    target = parse_user_id(event, args)
    if not target:
        return safe_send(peer_id, "❌ Укажите пользователя.")
    set_role_db(target, role_name, peer_id)
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    safe_send(peer_id, f"✅ {mention(target)} назначен(а) {role_name} в этой беседе.\nВыдал: {mention(from_id)}\nДата: {ts}")

def cmd_role_global(peer_id: int, from_id: int, event, args: List[str], role_name: str):
    if not is_owner(from_id):
        return safe_send(peer_id, "❌ Только владелец может назначать роли во всех беседах.")
    target = parse_user_id(event, args)
    if not target:
        return safe_send(peer_id, "❌ Укажите пользователя.")
    set_role_db(target, role_name, 0)
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    safe_send(peer_id, f"🌍 {mention(target)} назначен(а) {role_name} глобально.\nВыдал: {mention(from_id)}\nДата: {ts}")

def cmd_remove_role_local(peer_id: int, from_id: int, event, args: List[str]):
    if not (has_perm(from_id, "removerole", peer_id) or is_owner(from_id)):
        return safe_send(peer_id, "❌ Недостаточно прав.")
    target = parse_user_id(event, args)
    if not target:
        return safe_send(peer_id, "❌ Укажите пользователя.")
    remove_roles_db(target, peer_id)
    safe_send(peer_id, f"✅ С {mention(target)} сняты роли в этой беседе.")

def cmd_remove_role_global(peer_id: int, from_id: int, event, args: List[str]):
    if not is_owner(from_id):
        return safe_send(peer_id, "❌ Только владелец.")
    target = parse_user_id(event, args)
    if not target:
        return safe_send(peer_id, "❌ Укажите пользователя.")
    remove_roles_db(target, None)
    safe_send(peer_id, f"🌍 С {mention(target)} сняты все роли глобально.")

def cmd_blacklist(peer_id: int, from_id: int, event, args: List[str]):
    if not is_owner(from_id):
        return safe_send(peer_id, "❌ Только владелец может управлять ЧС.")
    if not args:
        return safe_send(peer_id, "Использование: /blacklist add/remove/list <слово>")
    action = args[0].lower()
    if action in ("add","добавить"):
        if len(args) < 2:
            return safe_send(peer_id, "❌ Укажите слово.")
        add_blacklist_db(args[1])
        safe_send(peer_id, f"✅ Слово '{args[1]}' добавлено в список запретных слов.")
    elif action in ("remove","удалить","rm"):
        if len(args) < 2:
            return safe_send(peer_id, "❌ Укажите слово.")
        remove_blacklist_db(args[1])
        safe_send(peer_id, f"✅ Слово '{args[1]}' удалено из списка запретных слов.")
    elif action in ("list","список"):
        bl = get_blacklist_db()
        safe_send(peer_id, "📜 ЧС слов:\n" + (", ".join(bl) if bl else "ЧС пуст."))
    else:
        safe_send(peer_id, "❌ Неизвестное действие.")

def cmd_wipe(peer_id: int, from_id: int, event, args: List[str]):
    if not is_owner(from_id):
        return safe_send(peer_id, "❌ Только владелец.")
    if not args:
        return safe_send(peer_id, "Использование: /wipe warns/bans/roles/blacklist/chats")
    t = args[0].lower()
    if t == "warns":
        db_execute("DELETE FROM warns")
        safe_send(peer_id, "🧹 Все варны очищены.")
    elif t == "bans":
        db_execute("DELETE FROM bans")
        safe_send(peer_id, "🧹 Все баны очищены.")
    elif t == "roles":
        db_execute("DELETE FROM roles")
        safe_send(peer_id, "🧹 Все роли очищены.")
    elif t == "blacklist":
        db_execute("DELETE FROM blacklist")
        safe_send(peer_id, "🧹 ЧС очищен.")
    elif t == "chats":
        db_execute("DELETE FROM chats")
        safe_send(peer_id, "🧹 Список чатов очищен.")
    else:
        safe_send(peer_id, "❌ Неверный параметр.")

def cmd_report(peer_id: int, from_id: int, event, args: List[str]):
    if not args:
        return safe_send(peer_id, "❌ Напишите текст репорта.")
    text = " ".join(args)
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload = f"📣 Репорт от {mention(from_id)}\n{text}\n{ts}"
    if OWNER_ID:
        safe_send(OWNER_ID, payload)
    safe_send(peer_id, "✅ Репорт отправлен владельцу.")

def cmd_gzov(peer_id: int, from_id: int, event, args: List[str]):
    if not (has_perm(from_id, "gzov", peer_id) or is_owner(from_id)):
        return safe_send(peer_id, "❌ Недостаточно прав.")
    if not args:
        return safe_send(peer_id, "❌ Укажите текст для рассылки.")
    try:
        u = vk.users.get(user_ids=from_id)[0]
        name = f"{u.get('first_name','')} {u.get('last_name','')}"
    except Exception:
        name = str(from_id)
    msg = f"@all Внимание! Информация от {name}!\n\n" + " ".join(args) + "\n\nСпасибо за внимание!"
    chats = get_chats()
    if not chats:
        return safe_send(peer_id, "❌ Бот не состоит ни в одной беседе.")
    ok = 0
    for p in chats:
        try:
            safe_send(p, msg)
            ok += 1
            time.sleep(0.02)
        except Exception:
            pass
    safe_send(peer_id, f"✅ Рассылка отправлена в {ok} бесед(ы).")

def cmd_ss(peer_id: int, from_id: int, event, args: List[str]):
    if not (has_perm(from_id, "ss", peer_id) or is_owner(from_id)):
        return safe_send(peer_id, "❌ Недостаточно прав.")
    safe_send(peer_id, "@all Старший состав в игру! Даю 5 минут.")

def cmd_admins(peer_id: int, from_id: int, event, args: List[str]):
    roles = db_select("SELECT user_id, role FROM roles WHERE peer_id=?", (peer_id,))
    if not roles:
        safe_send(peer_id, "⚠ В этом чате пока нет администраторов.")
        return

    grouped = {"owner": [], "admin": [], "moderator": [], "helper": []}
    for user_id, role in roles:
        if role in grouped:
            grouped[role].append(user_id)

    msg = "👑 Список администрации чата:\n\n"
    if grouped["owner"]:
        msg += "👑 Владельцы:\n" + "\n".join([f"[id{uid}|Пользователь]" for uid in grouped["owner"]]) + "\n\n"
    if grouped["admin"]:
        msg += "🛡 Админы:\n" + "\n".join([f"[id{uid}|Пользователь]" for uid in grouped["admin"]]) + "\n\n"
    if grouped["moderator"]:
        msg += "🔨 Модераторы:\n" + "\n".join([f"[id{uid}|Пользователь]" for uid in grouped["moderator"]]) + "\n\n"
    if grouped["helper"]:
        msg += "🤝 Помощники:\n" + "\n".join([f"[id{uid}|Пользователь]" for uid in grouped["helper"]]) + "\n\n"

    safe_send(peer_id, msg.strip())

# ----------------- Команды владельца (лок/глоб) -----------------
def cmd_setowner_local(peer_id: int, from_id: int, event, args: List[str]):
    if not is_owner(from_id):
        return safe_send(peer_id, "❌ Только владелец может назначать владельцев.")
    target = parse_user_id(event, args)
    if not target:
        return safe_send(peer_id, "❌ Укажите пользователя.")
    set_role_db(target, "owner", peer_id)
    safe_send(peer_id, f"✅ {mention(target)} назначен(а) владельцем в этой беседе.")

def cmd_setowner_global(peer_id: int, from_id: int, event, args: List[str]):
    if not is_owner(from_id):
        return safe_send(peer_id, "❌ Только владелец.")
    target = parse_user_id(event, args)
    if not target:
        return safe_send(peer_id, "❌ Укажите пользователя.")
    set_role_db(target, "owner", 0)
    safe_send(peer_id, f"🌍 {mention(target)} назначен(а) владельцем глобально.")

# ----------------- Бэкап и экспорт логов -----------------
def create_backup_file() -> Optional[str]:
    try:
        os.makedirs("backups", exist_ok=True)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = os.path.join("backups", f"moder_bot_backup_{ts}.db")
        shutil.copyfile(DB_PATH, fname)
        return fname
    except Exception as e:
        logger.exception("create_backup_file error: %s", e)
        return None

def cmd_backup(peer_id: int, from_id: int, event, args: List[str]):
    if not is_owner(from_id):
        return safe_send(peer_id, "❌ Только владелец может делать бэкап.")
    fname = create_backup_file()
    if not fname:
        return safe_send(peer_id, "❌ Ошибка при создании бэкапа.")
    try:
        doc = upload.document_message(fname, title=os.path.basename(fname), peer_id=from_id)
        attach = f"doc{doc['doc']['owner_id']}_{doc['doc']['id']}"
        vk.messages.send(peer_id=from_id, random_id=random.randint(1, 2**31-1), attachment=attach, message="✅ Бэкап базы")
        safe_send(peer_id, "✅ Бэкап создан и отправлен владельцу в ЛС.")
    except Exception as e:
        logger.exception("backup upload error: %s", e)
        safe_send(peer_id, f"⚠️ Бэкап создан: {fname}, но не удалось отправить в ЛС.")

def export_logs_file() -> Optional[str]:
    try:
        os.makedirs("logs_export", exist_ok=True)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        dst = os.path.join("logs_export", f"moder_bot_log_{ts}.log")
        shutil.copyfile(LOG_PATH, dst)
        return dst
    except Exception as e:
        logger.exception("export_logs_file error: %s", e)
        return None

def cmd_export_logs(peer_id: int, from_id: int, event, args: List[str]):
    if not (has_perm(from_id, "exportlogs", peer_id) or is_owner(from_id)):
        return safe_send(peer_id, "❌ Недостаточно прав.")
    dst = export_logs_file()
    if not dst:
        return safe_send(peer_id, "❌ Ошибка экспорта логов.")
    try:
        doc = upload.document_message(dst, title=os.path.basename(dst), peer_id=from_id)
        attach = f"doc{doc['doc']['owner_id']}_{doc['doc']['id']}"
        vk.messages.send(peer_id=from_id, random_id=random.randint(1, 2**31-1), attachment=attach, message="🗒️ Экспорт логов")
        safe_send(peer_id, "✅ Логи экспортированы и отправлены владельцу.")
    except Exception as e:
        logger.exception("logs upload error: %s", e)
        safe_send(peer_id, f"⚠️ Логи экспортированы: {dst}, но не удалось отправить в ЛС.")

# ----------------- Команда clear (/удалить) -----------------
def cmd_clear(peer_id, from_id, args, event, vk):
    role = get_role_db(from_id, peer_id)

    # Проверка прав
    allowed_roles = ["moderator", "admin", "owner"]
    if role not in allowed_roles:
        safe_send(peer_id, "⛔ У вас нет прав для использования этой команды.")
        return

    # Проверяем ответ на сообщение
    if not event.get("reply_message"):
        safe_send(peer_id, "⚠ Используйте команду ответом на сообщение, которое нужно удалить.")
        return

    target_msg_id = event["reply_message"]["id"]
    target_user_id = event["reply_message"]["from_id"]

    # Проверка по ролям
    target_role = get_role_db(target_user_id, peer_id)
    hierarchy = {"user": 0, "helper": 1, "moderator": 2, "admin": 3, "owner": 4}

    if hierarchy.get(role, 0) <= hierarchy.get(target_role, 0):
        safe_send(peer_id, "⛔ Вы не можете удалить сообщение этого пользователя.")
        return

    try:
        # Удаляем сообщение жертвы
        vk.messages.delete(peer_id=peer_id, cmids=[target_msg_id], delete_for_all=1)
        # Удаляем команду
        vk.messages.delete(peer_id=peer_id, cmids=[event["conversation_message_id"]], delete_for_all=1)

        # Отчёт
        report = f"""
🗑 Сообщение удалено!
👮 Удалил: [id{from_id}|Пользователь]
👤 У кого: [id{target_user_id}|Пользователь]
⏰ Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
        safe_send(peer_id, report)
    except Exception as e:
        safe_send(peer_id, f"⚠ Ошибка при удалении: {e}")

# ----------------- Автоматические задачи -----------------
def mute_watcher():
    while True:
        try:
            rows = db_execute("SELECT id, user_id, issued_by, until, reason, peer_id FROM mutes", fetch=True) or []
            now = datetime.datetime.now()
            for r in rows:
                try:
                    mid, uid, issued_by, until_s, reason, peer_id = r
                    until = datetime.datetime.strptime(until_s, "%Y-%m-%d %H:%M:%S")
                    if until <= now:
                        delete_mute_db(mid)
                        text = f"🔔 Мут снят: {mention(uid)}\nПричина: {reason}\nВыдал: {mention(issued_by)}\nВремя: {until_s}"
                        if peer_id and peer_id >= 2000000000:
                            safe_send(peer_id, text)
                        else:
                            if OWNER_ID:
                                safe_send(OWNER_ID, text)
                except Exception:
                    pass
        except Exception as e:
            logger.exception("mute_watcher loop error: %s", e)
        time.sleep(10)

from zoneinfo import ZoneInfo   # импорт в начале файла

def wait_until_next(hour: int = 23, minute: int = 59):
    tz = ZoneInfo("Europe/Moscow")  # московская зона
    now = datetime.datetime.now(tz=tz)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now >= target:
        target = target + datetime.timedelta(days=1)
    delta = (target - now).total_seconds()
    return max(0, delta)

def periodic_backup_and_logs():
    """
    Ежедневно в 23:59 по локальному времени выполняет:
    - бэкап БД
    - экспорт логов
    и отправляет результаты владельцу.
    """
    while True:
        try:
            secs = wait_until_next(23, 59)
            logger.info("Автозадача: ожидание %s секунд до 23:59", secs)
            time.sleep(secs + 1)  # приблизительно к 23:59:01
            # бэкап
            try:
                bfile = create_backup_file()
                if bfile and OWNER_ID:
                    try:
                        doc = upload.document_message(bfile, title=os.path.basename(bfile), peer_id=OWNER_ID)
                        attach = f"doc{doc['doc']['owner_id']}_{doc['doc']['id']}"
                        vk.messages.send(peer_id=OWNER_ID, random_id=random.randint(1,2**31-1), attachment=attach, message=f"Автобэкап базы выполнен: {os.path.basename(bfile)}")
                    except Exception as e:
                        logger.exception("periodic backup upload error: %s", e)
            except Exception:
                logger.exception("periodic backup error")
            # экспорт логов
            try:
                logfile = export_logs_file()
                if logfile and OWNER_ID:
                    try:
                        doc = upload.document_message(logfile, title=os.path.basename(logfile), peer_id=OWNER_ID)
                        attach = f"doc{doc['doc']['owner_id']}_{doc['doc']['id']}"
                        vk.messages.send(peer_id=OWNER_ID, random_id=random.randint(1,2**31-1), attachment=attach, message=f"Автоэкспорт логов: {os.path.basename(logfile)}")
                    except Exception as e:
                        logger.exception("periodic logs upload error: %s", e)
            except Exception:
                logger.exception("periodic logs error")
            # после выполнения — ждём снова на следующий день
        except Exception as e:
            logger.exception("periodic backup/logs loop error: %s", e)
            time.sleep(60)

# Запускаем фоновые потоки
threading.Thread(target=mute_watcher, daemon=True).start()
threading.Thread(target=periodic_backup_and_logs, daemon=True).start()

# ----------------- Диспетчер команд -----------------
def handle_command(event, cmd_text: str, args: List[str]):
    msg = getattr(event, "message", None) or (event.obj.get("message") if hasattr(event, "obj") and isinstance(event.obj, dict) else None)
    if not msg:
        return
    peer_id = msg.get("peer_id") if isinstance(msg, dict) else getattr(msg, "peer_id", None)
    from_id = msg.get("from_id") if isinstance(msg, dict) else getattr(msg, "from_id", None)
    if peer_id and peer_id >= 2000000000:
        add_chat(peer_id)
    key = resolve_alias(cmd_text)
    if not key:
        return
    try:
        # map key to function
        if key == "help":
            return cmd_help(peer_id, from_id, event, args)
        if key == "info":
            return cmd_info(peer_id, from_id, event, args)
        if key == "warn":
            return cmd_warn(peer_id, from_id, event, args)
        if key == "warns":
            return cmd_warns(peer_id, from_id, event, args)
        if key == "unwarn":
            return cmd_unwarn(peer_id, from_id, event, args)
        if key == "mute":
            return cmd_mute(peer_id, from_id, event, args)
        if key == "unmute":
            return cmd_unmute(peer_id, from_id, event, args)
        if key == "kick":
            return cmd_kick(peer_id, from_id, event, args)
        if key == "skick":
            return cmd_skick(peer_id, from_id, event, args)
        if key == "ban":
            return cmd_ban(peer_id, from_id, event, args)
        if key == "unban":
            return cmd_unban_local(peer_id, from_id, event, args)
        if key == "sban":
            return cmd_sban(peer_id, from_id, event, args)
        if key == "sunban":
            return cmd_sunban(peer_id, from_id, event, args)
        if key == "add":
            return cmd_add(peer_id, from_id, event, args)
        if key == "blacklist":
            return cmd_blacklist(peer_id, from_id, event, args)
        if key == "wipe":
            return cmd_wipe(peer_id, from_id, event, args)
        if key == "report":
            return cmd_report(peer_id, from_id, event, args)
        if key == "gzov":
            return cmd_gzov(peer_id, from_id, event, args)
        if key == "ss":
            return cmd_ss(peer_id, from_id, event, args)
        if key == "admins":
            return cmd_admins(peer_id, from_id, event, args)
        if key == "setowner":
            return cmd_setowner_local(peer_id, from_id, event, args)
        if key == "allowner":
            return cmd_setowner_global(peer_id, from_id, event, args)
        if key == "setadmin":
            return cmd_role_local(peer_id, from_id, event, args, "admin")
        if key == "setmoder":
            return cmd_role_local(peer_id, from_id, event, args, "moder")
        if key == "sethelper":
            return cmd_role_local(peer_id, from_id, event, args, "helper")
        if key == "alladmin":
            return cmd_role_global(peer_id, from_id, event, args, "admin")
        if key == "allmoder":
            return cmd_role_global(peer_id, from_id, event, args, "moder")
        if key == "allhelper":
            return cmd_role_global(peer_id, from_id, event, args, "helper")
        if key == "removerole":
            return cmd_remove_role_local(peer_id, from_id, event, args)
        if key == "allremoverole":
            return cmd_remove_role_global(peer_id, from_id, event, args)
        if key == "backup":
            return cmd_backup(peer_id, from_id, event, args)
        if key == "clear":
            return cmd_clear(peer_id, from_id, event, args)
        if key == "exportlogs":
            return cmd_export_logs(peer_id, from_id, event, args)
    except Exception as e:
        logger.exception("handle_command exception: %s", e)
        safe_send(peer_id, "❌ Ошибка при выполнении команды.")

# ----------------- Обработка входящих сообщений -----------------
def process_new_message(event):
    try:
        msg = getattr(event, "message", None) or (event.obj.get("message") if hasattr(event, "obj") and isinstance(event.obj, dict) else None)
        if not msg:
            return
        peer_id = msg.get("peer_id") if isinstance(msg, dict) else getattr(msg, "peer_id", None)
        from_id = msg.get("from_id") if isinstance(msg, dict) else getattr(msg, "from_id", None)
        if peer_id and peer_id >= 2000000000:
            add_chat(peer_id)
        action = msg.get("action") if isinstance(msg, dict) else getattr(msg, "action", None)
        if action:
            handle_invite_action(event)
        if handle_blacklist_on_message(event):
            return
        try:
            mutes = get_mutes_db(from_id) or []
            now = datetime.datetime.now()
            for m in mutes:
                if len(m) < 6:
                    continue
                try:
                    until = datetime.datetime.strptime(m[3], "%Y-%m-%d %H:%M:%S")
                except Exception:
                    continue
                m_peer = m[5]
                if until > now and (m_peer == 0 or m_peer == peer_id):
                    conv_id = msg.get("conversation_message_id") if isinstance(msg, dict) else getattr(msg, "conversation_message_id", None)
                    mid = msg.get("id") if isinstance(msg, dict) else getattr(msg, "id", None)
                    try:
                        if conv_id:
                            vk.messages.delete(conversation_message_ids=[conv_id], peer_id=peer_id, delete_for_all=1)
                        elif mid:
                            vk.messages.delete(message_ids=[mid], delete_for_all=1)
                    except Exception:
                        pass
                    return
        except Exception:
            pass
    except Exception as e:
        logger.exception("process_new_message error: %s", e)

# ----------------- Главный цикл -----------------
def main():
    logger.info("Бот запущен...")
    try:
        if OWNER_ID:
            safe_send(OWNER_ID, "✅ Бот запущен и слушает события.")
    except Exception:
        pass

    for event in longpoll.listen():
        try:
            if event.type == VkBotEventType.MESSAGE_NEW:
                process_new_message(event)
                msg = getattr(event, "message", None) or (event.obj.get("message") if hasattr(event, "obj") and isinstance(event.obj, dict) else None)
                if not msg:
                    continue
                text = (msg.get("text") if isinstance(msg, dict) else getattr(msg, "text", "")) or ""
                text = text.strip()
                if not text:
                    continue
                parts = text.split()
                cmd = parts[0].lower()
                args = parts[1:]
                if cmd.startswith("!") or cmd.startswith("/"):
                    handle_command(event, cmd, args)
                else:
                    lw = text.lower()
                    if lw in ("привет","hi","hello"):
                        safe_send(msg.get("peer_id"), "Привет!")
                    elif lw in ("пока","bye"):
                        safe_send(msg.get("peer_id"), "До встречи 👋")
        except Exception as e:
            logger.exception("Main loop error: %s", e)
            time.sleep(1)

if __name__ == "__main__":
    main()
