"""
Конфигурация проекта — загрузка API credentials из .env файла.
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

# Telegram API
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE = os.getenv("PHONE")

# Gemini AI
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_PROXY = os.getenv("GEMINI_PROXY")
USE_PROXY = os.getenv("USE_PROXY", "true").lower() in ("true", "1", "yes")

# Google Maps (Fallback)
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# Spider Bot
BOT_TOKEN = os.getenv("BOT_TOKEN")
SPIDER_CHANNEL_ID = int(os.getenv("SPIDER_CHANNEL_ID", "0")) or None
TG_FOLDER_NAME = os.getenv("TG_FOLDER_NAME", "EventParser")
AUTO_JOIN_CHECK_INTERVAL = int(os.getenv("AUTO_JOIN_CHECK_INTERVAL", "300"))

# PostgreSQL
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "tg_parser")
DB_USER = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")


def get_dsn() -> str:
    """Собрать PostgreSQL DSN из компонентов."""
    from urllib.parse import quote
    if DB_USER:
        auth = f"{quote(DB_USER, safe='')}:{quote(DB_PASSWORD, safe='')}@"
    else:
        auth = ""
    return f"postgresql://{auth}{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Настройки по умолчанию
SESSION_NAME = "tg_parser_session"
DEFAULT_OUTPUT_DIR = "output"
DEFAULT_MEDIA_DIR = "media"
DEFAULT_LIMIT = None  # None = все сообщения
EVENTS_FILE = "events.jsonl"


def validate():
    """Проверка обязательных параметров Telegram."""
    missing = []
    if not API_ID:
        missing.append("API_ID")
    if not API_HASH:
        missing.append("API_HASH")
    if not PHONE:
        missing.append("PHONE")

    if missing:
        print(f"❌ Отсутствуют обязательные переменные в .env: {', '.join(missing)}")
        print("   Скопируйте .env.example в .env и заполните значения.")
        sys.exit(1)

    return int(API_ID), API_HASH, PHONE


def validate_gemini():
    """Проверка параметров Gemini API."""
    if not GEMINI_API_KEY:
        print("❌ Отсутствует GEMINI_API_KEY в .env")
        sys.exit(1)

    proxy = GEMINI_PROXY if USE_PROXY else None
    return GEMINI_API_KEY, proxy

