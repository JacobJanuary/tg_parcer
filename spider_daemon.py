#!/usr/bin/env python3
"""
Spider Daemon — фоновый процесс для обработки Telegram-кнопок 24/7.
Слушает события "✅ Подписался" и "❌ Отклонить" от spider_notify.py.
"""

import asyncio
import logging
import signal
import sys
import os

from telethon import TelegramClient

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import config
from db import Database
from spider_bot import _register_callbacks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("SpiderDaemon")

async def main():
    logger.info("Initializing Spider Daemon...")

    if not config.BOT_TOKEN or not config.SPIDER_CHANNEL_ID:
        logger.error("BOT_TOKEN or SPIDER_CHANNEL_ID not set. Exiting.")
        sys.exit(1)

    # Database
    db = Database(config.get_dsn())
    try:
        await db.connect()
        logger.info("✅ PostgreSQL connected")
    except Exception as e:
        logger.error(f"❌ PostgreSQL connection failed: {e}")
        sys.exit(1)

    # Telegram Client
    bot_client = TelegramClient(
        "spider_bot_session",
        int(config.API_ID),
        config.API_HASH,
    )
    
    try:
        await bot_client.start(bot_token=config.BOT_TOKEN)
        
        # Регистрация обработчиков кнопок
        _register_callbacks(bot_client, db)
        
        me = await bot_client.get_me()
        logger.info(f"🦇 Spider Daemon running as @{me.username}")
        logger.info("Listening for button clicks... (Press Ctrl+C to stop)")
        
        # Изящное завершение работы
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.ensure_future(bot_client.disconnect()))

        # Блокирующий цикл
        await bot_client.run_until_disconnected()

    except Exception as e:
        logger.error(f"Telegram Client error: {e}")
    finally:
        await db.close()
        logger.info("🔌 Spider Daemon stopped.")

if __name__ == "__main__":
    asyncio.run(main())
