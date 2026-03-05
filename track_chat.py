#!/usr/bin/env python3
import asyncio
import sys
from telethon import TelegramClient
from telethon.tl.types import Channel, Chat

sys.path.insert(0, '/home/elcrypto/TG_parcer')
import config, db

async def process_chat(client, pg, user_input):
    chat_identifier = user_input.strip()
    if not chat_identifier:
        return
        
    if "t.me/" in chat_identifier:
        chat_identifier = chat_identifier.split("t.me/")[-1].strip("/")
        if chat_identifier.startswith("+") or chat_identifier.startswith("joinchat/"):
            print("❌ Ошибка: Скрипт пока не умеет сам вступать в закрытые чаты по инвайт-ссылкам.")
            print("   Вступите в чат с телефона, а затем добавьте его сюда по названию или ID.\n")
            return
    
    if chat_identifier.startswith("@"):
        chat_identifier = chat_identifier[1:]

    print(f"🔍 Ищем: {user_input} ...")
    try:
        # Пробуем найти через Telethon
        if chat_identifier.lstrip("-").isdigit():
            entity = await client.get_entity(int(chat_identifier))
        else:
            entity = await client.get_entity(chat_identifier)
        
        title = getattr(entity, "title", str(entity.id))
        is_channel = getattr(entity, "broadcast", False)
        is_megagroup = getattr(entity, "megagroup", False)
        
        if is_channel:
            chat_type = "channel"
        elif is_megagroup:
            chat_type = "megagroup"
        else:
            chat_type = "group"
            
        print(f"✅ Найден: {title} (ID: {entity.id} / Type: {chat_type})")
        
        # Заносим в базу активных чатов
        await pg.upsert_chat(entity.id, title, chat_type, is_active=True)
        
        # Помечаем в пауке как self, чтобы он не спамил алертами
        await pg.upsert_discovered(
            chat_id=entity.id,
            username=getattr(entity, "username", chat_identifier),
            title=title,
            chat_type=chat_type,
            status="self",
            resolved=True
        )
        print("💾 Успешно добавлено в базу данных PostgreSQL!\n")

    except ValueError:
        print(f"❌ Чат '{user_input}' не найден. Возможно, нужно сначала вступить в него с телефона?\n")
    except Exception as e:
        print(f"❌ Ошибка при поиске: {e}\n")

async def main():
    print("=" * 60)
    print(" 📡 ИНТЕРАКТИВНОЕ ДОБАВЛЕНИЕ ЧАТОВ В ПРОСЛУШКУ")
    print(" Вставьте @username, t.me/ссылку или ID чата.")
    print(" Введите 'q', 'exit' или нажмите Ctrl+C для выхода.")
    print("=" * 60 + "\n")

    pg = db.Database(config.get_dsn())
    await pg.connect()
    api_id, api_hash, phone = config.validate()
    
    client = TelegramClient('temp_track_session', api_id, api_hash)
    await client.start(phone=phone)

    try:
        while True:
            # Получаем ввод (блокируем поток, но т.к консоль это нормально)
            user_input = await asyncio.to_thread(input, "📝 Введите чат (q: выход) > ")
            user_input = user_input.strip()
            
            if user_input.lower() in ('q', 'exit', 'quit'):
                break
                
            if not user_input:
                continue

            await process_chat(client, pg, user_input)

    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        print("\n🔌 Сохранение и выход... Не забудьте:")
        print("   sudo systemctl restart tg-listener")
        await client.disconnect()
        await pg.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
