#!/usr/bin/env python3
"""
Сбор тестовых сообщений из выбранных чатов для отладки фильтров.

Парсит последние N сообщений из каждого чата в selected_chats.json
и сохраняет в samples/raw_messages.jsonl.

Затем прогоняет через filters.py и показывает статистику.

Использование:
    python sample_messages.py                # 50 сообщений с каждого чата
    python sample_messages.py --limit 100    # 100 сообщений
    python sample_messages.py --test-ai      # также прогнать через Gemini (первые 5 прошедших)
"""

import argparse
import asyncio
import json
import os

from telethon import TelegramClient
from telethon.tl.types import User

import config
import filters


SAMPLES_DIR = "samples"


async def collect_samples(limit_per_chat: int = 50, test_ai: bool = False):
    """Сбор тестовых сообщений."""
    # Загрузка выбранных чатов
    chats_file = "selected_chats.json"
    if not os.path.exists(chats_file):
        print("❌ Файл selected_chats.json не найден.")
        print("   Запустите: python list_chats.py --select")
        return

    with open(chats_file, "r", encoding="utf-8") as f:
        chats = json.load(f)

    if not chats:
        print("❌ Пустой список чатов.")
        return

    print(f"📋 Чатов для сбора: {len(chats)}")
    print(f"📊 Лимит на чат: {limit_per_chat}")

    # Авторизация
    api_id, api_hash, phone = config.validate()
    client = TelegramClient(config.SESSION_NAME, api_id, api_hash)
    await client.start(phone=phone)

    me = await client.get_me()
    print(f"✅ Авторизован: {me.first_name}\n")

    # Сбор
    os.makedirs(SAMPLES_DIR, exist_ok=True)
    raw_path = os.path.join(SAMPLES_DIR, "raw_messages.jsonl")
    all_messages = []

    with open(raw_path, "w", encoding="utf-8") as f:
        for chat_info in chats:
            chat_id = chat_info["id"]
            chat_title = chat_info["title"]
            print(f"🔄 {chat_title} (ID: {chat_id})...")

            try:
                entity = await client.get_entity(chat_id)
            except Exception as e:
                print(f"   ⚠️ Не удалось получить: {e}")
                continue

            count = 0
            async for message in client.iter_messages(entity, limit=limit_per_chat):
                # Информация об отправителе
                sender_name = ""
                if message.sender:
                    if isinstance(message.sender, User):
                        parts = [message.sender.first_name or "", message.sender.last_name or ""]
                        sender_name = " ".join(p for p in parts if p)
                    else:
                        sender_name = getattr(message.sender, "title", "")

                # Тип медиа
                media_type = None
                if message.photo:
                    media_type = "photo"
                elif message.video:
                    media_type = "video"
                elif message.document:
                    media_type = "document"

                msg_data = {
                    "chat_id": chat_id,
                    "chat_title": chat_title,
                    "message_id": message.id,
                    "date": message.date.isoformat() if message.date else None,
                    "sender_name": sender_name,
                    "text": message.text or "",
                    "media_type": media_type,
                }

                f.write(json.dumps(msg_data, ensure_ascii=False) + "\n")
                all_messages.append(msg_data)
                count += 1

            print(f"   ✓ собрано: {count}")

    await client.disconnect()
    print(f"\n💾 Всего сообщений: {len(all_messages)}")
    print(f"   Сохранено в: {raw_path}")

    # ─── Прогон через фильтры ───
    print(f"\n{'=' * 60}")
    print("🔍 Прогон через filters.py")
    print(f"{'=' * 60}")

    stats = filters.check_batch(all_messages)

    print(f"\n📊 Статистика фильтрации:")
    print(f"   Всего:    {stats['total']}")
    print(f"   Прошло:   {stats['passed']} ({stats['pass_rate']})")
    print(f"   Отсеяно:  {stats['dropped']}")

    # Показываем примеры прошедших
    if stats["passed_messages"]:
        print(f"\n✅ Примеры прошедших (первые 10):")
        for msg in stats["passed_messages"][:10]:
            text_preview = (msg["text"][:100] + "...") if len(msg["text"]) > 100 else msg["text"]
            text_preview = text_preview.replace("\n", " ↵ ")
            print(f"   [{msg['_filter']['reason']}]")
            print(f"   💬 {msg['chat_title']}: {text_preview}\n")

    # Сохраняем прошедшие
    passed_path = os.path.join(SAMPLES_DIR, "passed_messages.jsonl")
    with open(passed_path, "w", encoding="utf-8") as f:
        for msg in stats["passed_messages"]:
            f.write(json.dumps(msg, ensure_ascii=False, default=str) + "\n")
    print(f"💾 Прошедшие сохранены: {passed_path}")

    # Показываем причины отсева
    print(f"\n❌ Причины отсева:")
    reason_counts = {}
    for msg in stats["dropped_messages"]:
        reason_key = msg["_filter"]["reason"].split(":")[0]
        reason_counts[reason_key] = reason_counts.get(reason_key, 0) + 1
    for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
        print(f"   {reason}: {count}")

    # ─── Тест AI (опционально) ───
    if test_ai and stats["passed_messages"]:
        print(f"\n{'=' * 60}")
        print("🤖 Тест Gemini AI (первые 5 прошедших)")
        print(f"{'=' * 60}")

        from ai_analyzer import EventAnalyzer
        analyzer = EventAnalyzer()

        for msg in stats["passed_messages"][:5]:
            print(f"\n💬 [{msg['chat_title']}]: {msg['text'][:120]}...")
            result = await analyzer.analyze(msg["text"], msg["chat_title"])
            if result:
                print(f"   🤖 → {json.dumps(result, ensure_ascii=False)}")
            else:
                print(f"   🤖 → ошибка анализа")

        analyzer.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="📦 Сбор тестовых сообщений")
    p.add_argument("--limit", type=int, default=50, help="Сообщений на чат (по умолчанию: 50)")
    p.add_argument("--test-ai", action="store_true", help="Тестировать Gemini на прошедших")
    args = p.parse_args()

    asyncio.run(collect_samples(limit_per_chat=args.limit, test_ai=args.test_ai))
