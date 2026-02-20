#!/usr/bin/env python3
"""
Telegram Chat Parser — CLI интерфейс.

Примеры использования:
    python main.py --chat @durov --limit 100 --format json
    python main.py --chat https://t.me/some_channel --format both --media
    python main.py --chat -1001234567890 --limit 500 --format csv --media --media-types photo,document
"""

import argparse
import asyncio
import os
import sys
import re

from parser import TelegramParser
from exporter import export_json, export_csv
import config


def parse_args():
    """Парсинг аргументов командной строки."""
    p = argparse.ArgumentParser(
        description="🔍 Telegram Chat Parser — парсер чатов Telegram",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  %(prog)s --chat @durov --limit 100
  %(prog)s --chat https://t.me/some_channel --format both --media
  %(prog)s --chat -1001234567890 --format csv --media --media-types photo,document
        """,
    )

    p.add_argument(
        "--chat",
        required=True,
        help="Username (@channel), ссылка (https://t.me/...) или ID чата",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Максимальное кол-во сообщений (по умолчанию: все)",
    )
    p.add_argument(
        "--format",
        choices=["json", "csv", "both"],
        default="json",
        help="Формат экспорта (по умолчанию: json)",
    )
    p.add_argument(
        "--media",
        action="store_true",
        help="Скачивать медиафайлы (фото, видео, документы)",
    )
    p.add_argument(
        "--media-types",
        default="photo,video,document",
        help="Типы медиа через запятую (по умолчанию: photo,video,document)",
    )
    p.add_argument(
        "--output-dir",
        default=config.DEFAULT_OUTPUT_DIR,
        help=f"Директория для сохранения (по умолчанию: {config.DEFAULT_OUTPUT_DIR}/)",
    )

    return p.parse_args()


def sanitize_dirname(name: str) -> str:
    """Очистка имени для использования как директория."""
    # Удаляем символы, недопустимые в именах директорий
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = name.strip('. ')
    return name or "unknown_chat"


async def main():
    """Основная логика."""
    args = parse_args()

    print("=" * 50)
    print("🔍 Telegram Chat Parser")
    print("=" * 50)

    # Инициализация и подключение
    parser = TelegramParser()
    await parser.connect()

    try:
        # Резолвим чат
        print(f"\n🔎 Ищу чат: {args.chat}")
        entity = await parser.resolve_chat(args.chat)

        if entity is None:
            print("❌ Чат не найден. Проверьте правильность ссылки/username/ID.")
            return

        # Инфо о чате
        chat_info = await parser.get_chat_info(entity)
        chat_name = chat_info["title"] or chat_info["username"] or str(chat_info["id"])

        print(f"\n📌 Чат: {chat_name}")
        print(f"   Тип: {chat_info['type']}")
        if chat_info["participants_count"]:
            print(f"   Участников: {chat_info['participants_count']:,}")
        if chat_info["username"]:
            print(f"   Username: @{chat_info['username']}")

        # Подготовка директории
        safe_name = sanitize_dirname(chat_name)
        output_dir = os.path.join(args.output_dir, safe_name)
        os.makedirs(output_dir, exist_ok=True)

        # Парсинг медиа-типов
        media_types = [t.strip() for t in args.media_types.split(",")]

        # Парсинг сообщений
        messages = await parser.parse_messages(
            entity=entity,
            limit=args.limit,
            download_media=args.media,
            media_types=media_types,
            output_dir=output_dir,
        )

        if not messages:
            print("\n⚠️ Сообщения не найдены.")
            return

        # Экспорт
        print(f"\n💾 Экспорт данных...")

        if args.format in ("json", "both"):
            json_path = os.path.join(output_dir, "messages.json")
            export_json(messages, json_path)

        if args.format in ("csv", "both"):
            csv_path = os.path.join(output_dir, "messages.csv")
            export_csv(messages, csv_path)

        # Итоговая статистика
        print(f"\n{'=' * 50}")
        print(f"📊 Результаты:")
        print(f"   Чат: {chat_name}")
        print(f"   Сообщений: {len(messages)}")

        media_count = sum(1 for m in messages if m.get("media_type"))
        if media_count:
            print(f"   С медиа: {media_count}")
            if args.media:
                downloaded = sum(1 for m in messages if m.get("media_file"))
                print(f"   Скачано файлов: {downloaded}")

        print(f"   Сохранено в: {os.path.abspath(output_dir)}/")
        print(f"{'=' * 50}")

    finally:
        await parser.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
