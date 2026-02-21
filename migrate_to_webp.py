#!/usr/bin/env python3
"""
Скрипт миграции существующих картинок в WebP.
1. Находит все ивенты, у которых image_path заканчивается на .jpg
2. Открывает файл, ресайзит до 600px ширины (Lanczos)
3. Сохраняет как .webp
4. Обновляет запись в БД
5. Удаляет старый .jpg файл
"""

import asyncio
import os
import logging
from io import BytesIO
from PIL import Image
import config
from db import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

async def migrate_images():
    db = Database(config.get_dsn())
    await db.connect()
    logger.info("🐘 Подключились к БД")

    # Получаем все ивенты с .jpg картинками
    rows = await db.pool.fetch(
        "SELECT id, image_path FROM events WHERE image_path LIKE '%.jpg' OR image_path LIKE '%.jpeg'"
    )
    logger.info(f"🔍 Найдено {len(rows)} ивентов для миграции")

    media_dir = config.DEFAULT_MEDIA_DIR
    success_count = 0
    error_count = 0

    target_width = 600

    for row in rows:
        event_id = row["id"]
        old_filename = row["image_path"]
        old_filepath = os.path.join(media_dir, old_filename)

        if not os.path.exists(old_filepath):
            logger.warning(f"⚠️ Файл не найден: {old_filepath}")
            error_count += 1
            continue

        try:
            # Открываем оригинал
            image = Image.open(old_filepath)
            if image.mode != "RGB":
                image = image.convert("RGB")

            # Ресайз
            w, h = image.size
            if w > target_width:
                target_height = int(h * (target_width / w))
                resample_filter = getattr(Image, "Resampling", Image).LANCZOS
                image = image.resize((target_width, target_height), resample_filter)

            # Формируем новое имя файла (просто меняем расширение)
            # Например event_party_a1b2c3d4.jpg -> event_party_a1b2c3d4.webp
            fname_without_ext = os.path.splitext(old_filename)[0]
            new_filename = f"{fname_without_ext}.webp"
            new_filepath = os.path.join(media_dir, new_filename)

            # Сохраняем WebP
            image.save(new_filepath, "WEBP", quality=85, method=6)

            # Обновляем БД
            await db.pool.execute(
                "UPDATE events SET image_path = $1 WHERE id = $2",
                new_filename, event_id
            )

            # Удаляем старый файл
            os.remove(old_filepath)

            logger.info(f"✅ Мигрирован [{event_id}]: {old_filename} -> {new_filename}")
            success_count += 1

        except Exception as e:
            logger.error(f"❌ Ошибка с файлом {old_filename}: {e}")
            error_count += 1

    await db.close()
    logger.info(f"\n🎉 МИГРАЦИЯ ЗАВЕРШЕНА!")
    logger.info(f"   Успешно: {success_count}")
    logger.info(f"   Ошибок:  {error_count}")

if __name__ == "__main__":
    asyncio.run(migrate_images())
