import asyncio
from db import Database
from image_generator import EventImageGenerator
import logging

logging.basicConfig(level=logging.INFO)

async def main():
    print("🔌 Подключение к БД...")
    db = Database()
    await db.connect()
    
    gen = EventImageGenerator(db=db)
    
    # Ищем все ивенты, у которых нет картинки
    rows = await db.pool.fetch("""
        SELECT id, original_text, category, title 
        FROM events 
        WHERE image_path IS NULL OR image_path = ''
    """)
    
    if not rows:
        print("✅ Все ивенты в базе уже имеют картинки!")
        return
        
    print(f"🔍 Найдено {len(rows)} ивентов без обложек. Запускаем умную генерацию...\n")
    
    # Создаем задачи для генератора. Семафор внутри EventImageGenerator
    # сам выстроит их в правильную очередь по 2 штуки.
    tasks = []
    for r in rows:
        ev_id = r['id']
        text = r['original_text']
        cat = r['category'] or "Party"
        title = r['title']
        
        print(f"⏳ В очередь: [ID {ev_id}] {title[:30]}...")
        task = asyncio.create_task(gen.generate_cover(text, cat, ev_id))
        tasks.append(task)
        
    print(f"\n🎨 Ожидание завершения генерации {len(tasks)} обложек. Это займет некоторое время...")
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    success = sum(1 for r in results if isinstance(r, str))
    errors = len(tasks) - success
    
    print(f"\n✅ Готово! Успешно нарисовано: {success}. Ошибок: {errors}.")

if __name__ == "__main__":
    asyncio.run(main())
