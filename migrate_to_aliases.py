import asyncio
import os
from dotenv import load_dotenv

load_dotenv()
import config
from db import Database

async def migrate_db():
    db = Database(config.get_dsn())
    await db.connect()

    print("🚀 НАЧАЛО МИГРАЦИИ БАЗЫ ДАННЫХ К 3NF АРХИТЕКТУРЕ (ALIASES)...\n")

    async with db.pool.acquire() as conn:
        async with conn.transaction():
            # 1. Создание таблицы venue_aliases
            print("1️⃣ Создание таблицы venue_aliases...")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS venue_aliases (
                    query VARCHAR(255) PRIMARY KEY,
                    venue_id INTEGER REFERENCES venues(id) ON DELETE CASCADE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc', now())
                )
            """)

            # 2. Клонирование существующих queries в алиасы
            print("2️⃣ Миграция данных из venues.query -> venue_aliases...")
            await conn.execute("""
                INSERT INTO venue_aliases (query, venue_id)
                SELECT query, CASE WHEN found THEN id ELSE NULL END
                FROM venues
                ON CONFLICT (query) DO NOTHING
            """)

            # 3. Дедупликация идентичных площадок по name
            print("3️⃣ Дедупликация venues по name и ре-линковка связей в events...")
            dupes = await conn.fetch("""
                SELECT name, array_agg(id) as ids 
                FROM venues 
                WHERE name IS NOT NULL AND name != '' 
                GROUP BY name 
                HAVING COUNT(*) > 1
            """)
            
            for row in dupes:
                name = row['name']
                ids = row['ids']
                keep_id = min(ids)
                drop_ids = [i for i in ids if i != keep_id]
                
                print(f"   • Схлопываем {len(drop_ids)} дублей для '{name}'. Оставляем ID {keep_id}")
                
                # Обновляем ссылки в venue_aliases
                await conn.execute("""
                    UPDATE venue_aliases SET venue_id = $1 WHERE venue_id = ANY($2)
                """, keep_id, drop_ids)
                
                # Обновляем ссылки в events
                await conn.execute("""
                    UPDATE events SET venue_id = $1 WHERE venue_id = ANY($2)
                """, keep_id, drop_ids)
                
                # Удаляем сами дубли из venues
                await conn.execute("""
                    DELETE FROM venues WHERE id = ANY($1)
                """, drop_ids)

            # 4. Удаление venues WHERE found = false (теперь они означают venue_id = NULL в aliases)
            print("4️⃣ Удаление мусорных failed venues...")
            await conn.execute("DELETE FROM venues WHERE found = false")

            # 5. Дропаем устаревшие колонки из venues
            print("5️⃣ Удаление колонок query и found из venues...")
            await conn.execute("ALTER TABLE venues DROP COLUMN IF EXISTS query")
            await conn.execute("ALTER TABLE venues DROP COLUMN IF EXISTS found")

            # 6. Устанавливаем UNIQUE индекс на name (для будущих upserts)
            print("6️⃣ Установка UNIQUE индекса на venues.name...")
            await conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_venue_name 
                ON venues(name) WHERE name IS NOT NULL AND name != ''
            """)

    print("\n✅ МИГРАЦИЯ УСПЕШНО ЗАВЕРШЕНА!")

    await db.close()

if __name__ == '__main__':
    asyncio.run(migrate_db())
