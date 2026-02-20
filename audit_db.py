import asyncio
import asyncpg
import config

async def main():
    dsn = config.get_dsn()
    pool = await asyncpg.create_pool(dsn)
    
    total_events = await pool.fetchval("SELECT COUNT(*) FROM events")
    missing_images = await pool.fetchval("SELECT COUNT(*) FROM events WHERE image_path IS NULL OR image_path = ''")
    
    dups = await pool.fetch("""
        SELECT fingerprint, count(*) as count 
        FROM events 
        GROUP BY fingerprint 
        HAVING COUNT(*) > 1
    """)
    duplicate_count = sum(d['count'] - 1 for d in dups)
    
    total_venues = await pool.fetchval("SELECT COUNT(*) FROM venues")
    parsed_venues = await pool.fetchval("SELECT COUNT(*) FROM venues WHERE lat IS NOT NULL AND lng IS NOT NULL")
    linked_events = await pool.fetchval("SELECT COUNT(*) FROM events WHERE venue_id IS NOT NULL")
    
    print(f"\n--- 📊 ОТЧЁТ ПО БАЗЕ ДАННЫХ ---")
    print(f"Всего уникальных ивентов в БД: {total_events}")
    
    if missing_images == 0:
        print(f"✅ Картинки созданы для ВСЕХ {total_events} ивентов без исключения.")
    else:
        print(f"⚠️ Ивентов без картинок: {missing_images} (из {total_events})")
        
    if duplicate_count == 0:
        print(f"✅ Дубликатов в базе нет. Fingerprints уникальны.")
    else:
        print(f"❌ НАЙДЕНЫ ДУБЛИКАТЫ (по fingerprint): {duplicate_count}")
        
    print(f"\nВсего уникальных заведений (venues): {total_venues}")
    print(f"Из них успешно распарсены Gemini (имеют координаты): {parsed_venues} (из {total_venues})")
    
    if linked_events == total_events:
        print(f"✅ Для всех {total_events} ивентов привязана гео-локация.")
    else:
        print(f"📎 Ивентов с успешно привязанными гео-локациями: {linked_events} (из {total_events})")
    print(f"--------------------------------\n")
    
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
