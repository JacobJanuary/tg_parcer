import asyncio
import asyncpg
import config

async def main():
    dsn = config.get_dsn()
    pool = await asyncpg.create_pool(dsn)
    
    unmapped_events = await pool.fetch("""
        SELECT id, title, location_name, category, summary 
        FROM events 
        WHERE venue_id IS NULL
        ORDER BY id DESC
    """)
    
    print(f"\n=== АНАЛИЗ {len(unmapped_events)} ИВЕНТОВ БЕЗ ЛОКАЦИИ ===")
    
    null_location_names = sum(1 for e in unmapped_events if not e['location_name'])
    has_location_names = len(unmapped_events) - null_location_names
    
    print(f"Ивентов, где 'location_name' пустое (AI не нашел имя локации): {null_location_names}")
    print(f"Ивентов, где 'location_name' есть, но venue_id не привязан: {has_location_names}\n")
    
    print("📋 Примеры ивентов С 'location_name', но БЕЗ успешного поиска координат:")
    for e in unmapped_events:
        if e['location_name']:
            print(f"- ID: {e['id']} | Локация: '{e['location_name']}' | Название: {e['title'][:40]} | Категория: {e['category']}")
            
    print("\n📋 Примеры ивентов БЕЗ 'location_name' (выборка):")
    count = 0
    for e in unmapped_events:
        if not e['location_name']:
            print(f"- ID: {e['id']} | Название: {e['title'][:50]} | Категория: {e['category']}")
            count += 1
            if count >= 10:
                print("  ... и так далее.")
                break
                
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
