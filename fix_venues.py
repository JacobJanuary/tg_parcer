import asyncio
from db import Database

async def fix():
    db = Database()
    await db.connect()
    
    # Merge sati yoga
    print("Merging sati yoga...")
    await db.pool.execute("UPDATE events SET venue_id = 44 WHERE venue_id = 15")
    await db.pool.execute("DELETE FROM venues WHERE id = 15")
    
    # Merge planktone
    print("Merging planktone...")
    await db.pool.execute("UPDATE events SET venue_id = 31 WHERE venue_id = 33")
    await db.pool.execute("DELETE FROM venues WHERE id = 33")
    
    print("Done merging venues!")
    await db.close()

if __name__ == '__main__':
    asyncio.run(fix())
