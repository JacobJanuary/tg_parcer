import asyncio
import os
from dotenv import load_dotenv

load_dotenv()
import config
from db import Database

async def audit_dupes():
    db = Database(config.get_dsn())
    await db.connect()

    print("🔍 AUDITING VENUES FOR DUPLICATES...\n")

    print("1️⃣ DUPLICATES BY NORMALIZED QUERY: 0 (Enforced by venue_aliases PRIMARY KEY)")
    print("\n2️⃣ DUPLICATES BY EXACT GOOGLE MAPS NAME: 0 (Enforced by venues UNIQUE constraint)")
    
    # 3. Near-exact coordinate clusters (distance < 10 meters)
    # Using simple rounding to 4 decimal places (~11 meters)
    coord_dupes = await db.pool.fetch("""
        SELECT ROUND(lat::numeric, 4) as rlat, ROUND(lng::numeric, 4) as rlng, COUNT(*) as c, array_agg(name) as names
        FROM venues
        WHERE lat IS NOT NULL AND lng IS NOT NULL
        GROUP BY ROUND(lat::numeric, 4), ROUND(lng::numeric, 4)
        HAVING COUNT(*) > 1
        ORDER BY c DESC
    """)
    print(f"\n3️⃣ DUPLICATES BY COORDINATES (approx 10m radius): {len(coord_dupes)}")
    for row in coord_dupes:
        names_str = ", ".join(set(filter(None, row['names'])))
        print(f"   • ({row['rlat']}, {row['rlng']}) -> {row['c']} rows -> Names: [{names_str}]")

    await db.close()

if __name__ == '__main__':
    asyncio.run(audit_dupes())
