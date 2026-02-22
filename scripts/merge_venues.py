import asyncio
import logging
import re
from difflib import SequenceMatcher
import sys
import os

# Add parent directory to path so absolute imports work
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import Database

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()

def simplify_name(name: str) -> str:
    # 1. Lowercase
    name = name.lower()
    # 2. Replaces common equivalents
    name = name.replace("&", "and")
    # 3. Remove fluff words that often differ
    fluff = ["center", "resort", "club", "music", "viewpoint", "arena", "phangan", "koh phangan", "ko phangan", "thailand", "retreat", "lounge"]
    for word in fluff:
        name = re.sub(rf'\b{word}\b', '', name)
    # 4. Remove non-alphanumeric completely
    name = re.sub(r'[^a-z0-9]', '', name)
    return name

async def run_merge():
    db = Database()
    await db.connect()
    
    # We find all venues that share exact coordinates.
    # We group them by lat, lng.
    groups_query = '''
        SELECT lat, lng, array_agg(id ORDER BY id ASC) as ids, array_agg(name ORDER BY id ASC) as names 
        FROM venues 
        WHERE lat IS NOT NULL AND lng IS NOT NULL 
        GROUP BY lat, lng 
        HAVING COUNT(*) > 1
    '''
    groups = await db.pool.fetch(groups_query)
    
    merged_count = 0
    
    for group in groups:
        ids = group['ids']
        names = group['names']
        
        # Compare pairs inside the group
        for i in range(len(ids)):
            merger_happened = False
            for j in range(i + 1, len(ids)):
                id1, name1 = ids[i], names[i]
                id2, name2 = ids[j], names[j]
                
                # Check logic
                simp1 = simplify_name(name1)
                simp2 = simplify_name(name2)
                
                is_dupe = False
                if simp1 == simp2 and len(simp1) > 3:
                     is_dupe = True
                elif (simp1 in simp2 or simp2 in simp1) and min(len(simp1), len(simp2)) > 5:
                    is_dupe = True
                elif similarity(simp1, simp2) > 0.85:
                    is_dupe = True
                
                if is_dupe:
                    logger.info(f"📍 Duplicate Detected: ID {id1} ('{name1}') <-> ID {id2} ('{name2}')")
                    logger.info(f"   Action: Merging {id2} into {id1} (Primary)")
                    
                    try:
                        async with db.pool.acquire() as conn:
                            async with conn.transaction():
                                # 1. Transfer events
                                await conn.execute("UPDATE events SET venue_id = $1 WHERE venue_id = $2", id1, id2)
                                # 2. Transfer aliases
                                await conn.execute("UPDATE venue_aliases SET venue_id = $1 WHERE venue_id = $2", id1, id2)
                                # 3. Delete secondary venue
                                await conn.execute("DELETE FROM venues WHERE id = $1", id2)
                                
                        merged_count += 1
                        logger.info("   ✅ Successfully merged.")
                        merger_happened = True
                        break
                    except Exception as e:
                        logger.error(f"   Failed to merge: {e}")
            if merger_happened:
                break
            
    logger.info(f"Routine complete. Merged {merged_count} duplicates.")
    await db.close()

if __name__ == "__main__":
    asyncio.run(run_merge())
