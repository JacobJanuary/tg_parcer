import asyncio
import logging
from difflib import SequenceMatcher
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import Database

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

async def run_event_merge():
    db = Database()
    await db.connect()
    logger.info("Starting Event Deduplication Routine...")
    
    # 1. Exact Name & Time Duplicates
    exact_query = '''
        SELECT venue_id, event_time, title->>'en' as title_en, array_agg(id ORDER BY id ASC) as ids
        FROM events
        WHERE venue_id IS NOT NULL AND event_time IS NOT NULL AND title->>'en' IS NOT NULL
        GROUP BY venue_id, event_time, title->>'en'
        HAVING COUNT(*) > 1
    '''
    exact_groups = await db.pool.fetch(exact_query)
    
    merged = 0
    # Process Exact
    for group in exact_groups:
        ids = group['ids']
        # Keep the first (oldest or preferred source), delete the rest
        primary_id = ids[0]
        duplicates = ids[1:]
        
        for dup in duplicates:
            logger.info(f"🗑️ Exact Dupe (ID {dup}) -> Merging into (ID {primary_id})")
            await db.pool.execute("DELETE FROM events WHERE id = $1", dup)
            merged += 1
            
    # 2. Fuzzy Title Duplicates (Same Day & Venue)
    fuzzy_query = '''
        SELECT venue_id, event_time, array_agg(id ORDER BY id ASC) as ids, array_agg(title->>'en' ORDER BY id ASC) as titles
        FROM events
        WHERE venue_id IS NOT NULL AND event_time IS NOT NULL AND title->>'en' IS NOT NULL
        GROUP BY venue_id, event_time
        HAVING COUNT(*) > 1
    '''
    fuzzy_groups = await db.pool.fetch(fuzzy_query)
    
    for group in fuzzy_groups:
        ids = group['ids']
        titles = group['titles']
        
        # Compare pairs
        for i in range(len(ids)):
            merger_happened = False
            for j in range(i + 1, len(ids)):
                id1, t1 = ids[i], titles[i]
                id2, t2 = ids[j], titles[j]
                
                # If titles are 70%+ similar, consider them duplicates of the same event
                # (e.g. "Muay Thai Boxing Fight Night" vs "Muay Thai Fight Night")
                if similarity(t1, t2) > 0.65 or t1.lower() in t2.lower() or t2.lower() in t1.lower():
                    logger.info(f"🧩 Fuzzy Dupe: '{t1}' (ID {id1}) <-> '{t2}' (ID {id2})")
                    logger.info(f"   Action: Deleting {id2}, keeping {id1}")
                    
                    try:
                        await db.pool.execute("DELETE FROM events WHERE id = $1", id2)
                        merged += 1
                        merger_happened = True
                        break
                    except Exception as e:
                        logger.error(f"Failed to delete event duplicate {id2}: {e}")
            if merger_happened:
                break
                
    logger.info(f"Event Deduplication Complete. Removed {merged} duplicates.")
    await db.close()

if __name__ == "__main__":
    asyncio.run(run_event_merge())
