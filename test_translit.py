import asyncio
import os
from dotenv import load_dotenv
import logging
logging.basicConfig(level=logging.INFO)

load_dotenv()

from venue_enricher import VenueEnricher

async def deep_dive():
    enricher = VenueEnricher()
    # Bypass cache manually by emptying it temporarily
    enricher.cache._data = {}
    targets = ["Кефир", "ДОБРОПАР ПАНГАН"]
    
    print("🚀 Testing the Full Senior Transliteration Engine:")
    for t in targets:
        print(f"\n{'='*50}")
        print(f"🎯 Pinging VenueEnricher.enrich() for: {t}")
        
        try:
            # Manually invoke the full pipeline (bypassing DB cache by force)
            enricher.cache.aget = lambda x: _async_none() # Mock DB miss
            result = await enricher.enrich(t)
            print("🟢 RAW GEMINI RETURNED EVALUATED DICTIONARY:")
            import json
            print(json.dumps(result, indent=2, ensure_ascii=False))
        except Exception as e:
            print(f"🔴 GEMINI FAILED HORRIBLY WITH EXCEPTION: {type(e).__name__} - {e}")

async def _async_none():
    return None

if __name__ == '__main__':
    asyncio.run(deep_dive())
