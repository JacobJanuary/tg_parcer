import asyncio
import os
from dotenv import load_dotenv
import logging
logging.basicConfig(level=logging.INFO)

load_dotenv()

from venue_enricher import VenueEnricher

async def deep_dive():
    enricher = VenueEnricher()
    targets = ["Kefir Koh Phangan", "Kefir", "Dobropar Koh Phangan"]
    
    print("🚀 Bypassing database to directly ping Gemini with Google Search enabled")
    for t in targets:
        print(f"\n{'='*50}")
        print(f"🎯 Pinging Gemini for: {t}")
        prompt = f"""Найди координаты площадки на острове Панган (или соседних Самуи/Ко Тао).
Площадка: "{t}"

Ответь СТРОГО и ТОЛЬКО в формате JSON, без маркдауна, без пояснений.
Обязателен ключ "found" (boolean). Если место не найдено, верни {{"found": false}}.
"""
        
        try:
            # Manually invoke the exact underlying API network call
            result = await enricher._call_gemini(enricher.model, prompt)
            print("🟢 RAW GEMINI RETURNED EVALUATED DICTIONARY:")
            import json
            print(json.dumps(result, indent=2, ensure_ascii=False))
        except Exception as e:
            print(f"🔴 GEMINI FAILED HORRIBLY WITH EXCEPTION: {type(e).__name__} - {e}")

if __name__ == '__main__':
    asyncio.run(deep_dive())
