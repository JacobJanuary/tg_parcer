#!/usr/bin/env python3
"""
Phantom Protocol — Factory for generating highly realistic AI users (Phantoms)
to solve the Cold Start Problem in the Event Discovery App.

Features:
- Total: 100 Phantoms
- Distribution: 50 Party, 30 Spiritual, 20 Business
- Gender mix: 60% Female, 40% Male
- AI avatars via Google Imagen 4 (1:1 ratio, photorealistic, no text)
- PostgreSQL persistence (asyncpg)
"""

import asyncio
import os
import random
import uuid
import logging
from io import BytesIO

from PIL import Image
from faker import Faker
from dotenv import load_dotenv

from google import genai
from google.genai import types

import sys
# Add parent directory to path to import config and db modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from db import Database

# ─── Configuration ───

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("PhantomFactory")

TARGET_PHANTOMS = 100
MOOD_DISTRIBUTION = {
    'party': 50,
    'spiritual': 30,
    'business': 20
}
# 60% Female, 40% Male
GENDER_WEIGHTS = ['female'] * 6 + ['male'] * 4 

fake = Faker(['en_US', 'ru_RU'])

MEDIA_DIR = os.path.join(config.DEFAULT_MEDIA_DIR, "avatars")
os.makedirs(MEDIA_DIR, exist_ok=True)


# ─── Prompt Engineering ───

def build_phantom_prompt(gender: str, mood: str) -> str:
    """Secret Sauce: Dynamic prompt generation for Imagen 4."""
    
    base = (
        f"A raw, candid, unedited smartphone selfie of a beautiful 25-year-old {gender}, "
        "slightly tanned skin, highly realistic amateur photography, visible skin texture, "
        "Instagram story style, Koh Phangan expat aesthetic, looking directly at camera. "
        "ABSOLUTELY NO TEXT, NO LETTERS, NO WORDS, NO LOGOS."
    )
    
    if mood == 'party':
        mood_details = (
            "dark jungle rave background, neon purple and red lighting, "
            "sweaty glowing skin, blurry dancing crowd, wearing a festival outfit, "
            "dark tropical night vibe."
        )
    elif mood == 'spiritual':
        mood_details = (
            "zen morning beach Koh Phangan background, wearing breathable organic linen, "
            "natural no-makeup look, warm golden hour sunlight, peaceful aura."
        )
    elif mood == 'business':
        mood_details = (
            "smart casual tropical wear, bright daytime lighting, sunglasses on head, "
            "blurred trendy coworking cafe background, laptop edge slightly visible."
        )
    else:
        mood_details = "bright tropical background."
        
    return f"{base} {mood_details}"


# ─── Avatar Generation ───

def _generate_avatar_sync(client: genai.Client, prompt: str) -> str | None:
    """Synchronous call to Google GenAI SDK (Imagen)."""
    
    models = ["imagen-3.0-generate-002", "imagen-3.0-fast-generate-001"] # Fallback models
    
    for model_name in models:
        try:
            result = client.models.generate_images(
                model=model_name,
                prompt=prompt,
                config=types.GenerateImagesConfig(
                    number_of_images=1,
                    aspect_ratio="1:1",
                    person_generation="ALLOW_ADULT",
                    output_mime_type="image/jpeg"
                )
            )
            
            for generated_image in result.generated_images:
                image_bytes = generated_image.image.image_bytes
                
                # Verify and process image
                image = Image.open(BytesIO(image_bytes))
                if image.mode != "RGB":
                    image = image.convert("RGB")
                
                filename = f"phantom_{uuid.uuid4().hex[:8]}.jpg"
                filepath = os.path.join(MEDIA_DIR, filename)
                
                # Save as optimized JPEG
                image.save(filepath, "JPEG", quality=90)
                return os.path.join("avatars", filename) # Return relative path
                
        except Exception as e:
            logger.debug(f"  [Model {model_name} failed] {e}")
            continue
            
    logger.error("  ❌ All model fallbacks failed for this prompt.")
    return None

async def generate_avatar(client: genai.Client, prompt: str) -> str | None:
    """Async wrapper for avatar generation."""
    return await asyncio.to_thread(_generate_avatar_sync, client, prompt)


# ─── Main Factory Logic ───

async def run_factory():
    logger.info("👻 Initializing Phantom Protocol...")
    
    # 1. DB Setup
    db = Database(config.get_dsn())
    try:
        await db.connect()
    except Exception as e:
        logger.error(f"❌ DB Connection failed: {e}")
        return

    # Verify users table existence is handled.
    
    # 2. GenAI Setup
    gemini_key, gemini_proxy = config.validate_gemini()
    client = genai.Client(api_key=gemini_key)
    if gemini_proxy:
        import httpx
        http_client = httpx.Client(proxy=gemini_proxy, timeout=120.0)
        client._api_client._httpx_client = http_client

    # 3. Determine remaining required phantoms per mood
    # We query to see how many we already have so the script is idempotent
    existing = await db.pool.fetch("""
        SELECT mood, COUNT(*) as cnt 
        FROM users 
        WHERE is_phantom = TRUE 
        GROUP BY mood
    """)
    current_counts = {r['mood']: r['cnt'] for r in existing}
    
    target_plan = []
    for mood, target in MOOD_DISTRIBUTION.items():
        current = current_counts.get(mood, 0)
        needed = max(0, target - current)
        for _ in range(needed):
            target_plan.append(mood)
            
    random.shuffle(target_plan) # Randomize creation order
    
    if not target_plan:
        logger.info("✅ Phantom Protocol already complete. 100 users exist.")
        await db.close()
        return
        
    logger.info(f"🏭 Factory Plan: Generating {len(target_plan)} new Phantoms.")
    
    # 4. Crafting Loop
    success_count = 0
    total_needed = len(target_plan)
    
    for i, mood in enumerate(target_plan, 1):
        # We loop until this specific phantom is successfully created
        while True:
            # 4.1 Generate Identity
            gender = random.choice(GENDER_WEIGHTS)
            
            # Faker logic: match name to gender
            if gender == 'female':
                first_name = fake.first_name_female()
            else:
                first_name = fake.first_name_male()
                
            prompt = build_phantom_prompt(gender, mood)
            
            logger.info(f"✨ [{success_count+1}/{total_needed}] Casting {gender.title()} {mood.title()} Phantom: '{first_name}'...")
            
            # 4.2 Render Avatar
            relative_path = await generate_avatar(client, prompt)
            
            if not relative_path:
                logger.warning(f"  ⚠️ Generation failed for '{first_name}'. Retrying in 5s...")
                await asyncio.sleep(5)
                continue # Retry same mood iteration
                
            # 4.3 Save to DB
            try:
                await db.pool.execute("""
                    INSERT INTO users (telegram_id, first_name, gender, mood, avatar_path, is_phantom)
                    VALUES ($1, $2, $3, $4, $5, $6)
                """, None, first_name, gender, mood, relative_path, True)
                
                logger.info(f"  ✅ Saved to DB: {first_name} -> {relative_path}")
                success_count += 1
                
                # Rate limit safety
                await asyncio.sleep(4)
                break # Move to next phantom in the plan
                
            except Exception as e:
                logger.error(f"  ❌ DB Insert error: {e}")
                # We generated the image but failed to save. We should break to avoid infinite loops on DB failure
                break 

    logger.info(f"\n🎉 Phantom Protocol Complete! Created {success_count} Phantoms.")
    await db.close()

if __name__ == "__main__":
    asyncio.run(run_factory())
