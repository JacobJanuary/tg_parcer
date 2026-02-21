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

def _process_and_save_image(image_bytes: bytes) -> str:
    """Ресайз под мобилки (макс ширина 600px) и конвертация в WebP."""
    image = Image.open(BytesIO(image_bytes))
    if image.mode != "RGB":
        image = image.convert("RGB")
        
    target_width = 600
    w, h = image.size
    if w > target_width:
        target_height = int(h * (target_width / w))
        resample_filter = getattr(Image, "Resampling", Image).LANCZOS
        image = image.resize((target_width, target_height), resample_filter)
        
    filename = f"phantom_{uuid.uuid4().hex[:8]}.webp"
    filepath = os.path.join(MEDIA_DIR, filename)
    image.save(filepath, "WEBP", quality=85, method=6)
    return os.path.join("avatars", filename)


def _sync_render_image(client: genai.Client, prompt: str, model_name: str) -> str | None:
    """Synchronous call to Google GenAI SDK (Imagen или Gemini)."""
    
    logger.debug(f"⏳ Рисуем через {model_name}...")
    
    try:
        if "gemini" in model_name:
            result = client.models.generate_content(
                model=model_name,
                contents=[prompt],
                config=types.GenerateContentConfig(
                    response_modalities=["IMAGE"],
                    image_config=types.ImageConfig(
                        aspect_ratio="1:1"
                    )
                )
            )
            for part in result.candidates[0].content.parts:
                if part.inline_data:
                    return _process_and_save_image(part.inline_data.data)
        else:
            result = client.models.generate_images(
                model=model_name,
                prompt=prompt,
                config=types.GenerateImagesConfig(
                    number_of_images=1,
                    aspect_ratio="1:1",
                    person_generation="ALLOW_ADULT"
                )
            )
            
            for generated_image in result.generated_images:
                return _process_and_save_image(generated_image.image.image_bytes)
    except Exception as e:
        logger.debug(f"❌ Ошибка {model_name}: {e}")
        return None
        
    return None

async def generate_avatar(client: genai.Client, prompt: str) -> str | None:
    """Асинхронная обертка с каскадным Failover (Rate Limit Protector)."""
    
    # Такая же логика как в image_generator.py
    fallback_models = [
        "imagen-4.0-fast-generate-001",
        "imagen-4.0-generate-001",
        "gemini-2.5-flash-image"
    ]
    
    for attempt, model_name in enumerate(fallback_models):
        relative_path = await asyncio.to_thread(_sync_render_image, client, prompt, model_name)
        if relative_path:
            return relative_path
            
        logger.warning(f"  ⚠️ Модель {model_name} не справилась. Пробуем следующую..." if attempt < len(fallback_models)-1 else "  ❌ Все модели упали.")
        await asyncio.sleep(2) # Пауза перед фоллбеком
        
    return None


# ─── Main Factory Logic ───

async def run_factory(is_test_mode: bool = False):
    logger.info("👻 Initializing Phantom Protocol...")
    
    # 1. DB Setup
    db = Database(config.get_dsn())
    try:
        await db.connect()
    except Exception as e:
        logger.error(f"❌ DB Connection failed: {e}")
        return
    
    # 2. GenAI Setup
    gemini_key, gemini_proxy = config.validate_gemini()
    client = genai.Client(api_key=gemini_key)
    if gemini_proxy:
        import httpx
        http_client = httpx.Client(proxy=gemini_proxy, timeout=120.0)
        client._api_client._httpx_client = http_client

    # 3. Build Plan
    target_plan = []
    
    if is_test_mode:
        logger.info("🧪 TEST MODE: Generating exactly 6 phantoms (1 male, 1 female per mood).")
        # 3 moods * 2 genders = 6 phantoms total
        for mood in MOOD_DISTRIBUTION.keys():
            target_plan.append((mood, 'female'))
            target_plan.append((mood, 'male'))
    else:
        # Check existing to make script idempotent
        existing = await db.pool.fetch("""
            SELECT mood, COUNT(*) as cnt 
            FROM users 
            WHERE is_phantom = TRUE 
            GROUP BY mood
        """)
        current_counts = {r['mood']: r['cnt'] for r in existing}
        
        for mood, target in MOOD_DISTRIBUTION.items():
            current = current_counts.get(mood, 0)
            needed = max(0, target - current)
            for _ in range(needed):
                target_plan.append((mood, random.choice(GENDER_WEIGHTS)))
                
        random.shuffle(target_plan) # Randomize creation order
    
    if not target_plan:
        logger.info("✅ Phantom Protocol already complete. Users exist.")
        await db.close()
        return
        
    logger.info(f"🏭 Factory Plan: Generating {len(target_plan)} new Phantoms.")
    
    # 4. Crafting Loop
    success_count = 0
    total_needed = len(target_plan)
    
    for i, (mood, gender) in enumerate(target_plan, 1):
        # We loop until this specific phantom is successfully created
        while True:
            # 4.1 Generate Identity
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
                continue # Retry same iteration
                
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
                break 

    logger.info(f"\n🎉 Phantom Protocol Complete! Created {success_count} Phantoms.")
    await db.close()

if __name__ == "__main__":
    is_test = "--test" in sys.argv
    asyncio.run(run_factory(is_test_mode=is_test))
