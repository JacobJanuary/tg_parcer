"""
Image Generator Module — Двухступенчатый пайплайн генерации обложек.
Шаг 1: Gemini Flash (Art Director) пишет промпт.
Шаг 2: Imagen 4 / Imagen 3 (Artist) рендерит изображение (3:4, ALLOW_ADULT), с фоллбеком.
"""

import asyncio
import os
import logging
from io import BytesIO
import httpx
from PIL import Image

from google import genai
from google.genai import types

import config
from db import Database

logger = logging.getLogger(__name__)

class EventImageGenerator:
    def __init__(self, db: Database = None):
        self.db = db
        gemini_key, gemini_proxy = config.validate_gemini()

        self._http_client = None
        if gemini_proxy:
            self._http_client = httpx.Client(
                proxy=gemini_proxy,
                timeout=120.0,  # Увеличиваем таймаут для генерации картинок
            )

        self.client = genai.Client(
            api_key=gemini_key,
            http_options={"timeout": 120_000},
        )

        if self._http_client:
            self.client._api_client._httpx_client = self._http_client
            
        self.media_dir = config.DEFAULT_MEDIA_DIR
        os.makedirs(self.media_dir, exist_ok=True)
        self.concurrency_limit = asyncio.Semaphore(2)

    def _sync_get_prompt(self, raw_tg_text: str, category: str, override_prompt: str = None, reference_image_path: str = None) -> str | None:
        """ШАГ 1: AI-Арт-Директор пишет промпт (Prompt-Inception)"""
        if override_prompt:
            logger.info("🎬 Используем переданный вручную промпт для художника.")
            return override_prompt

        director_prompt = f"""
You are an expert Art Director for a premium event app in Koh Phangan.
Read this raw event description (and analyze the attached reference image if provided). 
Write a highly detailed visual prompt for an AI image generator.

CRITICAL RULES FOR THE IMAGE PROMPT:
1. Describe a cinematic, highly aesthetic, photorealistic scene.
2. ABSOLUTELY NO TEXT, NO LETTERS, NO WORDS, NO SIGNBOARDS, NO LOGOS in the image. It must be clean background art.
3. Capture the specific vibe (e.g., dark jungle techno, sunny beach yoga, cozy acoustic sunset).
4. If a reference image is provided, recreate its exact core subject, mood, colors, and setting, but vividly enhance it into a premium 8k masterpiece.
5. Include keywords: "Cinematic, 8k resolution, highly detailed, tropical Koh Phangan aesthetic, premium photography".

Event Category: {category}
Raw Event Text: "{raw_tg_text}"

Return ONLY the English visual prompt, nothing else. Keep it under 60 words.
"""
        contents_list = [director_prompt]
        if reference_image_path and os.path.exists(reference_image_path):
            try:
                ref_img = Image.open(reference_image_path)
                logger.info(f"📸 Добавляем картинку {reference_image_path} как референс для Арт-Директора.")
                contents_list.append(ref_img)
            except Exception as e:
                logger.warning(f"Failed to load reference image {reference_image_path}: {e}")

        try:
            logger.debug(f"🕵️‍♂️ 1. Арт-Директор анализирует текст [{category}]...")
            response = self.client.models.generate_content(
                model='gemini-2.5-flash',
                contents=contents_list,
            )
            raw_text = getattr(response, 'text', None)
            if not raw_text:
                logger.warning(f"⚠️ Арт-Директор вернул пустой ответ (response.text is None)")
                return None
            image_prompt = raw_text.strip()
            if not image_prompt:
                logger.warning(f"⚠️ Арт-Директор вернул пустую строку после strip()")
                return None
            logger.info(f"🎬 2. Промпт готов ({len(image_prompt)} симв.)")
            return image_prompt
        except Exception as e:
            logger.error(f"❌ Ошибка генерации промпта (Арт-Директор): {e}")
            return None

    def _process_and_save_image(self, image_bytes: bytes, category: str) -> str:
        """Ресайз под мобилки (макс ширина 600px) и конвертация в WebP."""
        image = Image.open(BytesIO(image_bytes))
        if image.mode != "RGB":
            image = image.convert("RGB")
            
        # Оптимизация под мобильные экраны (обычно карточка 300-400px, x2 для Retina = 600-800px)
        target_width = 600
        w, h = image.size
        # Уменьшаем только если оригинал больше
        if w > target_width:
            target_height = int(h * (target_width / w))
            # Используем LANCZOS для качественного даунскейла
            # Для совместимости с разными версиями Pillow пробуем оба варианта
            resample_filter = getattr(Image, "Resampling", Image).LANCZOS
            image = image.resize((target_width, target_height), resample_filter)
            
        filename = f"event_{category.lower()}_{os.urandom(4).hex()}.webp"
        filepath = os.path.join(self.media_dir, filename)
        # Сохраняем в WebP (quality 85 и method 6 дают лучшее сжатие при отличном качестве)
        image.save(filepath, "WEBP", quality=85, method=6)
        return filename

    def _sync_render_image(self, image_prompt: str, category: str, model_name: str) -> str:
        """ШАГ 2: Визуальная модель рисует шедевр"""
        logger.debug(f"⏳ 3. Рисуем картинку через {model_name}...")
        
        if "gemini" in model_name:
            result = self.client.models.generate_content(
                model=model_name,
                contents=[image_prompt],
                config=types.GenerateContentConfig(
                    response_modalities=["IMAGE"],
                    image_config=types.ImageConfig(
                        aspect_ratio="3:4"
                    )
                )
            )
            for part in result.candidates[0].content.parts:
                if part.inline_data:
                    filename = self._process_and_save_image(part.inline_data.data, category)
                    logger.info(f"✅ Успех ({model_name})! Сочная обложка сохранена: {filename}")
                    return filename
        else:
            result = self.client.models.generate_images(
                model=model_name,
                prompt=image_prompt,
                config=types.GenerateImagesConfig(
                    number_of_images=1,
                    # output_mime_type="image/jpeg", # Imagen 3 is fine without this if we convert bytes later 
                    aspect_ratio="3:4", 
                    person_generation="ALLOW_ADULT"
                )
            )
            
            for generated_image in result.generated_images:
                filename = self._process_and_save_image(generated_image.image.image_bytes, category)
                logger.info(f"✅ Успех ({model_name})! Сочная обложка сохранена: {filename}")
                return filename
            
        raise ValueError(f"No images returned from {model_name} API")

    async def generate_cover(self, raw_tg_text: str, category: str, event_id: int = None, reference_image_path: str = None) -> str | None:
        """Асинхронная обертка с семафором и каскадным Failover (Rate Limit Protector)."""
        
        # 1. Получаем промпт с ретраями (до 3 попыток)
        image_prompt = None
        for prompt_attempt in range(3):
            image_prompt = await asyncio.to_thread(
                self._sync_get_prompt, raw_tg_text, category,
                override_prompt=None, reference_image_path=reference_image_path
            )
            if image_prompt:
                break
            wait = 3 * (prompt_attempt + 1)
            logger.warning(f"⏳ Арт-Директор: попытка {prompt_attempt + 1}/3 не удалась. Ретрай через {wait}с...")
            await asyncio.sleep(wait)
        
        if not image_prompt:
            # Fallback: генерируем простой промпт из категории, чтобы обложка всё равно была
            fallback_prompts = {
                "Party": "Cinematic tropical night party under palm trees, colorful neon lights, dancers silhouettes, Koh Phangan beach, 8k resolution, highly detailed, premium photography",
                "Sport": "Dynamic athletic activity on a tropical beach at golden hour, energetic movement, coconut palms, Koh Phangan island aesthetic, 8k cinematic, premium photography",
                "Chill": "Serene tropical sunset meditation scene, soft golden light through palm trees, peaceful ocean, Koh Phangan vibes, 8k resolution, highly detailed, premium photography",
                "Education": "Intimate workshop gathering in a beautiful open-air tropical pavilion, warm ambient light, lush greenery, Koh Phangan aesthetic, 8k cinematic, premium photography",
                "Music": "Live music performance at a stunning tropical open-air venue, magical lighting, ocean backdrop, Koh Phangan nightlife, 8k resolution, highly detailed, premium photography",
            }
            image_prompt = fallback_prompts.get(category, fallback_prompts["Chill"])
            logger.warning(f"🔄 Арт-Директор не справился за 3 попытки. Используем fallback промпт для [{category}].")
            
        fallback_models = [
            "imagen-4.0-generate-001",
            "imagen-4.0-fast-generate-001",
            "gemini-2.5-flash-image",
            "gemini-2.5-flash-image",
            "gemini-2.5-flash-image"
        ]
            
        async with self.concurrency_limit:
            for attempt, model_name in enumerate(fallback_models):
                try:
                    filename = await asyncio.to_thread(self._sync_render_image, image_prompt, category, model_name)
                    
                    if filename and event_id and self.db:
                        try:
                            await self.db.pool.execute(
                                "UPDATE events SET image_path = $1 WHERE id = $2",
                                filename, event_id
                            )
                            logger.debug(f"💾 image_path '{filename}' привязан к event ID {event_id}")
                        except Exception as e:
                            logger.error(f"Ошибка привязки image_path к базе: {e}")
                            
                    await asyncio.sleep(2) # Throttle 15 RPM
                    return filename
                    
                except Exception as e:
                    if attempt < len(fallback_models) - 1:
                        wait = 5 * (attempt + 1)
                        err_msg = str(e).replace('\n', ' ')
                        logger.warning(f"⏳ Ошибка {model_name} ({type(e).__name__}). Переключаемся на {fallback_models[attempt+1]} через {wait}с... Ошибка: {err_msg[:100]}")
                        await asyncio.sleep(wait)
                    else:
                        logger.error(f"❌ Все {len(fallback_models)} модели исчерпали лимиты или сломались: {e}")
            return None

# ТЕСТИРОВАНИЕ напрямую
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    async def run_test():
        gen = EventImageGenerator()
        
        spam_party = "🔥🔥 Девчонки и парни! Сегодня рвем танцпол в джунглях! 🌴🎧 Секретный лайн-ап, старт в 23:00, вход фри до полуночи. Локация - Хаад Рин горы."
        print(f"\\n--- ТЕСТ: ВЕЧЕРИНКА ---")
        img1 = await gen.generate_cover(spam_party, "Party")
        
        spam_yoga = "Завтра в 7 утра собираемся на Зен Бич. Берем коврики. Будет пранаяма и хатха йога, оплата донейшн."
        print(f"\\n--- ТЕСТ: ЙОГА ---")
        img2 = await gen.generate_cover(spam_yoga, "Yoga")
        
        print(f"\\n[ИТОГИ ТЕСТА] Party: {img1}, Yoga: {img2}")

    asyncio.run(run_test())
