"""
Spider Notify — обработка discovered_chats.

Resolve → Relevance Filter (keywords + Gemini) → Bot Notify.

Использование:
    python spider_notify.py --dry-run     # только статистика
    python spider_notify.py --resolve     # resolve + фильтр + уведомления
    python spider_notify.py --skip-gemini # без Gemini фильтра
"""

import asyncio
import argparse
import json
import re
import sys
import logging

logger = logging.getLogger(__name__)

from dotenv import load_dotenv
load_dotenv()

import config
from db import Database
from display import Colors


# ─── Настройки фильтрации ───

MIN_PARTICIPANTS = 50  # Минимум участников для прохождения

# Ключевые слова — точно Панган
PHANGAN_KEYWORDS = [
    "phangan", "панган", "пханган", "пнг", "koh phangan",
    "ko phangan", "ko pha ngan", "haad rin", "хаад рин",
    "thong sala", "тонг сала", "bantai", "бантай",
    "srithanu", "шритану", "chaloklum", "чалоклам",
]

# Ключевые слова — точно НЕ наша тема (тип контента)
TOPIC_REJECT_KEYWORDS = [
    "garage sale", "распродаж", "sale", "барахолк",
    "виз", "visa",  # визы, визам, визовые
    "обмен валют", "exchange money", "exchangemoney",
    "бьюти", "beauty", "маникюр", "наращ",
    "lazada", "shopee",
    "молитв", "закрытый клуб",
    "бот ", "_bot",
    "зелёный коридор", "зеленый коридор",
    "bud", "шишк", "weed", "ganja", "ганж",
    "cannabis", "каннабис", "420", "stash", "трав",
]

# Ключевые слова — точно не Панган (другая локация)
LOCATION_REJECT_KEYWORDS = [
    "moscow", "москв", "phuket", "пхукет", "samui", "самуи",
    "bali", "бали", "petersburg", "питер", "спб",
    "новосибирск", "екатеринбург", "краснодар", "казань",
    "dubai", "дубай", "goa", "гоа", "pattaya", "паттайя",
]


def quick_relevance_check(title: str, username: str) -> str:
    """
    Быстрая проверка релевантности.
    Returns: 'relevant', 'irrelevant', 'ambiguous'
    """
    text = f"{title} {username}".lower()

    # Точно Панган
    for kw in PHANGAN_KEYWORDS:
        if kw in text:
            # Но может быть off-topic даже на Пангане (sale, visa)
            for tk in TOPIC_REJECT_KEYWORDS:
                if tk in text:
                    return "irrelevant"
            return "relevant"

    # Точно не наша тема
    for kw in TOPIC_REJECT_KEYWORDS:
        if kw in text:
            return "irrelevant"

    # Точно другая локация
    for kw in LOCATION_REJECT_KEYWORDS:
        if kw in text:
            return "irrelevant"

    return "ambiguous"


def _is_transient(error: Exception) -> bool:
    """Проверяет, является ли ошибка временной (стоит retry)."""
    s = str(error)
    return any(x in s for x in (
        "429", "RESOURCE_EXHAUSTED", "504", "503",
        "DEADLINE_EXCEEDED", "CANCELLED", "timed out", "timeout",
        "TOO_MANY_TOOL_CALLS", "ServerError",
    ))


async def gemini_check_location(gemini_client, title: str, username: str,
                                 chat_type: str, participants: int = None) -> dict:
    """
    Gemini проверка локации с Google Search grounding.
    Primary: gemini-2.5-flash, fallback: gemini-2.5-flash-lite.
    Retry до 3 раз с exponential backoff для transient ошибок.
    Returns: {'verdict': 'relevant'|'reject'|'manual', 'reason': str, 'location': str}
    """
    import google.genai as genai
    from google.genai.types import Tool, GoogleSearch
    from pydantic import BaseModel, Field

    class LocationVerdict(BaseModel):
        verdict: str = Field(description="One of: 'relevant', 'reject', 'manual'")
        reason: str = Field(description="Brief explanation of the verdict")
        location: str | None = Field(description="Island/city name, or null if unknown")

    info = f"Title: {title}"
    if username:
        info += f"\nUsername: @{username}"
    if chat_type:
        info += f"\nType: {chat_type}"
    if participants:
        info += f"\nParticipants: {participants}"

    prompt = f"""Determine the physical location of this Telegram chat/channel.

{info}

Steps:
1. Search for information about what this place/community is and where it is physically located.
2. If it is on Koh Phangan island — verdict = "relevant"
3. If it is on Phuket, Samui, Bali or another specific location — verdict = "reject"
4. If you cannot determine the location — verdict = "manual"
5. Reject if it is a general-topic channel with no location binding, a personal blog, bot, or spam.

NOTE: The title may be in Russian, English, or mixed. Analyze content regardless of language.

Reply STRICTLY with a valid JSON object matching this schema:
{{
  "verdict": "relevant" | "reject" | "manual",
  "reason": "Brief explanation",
  "location": "City/Island or null"
}}
CRITICAL RULES:
1. Do not wrap code in markdown blocks (e.g. ```json).
2. Use DOUBLE quotes for all property names and strings.
3. ABSOLUTELY NO comments inside the JSON.
4. ABSOLUTELY NO trailing commas."""

    grounding_tool = Tool(google_search=GoogleSearch())
    models_to_try = ["gemini-2.5-flash", "gemini-2.5-flash-lite"]
    max_retries = 3
    backoff = 5  # seconds

    for model in models_to_try:
        for attempt in range(1, max_retries + 1):
            try:
                response = await asyncio.to_thread(
                    gemini_client.models.generate_content,
                    model=model,
                    contents=prompt,
                    config=genai.types.GenerateContentConfig(
                        temperature=0.1,
                        max_output_tokens=8192,
                        tools=[grounding_tool],
                    ),
                )
                
                text = None
                finish_reason = ""
                if response.candidates:
                    finish_reason = str(response.candidates[0].finish_reason)
                
                if response.text:
                    text = response.text.strip()
                elif response.candidates:
                    for part in (response.candidates[0].content.parts or []):
                        if hasattr(part, 'text') and part.text:
                            text = part.text.strip()
                            break

                if text:
                    match = re.search(r'(\{.*\})', text, re.DOTALL)
                    if match:
                        text = match.group(1).strip()
                    else:
                        text = text.replace("```json", "").replace("```", "").strip()

                if not text:
                    if "TOO_MANY_TOOL_CALLS" in finish_reason:
                        raise ValueError("TOO_MANY_TOOL_CALLS: model looped on tools")
                    raise ValueError(f"Empty response from {model} (finish_reason: {finish_reason})")

                try:
                    parsed = json.loads(text)
                except json.JSONDecodeError as je:
                    import ast
                    try:
                        loose_text = text.replace('true', 'True').replace('false', 'False').replace('null', 'None')
                        parsed = ast.literal_eval(loose_text)
                    except Exception:
                        logger.warning(f"SpiderNotify raw text failure:\n{text}")
                        raise je

                if parsed and "verdict" in parsed:
                    return parsed
                # Ошибка схемы — пробуем следующую модель
                break

            except Exception as e:
                if _is_transient(e) and attempt < max_retries:
                    wait = backoff * (2 ** (attempt - 1))  # 5→10→20
                    logger.warning(
                        f"⏳ {model} attempt {attempt}/{max_retries} "
                        f"failed ({type(e).__name__}), retry in {wait}s"
                    )
                    await asyncio.sleep(wait)
                    continue
                if attempt == max_retries or not _is_transient(e):
                    # Last attempt or non-transient → try next model
                    logger.warning(f"💥 {model} failed: {e}")
                    break

    return {"verdict": "manual", "reason": "all models exhausted", "location": None}


async def main():
    parser = argparse.ArgumentParser(description="Spider Notify — обработка discovered_chats")
    parser.add_argument("--dry-run", action="store_true", help="Только показать статистику")
    parser.add_argument("--resolve", action="store_true", help="Резолвить + фильтр + уведомления")
    parser.add_argument("--notify-all", action="store_true", help="Уведомить обо всех new+resolved")
    parser.add_argument("--skip-gemini", action="store_true", help="Не использовать Gemini")
    args = parser.parse_args()

    # 1. PG
    db = Database(config.get_dsn())
    try:
        await db.connect()
        print(f"🐘 PostgreSQL подключён")
    except Exception as e:
        print(f"❌ PostgreSQL: {e}")
        sys.exit(1)

    try:
        # 2. Статистика
        all_discovered = await db.get_all_discovered()
        new_chats = [r for r in all_discovered if r["status"] == "new"]
        resolved_new = [r for r in new_chats if r.get("resolved")]
        unresolved_new = [r for r in new_chats if not r.get("resolved")]

        by_status = {}
        for r in all_discovered:
            by_status[r["status"]] = by_status.get(r["status"], 0) + 1

        print(f"\n{'=' * 50}")
        print(f"🕷️ Spider Discovered Chats")
        print(f"{'=' * 50}")
        print(f"   Всего:             {len(all_discovered)}")
        print(f"   Status=new:        {len(new_chats)}")
        print(f"     ├─ resolved:     {len(resolved_new)}")
        print(f"     └─ unresolved:   {len(unresolved_new)}")
        print(f"   Min participants:  {MIN_PARTICIPANTS}")
        for s, cnt in sorted(by_status.items()):
            print(f"   [{s}]: {cnt}")
        print(f"{'=' * 50}\n")

        if args.dry_run:
            for i, r in enumerate(new_chats, 1):
                res = "✅" if r.get("resolved") else "❌"
                title = r.get("title") or r.get("username") or r.get("invite_link") or str(r.get("chat_id"))
                members = r.get("participants_count")
                members_str = f" ({members} уч.)" if members else ""
                chat_type = r.get("type", "?")

                # Quick filter
                relevance = quick_relevance_check(title, r.get("username", ""))
                if members is not None and members < MIN_PARTICIPANTS:
                    relevance = "irrelevant"
                rel_icon = {"relevant": "🟢", "irrelevant": "🔴", "ambiguous": "🟡"}.get(relevance, "⚪")

                print(f"  {i:>2}. {res} {rel_icon} {title} [{chat_type}]{members_str}")
            await db.close()
            return

        # ─── RESOLVE ───

        if args.resolve and unresolved_new:
            from telethon import TelegramClient
            api_id, api_hash, phone = config.validate()
            tg_client = TelegramClient(config.SESSION_NAME, api_id, api_hash)
            await tg_client.start(phone=phone)
            print(f"\n📱 Telegram подключён")

            resolved_count = 0
            for i, r in enumerate(unresolved_new, 1):
                try:
                    username = r.get("username")
                    invite_link = r.get("invite_link")

                    if username:
                        entity = await tg_client.get_entity(username)
                        from telethon.tl.types import Channel, Chat as TChat
                        from telethon.tl.functions.channels import GetFullChannelRequest

                        updates = {
                            "chat_id": entity.id,
                            "title": getattr(entity, "title", None),
                            "resolved": True,
                        }

                        if isinstance(entity, Channel):
                            updates["type"] = "channel" if entity.broadcast else "megagroup"
                            try:
                                full = await tg_client(GetFullChannelRequest(entity))
                                updates["participants_count"] = full.full_chat.participants_count
                            except Exception:
                                updates["participants_count"] = getattr(entity, "participants_count", None)
                        elif isinstance(entity, TChat):
                            updates["type"] = "group"
                        else:
                            updates["type"] = "user"
                            updates["status"] = "rejected"

                        await db.update_discovered(r["id"], **updates)
                        label = updates.get("title") or username
                        members = updates.get("participants_count", "?")
                        print(f"  [{i}/{len(unresolved_new)}] ✅ {label} ({updates.get('type')}, {members} уч.)")
                        resolved_count += 1

                    elif invite_link:
                        from telethon.tl.functions.messages import CheckChatInviteRequest
                        if "/+" in invite_link:
                            invite_hash = invite_link.split("/+")[-1]
                        elif "/joinchat/" in invite_link:
                            invite_hash = invite_link.split("/joinchat/")[-1]
                        else:
                            print(f"  [{i}/{len(unresolved_new)}] ⏭️ Bad link: {invite_link}")
                            continue

                        result = await tg_client(CheckChatInviteRequest(hash=invite_hash))
                        updates = {"resolved": True}
                        if hasattr(result, "chat"):
                            updates["chat_id"] = result.chat.id
                            updates["title"] = getattr(result.chat, "title", None)
                            updates["participants_count"] = getattr(result.chat, "participants_count", None)
                        elif hasattr(result, "title"):
                            updates["title"] = result.title
                            updates["participants_count"] = getattr(result, "participants_count", None)

                        await db.update_discovered(r["id"], **updates)
                        label = updates.get("title") or invite_link
                        print(f"  [{i}/{len(unresolved_new)}] ✅ {label}")
                        resolved_count += 1
                        
                    else:
                        # No username and no invite link (e.g. private forward)
                        await db.update_discovered(r["id"], status="rejected", resolved=True)
                        label = r.get("title") or str(r.get("chat_id", "Unknown"))
                        print(f"  [{i}/{len(unresolved_new)}] ❌ {label}: No public username or link → rejected")
                        resolved_count += 1

                except Exception as e:
                    label = r.get("username") or r.get("invite_link") or str(r.get("chat_id"))
                    err_str = str(e).lower()
                    # Permanent failure → reject
                    if any(x in err_str for x in ("no user has", "nobody is using", "username not occupied",
                                                   "username invalid", "invite hash expired",
                                                   "the channel specified is private")):
                        await db.update_discovered(r["id"], status="rejected", resolved=True)
                        print(f"  [{i}/{len(unresolved_new)}] ❌ {label}: {e} → rejected")
                    else:
                        # Temporary (flood, network) — оставляем new для retry
                        print(f"  [{i}/{len(unresolved_new)}] ⚠️ {label}: {e} (retry next time)")

                await asyncio.sleep(2)

            await tg_client.disconnect()
            print(f"\n🕷️ Resolved: {resolved_count}/{len(unresolved_new)}\n")

        # ─── RELEVANCE FILTER ───

        all_discovered = await db.get_all_discovered()
        new_chats = [r for r in all_discovered if r["status"] == "new"]
        resolved_new = [r for r in new_chats if r.get("resolved")]

        if not resolved_new:
            print("✅ Нет resolved чатов для фильтрации")
            await db.close()
            return

        print(f"\n🔍 Фильтрация {len(resolved_new)} чатов...\n")

        # Gemini
        gemini_client = None
        if not args.skip_gemini and config.GEMINI_API_KEY:
            import google.genai as genai
            import httpx
            gemini_client = genai.Client(
                api_key=config.GEMINI_API_KEY,
                http_options={"timeout": 60_000},
            )
            proxy = config.GEMINI_PROXY if config.USE_PROXY else None
            if proxy:
                http_cl = httpx.Client(proxy=proxy, timeout=30.0)
                gemini_client._api_client._httpx_client = http_cl
            print(f"🤖 Gemini подключён\n")

        relevant_chats = []
        rejected_count = 0
        small_count = 0

        for i, r in enumerate(resolved_new, 1):
            title = r.get("title") or ""
            username = r.get("username") or ""
            members = r.get("participants_count")

            # Фильтр по участникам
            if members is not None and members < MIN_PARTICIPANTS:
                print(f"  {i}. ⛔ {title or username} — {members} уч. (< {MIN_PARTICIPANTS})")
                await db.update_discovered(r["id"], status="rejected")
                small_count += 1
                rejected_count += 1
                continue

            # Quick keyword check
            quick = quick_relevance_check(title, username)

            if quick == "relevant":
                print(f"  {i}. 🟢 {title or username} — RELEVANT")
                relevant_chats.append(r)

            elif quick == "irrelevant":
                print(f"  {i}. 🔴 {title or username} — REJECTED (off-topic)")
                await db.update_discovered(r["id"], status="rejected")
                rejected_count += 1

            elif quick == "ambiguous":
                if gemini_client:
                    result = await gemini_check_location(
                        gemini_client, title, username,
                        r.get("type", ""),
                        members,
                    )
                    location = result.get("location", "")
                    reason = result.get("reason", "?")
                    verdict = result.get("verdict", "manual")

                    if verdict == "relevant":
                        print(f"  {i}. 🟢 {title or username} — RELEVANT (Gemini: {reason}, loc: {location})")
                        relevant_chats.append(r)
                    elif verdict == "reject":
                        print(f"  {i}. 🔴 {title or username} — REJECTED (Gemini: {reason}, loc: {location})")
                        await db.update_discovered(r["id"], status="rejected")
                        rejected_count += 1
                    else:  # manual
                        print(f"  {i}. 🟡 {title or username} — MANUAL (Gemini: {reason}) → в канал")
                        relevant_chats.append(r)
                    await asyncio.sleep(3)  # rate limit for Search grounding
                else:
                    print(f"  {i}. 🟡 {title or username} — AMBIGUOUS → в канал")
                    relevant_chats.append(r)

        print(f"\n📊 Итого: {len(relevant_chats)} relevant, {rejected_count} rejected (из них {small_count} малые)")

        # ─── BOT NOTIFY ───

        if not relevant_chats:
            print("\n✅ Нет чатов для уведомления")
            await db.close()
            return

        if not config.BOT_TOKEN or not config.SPIDER_CHANNEL_ID:
            print("\n⚠️  BOT_TOKEN / SPIDER_CHANNEL_ID не заданы")
            for r in relevant_chats:
                print(f"  • {r.get('title') or r.get('username')}")
            await db.close()
            return

        from telethon import TelegramClient
        from spider_bot import format_card, make_buttons, _register_callbacks

        bot_client = TelegramClient(
            "spider_bot_session",
            int(config.API_ID),
            config.API_HASH,
        )
        await bot_client.start(bot_token=config.BOT_TOKEN)
        _register_callbacks(bot_client, db)

        me = await bot_client.get_me()
        print(f"\n🤖 Bot: @{me.username}")

        from spider import DiscoveredChat
        sent = 0
        for r in relevant_chats:
            try:
                dc = DiscoveredChat(
                    chat_id=r.get("chat_id"),
                    username=r.get("username"),
                    invite_link=r.get("invite_link"),
                    title=r.get("title"),
                    type=r.get("type"),
                    source_type=r.get("source_type", "unknown"),
                    found_in_chat=r.get("found_in_chat", ""),
                    first_seen=str(r.get("first_seen", "")),
                    last_seen=str(r.get("last_seen", "")),
                    times_seen=r.get("times_seen", 0),
                    status=r.get("status", "new"),
                    resolved=r.get("resolved", False),
                    participants_count=r.get("participants_count"),
                )

                text = format_card(dc)
                buttons = make_buttons(dc)

                await bot_client.send_message(
                    config.SPIDER_CHANNEL_ID,
                    text,
                    buttons=buttons,
                    parse_mode="html",
                )
                print(f"  📤 {dc.title or dc.username}")
                sent += 1
                await asyncio.sleep(0.5)

            except Exception as e:
                print(f"  ❌ {e}")

        print(f"\n✅ Отправлено {sent}/{len(relevant_chats)}")
        print(f"\n👂 Слушаю нажатия кнопок... (Ctrl+C для выхода)")

        import signal
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.ensure_future(bot_client.disconnect()))

        try:
            await bot_client.run_until_disconnected()
        except Exception:
            pass

        print("\n🔌 Бот остановлен")

    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
