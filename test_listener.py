#!/usr/bin/env python3
"""
Интеграционное тестирование — полный цикл проверки всех модулей.

1. Загружает последние 100 сообщений из каждого чата
2. Прогоняет через Spider → Filter → AI (pre-screen → extract)
3. Параллельно слушает новые сообщения (listener)
4. Выводит подробный отчёт

Использование:
    python test_listener.py
"""

import asyncio
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime

from telethon import TelegramClient, events
from telethon.tl.types import MessageMediaPhoto

import chats
import config
import filters
from db import Database
from display import Colors, print_event
from spider import ChatSpider


# ─── Результаты тестов ───

class TestResults:
    def __init__(self):
        self.start_time = time.time()

        # Чаты
        self.chats_loaded = 0
        self.chats_resolved = 0

        # Сообщения (batch)
        self.batch_total = 0
        self.batch_per_chat = Counter()
        self.batch_empty = 0

        # Spider
        self.spider_new = 0
        self.spider_by_type = Counter()
        self.spider_errors = []

        # Фильтр
        self.filter_passed = 0
        self.filter_rejected = 0
        self.filter_reasons = Counter()
        self.filter_scores = []

        # AI — две стадии
        self.ai_screened = 0
        self.ai_screen_passed = 0
        self.ai_extracted = 0
        self.ai_events = 0
        self.ai_not_events = 0
        self.ai_errors = []
        self.ai_events_list = []
        self.ai_screen_latencies = []
        self.ai_extract_latencies = []

        # Listener (live)
        self.live_messages = 0
        self.live_filter_passed = 0
        self.live_ai_events = 0
        self.live_spider_new = 0

    @property
    def elapsed(self):
        return time.time() - self.start_time


results = TestResults()

_bg_tasks: set[asyncio.Task] = set()

def _fire_and_forget(coro):
    """Запускает корутину как background task, не блокируя цикл."""
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


# ─── Тест ───

async def main():
    print(f"\n{'=' * 70}")
    print(f" 🧪 ИНТЕГРАЦИОННЫЙ ТЕСТ v2 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f" Двухступенчатый AI: pre-screen (2.5-flash-lite) → extract (2.5-flash)")
    print(f"{'=' * 70}\n")

    # PostgreSQL (optional for tests)
    global db
    db = None
    try:
        db = Database(config.get_dsn())
        await db.connect()
        print(f"🐘 PostgreSQL подключён")
    except Exception as e:
        print(f"⚠️  PostgreSQL недоступен: {e} (продолжаем без PG)")
        db = None

    # ─── 1. Модуль chats.py ───
    print(f"{Colors.BOLD}[1/6] 📋 Загрузка чатов{Colors.RESET}")

    # DB-first, JSON fallback
    if db:
        selected = await chats.load_from_db(db)
        if selected:
            print(f"  ✅ Из PostgreSQL: {len(selected)} активных чатов")
        else:
            selected = chats.load()
            if selected:
                print(f"  ⚠️  PG пуст, fallback на JSON: {len(selected)} чатов")
                await chats.save_to_db(db, selected)
    else:
        selected = chats.load()
        if selected:
            print(f"  ⚠️  Без PG, из JSON: {len(selected)} чатов")

    if not selected:
        print("  ❌ FAIL: нет чатов ни в БД, ни в JSON")
        sys.exit(1)

    # Добавляем approved spider chats
    if db:
        try:
            existing_ids = {c["id"] for c in selected}
            approved = await db.get_all_discovered(status="approved")
            spider_added = 0
            for row in approved:
                cid = row.get("chat_id")
                if cid and cid not in existing_ids:
                    selected.append({
                        "id": cid,
                        "title": row.get("title") or str(cid),
                        "type": row.get("type") or "megagroup",
                    })
                    existing_ids.add(cid)
                    spider_added += 1
            if spider_added:
                print(f"  🕷️ + {spider_added} spider approved чатов")
        except Exception as e:
            print(f"  ⚠️  Spider chats: {e}")

    results.chats_loaded = len(selected)
    print(f"  📋 Итого: {results.chats_loaded} чатов")

    # ─── 2. Модуль spider ───
    print(f"\n{Colors.BOLD}[2/6] 🕷️ Модуль spider.py{Colors.RESET}")
    spider = ChatSpider()
    print(f"  ✅ База загружена: {len(spider.discovered)} записей")
    stats = spider.get_stats()
    for status, count in stats.get("by_status", {}).items():
        print(f"     {status}: {count}")

    # ─── 3. Модуль filters ───
    print(f"\n{Colors.BOLD}[3/6] 🔍 Модуль filters.py{Colors.RESET}")
    test_cases = [
        ("Привет всем!", False, False, "обычное сообщение"),
        ("Йога завтра в 10:00 на пляже, приходите! 🧘", False, True, "ивент с датой"),
        ("Продам скутер, пишите в ЛС", False, False, "барахолка"),
        ("🎉 Вечеринка в пятницу! DJ Set на Holistic Space, вход 300 бат", True, True, "ивент с фото"),
    ]
    filter_ok = 0
    for text, has_media, expected, label in test_cases:
        r = filters.check(text, has_media)
        status = "✅" if r.passed == expected else "❌"
        if r.passed == expected:
            filter_ok += 1
        print(f"  {status} {label}: passed={r.passed} (expected={expected}), score={r.score}")
    print(f"  Итого: {filter_ok}/{len(test_cases)} тестов пройдено")

    # ─── 4. Модуль ai_analyzer ───
    print(f"\n{Colors.BOLD}[4/6] 🤖 Модуль ai_analyzer.py (2-stage){Colors.RESET}")
    try:
        from ai_analyzer import EventAnalyzer
        analyzer = EventAnalyzer()
        print(f"  ✅ Screen model: {analyzer.screen_model}")
        print(f"  ✅ Extract model: {analyzer.model}")
        print(f"  ✅ Fallback model: {analyzer.fallback_model}")
    except Exception as e:
        print(f"  ❌ FAIL: {e}")
        sys.exit(1)

    # Venue Enricher
    venue_enricher = None
    try:
        from venue_enricher import VenueEnricher
        venue_enricher = VenueEnricher(db=db)
        await venue_enricher.cache.load_from_pg()
        print(f"  📍 Venue Enricher: {len(venue_enricher.cache)} в кэше")
    except Exception as e:
        print(f"  ⚠️  Venue Enricher: {e}")

    # Image Generator
    image_generator = None
    if db:
        try:
            from image_generator import EventImageGenerator
            image_generator = EventImageGenerator(db=db)
            print("  🎨 Image Generator: активирован (Imagen 4.0)")
        except Exception as e:
            print(f"  ⚠️  Image Generator: {e}")

    # Test pre-screen
    print(f"\n  --- Pre-screen тесты ---")
    screen_tests = [
        ("Привет! Как дела?", False, "болтовня"),
        ("Продам байк Nmax 2023, 45000 бат", False, "продажа"),
        ("Завтра в 19:00 йога на закате на пляже Zen Beach", True, "ивент"),
        ("DJ Set в пятницу в Merkaba, вход 300 бат", True, "вечеринка"),
        ("Сдам виллу 2 спальни, 25000/мес", False, "аренда"),
    ]
    screen_ok = 0
    for text, expected, label in screen_tests:
        t0 = time.time()
        result = await analyzer.pre_screen(text, "Test")
        lat = time.time() - t0
        ok = result == expected
        if ok:
            screen_ok += 1
        icon = "✅" if ok else "❌"
        print(f"  {icon} {label}: is_event={result} (expected={expected}) [{lat:.1f}с]")
    print(f"  Pre-screen: {screen_ok}/{len(screen_tests)} пройдено")

    # Test full pipeline
    print(f"\n  --- Full pipeline тест ---")
    t0 = time.time()
    test_result = await analyzer.analyze(
        "Завтра в 19:00 йога на закате на пляже Zen Beach. Вход свободный.",
        "Тестовый чат"
    )
    latency = time.time() - t0
    if test_result and test_result.get("is_event"):
        print(f"  ✅ Full pipeline: ивент распознан ({latency:.1f}с)")
        print(f"     Title: {test_result.get('title')}")
        print(f"     Category: {test_result.get('category')}")
    else:
        print(f"  ⚠️  Full pipeline: не распознал ({latency:.1f}с)")

    # ─── 5. Подключение к Telegram ───
    print(f"\n{Colors.BOLD}[5/6] 📡 Подключение к Telegram{Colors.RESET}")
    api_id, api_hash, phone = config.validate()
    client = TelegramClient("tg_test_session", api_id, api_hash)
    await client.start(phone=phone)

    me = await client.get_me()
    print(f"  ✅ Авторизован: {me.first_name}")

    chat_ids = [c["id"] for c in selected]
    resolved_chats = await chats.resolve(client, chat_ids)
    results.chats_resolved = len(resolved_chats)
    print(f"  Зарезолвлено: {results.chats_resolved}/{results.chats_loaded}")

    if not resolved_chats:
        print("  ❌ FAIL: ни один чат не найден")
        await client.disconnect()
        return

    # ─── 6. Полный цикл: batch + live listener ───
    print(f"\n{Colors.BOLD}[6/6] 🔄 Полный цикл тестирования{Colors.RESET}")
    print(f"  Загрузка 30 сообщений из каждого чата + live listener\n")

    # Live listener
    @client.on(events.NewMessage(chats=resolved_chats))
    async def on_live_message(event):
        results.live_messages += 1
        text = event.text or ""
        chat = await event.get_chat()
        chat_title = getattr(chat, "title", "?")

        try:
            found = spider.process_message(event.message, chat_title)
            if found:
                results.live_spider_new += len(found)
                for d in found:
                    if db:
                        try:
                            await db.upsert_discovered(
                                chat_id=d.chat_id,
                                username=d.username,
                                invite_link=d.invite_link,
                                title=d.title,
                                source_type=d.source_type,
                                found_in_chat_id=getattr(chat, "id", None),
                                status="new"
                            )
                        except Exception as dbe:
                            print(f"  ⚠️ Spider DB Error: {dbe}")
                    label = d.title or d.username or d.invite_link
                    print(f"  {Colors.MAGENTA}🕷️ LIVE: {label} [{d.source_type}]{Colors.RESET}")
        except Exception:
            pass

        # Filter
        has_media = isinstance(event.media, MessageMediaPhoto)
        fr = filters.check(text, has_media)
        if fr.passed:
            if db:
                try:
                    if await db.is_text_exists(text):
                        print(f"  {Colors.YELLOW}⏭️ LIVE Пропуск (уже было){Colors.RESET}")
                        return
                except Exception:
                    pass

            results.live_filter_passed += 1
            try:
                ai_r = await analyzer.analyze(text, chat_title)
                if ai_r and ai_r.get("is_event"):
                    # Venue enrichment
                    if venue_enricher:
                        try:
                            await venue_enricher.enrich_event(ai_r)
                        except Exception:
                            pass
                    results.live_ai_events += 1
                    print(f"  {Colors.GREEN}🎯 LIVE EVENT: {ai_r.get('title', '?')} ({chat_title}){Colors.RESET}")
            except Exception:
                pass

    # Batch: загрузка истории — без AI бюджета (pre-screen дешёвый)
    for entity in resolved_chats:
        title = getattr(entity, "title", "?")
        msg_count = 0
        chat_events = 0

        print(f"\n  {'─' * 50}")
        print(f"  📥 {Colors.CYAN}{title}{Colors.RESET}")

        async for msg in client.iter_messages(entity, limit=30):
            msg_count += 1
            results.batch_total += 1
            text = msg.text or ""

            if not text.strip():
                results.batch_empty += 1
                continue

            try:
                found = spider.process_message(msg, title)
                if found:
                    results.spider_new += len(found)
                    for d in found:
                        if db:
                            try:
                                await db.upsert_discovery(
                                    chat_id=d.chat_id,
                                    username=d.username,
                                    invite_link=d.invite_link,
                                    title=d.title,
                                    source_type=d.source_type,
                                    found_in_chat_id=getattr(entity, "id", None),
                                    status="new"
                                )
                            except Exception as dbe:
                                pass
                        results.spider_by_type[d.source_type] += 1
            except Exception as e:
                results.spider_errors.append(str(e))

            # Filter
            has_media = isinstance(msg.media, MessageMediaPhoto)
            fr = filters.check(text, has_media)

            if fr.passed:
                if db:
                    try:
                        if await db.is_text_exists(text):
                            print(f"     ⏭️ Пропуск (текст уже в базе)")
                            continue
                    except Exception:
                        pass
                
                results.filter_passed += 1
                results.filter_scores.append(fr.score)

                # AI: full pipeline (pre_screen → extract)
                try:
                    t0 = time.time()

                    # Stage 1: pre-screen
                    t_screen = time.time()
                    is_event = await analyzer.pre_screen(text, title)
                    results.ai_screen_latencies.append(time.time() - t_screen)
                    results.ai_screened += 1

                    if is_event:
                        results.ai_screen_passed += 1
                        # Stage 2: extract
                        t_extract = time.time()
                        ai_r = await analyzer.extract(text, title)
                        results.ai_extract_latencies.append(time.time() - t_extract)
                        results.ai_extracted += 1

                        if ai_r and ai_r.get("is_event"):
                            # Venue enrichment
                            if venue_enricher:
                                try:
                                    await venue_enricher.enrich_event(ai_r)
                                except Exception as ve:
                                    print(f"     ⚠️ Venue: {ve}")

                            # Normalize TBD dates to None
                            ev_date = ai_r.get("date")
                            if ev_date in ("TBD", "N/A", "", None):
                                ai_r["date"] = None
                            ev_time = ai_r.get("time")
                            if ev_time in ("TBD", "N/A", "", None):
                                ai_r["time"] = None

                            results.ai_events += 1
                            chat_events += 1
                            results.ai_events_list.append({
                                "title": ai_r.get("title", "?"),
                                "category": ai_r.get("category", "?"),
                                "date": ai_r.get("date", None),
                                "time": ai_r.get("time", None),
                                "location_name": ai_r.get("location_name", "TBD"),
                                "description": ai_r.get("description", ""),
                                "price_thb": ai_r.get("price_thb", 0),
                                "venue": ai_r.get("venue"),
                                "chat": title,
                                "score": fr.score,
                            })

                            # Save to events table (dedup via fingerprint)
                            if db:
                                ai_r["_meta"] = {
                                    "chat_id": entity.id,
                                    "chat_title": title,
                                    "message_id": getattr(msg, "id", None),
                                    "sender": "",
                                    "filter_score": fr.score,
                                    "detected_at": datetime.now().isoformat(),
                                    "original_text": text,
                                }
                                try:
                                    ev_id, is_new, has_image = await db.insert_event(ai_r, source="test")
                                    if ev_id:
                                        if is_new:
                                            print(f"     💾 Saved NEW event #{ev_id}")
                                        else:
                                            print(f"     ♻️ Updated event #{ev_id} (Fingerprint Duplicate)")
                                            
                                        if image_generator and (is_new or not has_image):
                                            _fire_and_forget(image_generator.generate_cover(
                                                raw_tg_text=text,
                                                category=ai_r.get("category", "Party"),
                                                event_id=ev_id
                                            ))
                                except Exception as dbe:
                                    print(f"     ⚠️ DB: {dbe}")

                            print(f"     🎯 {ai_r.get('title', '?')} [{ai_r.get('category', '?')}]")
                        else:
                            results.ai_not_events += 1
                    else:
                        results.ai_not_events += 1
                except Exception as e:
                    results.ai_errors.append(f"{title}: {e}")
                    print(f"     ⚠️ AI error: {type(e).__name__}: {str(e)[:80]}")
            else:
                results.filter_rejected += 1
                results.filter_reasons[fr.reason[:50]] += 1

        results.batch_per_chat[title] = msg_count
        print(f"     Msgs: {msg_count} | Filter: {results.filter_passed} | Events: {chat_events}")

    # Ждём live сообщений (с поддержкой Ctrl+C)
    print(f"\n  ⏳ Ожидание live-сообщений (60 сек, Ctrl+C для досрочного завершения)...")
    try:
        await asyncio.sleep(60)
    except asyncio.CancelledError:
        print(f"\n  ⏹️ Остановлено пользователем")

    # ─── Отчёт ───
    if _bg_tasks:
        print(f"\n  🎨 Ожидание завершения генерации {len(_bg_tasks)} обложек...")
        try:
            await asyncio.gather(*_bg_tasks, return_exceptions=True)
            print("  ✅ Все обложки сгенерированы!")
        except Exception as e:
            print(f"  ⚠️ Ошибка генератора обложек: {e}")

    print_report(analyzer)
    await save_report()

    # Cleanup
    analyzer.close()
    if venue_enricher:
        venue_enricher.close()
    spider.save()
    await client.disconnect()
    print("🔌 Telegram отключён")


def print_report(analyzer):
    """Красивый вывод отчёта."""
    r = results
    elapsed = r.elapsed

    print(f"\n\n{'=' * 70}")
    print(f" 📊 ОТЧЁТ ИНТЕГРАЦИОННОГО ТЕСТИРОВАНИЯ v2")
    print(f" Время: {elapsed:.0f} сек")
    print(f"{'=' * 70}")

    print(f"\n{Colors.BOLD}📋 chats.py{Colors.RESET}")
    status = "✅" if r.chats_resolved == r.chats_loaded else "⚠️"
    print(f"  {status} {r.chats_resolved}/{r.chats_loaded} чатов доступны")

    print(f"\n{Colors.BOLD}🕷️ spider.py{Colors.RESET}")
    print(f"  Batch: {r.spider_new} новых находок")
    if r.spider_by_type:
        for t, c in r.spider_by_type.most_common():
            print(f"    {t}: {c}")
    print(f"  Live: {r.live_spider_new} новых находок")
    if r.spider_errors:
        print(f"  ❌ Ошибки: {len(r.spider_errors)}")

    print(f"\n{Colors.BOLD}🔍 filters.py{Colors.RESET}")
    total_filtered = r.filter_passed + r.filter_rejected
    pass_rate = (r.filter_passed / total_filtered * 100) if total_filtered > 0 else 0
    print(f"  Прошло: {r.filter_passed}/{total_filtered} ({pass_rate:.1f}%)")
    if r.filter_scores:
        print(f"  Avg score: {sum(r.filter_scores)/len(r.filter_scores):.1f}, Max: {max(r.filter_scores)}")
    print(f"  Топ отсева:")
    for reason, count in r.filter_reasons.most_common(5):
        print(f"    {count:4d} — {reason}")

    print(f"\n{Colors.BOLD}🤖 ai_analyzer.py (2-stage){Colors.RESET}")
    print(f"  Stage 1 — Pre-screen ({analyzer.screen_model}):")
    print(f"    Screened: {r.ai_screened}")
    print(f"    Passed:   {r.ai_screen_passed}")
    reject_rate = ((r.ai_screened - r.ai_screen_passed) / max(1, r.ai_screened)) * 100
    print(f"    Rejected: {r.ai_screened - r.ai_screen_passed} ({reject_rate:.0f}%)")
    if r.ai_screen_latencies:
        print(f"    Avg latency: {sum(r.ai_screen_latencies)/len(r.ai_screen_latencies):.1f}с")

    print(f"  Stage 2 — Extract ({analyzer.model}):")
    print(f"    Extracted: {r.ai_extracted}")
    print(f"    Events:    {r.ai_events}")
    print(f"    Not event: {r.ai_not_events}")
    if r.ai_extract_latencies:
        print(f"    Avg latency: {sum(r.ai_extract_latencies)/len(r.ai_extract_latencies):.1f}с")

    if r.ai_errors:
        print(f"  ❌ Ошибки: {len(r.ai_errors)}")
        for e in r.ai_errors[:3]:
            print(f"    - {e[:100]}")

    # Internal stats
    analyzer.print_stats()

    if r.ai_events_list:
        print(f"\n  Найденные ивенты:")
        for ev in r.ai_events_list:
            print(f"    🎯 {ev['title']} [{ev['category']}] (из {ev['chat']}, score={ev['score']})")

    print(f"\n{Colors.BOLD}📡 Live Listener{Colors.RESET}")
    print(f"  Messages: {r.live_messages} | Filter: {r.live_filter_passed} | Events: {r.live_ai_events}")

    print(f"\n{Colors.BOLD}📈 Воронка{Colors.RESET}")
    print(f"  {r.batch_total} сообщений")
    print(f"  → {r.batch_total - r.batch_empty} с текстом")
    print(f"  → {r.filter_passed} прошло фильтр ({pass_rate:.1f}%)")
    print(f"  → {r.ai_screened} pre-screened")
    print(f"  → {r.ai_screen_passed} screen passed")
    print(f"  → {r.ai_extracted} extracted")
    print(f"  → {r.ai_events} ивентов найдено")

    print(f"\n{'=' * 70}\n")


async def save_report():
    """Сохранить результаты в PostgreSQL."""
    r = results
    report = {
        "timestamp": datetime.now().isoformat(),
        "elapsed_sec": r.elapsed,
        "chats": {"loaded": r.chats_loaded, "resolved": r.chats_resolved},
        "batch": {
            "total_messages": r.batch_total,
            "empty": r.batch_empty,
            "per_chat": dict(r.batch_per_chat),
        },
        "spider": {
            "new_found": r.spider_new,
            "by_type": dict(r.spider_by_type),
            "errors": r.spider_errors[:10],
            "live_new": r.live_spider_new,
        },
        "filters": {
            "passed": r.filter_passed,
            "rejected": r.filter_rejected,
            "pass_rate": (r.filter_passed / max(1, r.filter_passed + r.filter_rejected)) * 100,
            "avg_score": sum(r.filter_scores) / max(1, len(r.filter_scores)),
            "top_reasons": dict(r.filter_reasons.most_common(10)),
        },
        "ai": {
            "screened": r.ai_screened,
            "screen_passed": r.ai_screen_passed,
            "extracted": r.ai_extracted,
            "events": r.ai_events,
            "not_events": r.ai_not_events,
            "errors": r.ai_errors[:10],
            "events_list": r.ai_events_list,
            "avg_screen_latency": sum(r.ai_screen_latencies) / max(1, len(r.ai_screen_latencies)),
            "avg_extract_latency": sum(r.ai_extract_latencies) / max(1, len(r.ai_extract_latencies)),
        },
        "live": {
            "messages": r.live_messages,
            "filter_passed": r.live_filter_passed,
            "ai_events": r.live_ai_events,
            "spider_new": r.live_spider_new,
        },
    }

    if db:
        try:
            run_id = await db.save_test_run(report)
            print(f"🐘 Отчёт сохранён в PG (id={run_id})")
        except Exception as e:
            print(f"⚠️  PG save error: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⏹️  Тест прерван пользователем (Ctrl+C)")
