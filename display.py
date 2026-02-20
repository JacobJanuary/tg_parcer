"""
Общий модуль отображения — цвета, иконки, форматирование.
Используется в listener.py, review_discovered.py и др.
"""

from datetime import datetime


class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    MAGENTA = "\033[95m"
    WHITE = "\033[97m"
    BG_GREEN = "\033[42m"


CATEGORY_ICONS = {
    "Party": "🎉",
    "Sport": "🏃",
    "Business": "💼",
    "Education": "📚",
    "Chill": "🌴",
}

SOURCE_ICONS = {
    "forward": "🔀",
    "invite_link": "🔗",
    "public_link": "🌐",
    "mention": "💬",
}

STATUS_ICONS = {
    "new": "🆕",
    "approved": "✅",
    "rejected": "❌",
    "self": "📌",
}


def format_timestamp(dt: datetime) -> str:
    return dt.strftime("%H:%M:%S")


def print_event(event_data: dict, chat_title: str):
    """Красивый вывод обнаруженного ивента."""
    icon = CATEGORY_ICONS.get(event_data.get("category", ""), "📌")
    title = event_data.get("title", "N/A")
    category = event_data.get("category", "N/A")
    date = event_data.get("date", "TBD")
    time = event_data.get("time", "TBD")
    location = event_data.get("location_name", "TBD")
    price = event_data.get("price_thb", 0)
    summary = event_data.get("summary", "")

    price_str = f"{price}฿" if price > 0 else "FREE"

    print(f"\n{Colors.BG_GREEN}{Colors.WHITE}{Colors.BOLD} 🎯 EVENT DETECTED {Colors.RESET}")
    print(f"  {icon} {Colors.BOLD}{title}{Colors.RESET}  [{category}]")
    print(f"  📅 {date}  ⏰ {time}  💰 {price_str}")
    print(f"  📍 {location}")
    print(f"  💬 {Colors.DIM}{summary}{Colors.RESET}")
    print(f"  {Colors.DIM}от: {chat_title}{Colors.RESET}")
    print()
