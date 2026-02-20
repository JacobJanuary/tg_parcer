"""
Модуль экспорта данных в JSON и CSV.
"""

import json
import csv
import os


# Порядок колонок в CSV
CSV_FIELDS = [
    "id",
    "date",
    "sender_id",
    "sender_name",
    "text",
    "views",
    "forwards",
    "reply_to_msg_id",
    "media_type",
    "media_file",
]


def export_json(messages: list, filepath: str) -> str:
    """
    Экспорт сообщений в JSON.

    Args:
        messages: Список словарей с данными сообщений.
        filepath: Путь для сохранения файла.

    Returns:
        Абсолютный путь к созданному файлу.
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(messages, f, ensure_ascii=False, indent=2, default=str)

    print(f"📄 JSON сохранён: {filepath} ({len(messages)} сообщений)")
    return os.path.abspath(filepath)


def export_csv(messages: list, filepath: str) -> str:
    """
    Экспорт сообщений в CSV.

    Args:
        messages: Список словарей с данными сообщений.
        filepath: Путь для сохранения файла.

    Returns:
        Абсолютный путь к созданному файлу.
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=CSV_FIELDS,
            extrasaction="ignore",
            quoting=csv.QUOTE_ALL,
        )
        writer.writeheader()
        writer.writerows(messages)

    print(f"📊 CSV сохранён: {filepath} ({len(messages)} сообщений)")
    return os.path.abspath(filepath)
