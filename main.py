import csv
import requests
from io import StringIO
import sqlite3
from telethon import TelegramClient, errors
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
from telethon.tl.functions.account import UpdateNotifySettingsRequest
from telethon.tl.types import InputPeerNotifySettings
import logging
import asyncio
from openai import OpenAI

import warnings

from CONFIG import api_id, api_hash, openai_client_c, phone_number, target_channel, csv_url

warnings.filterwarnings("ignore", category=UserWarning, module="urllib3")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("userbot.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

openai_client = OpenAI(api_key= openai_client_c)


client = TelegramClient('userbot_session', api_id, api_hash)
db_file = "channels.db"

# Интервалы проверки
table_scan_interval = 1200  # Проверка Google-таблицы
message_scan_interval = 600  # Проверка сообщений

def setup_database():
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE,
            chat_id INTEGER,
            last_message_id INTEGER DEFAULT 0
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS advertisements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id INTEGER UNIQUE,
            channel_username TEXT
        )
    """)
    conn.commit()
    conn.close()

def get_tracked_channels():
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT username, last_message_id FROM channels")
    channels = cursor.fetchall()
    conn.close()
    return channels

def add_channel_to_db(username, chat_id, last_message_id=0):
    """
    Добавляет канал в базу данных.
    """
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR IGNORE INTO channels (username, chat_id, last_message_id)
        VALUES (?, ?, ?)
    """, (username, chat_id, last_message_id))
    conn.commit()
    conn.close()

def update_last_message_id(channel_username, message_id):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("UPDATE channels SET last_message_id = ? WHERE username = ?", (message_id, channel_username))
    conn.commit()
    conn.close()

def is_advertisement_post(message_id):
    """
  Пометка рекламного поста
    """
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM advertisements WHERE message_id = ?", (message_id,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def add_advertisement_post(message_id, channel_username):
    """
    Добавляет ID рекламного поста в бд
    """
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO advertisements (message_id, channel_username) VALUES (?, ?)", (message_id, channel_username))
    conn.commit()
    conn.close()

async def is_advertisement(text):
    try:
        if isinstance(text, bytes):
            text = text.decode('utf-8', errors='ignore')  # Декодирует с игнорированием ошибок
        import re
        text = re.sub(r'[^\w\s.,!?а-яА-Я]', '', text)  # Удаляет эмодзи и другие специальные символы
        logging.info(f"Текст для анализа: {text}")

        if not text.strip():
            logging.warning("Текст пуст после очистки, скип")
            return False

        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Ты выступаешь в роли главного редактора криптовалютного журнала. "
                                              "Твоя задача — анализировать поступающие новости и посты, чтобы отобрать только те, которые имеют ценность для аудитории, "
                                              "а остальные отклонить как мусор."},

                {
                    "role": "user",
                    "content": (
                        "Твои критерии для отбора постов: "
                        "1. Содержательные посты (НЕ МУСОР): "
                        "• Новость или информация уникальна, связана с криптовалютами, блокчейном или трендами в экосистеме. "
                        "• Пост содержит конкретные данные, аналитику, прогнозы, факты или анонсы (например, запуск проектов, изменение цены, актуальные события). "
                        "• Есть ссылки на полезные материалы, примеры или подтверждение информации. "
                        "• Текст написан грамотно, не содержит избыточного самопиара, случайного набора мыслей или бессвязного текста. "
                        "2. Мусорные посты (МУСОР): "
                        "• Посты, рекламирующие розыгрыши, конкурсы, акции без явной пользы или ценности для аудитории. "
                        "• Личные рассуждения без конкретных выводов, рекомендаций или аналитики. "
                        "• Сильно ориентированные на самопиар, продвижение автора или проекта без фактического подтверждения его значимости. "
                        "• Посты, не относящиеся к криптовалютам или блокчейну. "
                        "Формат работы: "
                        "Для каждого поста: "
                        "• Определи, является ли он “МУСОР” или “НЕ МУСОР”. "
                        "• Если пост “НЕ МУСОР”, кратко объясни, в чем его ценность. "
                        "• Если пост “МУСОР”, объясни, почему он не подходит. "
                        "Пример анализа: "
                        "Пост: Зачем абузить розыгрыши? (и далее весь текст) "
                        "Ответ: "
                        "• МУСОР. Пост сфокусирован на розыгрышах и абузах, что не представляет уникальной ценности для аудитории. "
                        "Отсутствует конкретная аналитика или новостной контекст, текст содержит избыточный самопиар. "
                        "Пост: $PENGU - Новый нарратив для нового года (и далее весь текст) "
                        "Ответ: "
                        "• НЕ МУСОР. Пост предоставляет информацию о перспективном токене, содержит технический анализ, "
                        "данные о катализаторе (Abstract Chain), что делает его полезным для инвесторов и трейдеров. "
                        "Если текст соответствует хотя бы двум из перечисленных признаков, ответь 'Да'. Если текст не содержит "
                        "рекламного контента, ответь 'Нет'. "
                        f"Вот текст для анализа: {text}. Не забывай - ответ в твоем сообщении ТОЛЬКО 'да' или 'нет'."
                    )
                }
            ],
            max_tokens=10,
            temperature=0
        )
        result = response.choices[0].message.content.strip().lower()
        logging.info(f"Это реклама? Ответ нейроночки: {result}")
        return result == "да"
    except Exception as e:
        logging.error(f"Ошибка при обращении к OpenAI API: {e}")
        return False

async def process_channel(channel, last_message_id):
    try:
        messages = await client.get_messages(channel, min_id=last_message_id)
        for message in messages:
            if is_advertisement_post(message.id):
                logging.info(f"Пост {message.id} уже помечен как рекламный - скип")
                continue

            # Пропускаем посты с медиафайлами (видео, голосовые, фото и т.д.), если у них нет текста
            if (message.video or message.voice or message.photo or message.document) and not message.text:
                logging.info(f"Пост {message.id} содержит медиафайл и не имеет текста - скип")
                continue

            # Если у сообщения нет текста, но оно не содержит медиафайлов, пересылаем его
            if not message.text:
                try:
                    await message.forward_to(target_channel)
                    logging.info(f"Переслано сообщение без текста из {channel} в {target_channel}")
                except errors.FloodWaitError as e:
                    logging.warning(f"FloodWaitError: ожидание {e.seconds} секунд перед пересылкой из {channel}")
                    await asyncio.sleep(e.seconds)

                update_last_message_id(channel, message.id)
                continue

            message_text = message.text
            if isinstance(message_text, bytes):
                message_text = message_text.decode('utf-8')

            # Проверка на рекламу
            if await is_advertisement(message_text):
                logging.info(f"Рекламное сообщение обнаружено в {channel}: {message_text[:10]}...")
                add_advertisement_post(message.id, channel)
                continue

            # Пересылка сообщения с текстом
            try:
                await message.forward_to(target_channel)
                logging.info(f"Переслано сообщение из {channel} в {target_channel}")
            except errors.FloodWaitError as e:
                logging.warning(f"FloodWaitError: ожидание {e.seconds} секунд перед пересылкой из {channel}")
                await asyncio.sleep(e.seconds)

            # Обновление последнего обработанного ID
            update_last_message_id(channel, message.id)
    except Exception as e:
        logging.error(f"Ошибка при обработке сообщений из {channel}: {e}")

async def remove_channel(channel_username):

    """ удаляет канал из бд и отписывается от него """
    try:
        await client(LeaveChannelRequest(channel=channel_username))
        logging.info(f"Отписались от канала: {channel_username}")
    except Exception as e:
        logging.error(f"Ошибка при отписке от канала {channel_username}: {e}")

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM channels WHERE username = ?", (channel_username,))
    conn.commit()
    conn.close()
    logging.info(f"Канал {channel_username} удалён из базы данных")

async def fetch_channels():
    """
обновление базы данных
    """
    try:
        response = requests.get(csv_url)
        response.raise_for_status()
        csv_content = StringIO(response.text)
        csv_reader = csv.reader(csv_content, delimiter=',')
        google_channels = [row[0].strip() for row in csv_reader if row and row[0].strip().startswith('@')]

        db_channels = [channel[0] for channel in get_tracked_channels()]
        new_channels = set(google_channels) - set(db_channels)
        removed_channels = set(db_channels) - set(google_channels)


        # Добавляем новые каналы с задержкой
        for channel in new_channels:
            try:
                logging.info(f"Подписываемся на канал: {channel}")
                result = await client(JoinChannelRequest(channel))
                await asyncio.sleep(5)  # Задержка между подписками
                await client(UpdateNotifySettingsRequest(
                    peer=channel,
                    settings=InputPeerNotifySettings(mute_until=2**31 - 1)
                ))
                logging.info(f"Уведомления отключены для канала: {channel}")
                chat_id = result.chats[0].id

                # Получаем самый недавний пост как точку отсчёта
                messages = await client.get_messages(channel, limit=1)
                last_message_id = messages[0].id if messages else 0
                add_channel_to_db(channel, chat_id, last_message_id)
            except errors.FloodWaitError as e:
                logging.warning(f"FloodWaitError: ожидание {e.seconds} секунд перед подпиской на {channel}")
                await asyncio.sleep(e.seconds)
            except Exception as e:
                logging.error(f"Ошибка при подписке на канал {channel}: {e}")

        for channel in removed_channels:
            try:
                await remove_channel(channel)
            except Exception as e:
                logging.error(f"Ошибка при удалении канала {channel}: {e}")
    except Exception as e:
        logging.error(f"Ошибка при загрузке Google-таблицы: {e}")

async def fetch_unread_messages():
    channels = get_tracked_channels()
    for channel, last_message_id in channels:
        await process_channel(channel, last_message_id)
        await asyncio.sleep(5)  # Задержка между обработкой каналов

async def main():
    setup_database()
    await client.start(phone=phone_number)
    logging.info("Бот запущен!")

    while True:
        await fetch_channels()
        await fetch_unread_messages()
        await asyncio.sleep(table_scan_interval)

with client:
    client.loop.run_until_complete(main())