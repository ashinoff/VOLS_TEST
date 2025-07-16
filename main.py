"""
ВОЛС Ассистент - Telegram бот для управления уведомлениями о бездоговорных ВОЛС
Версия: 2.1.0
"""

BOT_VERSION = "2.1.0"

import os
import logging
import csv
import io
import re
import json
import signal
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, InputFile
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
import pandas as pd
from io import BytesIO
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import asyncio
import aiohttp
import pytz

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Константы
BOT_TOKEN = os.environ.get('BOT_TOKEN')
WEBHOOK_URL = os.environ.get('WEBHOOK_URL')
PORT = int(os.environ.get('PORT', 5000))
ZONES_CSV_URL = os.environ.get('ZONES_CSV_URL')

# Email настройки
SMTP_SERVER = os.environ.get('SMTP_SERVER', 'smtp.mail.ru')
SMTP_PORT = int(os.environ.get('SMTP_PORT', '465'))
SMTP_EMAIL = os.environ.get('SMTP_EMAIL')
SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD')

# Московский часовой пояс
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

# Списки филиалов
ROSSETI_KUBAN_BRANCHES = [
    "Юго-Западные ЭС", "Усть-Лабинские ЭС", "Тимашевские ЭС", "Тихорецкие ЭС",
    "Славянские ЭС", "Ленинградские ЭС", "Лабинские ЭС",
    "Краснодарские ЭС", "Армавирские ЭС", "Адыгейские ЭС", "Сочинские ЭС"
]

ROSSETI_YUG_BRANCHES = [
    "Юго-Западные ЭС", "Центральные ЭС", "Западные ЭС", "Восточные ЭС",
    "Южные ЭС", "Северо-Восточные ЭС", "Юго-Восточные ЭС", "Северные ЭС"
]

# Хранилище уведомлений
notifications_storage = {
    'RK': [],
    'UG': []
}

# Состояния пользователей
user_states = {}

# Кеш данных пользователей
users_cache = {}
users_cache_backup = {}

# Последние сгенерированные отчеты
last_reports = {}

# Кэш документов
documents_cache = {}
documents_cache_time = {}

# Хранилище активности пользователей
user_activity = {}

# Словарь для отслеживания кто запускал бота
bot_users = {}

# Справочные документы
REFERENCE_DOCS = {
    'План по выручке ВОЛС на ВЛ 24-26 годы': os.environ.get('DOC_PLAN_VYRUCHKA_URL'),
    'Регламент ВОЛС': os.environ.get('DOC_REGLAMENT_VOLS_URL'),
    'Форма акта инвентаризации': os.environ.get('DOC_AKT_INVENTARIZACII_URL'),
    'Форма гарантийного письма': os.environ.get('DOC_GARANTIJNOE_PISMO_URL'),
    'Форма претензионного письма': os.environ.get('DOC_PRETENZIONNOE_PISMO_URL'),
    'Отчет по контрагентам': os.environ.get('DOC_OTCHET_KONTRAGENTY_URL'),
}

# URL руководства пользователя
USER_GUIDE_URL = os.environ.get('USER_GUIDE_URL', 'https://your-domain.com/vols-guide')

BOT_USERS_FILE = os.environ.get('BOT_USERS_FILE', 'bot_users.json')

def save_bot_users():
    """Сохранить данные о пользователях бота в файл"""
    try:
        serializable_data = {}
        for uid, data in bot_users.items():
            serializable_data[uid] = {
                'first_start': data['first_start'].isoformat() if isinstance(data['first_start'], datetime) else data['first_start'],
                'last_start': data['last_start'].isoformat() if isinstance(data['last_start'], datetime) else data['last_start'],
                'username': data.get('username', ''),
                'first_name': data.get('first_name', '')
            }
        
        with open(BOT_USERS_FILE, 'w', encoding='utf-8') as f:
            json.dump(serializable_data, f, ensure_ascii=False, indent=2)
        logger.info(f"Сохранено {len(bot_users)} пользователей бота в {BOT_USERS_FILE}")
    except Exception as e:
        logger.error(f"Ошибка сохранения данных пользователей бота: {e}")

def load_bot_users():
    """Загрузить данные о пользователях бота из файла"""
    global bot_users
    try:
        logger.info(f"Пытаемся загрузить данные пользователей из {BOT_USERS_FILE}")
        if os.path.exists(BOT_USERS_FILE):
            with open(BOT_USERS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            bot_users = {}
            for uid, user_data in data.items():
                try:
                    first_start = datetime.fromisoformat(user_data['first_start'])
                    last_start = datetime.fromisoformat(user_data['last_start'])
                    
                    if first_start.tzinfo is None:
                        first_start = MOSCOW_TZ.localize(first_start)
                    if last_start.tzinfo is None:
                        last_start = MOSCOW_TZ.localize(last_start)
                    
                    bot_users[uid] = {
                        'first_start': first_start,
                        'last_start': last_start,
                        'username': user_data.get('username', ''),
                        'first_name': user_data.get('first_name', '')
                    }
                except Exception as e:
                    logger.error(f"Ошибка загрузки данных пользователя {uid}: {e}")
            
            logger.info(f"Загружено {len(bot_users)} пользователей бота из файла")
        else:
            logger.info(f"Файл {BOT_USERS_FILE} не найден, начинаем с пустого списка")
    except Exception as e:
        logger.error(f"Ошибка загрузки данных пользователей бота: {e}")
        bot_users = {}

def get_moscow_time():
    """Получить текущее время в Москве"""
    return datetime.now(MOSCOW_TZ)

async def download_document(url: str) -> Optional[BytesIO]:
    """Скачать документ по URL"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    content = await response.read()
                    return BytesIO(content)
    except Exception as e:
        logger.error(f"Ошибка загрузки документа: {e}")
    return None

async def get_cached_document(doc_name: str, doc_url: str) -> Optional[BytesIO]:
    """Получить документ из кэша или загрузить"""
    now = datetime.now()
    
    if doc_name in documents_cache:
        cache_time = documents_cache_time.get(doc_name)
        if cache_time and (now - cache_time) < timedelta(hours=1):
            cached_doc = documents_cache[doc_name]
            cached_doc.seek(0)
            return BytesIO(cached_doc.read())
    
    logger.info(f"Загружаем документ {doc_name} из {doc_url}")
    
    if 'docs.google.com/document' in doc_url and '/d/' in doc_url:
        doc_id = doc_url.split('/d/')[1].split('/')[0]
        download_url = f"https://docs.google.com/document/d/{doc_id}/export?format=pdf"
    elif 'docs.google.com/spreadsheets' in doc_url and '/d/' in doc_url:
        doc_id = doc_url.split('/d/')[1].split('/')[0]
        download_url = f"https://docs.google.com/spreadsheets/d/{doc_id}/export?format=xlsx"
    elif 'drive.google.com' in doc_url and '/file/d/' in doc_url:
        file_id = doc_url.split('/file/d/')[1].split('/')[0]
        download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    else:
        download_url = doc_url
    
    document = await download_document(download_url)
    
    if document:
        document.seek(0)
        documents_cache[doc_name] = BytesIO(document.read())
        documents_cache_time[doc_name] = now
        document.seek(0)
        
    return document

def get_env_key_for_branch(branch: str, network: str, is_reference: bool = False) -> str:
    """Получить ключ переменной окружения для филиала"""
    logger.info(f"get_env_key_for_branch вызван с параметрами: branch='{branch}', network='{network}', is_reference={is_reference}")
    
    # НЕ нормализуем если филиал из прав пользователя
    # Нормализуем только если это филиал из списка (с "ЭС")
    if ' ЭС' in branch:
        normalized_branch = normalize_branch_name(branch)
        if normalized_branch != branch:
            logger.info(f"Филиал '{branch}' нормализован к '{normalized_branch}'")
            branch = normalized_branch
    
    translit_map = {
        'Юго-Западные': 'YUGO_ZAPADNYE',
        'Усть-Лабинские': 'UST_LABINSKIE', 
        'Тимашевские': 'TIMASHEVSKIE',
        'Тимашевский': 'TIMASHEVSKIE',
        'Тихорецкие': 'TIKHORETSKIE',
        'Тихорецкий': 'TIKHORETSKIE',
        'Сочинские': 'SOCHINSKIE',
        'Сочинский': 'SOCHINSKIE',
        'Славянские': 'SLAVYANSKIE',
        'Славянский': 'SLAVYANSKIE',
        'Ленинградские': 'LENINGRADSKIE',
        'Ленинградский': 'LENINGRADSKIE',
        'Лабинские': 'LABINSKIE',
        'Лабинский': 'LABINSKIE',
        'Краснодарские': 'KRASNODARSKIE',
        'Краснодарский': 'KRASNODARSKIE',
        'Армавирские': 'ARMAVIRSKIE',
        'Армавирский': 'ARMAVIRSKIE',
        'Адыгейские': 'ADYGEYSKIE',
        'Адыгейский': 'ADYGEYSKIE',
        'Центральные': 'TSENTRALNYE',
        'Центральный': 'TSENTRALNYE',
        'Западные': 'ZAPADNYE',
        'Западный': 'ZAPADNYE',
        'Восточные': 'VOSTOCHNYE',
        'Восточный': 'VOSTOCHNYE',
        'Южные': 'YUZHNYE',
        'Южный': 'YUZHNYE',
        'Северо-Восточные': 'SEVERO_VOSTOCHNYE',
        'Северо-Восточный': 'SEVERO_VOSTOCHNYE',
        'Юго-Восточные': 'YUGO_VOSTOCHNYE',
        'Юго-Восточный': 'YUGO_VOSTOCHNYE',
        'Северные': 'SEVERNYE',
        'Северный': 'SEVERNYE',
        'Туапсинские': 'TUAPSINSKIE',
        'Туапсинский': 'TUAPSINSKIE'
    }
    
    branch_clean = branch.replace(' ЭС', '').strip()
    logger.info(f"Очищенное название филиала: '{branch_clean}'")
    
    branch_key = translit_map.get(branch_clean, branch_clean.upper().replace(' ', '_').replace('-', '_'))
    logger.info(f"Ключ после транслитерации: '{branch_key}'")
    
    suffix = f"_{network}_SP" if is_reference else f"_{network}"
    env_key = f"{branch_key}_URL{suffix}"
    logger.info(f"Итоговый ключ переменной окружения: {env_key}")
    return env_key

def load_csv_from_url(url: str) -> List[Dict]:
    """Загрузить CSV файл по URL"""
    try:
        logger.info(f"Загружаем CSV из {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        response.encoding = 'utf-8-sig'
        
        csv_file = io.StringIO(response.text)
        reader = csv.DictReader(csv_file)
        
        data = []
        for row in reader:
            normalized_row = {key.strip(): value.strip() if value else '' for key, value in row.items()}
            data.append(normalized_row)
        
        logger.info(f"Успешно загружено {len(data)} строк из CSV")
        return data
    except requests.exceptions.Timeout:
        logger.error(f"Таймаут при загрузке CSV из {url}")
        return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка сети при загрузке CSV: {e}")
        return []
    except Exception as e:
        logger.error(f"Ошибка загрузки CSV: {e}", exc_info=True)
        return []

def load_users_data():
    """Загрузить данные пользователей из CSV"""
    global users_cache, users_cache_backup
    try:
        if not ZONES_CSV_URL:
            logger.error("ZONES_CSV_URL не задан в переменных окружения!")
            return
            
        logger.info(f"Начинаем загрузку данных из {ZONES_CSV_URL}")
        data = load_csv_from_url(ZONES_CSV_URL)
        
        if not data:
            logger.error("Получен пустой список данных из CSV")
            if users_cache_backup:
                logger.warning("Используем резервную копию данных пользователей")
                users_cache = users_cache_backup.copy()
            return
            
        if users_cache:
            users_cache_backup = users_cache.copy()
            
        users_cache = {}
        
        if data:
            logger.info(f"Структура CSV (первая строка): {list(data[0].keys())}")
        
        for row in data:
            telegram_id = row.get('Telegram ID', '').strip()
            if telegram_id:
                name_parts = []
                fio = row.get('ФИО', '').strip()
                
                if 'Фамилия' in row:
                    surname = row.get('Фамилия', '').strip()
                else:
                    surname = ''
                    if telegram_id in ['248207151', '1409325335']:
                        logger.warning("Колонка 'Фамилия' отсутствует в CSV файле")
                
                if fio:
                    name_parts.append(fio)
                if surname:
                    name_parts.append(surname)
                
                full_name = ' '.join(name_parts) if name_parts else 'Неизвестный'
                
                users_cache[telegram_id] = {
                    'visibility': row.get('Видимость', '').strip(),
                    'branch': row.get('Филиал', '').strip(),
                    'res': row.get('РЭС', '').strip(),
                    'name': full_name,
                    'name_without_surname': fio if fio else 'Неизвестный',
                    'responsible': row.get('Ответственный', '').strip(),
                    'email': row.get('Email', '').strip()
                }
        
        if users_cache:
            users_cache_backup = users_cache.copy()
            
        logger.info(f"Загружено {len(users_cache)} пользователей")
        
        if users_cache:
            sample_users = list(users_cache.items())[:3]
            for uid, udata in sample_users:
                logger.info(f"Пример пользователя: ID={uid}, visibility={udata.get('visibility')}, name={udata.get('name')}, name_no_surname={udata.get('name_without_surname')}")
                
    except Exception as e:
        logger.error(f"Ошибка загрузки данных пользователей: {e}", exc_info=True)
        if users_cache_backup:
            logger.warning("Восстанавливаем данные из резервной копии после ошибки")
            users_cache = users_cache_backup.copy()

def get_user_permissions(user_id: str) -> Dict:
    """Получить права пользователя"""
    if not users_cache:
        logger.warning(f"users_cache пустой при запросе прав для пользователя {user_id}, пытаемся загрузить")
        load_users_data()
    
    user_data = users_cache.get(str(user_id), {
        'visibility': None,
        'branch': None,
        'res': None,
        'name': 'Неизвестный',
        'name_without_surname': 'Неизвестный',
        'responsible': None
    })
    
    logger.info(f"Права пользователя {user_id}: visibility={user_data.get('visibility')}, branch={user_data.get('branch')}")
    
    return user_data

def normalize_branch_name(branch_name: str) -> str:
    """Нормализует название филиала к стандартному формату"""
    # Если уже нормализовано (из списка филиалов) - возвращаем как есть
    if branch_name in ROSSETI_KUBAN_BRANCHES or branch_name in ROSSETI_YUG_BRANCHES:
        return branch_name
    
    singular_to_plural = {
        'Тимашевский': 'Тимашевские',
        'Тихорецкий': 'Тихорецкие',
        'Сочинский': 'Сочинские',
        'Славянский': 'Славянские',
        'Ленинградский': 'Ленинградские',
        'Лабинский': 'Лабинские',
        'Краснодарский': 'Краснодарские',
        'Армавирский': 'Армавирские',
        'Адыгейский': 'Адыгейские',
        'Центральный': 'Центральные',
        'Западный': 'Западные',
        'Восточный': 'Восточные',
        'Южный': 'Южные',
        'Северо-Восточный': 'Северо-Восточные',
        'Юго-Восточный': 'Юго-Восточные',
        'Северный': 'Северные',
        'Юго-Западный': 'Юго-Западные',
        'Усть-Лабинский': 'Усть-Лабинские',
        'Туапсинский': 'Туапсинские'
    }
    
    branch_clean = branch_name.replace(' ЭС', '').strip()
    
    if branch_clean in singular_to_plural:
        normalized = singular_to_plural[branch_clean]
        return f"{normalized} ЭС" if ' ЭС' in branch_name else normalized
    
    return branch_name

def normalize_tp_name(name: str) -> str:
    """Нормализовать название ТП для поиска"""
    return ''.join(filter(str.isdigit, name))

def search_tp_in_data(tp_query: str, data: List[Dict], column: str) -> List[Dict]:
    """Поиск ТП в данных"""
    normalized_query = normalize_tp_name(tp_query)
    results = []
    
    for row in data:
        tp_name = row.get(column, '')
        normalized_tp = normalize_tp_name(tp_name)
        
        if normalized_query in normalized_tp:
            results.append(row)
    
    return results

def update_user_activity(user_id: str):
    """Обновить активность пользователя"""
    if user_id not in user_activity:
        user_activity[user_id] = {'last_activity': get_moscow_time(), 'count': 0}
    user_activity[user_id]['last_activity'] = get_moscow_time()

def get_main_keyboard(permissions: Dict) -> ReplyKeyboardMarkup:
    """Получить главную клавиатуру в зависимости от прав"""
    keyboard = []
    
    visibility = permissions.get('visibility')
    branch = permissions.get('branch')
    res = permissions.get('res')
    
    # РОССЕТИ кнопки
    if visibility == 'All':
        keyboard.append(['🏢 РОССЕТИ КУБАНЬ'])
        keyboard.append(['🏢 РОССЕТИ ЮГ'])
    elif visibility == 'RK':
        keyboard.append(['🏢 РОССЕТИ КУБАНЬ'])
    elif visibility == 'UG':
        keyboard.append(['🏢 РОССЕТИ ЮГ'])
    
    # Телефоны контрагентов
    keyboard.append(['📞 ТЕЛЕФОНЫ КОНТРАГЕНТОВ'])
    
    # Отчеты
    if branch == 'All' and visibility in ['All', 'RK', 'UG']:
        keyboard.append(['📊 ОТЧЕТЫ'])
    
    # Справка
    keyboard.append(['ℹ️ СПРАВКА'])
    
    # Персональные настройки
    keyboard.append(['⚙️ МОИ НАСТРОЙКИ'])
    
    # Администрирование
    if visibility == 'All':
        keyboard.append(['🛠 АДМИНИСТРИРОВАНИЕ'])
         
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_branch_keyboard(branches: List[str]) -> ReplyKeyboardMarkup:
    """Получить клавиатуру с филиалами"""
    keyboard = []
    
    if len(branches) == 11:  # РОССЕТИ КУБАНЬ
        for i in range(0, 10, 2):
            keyboard.append([f'⚡ {branches[i]}', f'⚡ {branches[i+1]}'])
        keyboard.append([f'⚡ {branches[10]}'])
    elif len(branches) == 8:  # РОССЕТИ ЮГ  
        for i in range(0, 8, 2):
            keyboard.append([f'⚡ {branches[i]}', f'⚡ {branches[i+1]}'])
    else:
        for branch in branches:
            keyboard.append([f'⚡ {branch}'])
    
    keyboard.append(['⬅️ Назад'])
    
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_branch_menu_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура меню филиала"""
    keyboard = [
        ['🔍 Поиск по ТП'],
        ['📨 Отправить уведомление'],
        ['ℹ️ Справка'],
        ['⬅️ Назад']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_reports_keyboard(permissions: Dict) -> ReplyKeyboardMarkup:
    """Клавиатура отчетов"""
    keyboard = []
    visibility = permissions.get('visibility')
    
    if visibility == 'All':
        keyboard.append(['📊 Уведомления РОССЕТИ КУБАНЬ'])
        keyboard.append(['📊 Уведомления РОССЕТИ ЮГ'])
        keyboard.append(['📈 Активность РОССЕТИ КУБАНЬ'])
        keyboard.append(['📈 Активность РОССЕТИ ЮГ'])
    elif visibility == 'RK':
        keyboard.append(['📊 Уведомления РОССЕТИ КУБАНЬ'])
        keyboard.append(['📈 Активность РОССЕТИ КУБАНЬ'])
    elif visibility == 'UG':
        keyboard.append(['📊 Уведомления РОССЕТИ ЮГ'])
        keyboard.append(['📈 Активность РОССЕТИ ЮГ'])
    
    keyboard.append(['⬅️ Назад'])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_settings_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура персональных настроек"""
    keyboard = [
        ['📖 Руководство пользователя'],
        ['ℹ️ Моя информация'],
        ['⬅️ Назад']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_admin_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура администрирования"""
    keyboard = [
        ['📊 СТАТУС ПОЛЬЗОВАТЕЛЕЙ'],
        ['🔄 УВЕДОМИТЬ О ПЕРЕЗАПУСКЕ'],
        ['📢 МАССОВАЯ РАССЫЛКА'],
        ['⬅️ Назад']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_reference_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура справки с документами"""
    keyboard = []
    
    for doc_name, doc_url in REFERENCE_DOCS.items():
        if doc_url:
            button_text = doc_name
            if len(doc_name) > 30:
                if 'План по выручке' in doc_name:
                    button_text = '📊 План выручки ВОЛС 24-26'
                elif 'Форма акта инвентаризации' in doc_name:
                    button_text = '📄 Акт инвентаризации'
                elif 'Форма гарантийного письма' in doc_name:
                    button_text = '📄 Гарантийное письмо'
                elif 'Форма претензионного письма' in doc_name:
                    button_text = '📄 Претензионное письмо'
                else:
                    button_text = f'📄 {doc_name[:27]}...'
            else:
                button_text = f'📄 {doc_name}'
            
            keyboard.append([button_text])
    
    keyboard.append(['⬅️ Назад'])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_document_action_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура действий с документом"""
    keyboard = [
        ['📧 Отправить себе на почту'],
        ['⬅️ Назад']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_after_search_keyboard(tp_name: str = None) -> ReplyKeyboardMarkup:
    """Клавиатура после результатов поиска"""
    keyboard = [
        ['🔍 Новый поиск']
    ]
    
    if tp_name:
        keyboard.append([f'📨 Отправить уведомление по {tp_name}'])
    else:
        keyboard.append(['📨 Отправить уведомление'])
    
    keyboard.append(['⬅️ Назад'])
    
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_report_action_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура действий с отчетом"""
    keyboard = [
        ['📧 Отправить себе на почту'],
        ['⬅️ Назад']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

async def send_email(to_email: str, subject: str, body: str, attachment_data: BytesIO = None, attachment_name: str = None):
    """Отправка email через SMTP"""
    if not SMTP_EMAIL or not SMTP_PASSWORD:
        logger.error("Email настройки не заданы")
        return False
    
    try:
        msg = MIMEMultipart()
        msg['From'] = SMTP_EMAIL
        msg['To'] = to_email
        msg['Subject'] = subject
        
        msg.attach(MIMEText(body, 'plain', 'utf-8'))
        
        if attachment_data and attachment_name:
            attachment_data.seek(0)
            
            if attachment_name.endswith('.xlsx'):
                mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                mime_subtype = 'xlsx'
            elif attachment_name.endswith('.xls'):
                mime_type = 'application/vnd.ms-excel'
                mime_subtype = 'xls'
            elif attachment_name.endswith('.pdf'):
                mime_type = 'application/pdf'
                mime_subtype = 'pdf'
            elif attachment_name.endswith('.doc'):
                mime_type = 'application/msword'
                mime_subtype = 'doc'
            elif attachment_name.endswith('.docx'):
                mime_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                mime_subtype = 'docx'
            else:
                mime_type = 'application/octet-stream'
                mime_subtype = 'octet-stream'
            
            part = MIMEBase('application', mime_subtype)
            part.set_payload(attachment_data.read())
            encoders.encode_base64(part)
            part.add_header('Content-Type', mime_type)
            part.add_header('Content-Disposition', f'attachment; filename="{attachment_name}"')
            msg.attach(part)
        
        if SMTP_PORT == 465:
            import ssl
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context) as server:
                server.login(SMTP_EMAIL, SMTP_PASSWORD)
                server.send_message(msg)
        else:
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_EMAIL, SMTP_PASSWORD)
                server.send_message(msg)
        
        logger.info(f"Email успешно отправлен на {to_email}")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка отправки email на {to_email}: {e}")
        return False

# ========== ОБРАБОТЧИКИ ==========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user_id = str(update.effective_user.id)
    
    logger.info(f"Команда /start от пользователя {user_id} ({update.effective_user.first_name})")
    logger.info(f"Размер users_cache: {len(users_cache)}")
    
    current_time = get_moscow_time()
    if user_id not in bot_users:
        bot_users[user_id] = {
            'first_start': current_time,
            'last_start': current_time,
            'username': update.effective_user.username or '',
            'first_name': update.effective_user.first_name or ''
        }
    else:
        bot_users[user_id]['last_start'] = current_time
    
    save_bot_users()
    
    permissions = get_user_permissions(user_id)
    
    logger.info(f"Пользователь {user_id} ({update.effective_user.first_name}): visibility={permissions.get('visibility')}, branch={permissions.get('branch')}")
    
    if not permissions['visibility']:
        await update.message.reply_text(
            f"❌ У вас нет доступа к боту.\n"
            f"Ваш ID: {user_id}\n"
            f"Обратитесь к администратору для получения прав."
        )
        return
    
    user_states[user_id] = {'state': 'main'}
    
    await update.message.reply_text(
        f"👋 Добро пожаловать, {permissions.get('name_without_surname', permissions.get('name', 'Пользователь'))}!",
        reply_markup=get_main_keyboard(permissions)
    )

async def send_notification(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправить уведомление ответственным лицам"""
    user_id = str(update.effective_user.id)
    user_data = user_states.get(user_id, {})
    
    sender_info = get_user_permissions(user_id)
    
    tp_data = user_data.get('tp_data', {})
    selected_tp = user_data.get('selected_tp')
    selected_vl = user_data.get('selected_vl')
    location = user_data.get('location', {})
    photo_id = user_data.get('photo_id')
    comment = user_data.get('comment', '')
    
    logger.info(f"Отправка уведомления от пользователя {user_id}")
    logger.info(f"ТП: {selected_tp}, ВЛ: {selected_vl}")
    logger.info(f"tp_data: {tp_data}")
    
    branch_from_reference = tp_data.get('Филиал', '').strip()
    res_from_reference = tp_data.get('РЭС', '').strip()
    
    branch = user_data.get('branch')
    network = user_data.get('network')
    
    # Если branch не найден в состоянии, берем из прав пользователя
    if not branch:
        sender_permissions = get_user_permissions(user_id)
        branch = sender_permissions.get('branch')
        logger.warning(f"Branch не найден в состоянии, используем из прав пользователя: {branch}")
    
    # Если network не найден, определяем по branch
    if not network:
        if branch in ROSSETI_KUBAN_BRANCHES or any(branch.startswith(b.replace(' ЭС', '')) for b in ROSSETI_KUBAN_BRANCHES):
            network = 'RK'
        else:
            network = 'UG'
        logger.warning(f"Network не найден в состоянии, определен как: {network}")
    
    sending_messages = [
        "📨 Подготовка уведомления...",
        "🔍 Поиск ответственных лиц...",
        "📤 Отправка уведомлений...",
        "📧 Отправка email...",
        "✅ Почти готово..."
    ]
    
    loading_msg = await update.message.reply_text(sending_messages[0])
    
    for msg_text in sending_messages[1:]:
        await asyncio.sleep(0.5)
        try:
            await loading_msg.edit_text(msg_text)
        except Exception:
            pass
    
    responsible_users = []
    
    logger.info(f"Ищем ответственных для:")
    logger.info(f"  Филиал из справочника: '{branch_from_reference}'")
    logger.info(f"  РЭС из справочника: '{res_from_reference}'")
    
    for uid, udata in users_cache.items():
        responsible_for = udata.get('responsible', '').strip()
        
        if not responsible_for:
            continue
            
        if responsible_for == branch_from_reference or responsible_for == res_from_reference:
            responsible_users.append({
                'id': uid,
                'name': udata.get('name', 'Неизвестный'),
                'email': udata.get('email', ''),
                'responsible_for': responsible_for
            })
            logger.info(f"Найден ответственный: {udata.get('name')} (ID: {uid}) - отвечает за '{responsible_for}'")
    
    moscow_time = get_moscow_time()
    notification_text = f"""🚨 НОВОЕ УВЕДОМЛЕНИЕ О БЕЗДОГОВОРНОМ ВОЛС

📍 Филиал: {branch}
📍 РЭС: {res_from_reference}
📍 ТП: {selected_tp}
⚡ ВЛ: {selected_vl}

👤 Отправитель: {sender_info['name']}
🕐 Время: {moscow_time.strftime('%d.%m.%Y %H:%M')} МСК"""

    if location:
        lat = location.get('latitude')
        lon = location.get('longitude')
        notification_text += f"\n📍 Координаты: {lat:.6f}, {lon:.6f}"
        notification_text += f"\n🗺 [Открыть на карте](https://maps.google.com/?q={lat},{lon})"
    
    if comment:
        notification_text += f"\n\n💬 Комментарий: {comment}"
    
    recipients_info = ", ".join([u['name'] for u in responsible_users]) if responsible_users else "Не найдены"
    
    notification_data = {
        'branch': branch,
        'res': res_from_reference,
        'tp': selected_tp,
        'vl': selected_vl,
        'sender_name': sender_info['name'],
        'sender_id': user_id,
        'recipient_name': recipients_info,
        'recipient_id': ", ".join([u['id'] for u in responsible_users]) if responsible_users else 'Не найдены',
        'datetime': moscow_time.strftime('%d.%m.%Y %H:%M'),
        'coordinates': f"{location.get('latitude', 0):.6f}, {location.get('longitude', 0):.6f}" if location else 'Не указаны',
        'comment': comment,
        'has_photo': bool(photo_id)
    }
    
    notifications_storage[network].append(notification_data)
    
    if user_id not in user_activity:
        user_activity[user_id] = {'last_activity': get_moscow_time(), 'count': 0}
    user_activity[user_id]['count'] += 1
    user_activity[user_id]['last_activity'] = get_moscow_time()
    
    success_count = 0
    email_success_count = 0
    failed_users = []
    
    for responsible in responsible_users:
        try:
            await context.bot.send_message(
                chat_id=responsible['id'],
                text=notification_text,
                parse_mode='Markdown'
            )
            
            if location:
                await context.bot.send_location(
                    chat_id=responsible['id'],
                    latitude=location.get('latitude'),
                    longitude=location.get('longitude')
                )
            
            if photo_id:
                await context.bot.send_photo(
                    chat_id=responsible['id'],
                    photo=photo_id,
                    caption=f"Фото с {selected_tp}"
                )
            
            success_count += 1
            
            if responsible['email']:
                email_subject = f"ВОЛС: Уведомление от {sender_info['name']}"
                email_body = f"""Добрый день, {responsible['name']}!

Получено новое уведомление о бездоговорном ВОЛС.

Филиал: {branch}
РЭС: {res_from_reference}
ТП: {selected_tp}
ВЛ: {selected_vl}

Отправитель: {sender_info['name']}
Время: {moscow_time.strftime('%d.%m.%Y %H:%M')} МСК"""

                if location:
                    lat = location.get('latitude')
                    lon = location.get('longitude')
                    email_body += f"\n\nКоординаты: {lat:.6f}, {lon:.6f}"
                    email_body += f"\nСсылка на карту: https://maps.google.com/?q={lat},{lon}"
                
                if comment:
                    email_body += f"\n\nКомментарий: {comment}"
                    
                if photo_id:
                    email_body += f"\n\nК уведомлению приложено фото (доступно в Telegram)"
                
                email_body += f"""

Для просмотра деталей и фотографий откройте Telegram.

С уважением,
Бот ВОЛС Ассистент"""
                
                email_sent = await send_email(responsible['email'], email_subject, email_body)
                if email_sent:
                    email_success_count += 1
                    logger.info(f"Email успешно отправлен для {responsible['name']} на {responsible['email']}")
                else:
                    logger.error(f"Не удалось отправить email для {responsible['name']} на {responsible['email']}")
                
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления пользователю {responsible['name']} ({responsible['id']}): {e}")
            failed_users.append(f"{responsible['name']} ({responsible['id']}): {str(e)}")
    
    await loading_msg.delete()
    
    if responsible_users:
        if success_count == len(responsible_users):
            result_text = f"""✅ Уведомления успешно отправлены!

📨 Получатели ({success_count}):"""
            for responsible in responsible_users:
                result_text += f"\n• {responsible['name']} (отвечает за {responsible['responsible_for']})"
            
            if email_success_count > 0:
                result_text += f"\n\n📧 Email отправлено: {email_success_count} из {len([r for r in responsible_users if r['email']])}"
        else:
            result_text = f"""⚠️ Уведомления отправлены частично

✅ Успешно: {success_count} из {len(responsible_users)}
📧 Email отправлено: {email_success_count}

❌ Ошибки:"""
            for failed in failed_users:
                result_text += f"\n• {failed}"
    else:
        result_text = f"""❌ Ответственные не найдены

Для данной ТП не назначены ответственные лица.
Уведомление сохранено в системе и будет доступно в отчетах.

Отладочная информация:
- Филиал из справочника: "{branch_from_reference}"
- РЭС из справочника: "{res_from_reference}"
- Всего пользователей в базе: {len(users_cache)}"""
    
    user_states[user_id] = {'state': f'branch_{branch}', 'branch': branch, 'network': network}
    
    await update.message.reply_text(
        result_text,
        reply_markup=get_branch_menu_keyboard()
    )

async def generate_report(update: Update, context: ContextTypes.DEFAULT_TYPE, network: str, permissions: Dict):
    """Генерация отчета"""
    try:
        user_id = str(update.effective_user.id)
        
        if permissions['branch'] != 'All':
            await update.message.reply_text("❌ У вас нет доступа к отчетам")
            return
        
        notifications = notifications_storage[network]
        
        if not notifications:
            await update.message.reply_text("📊 Нет данных для отчета")
            return
        
        report_messages = [
            "📊 Собираю данные...",
            "📈 Формирую статистику...",
            "📝 Создаю таблицы...",
            "🎨 Оформляю отчет...",
            "💾 Сохраняю файл..."
        ]
        
        loading_msg = await update.message.reply_text(report_messages[0])
        
        for msg_text in report_messages[1:]:
            await asyncio.sleep(0.5)
            try:
                await loading_msg.edit_text(msg_text)
            except Exception:
                pass
        
        df = pd.DataFrame(notifications)
        
        required_columns = ['branch', 'res', 'tp', 'vl', 'sender_name', 'recipient_name', 'datetime', 'coordinates']
        existing_columns = [col for col in required_columns if col in df.columns]
        
        if not existing_columns:
            await loading_msg.delete()
            await update.message.reply_text("📊 Недостаточно данных для формирования отчета")
            return
            
        df = df[existing_columns]
        
        column_mapping = {
            'branch': 'ФИЛИАЛ',
            'res': 'РЭС',
            'tp': 'ТП',
            'vl': 'ВЛ',
            'sender_name': 'ФИО ОТПРАВИТЕЛЯ',
            'recipient_name': 'ФИО ПОЛУЧАТЕЛЯ',
            'datetime': 'ВРЕМЯ ДАТА',
            'coordinates': 'КООРДИНАТЫ'
        }
        df.rename(columns=column_mapping, inplace=True)
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Уведомления', index=False)
            
            workbook = writer.book
            worksheet = writer.sheets['Уведомления']
            
            header_format = workbook.add_format({
                'bg_color': '#FFE6E6',
                'bold': True,
                'text_wrap': True,
                'valign': 'vcenter',
                'align': 'center',
                'border': 1
            })
            
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            for i, col in enumerate(df.columns):
                column_len = df[col].astype(str).map(len).max()
                column_len = max(column_len, len(col)) + 2
                worksheet.set_column(i, i, column_len)
        
        output.seek(0)
        
        await loading_msg.delete()
        
        network_name = "РОССЕТИ КУБАНЬ" if network == 'RK' else "РОССЕТИ ЮГ"
        moscow_time = get_moscow_time()
        filename = f"Уведомления_{network_name}_{moscow_time.strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        user_states[user_id]['last_report'] = {
            'data': output.getvalue(),
            'filename': filename,
            'caption': f"📊 Отчет по уведомлениям {network_name}\n🕐 Сформировано: {moscow_time.strftime('%d.%m.%Y %H:%M')} МСК"
        }
        user_states[user_id]['state'] = 'report_actions'
        
        await update.message.reply_document(
            document=InputFile(output, filename=filename),
            caption=f"📊 Отчет по уведомлениям {network_name}\n🕐 Сформировано: {moscow_time.strftime('%d.%m.%Y %H:%M')} МСК",
            reply_markup=get_report_action_keyboard()
        )
                
    except Exception as e:
        logger.error(f"Ошибка генерации отчета: {e}")
        if 'loading_msg' in locals():
            await loading_msg.delete()
        await update.message.reply_text(f"❌ Ошибка генерации отчета: {str(e)}")

async def generate_activity_report(update: Update, context: ContextTypes.DEFAULT_TYPE, network: str, permissions: Dict):
    """Генерация отчета по активности пользователей с полным реестром"""
    try:
        user_id = str(update.effective_user.id)
        
        if permissions['branch'] != 'All':
            await update.message.reply_text("❌ У вас нет доступа к отчетам")
            return
        
        loading_msg = await update.message.reply_text("📈 Формирую полный отчет активности...")
        
        all_users_data = []
        
        for uid, user_info in users_cache.items():
            if user_info.get('visibility') not in ['All', network]:
                continue
            
            activity = user_activity.get(uid, None)
            
            if activity:
                is_active = True
                notification_count = activity['count']
                last_activity = activity['last_activity'].strftime('%d.%m.%Y %H:%M')
            else:
                is_active = False
                notification_count = 0
                last_activity = 'Нет активности'
            
            all_users_data.append({
                'ФИО': user_info.get('name', 'Не указано'),
                'Филиал': user_info.get('branch', '-'),
                'РЭС': user_info.get('res', '-'),
                'Ответственный': user_info.get('responsible', '-'),
                'Email': user_info.get('email', '-'),
                'Статус': 'Активный' if is_active else 'Неактивный',
                'Количество уведомлений': notification_count,
                'Последняя активность': last_activity
            })
        
        if not all_users_data:
            await loading_msg.delete()
            await update.message.reply_text("📈 Нет данных для отчета")
            return
        
        df = pd.DataFrame(all_users_data)
        df = df.sort_values(['Статус', 'Количество уведомлений'], ascending=[True, False])
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Активность пользователей', index=False)
            
            workbook = writer.book
            worksheet = writer.sheets['Активность пользователей']
            
            header_format = workbook.add_format({
                'bg_color': '#4B4B4B',
                'font_color': 'white',
                'bold': True,
                'text_wrap': True,
                'valign': 'vcenter',
                'align': 'center',
                'border': 1
            })
            
            active_format = workbook.add_format({
                'bg_color': '#E8F5E9',
                'border': 1
            })
            
            inactive_format = workbook.add_format({
                'bg_color': '#FFEBEE',
                'border': 1
            })
            
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            for row_num, (index, row) in enumerate(df.iterrows(), start=1):
                cell_format = active_format if row['Статус'] == 'Активный' else inactive_format
                for col_num, value in enumerate(row):
                    worksheet.write(row_num, col_num, value, cell_format)
            
            for i, col in enumerate(df.columns):
                column_len = df[col].astype(str).map(len).max()
                column_len = max(column_len, len(col)) + 2
                worksheet.set_column(i, i, min(column_len, 40))
            
            worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
        
        output.seek(0)
        
        await loading_msg.delete()
        
        active_count = len(df[df['Статус'] == 'Активный'])
        inactive_count = len(df[df['Статус'] == 'Неактивный'])
        
        network_name = "РОССЕТИ КУБАНЬ" if network == 'RK' else "РОССЕТИ ЮГ"
        moscow_time = get_moscow_time()
        filename = f"Полный_реестр_активности_{network_name}_{moscow_time.strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        caption = f"""📈 Полный отчет активности {network_name}

👥 Всего пользователей: {len(df)}
✅ Активных: {active_count} (зеленый)
❌ Неактивных: {inactive_count} (красный)

📊 Отчет содержит полный реестр пользователей с цветовой индикацией активности
🕐 Сформировано: {moscow_time.strftime('%d.%m.%Y %H:%M')} МСК"""
        
        user_states[user_id]['last_report'] = {
            'data': output.getvalue(),
            'filename': filename,
            'caption': caption
        }
        user_states[user_id]['state'] = 'report_actions'
        
        await update.message.reply_document(
            document=InputFile(output, filename=filename),
            caption=caption,
            reply_markup=get_report_action_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Ошибка генерации отчета активности: {e}")
        if 'loading_msg' in locals():
            await loading_msg.delete()
        await update.message.reply_text(f"❌ Ошибка генерации отчета: {str(e)}")

async def generate_ping_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Генерация отчета статуса пользователей - кто заходил в бота"""
    try:
        user_id = str(update.effective_user.id)
        permissions = get_user_permissions(user_id)
        
        if permissions.get('visibility') != 'All':
            await update.message.reply_text("❌ У вас нет доступа к этой функции")
            return
        
        if not users_cache:
            await update.message.reply_text(
                "❌ База пользователей не загружена.\n\n"
                "Попробуйте команду /reload для перезагрузки данных."
            )
            return
        
        loading_msg = await update.message.reply_text("📊 Формирую отчет статуса пользователей...")
        
        ping_data = []
        
        for uid, user_info in users_cache.items():
            bot_info = bot_users.get(uid)
            
            if bot_info:
                status = '✅ Активирован'
                first_start = bot_info['first_start'].strftime('%d.%m.%Y %H:%M')
                last_start = bot_info['last_start'].strftime('%d.%m.%Y %H:%M')
            else:
                status = '❌ Не активирован'
                first_start = '-'
                last_start = '-'
            
            ping_data.append({
                'ФИО': user_info.get('name', 'Не указано'),
                'Telegram ID': uid,
                'Филиал': user_info.get('branch', '-'),
                'РЭС': user_info.get('res', '-'),
                'Видимость': user_info.get('visibility', '-'),
                'Статус': status,
                'Первый вход': first_start,
                'Последний вход': last_start
            })
        
        if not ping_data:
            await loading_msg.delete()
            await update.message.reply_text("📊 Нет данных для отчета.\n\nВозможно база пользователей не загружена.")
            return
        
        df = pd.DataFrame(ping_data)
        df = df.sort_values(['Статус', 'ФИО'])
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Статус пользователей', index=False)
            
            workbook = writer.book
            worksheet = writer.sheets['Статус пользователей']
            
            header_format = workbook.add_format({
                'bg_color': '#4B4B4B',
                'font_color': 'white',
                'bold': True,
                'text_wrap': True,
                'valign': 'vcenter',
                'align': 'center',
                'border': 1
            })
            
            active_format = workbook.add_format({
                'bg_color': '#E8F5E9',
                'border': 1
            })
            
            inactive_format = workbook.add_format({
                'bg_color': '#FFEBEE',
                'border': 1
            })
            
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            for row_num, (index, row) in enumerate(df.iterrows(), start=1):
                cell_format = active_format if '✅' in row['Статус'] else inactive_format
                for col_num, value in enumerate(row):
                    worksheet.write(row_num, col_num, value, cell_format)
            
            for i, col in enumerate(df.columns):
                column_len = df[col].astype(str).map(len).max()
                column_len = max(column_len, len(col)) + 2
                worksheet.set_column(i, i, min(column_len, 40))
            
            worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
        
        output.seek(0)
        
        await loading_msg.delete()
        
        active_count = len(df[df['Статус'] == '✅ Активирован'])
        inactive_count = len(df[df['Статус'] == '❌ Не активирован'])
        total_count = len(df)
        
        moscow_time = get_moscow_time()
        filename = f"Статус_пользователей_{moscow_time.strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        caption = f"""📊 Отчет статуса пользователей

👥 Всего в базе: {total_count}
✅ Активировали бота: {active_count} ({active_count/total_count*100:.1f}%)
❌ Не активировали: {inactive_count} ({inactive_count/total_count*100:.1f}%)

📋 Зеленым отмечены те, кто хотя бы раз запускал бота
🕐 Сформировано: {moscow_time.strftime('%d.%m.%Y %H:%M')} МСК

⚠️ Внимание: данные о запусках сохраняются только в текущей сессии бота.
После обновления/перезапуска бота статистика обнуляется!"""
        
        await update.message.reply_document(
            document=InputFile(output, filename=filename),
            caption=caption
        )
        
    except Exception as e:
        logger.error(f"Ошибка генерации PING отчета: {e}", exc_info=True)
        if 'loading_msg' in locals():
            await loading_msg.delete()
        await update.message.reply_text(f"❌ Ошибка генерации отчета: {str(e)}")


async def notify_restart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Уведомить всех пользователей о необходимости перезапуска бота"""
    user_id = str(update.effective_user.id)
    permissions = get_user_permissions(user_id)
    
    if permissions.get('visibility') != 'All':
        await update.message.reply_text("❌ У вас нет доступа к этой функции")
        return
    
    if not users_cache:
        await update.message.reply_text(
            "❌ База пользователей не загружена.\n\n"
            "Попробуйте команду /reload для перезагрузки данных."
        )
        return
    
    loading_msg = await update.message.reply_text("🔄 Начинаю отправку уведомлений о перезапуске...")
    
    restart_message = """🔄 *Обновление бота ВОЛС Ассистент*

Бот был обновлен и перезапущен.

Для продолжения работы, пожалуйста, нажмите команду:
👉 /start

Это необходимо для корректной работы всех функций бота после обновления.

_Приносим извинения за неудобства._"""
    
    success_count = 0
    failed_count = 0
    failed_users = []
    
    total_users = len(users_cache)
    
    for i, (uid, user_info) in enumerate(users_cache.items()):
        try:
            if i % 20 == 0:
                try:
                    await loading_msg.edit_text(
                        f"🔄 Отправка уведомлений о перезапуске...\n"
                        f"Прогресс: {i}/{total_users}"
                    )
                except:
                    pass
            
            await context.bot.send_message(
                chat_id=uid,
                text=restart_message,
                parse_mode='Markdown'
            )
            success_count += 1
            
            await asyncio.sleep(0.05)
            
        except Exception as e:
            failed_count += 1
            failed_users.append(f"{user_info.get('name', 'ID: ' + uid)}")
            logger.debug(f"Не удалось отправить пользователю {uid}: {e}")
    
    await loading_msg.delete()
    
    result_text = f"""✅ Уведомления о перезапуске отправлены!

📊 Статистика:
• Всего в базе: {total_users}
• ✅ Успешно отправлено: {success_count}
• ❌ Не удалось отправить: {failed_count}

💡 Пользователи, которым не удалось отправить, вероятно:
• Не запускали бота ни разу
• Заблокировали бота  
• Удалили аккаунт Telegram

🔄 Рекомендуется использовать эту функцию после каждого обновления бота!"""
    
    if failed_users and len(failed_users) <= 10:
        result_text += f"\n\n❌ Не удалось отправить:\n" + "\n".join(failed_users[:10])
        if len(failed_users) > 10:
            result_text += f"\n... и еще {len(failed_users) - 10} пользователей"
    
    await update.message.reply_text(result_text)


async def handle_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка массовой рассылки"""
    user_id = str(update.effective_user.id)
    permissions = get_user_permissions(user_id)
    
    if permissions.get('visibility') != 'All':
        await update.message.reply_text("❌ У вас нет доступа к этой функции")
        return
    
    state_data = user_states.get(user_id, {})
    broadcast_type = state_data.get('broadcast_type', 'bot_users')
    
    if broadcast_type == 'all_users' and not users_cache:
        await update.message.reply_text(
            "❌ База пользователей не загружена.\n\n"
            "Попробуйте команду /reload для перезагрузки данных."
        )
        return
    
    broadcast_text = update.message.text
    
    if broadcast_text == '❌ Отмена':
        user_states[user_id] = {'state': 'main'}
        await update.message.reply_text(
            "Рассылка отменена",
            reply_markup=get_main_keyboard(get_user_permissions(user_id))
        )
        return
    
    loading_msg = await update.message.reply_text("📤 Начинаю рассылку...")
    
    success_count = 0
    failed_count = 0
    failed_users = []
    
    if broadcast_type == 'all_users':
        recipients = users_cache
        recipient_type = "всем пользователям из базы"
    else:
        recipients = {uid: users_cache.get(uid, {'name': f'ID: {uid}'}) for uid in bot_users}
        recipient_type = "пользователям, запускавшим бота"
    
    total_users = len(recipients)
    
    for i, (uid, user_info) in enumerate(recipients.items()):
        try:
            if i % 20 == 0:
                try:
                    await loading_msg.edit_text(
                        f"📤 Отправка сообщений {recipient_type}...\n"
                        f"Прогресс: {i}/{total_users}"
                    )
                except:
                    pass
            
            await context.bot.send_message(
                chat_id=uid,
                text=broadcast_text,
                parse_mode='Markdown'
            )
            success_count += 1
            
            await asyncio.sleep(0.05)
            
        except Exception as e:
            failed_count += 1
            user_name = user_info.get('name', f'ID: {uid}')
            failed_users.append(user_name)
            logger.debug(f"Ошибка отправки пользователю {uid}: {e}")
    
    await loading_msg.delete()
    
    result_text = f"""✅ Рассылка завершена!

📊 Статистика:
• Тип рассылки: {recipient_type}
• Всего получателей: {total_users}
• ✅ Успешно: {success_count}
• ❌ Не удалось: {failed_count}"""
    
    if failed_users and len(failed_users) <= 10:
        result_text += f"\n\n❌ Не удалось отправить:\n" + "\n".join(failed_users[:10])
        if len(failed_users) > 10:
            result_text += f"\n... и еще {len(failed_users) - 10} пользователей"
    
    user_states[user_id] = {'state': 'main'}
    
    await update.message.reply_text(
        result_text,
        reply_markup=get_main_keyboard(get_user_permissions(user_id))
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений"""
    user_id = str(update.effective_user.id)
    text = update.message.text
    permissions = get_user_permissions(user_id)
    
    if not permissions['visibility']:
        await update.message.reply_text("❌ У вас нет доступа к боту.")
        return
    
    update_user_activity(user_id)
    
    state = user_states.get(user_id, {}).get('state', 'main')
    
    # Выбор типа рассылки
    if state == 'broadcast_choice':
        if text == '❌ Отмена':
            user_states[user_id] = {'state': 'main'}
            await update.message.reply_text(
                "Главное меню",
                reply_markup=get_main_keyboard(permissions)
            )
        elif text in ['📨 Всем кто запускал бота', '📋 Всем из базы данных']:
            if '📨' in text and len(bot_users) == 0:
                await update.message.reply_text(
                    "⚠️ Пока никто не запускал бота после последнего обновления.\n\n"
                    "Эта опция станет доступна после того, как пользователи начнут использовать бота.",
                    reply_markup=get_main_keyboard(permissions)
                )
                user_states[user_id] = {'state': 'main'}
            else:
                user_states[user_id]['state'] = 'broadcast_message'
                user_states[user_id]['broadcast_type'] = 'bot_users' if '📨' in text else 'all_users'
                keyboard = [['❌ Отмена']]
                
                recipients_info = ""
                if '📨' in text:
                    recipients_info = f"\n\n⚠️ Внимание: будут уведомлены только те, кто запускал бота после последнего обновления ({len(bot_users)} пользователей)"
                else:
                    recipients_info = f"\n\n📋 Будут уведомлены все пользователи из базы данных ({len(users_cache)} пользователей)"
                
                await update.message.reply_text(
                    "📢 Введите сообщение для массовой рассылки.\n\n"
                    f"Получатели: {text}"
                    f"{recipients_info}\n\n"
                    "Можно использовать Markdown форматирование:\n"
                    "*жирный* _курсив_ `код`",
                    reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                )
        return
    
    # Обработка массовой рассылки
    if state == 'broadcast_message':
        await handle_broadcast(update, context)
        return
    
    # Обработка кнопки Назад
    if text == '⬅️ Назад':
        if state in ['rosseti_kuban', 'rosseti_yug', 'reports', 'phones', 'settings', 'broadcast_message', 'broadcast_choice', 'admin']:
            user_states[user_id] = {'state': 'main'}
            await update.message.reply_text("Главное меню", reply_markup=get_main_keyboard(permissions))
        elif state == 'reference':
            previous_state = user_states[user_id].get('previous_state')
            if previous_state and previous_state.startswith('branch_'):
                branch = user_states[user_id].get('branch')
                user_states[user_id]['state'] = previous_state
                await update.message.reply_text(f"{branch}", reply_markup=get_branch_menu_keyboard())
            else:
                user_states[user_id] = {'state': 'main'}
                await update.message.reply_text("Главное меню", reply_markup=get_main_keyboard(permissions))
        elif state == 'document_actions':
            previous_state = user_states[user_id].get('previous_state')
            branch = user_states[user_id].get('branch')
            network = user_states[user_id].get('network')
            user_states[user_id] = {
                'state': 'reference',
                'previous_state': previous_state,
                'branch': branch,
                'network': network
            }
            await update.message.reply_text("Выберите документ", reply_markup=get_reference_keyboard())
        elif state == 'report_actions':
            user_states[user_id]['state'] = 'reports'
            await update.message.reply_text("Выберите тип отчета", reply_markup=get_reports_keyboard(permissions))
        elif state.startswith('branch_'):
            if permissions['branch'] != 'All':
                user_states[user_id] = {'state': 'main'}
                await update.message.reply_text("Главное меню", reply_markup=get_main_keyboard(permissions))
            else:
                network = user_states[user_id].get('network')
                if network == 'RK':
                    user_states[user_id] = {'state': 'rosseti_kuban', 'network': 'RK'}
                    branches = ROSSETI_KUBAN_BRANCHES
                else:
                    user_states[user_id] = {'state': 'rosseti_yug', 'network': 'UG'}
                    branches = ROSSETI_YUG_BRANCHES
                await update.message.reply_text("Выберите филиал", reply_markup=get_branch_keyboard(branches))
        elif state in ['search_tp', 'send_notification']:
            branch = user_states[user_id].get('branch')
            # Проверяем, откуда пришли в отправку уведомления
            if state == 'send_notification':
                action = user_states[user_id].get('action')
                
                # Если мы в процессе отправки уведомления, пришедшего из поиска
                if 'last_search_tp' in user_states[user_id]:
                    if action == 'select_vl':
                        # Возвращаемся к результатам поиска
                        user_states[user_id]['state'] = 'search_tp'
                        user_states[user_id]['action'] = 'after_results'
                        tp_name = user_states[user_id].get('last_search_tp', '')
                        await update.message.reply_text(
                            "Вернулись к результатам поиска",
                            reply_markup=get_after_search_keyboard(tp_name)
                        )
                    elif action in ['send_location', 'request_photo', 'add_comment']:
                        # Возвращаемся на шаг назад в процессе уведомления
                        if action == 'send_location':
                            # Возвращаемся к выбору ВЛ
                            user_states[user_id]['action'] = 'select_vl'
                            selected_tp = user_states[user_id].get('selected_tp')
                            
                            # Перезагружаем данные из справочника для получения списка ВЛ
                            branch = user_states[user_id].get('branch')
                            network = user_states[user_id].get('network')
                            
                            # Проверяем права пользователя
                            user_permissions = get_user_permissions(user_id)
                            user_branch = user_permissions.get('branch')
                            if user_branch and user_branch != 'All':
                                branch = user_branch
                            
                            env_key = get_env_key_for_branch(branch, network, is_reference=True)
                            csv_url = os.environ.get(env_key)
                            
                            if csv_url:
                                data = load_csv_from_url(csv_url)
                                results = search_tp_in_data(selected_tp, data, 'Наименование ТП')
                                
                                # Фильтруем по РЭС если нужно
                                user_res = user_permissions.get('res')
                                if user_res and user_res != 'All':
                                    results = [r for r in results if r.get('РЭС', '').strip() == user_res]
                                
                                if results:
                                    vl_list = list(set([r['Наименование ВЛ'] for r in results]))
                                    keyboard = []
                                    for vl in vl_list:
                                        keyboard.append([vl])
                                    keyboard.append(['⬅️ Назад'])
                                    
                                    await update.message.reply_text(
                                        f"📨 Отправка уведомления по ТП: {selected_tp}\n\n"
                                        f"Выберите ВЛ:",
                                        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                                    )
                                else:
                                    await update.message.reply_text("❌ Не удалось загрузить список ВЛ")
                            else:
                                await update.message.reply_text("❌ Справочник не найден")
                        elif action == 'request_photo':
                            # Возвращаемся к отправке локации
                            user_states[user_id]['action'] = 'send_location'
                            keyboard = [[KeyboardButton("📍 Отправить местоположение", request_location=True)]]
                            keyboard.append(['⬅️ Назад'])
                            
                            selected_tp = user_states[user_id].get('selected_tp')
                            selected_vl = user_states[user_id].get('selected_vl')
                            
                            await update.message.reply_text(
                                f"✅ Выбрана ВЛ: {selected_vl}\n"
                                f"📍 ТП: {selected_tp}\n\n"
                                "Теперь отправьте ваше местоположение:",
                                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                            )
                        elif action == 'add_comment':
                            # Возвращаемся к запросу фото
                            user_states[user_id]['action'] = 'request_photo'
                            keyboard = [
                                ['⏭ Пропустить и добавить комментарий'],
                                ['📤 Отправить без фото и комментария'],
                                ['⬅️ Назад']
                            ]
                            
                            selected_tp = user_states[user_id].get('selected_tp')
                            selected_vl = user_states[user_id].get('selected_vl')
                            
                            await update.message.reply_text(
                                f"📍 ТП: {selected_tp}\n"
                                f"⚡ ВЛ: {selected_vl}\n\n"
                                "📸 Сделайте фото бездоговорного ВОЛС\n\n"
                                "Как отправить фото:\n"
                                "📱 **Мобильный**: нажмите 📎 → Камера\n"
                                "Или выберите действие ниже:",
                                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
                                parse_mode='Markdown'
                            )
                else:
                    # Если пришли не из поиска - возвращаемся в меню филиала
                    user_states[user_id]['state'] = f'branch_{branch}'
                    await update.message.reply_text(f"{branch}", reply_markup=get_branch_menu_keyboard())
            else:
                # Для search_tp возвращаемся в меню филиала
                user_states[user_id]['state'] = f'branch_{branch}'
                await update.message.reply_text(f"{branch}", reply_markup=get_branch_menu_keyboard())
        return
    
    # Главное меню
    if state == 'main':
        if text == '🏢 РОССЕТИ КУБАНЬ':
            if permissions['visibility'] in ['All', 'RK']:
                if permissions['branch'] == 'All':
                    user_states[user_id] = {'state': 'rosseti_kuban', 'network': 'RK'}
                    await update.message.reply_text(
                        "Выберите филиал РОССЕТИ КУБАНЬ",
                        reply_markup=get_branch_keyboard(ROSSETI_KUBAN_BRANCHES)
                    )
                else:
                    user_branch = permissions['branch']
                    logger.info(f"Пользователь {user_id} имеет доступ только к филиалу: '{user_branch}'")
                    
                    normalized_branch = None
                    user_branch_clean = user_branch.replace(' ЭС', '').strip()
                    
                    for kb_branch in ROSSETI_KUBAN_BRANCHES:
                        kb_branch_clean = kb_branch.replace(' ЭС', '').strip()
                        if (kb_branch_clean == user_branch_clean or 
                            kb_branch_clean.startswith(user_branch_clean) or
                            user_branch_clean.startswith(kb_branch_clean)):
                            normalized_branch = kb_branch
                            break
                    
                    if not normalized_branch:
                        logger.warning(f"Не найдено соответствие для филиала '{user_branch}' в списке РОССЕТИ КУБАНЬ")
                        normalized_branch = user_branch
                    else:
                        logger.info(f"Филиал '{user_branch}' нормализован к '{normalized_branch}'")
                    
                    user_states[user_id] = {'state': f'branch_{normalized_branch}', 'branch': normalized_branch, 'network': 'RK'}
                    await update.message.reply_text(
                        f"{normalized_branch}",
                        reply_markup=get_branch_menu_keyboard()
                    )
        
        elif text == '🏢 РОССЕТИ ЮГ':
            if permissions['visibility'] in ['All', 'UG']:
                if permissions['branch'] == 'All':
                    user_states[user_id] = {'state': 'rosseti_yug', 'network': 'UG'}
                    await update.message.reply_text(
                        "Выберите филиал РОССЕТИ ЮГ",
                        reply_markup=get_branch_keyboard(ROSSETI_YUG_BRANCHES)
                    )
                else:
                    user_branch = permissions['branch']
                    logger.info(f"Пользователь {user_id} имеет доступ только к филиалу: '{user_branch}'")
                    
                    normalized_branch = None
                    user_branch_clean = user_branch.replace(' ЭС', '').strip()
                    
                    for ug_branch in ROSSETI_YUG_BRANCHES:
                        ug_branch_clean = ug_branch.replace(' ЭС', '').strip()
                        if (ug_branch_clean == user_branch_clean or 
                            ug_branch_clean.startswith(user_branch_clean) or
                            user_branch_clean.startswith(ug_branch_clean)):
                            normalized_branch = ug_branch
                            break
                    
                    if not normalized_branch:
                        logger.warning(f"Не найдено соответствие для филиала '{user_branch}' в списке РОССЕТИ ЮГ")
                        normalized_branch = user_branch
                    else:
                        logger.info(f"Филиал '{user_branch}' нормализован к '{normalized_branch}'")
                    
                    user_states[user_id] = {'state': f'branch_{normalized_branch}', 'branch': normalized_branch, 'network': 'UG'}
                    await update.message.reply_text(
                        f"{normalized_branch}",
                        reply_markup=get_branch_menu_keyboard()
                    )
        
        elif text == '📊 ОТЧЕТЫ':
            user_states[user_id] = {'state': 'reports'}
            await update.message.reply_text(
                "Выберите тип отчета",
                reply_markup=get_reports_keyboard(permissions)
            )
        
        elif text == 'ℹ️ СПРАВКА':
            user_states[user_id] = {'state': 'reference'}
            await update.message.reply_text(
                "Выберите документ",
                reply_markup=get_reference_keyboard()
            )
        
        elif text == '⚙️ МОИ НАСТРОЙКИ':
            user_states[user_id] = {'state': 'settings'}
            await update.message.reply_text(
                "⚙️ Персональные настройки",
                reply_markup=get_settings_keyboard()
            )
        
        elif text == '📞 ТЕЛЕФОНЫ КОНТРАГЕНТОВ':
            await update.message.reply_text("🚧 Раздел в разработке")
        
        elif text == '🛠 АДМИНИСТРИРОВАНИЕ':
            if permissions.get('visibility') == 'All':
                user_states[user_id] = {'state': 'admin'}
                await update.message.reply_text(
                    "🛠 Меню администрирования\n\n"
                    "Выберите действие:",
                    reply_markup=get_admin_keyboard()
                )
            else:
                await update.message.reply_text("❌ У вас нет доступа к этой функции")
    
    # Меню администрирования
    elif state == 'admin':
        if text == '📊 СТАТУС ПОЛЬЗОВАТЕЛЕЙ':
            await generate_ping_report(update, context)
            
        elif text == '🔄 УВЕДОМИТЬ О ПЕРЕЗАПУСКЕ':
            if len(bot_users) > 0:
                await notify_restart(update, context)
            else:
                await update.message.reply_text(
                    "⚠️ Это первый запуск бота после деплоя.\n"
                    "Пока никто не активировал бота командой /start.\n\n"
                    "Функция уведомления о перезапуске станет доступна после того, "
                    "как хотя бы один пользователь запустит бота."
                )
                
        elif text == '📢 МАССОВАЯ РАССЫЛКА':
            user_states[user_id] = {'state': 'broadcast_choice'}
            keyboard = [
                ['📨 Всем кто запускал бота'],
                ['📋 Всем из базы данных'],
                ['❌ Отмена']
            ]
            await update.message.reply_text(
                "📢 Выберите кому отправить рассылку:\n\n"
                "📨 *Всем кто запускал бота* - отправка только тем, кто использовал /start после последнего обновления\n\n"
                "📋 *Всем из базы данных* - отправка всем пользователям из зон доступа",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
                parse_mode='Markdown'
            )
    
    # Выбор филиала
    elif state in ['rosseti_kuban', 'rosseti_yug']:
        if text.startswith('⚡ '):
            branch = text[2:]
            user_states[user_id]['state'] = f'branch_{branch}'
            user_states[user_id]['branch'] = branch  # Сохраняем полное название с "ЭС"
            await update.message.reply_text(
                f"{branch}",
                reply_markup=get_branch_menu_keyboard()
            )
    
    # Меню филиала
    elif state.startswith('branch_'):
        if text == '🔍 Поиск по ТП':
            user_states[user_id]['state'] = 'search_tp'
            user_states[user_id]['action'] = 'search'
            keyboard = [['⬅️ Назад']]
            await update.message.reply_text(
                "🔍 Введите наименование ТП для поиска:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
        
        elif text == '📨 Отправить уведомление':
            user_states[user_id]['state'] = 'send_notification'
            user_states[user_id]['action'] = 'notification_tp'
            keyboard = [['⬅️ Назад']]
            await update.message.reply_text(
                "📨 Введите наименование ТП для уведомления:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
        
        elif text == 'ℹ️ Справка':
            current_data = user_states.get(user_id, {}).copy()
            user_states[user_id] = {
                'state': 'reference',
                'previous_state': state,
                'branch': current_data.get('branch'),
                'network': current_data.get('network')
            }
            await update.message.reply_text(
                "Выберите документ",
                reply_markup=get_reference_keyboard()
            )
    
    # Поиск ТП
    elif state == 'search_tp':
        if text == '🔍 Новый поиск':
            keyboard = [['⬅️ Назад']]
            await update.message.reply_text(
                "🔍 Введите наименование ТП для поиска:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
        elif text == '📨 Отправить уведомление':
            # Переход к отправке уведомления с уже найденной ТП
            if 'last_search_tp' in user_states[user_id]:
                selected_tp = user_states[user_id]['last_search_tp']
                branch = user_states[user_id].get('branch')
                network = user_states[user_id].get('network')
                
                # Проверяем права пользователя - может у него указан конкретный филиал
                user_permissions = get_user_permissions(user_id)
                user_branch = user_permissions.get('branch')
                
                # Если у пользователя указан конкретный филиал в правах - используем его БЕЗ нормализации
                if user_branch and user_branch != 'All':
                    branch = user_branch  # Используем как есть из прав (например "Сочинский")
                    logger.info(f"Используем филиал из прав пользователя для отправки уведомления БЕЗ изменений: {branch}")
                
                # Загружаем справочник для поиска ВЛ
                env_key = get_env_key_for_branch(branch, network, is_reference=True)
                csv_url = os.environ.get(env_key)
                
                if not csv_url:
                    await update.message.reply_text(f"❌ Справочник для филиала {branch} не найден")
                    return
                
                loading_msg = await update.message.reply_text("🔍 Загружаю данные из справочника...")
                
                data = load_csv_from_url(csv_url)
                results = search_tp_in_data(selected_tp, data, 'Наименование ТП')
                
                # Фильтруем по РЭС если у пользователя ограничения
                user_res = user_permissions.get('res')
                
                if user_res and user_res != 'All':
                    results = [r for r in results if r.get('РЭС', '').strip() == user_res]
                
                await loading_msg.delete()
                
                if not results:
                    await update.message.reply_text("❌ ТП не найдена в справочнике")
                    return
                
                # Переходим сразу к выбору ВЛ
                user_states[user_id]['state'] = 'send_notification'
                user_states[user_id]['action'] = 'select_vl'
                user_states[user_id]['selected_tp'] = selected_tp
                user_states[user_id]['tp_data'] = results[0]
                user_states[user_id]['branch'] = branch  # Сохраняем branch
                user_states[user_id]['network'] = network  # Сохраняем network
                
                logger.info(f"Сохранены данные для уведомления:")
                logger.info(f"  selected_tp: {selected_tp}")
                logger.info(f"  tp_data: {results[0]}")
                logger.info(f"  branch: {branch}")
                logger.info(f"  network: {network}")
                
                vl_list = list(set([r['Наименование ВЛ'] for r in results]))
                
                keyboard = []
                for vl in vl_list:
                    keyboard.append([vl])
                keyboard.append(['⬅️ Назад'])
                
                await update.message.reply_text(
                    f"📨 Отправка уведомления по ТП: {selected_tp}\n\n"
                    f"Выберите ВЛ:",
                    reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                )
            else:
                await update.message.reply_text("❌ Сначала выполните поиск ТП")
        elif user_states[user_id].get('action') == 'search':
            branch = user_states[user_id].get('branch')
            network = user_states[user_id].get('network')
            
            # Проверяем права пользователя - может у него указан конкретный филиал
            user_permissions = get_user_permissions(user_id)
            user_branch = user_permissions.get('branch')
            user_res = user_permissions.get('res')
            
            # Если у пользователя указан конкретный филиал в правах - используем его БЕЗ нормализации
            if user_branch and user_branch != 'All':
                branch = user_branch  # Используем как есть из прав (например "Сочинский")
                logger.info(f"Используем филиал из прав пользователя БЕЗ изменений: {branch}")
            else:
                # Только если выбрали из меню - нормализуем
                branch = normalize_branch_name(branch)
            
            logger.info(f"Поиск ТП для филиала: {branch}, сеть: {network}")
            if user_res and user_res != 'All':
                logger.info(f"Пользователь имеет доступ только к РЭС: {user_res}")
            
            search_messages = [
                "🔍 Ищу информацию...",
                "📡 Подключаюсь к базе данных...",
                "📊 Анализирую данные...",
                "🔄 Обрабатываю результаты..."
            ]
            
            loading_msg = await update.message.reply_text(search_messages[0])
            
            for i, msg_text in enumerate(search_messages[1:], 1):
                await asyncio.sleep(0.5)
                try:
                    await loading_msg.edit_text(msg_text)
                except Exception:
                    pass
            
            env_key = get_env_key_for_branch(branch, network)
            csv_url = os.environ.get(env_key)
            
            logger.info(f"Итоговые параметры поиска:")
            logger.info(f"  Филиал: {branch}")
            logger.info(f"  Сеть: {network}")
            logger.info(f"  Ключ переменной: {env_key}")
            logger.info(f"  URL из переменной: {csv_url}")
            
            if not csv_url:
                available_vars = [key for key in os.environ.keys() if 'URL' in key and network in key]
                logger.error(f"Доступные переменные для {network}: {available_vars}")
                await loading_msg.delete()
                await update.message.reply_text(
                    f"❌ Данные для филиала {branch} не найдены\n"
                    f"Искали переменную: {env_key}\n"
                    f"Доступные: {', '.join(available_vars[:5])}"
                )
                return
            
            data = load_csv_from_url(csv_url)
            results = search_tp_in_data(text, data, 'Наименование ТП')
            
            # Если у пользователя указан конкретный РЭС - фильтруем результаты
            if user_res and user_res != 'All':
                filtered_results = [r for r in results if r.get('РЭС', '').strip() == user_res]
                
                await loading_msg.delete()
                
                if not filtered_results:
                    # Проверяем, были ли вообще результаты до фильтрации
                    if results:
                        await update.message.reply_text(
                            f"❌ В {user_res} РЭС запрашиваемая ТП не найдена.\n\n"
                            f"ℹ️ Данная ТП присутствует в других РЭС филиала {branch}."
                        )
                    else:
                        await update.message.reply_text(
                            f"❌ ТП не найдена в {user_res} РЭС.\n\n"
                            "Попробуйте другой запрос."
                        )
                    return
                
                results = filtered_results
            else:
                await loading_msg.delete()
                
                if not results:
                    await update.message.reply_text("❌ ТП не найдено. Попробуйте другой запрос.")
                    return
            
            tp_list = list(set([r['Наименование ТП'] for r in results]))
            
            
            tp_list = list(set([r['Наименование ТП'] for r in results]))
            
            if len(tp_list) == 1:
                # Если найдена только одна ТП, сразу показываем результаты
                await show_tp_results(update, results, tp_list[0])
                # Устанавливаем action для корректной работы кнопок
                user_states[user_id]['action'] = 'after_results'
            else:
                # Показываем список найденных ТП
                keyboard = []
                for tp in tp_list[:10]:
                    keyboard.append([tp])
                keyboard.append(['⬅️ Назад'])
                
                user_states[user_id]['search_results'] = results
                user_states[user_id]['action'] = 'select_tp'
                
                await update.message.reply_text(
                    f"✅ Найдено {len(tp_list)} ТП. Выберите нужную:",
                    reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                )
        
        elif user_states[user_id].get('action') == 'select_tp':
            results = user_states[user_id].get('search_results', [])
            filtered_results = [r for r in results if r['Наименование ТП'] == text]
            
            if filtered_results:
                await show_tp_results(update, filtered_results, text)
                user_states[user_id]['action'] = 'search'
        
    # Уведомление - поиск ТП
    elif state == 'send_notification' and user_states[user_id].get('action') == 'notification_tp':
        branch = user_states[user_id].get('branch')
        network = user_states[user_id].get('network')
        
        # Проверяем права пользователя - может у него указан конкретный филиал
        user_permissions = get_user_permissions(user_id)
        user_branch = user_permissions.get('branch')
        user_res = user_permissions.get('res')
        
        # Если у пользователя указан конкретный филиал в правах - используем его БЕЗ нормализации
        if user_branch and user_branch != 'All':
            branch = user_branch  # Используем как есть из прав (например "Сочинский")
            logger.info(f"Используем филиал из прав пользователя для уведомления БЕЗ изменений: {branch}")
        
        notification_messages = [
            "🔍 Поиск в справочнике...",
            "📋 Проверяю базу данных...",
            "🌐 Загружаю информацию...",
            ]
        
        loading_msg = await update.message.reply_text(notification_messages[0])
        
        for msg_text in notification_messages[1:]:
            await asyncio.sleep(0.4)
            try:
                await loading_msg.edit_text(msg_text)
            except Exception:
                pass
        
        env_key = get_env_key_for_branch(branch, network, is_reference=True)
        csv_url = os.environ.get(env_key)
        
        logger.info(f"Загрузка справочника для уведомления:")
        logger.info(f"  Филиал: {branch}")
        logger.info(f"  Сеть: {network}")
        logger.info(f"  Ключ переменной: {env_key}")
        logger.info(f"  URL справочника: {csv_url}")
        
        if not csv_url:
            await loading_msg.delete()
            await update.message.reply_text(f"❌ Справочник для филиала {branch} не найден")
            return
        
        data = load_csv_from_url(csv_url)
        results = search_tp_in_data(text, data, 'Наименование ТП')
        
        # Если у пользователя указан конкретный РЭС - фильтруем результаты
        if user_res and user_res != 'All':
            filtered_results = [r for r in results if r.get('РЭС', '').strip() == user_res]
            
            await loading_msg.delete()
            
            if not filtered_results:
                if results:
                    await update.message.reply_text(
                        f"❌ В {user_res} РЭС запрашиваемая ТП не найдена.\n\n"
                        f"ℹ️ Данная ТП присутствует в других РЭС филиала {branch}.\n"
                        "Для отправки уведомления выберите ТП из вашего РЭС."
                    )
                else:
                    await update.message.reply_text(
                        f"❌ ТП не найдена в {user_res} РЭС.\n\n"
                        "Попробуйте другой запрос."
                    )
                return
            
            results = filtered_results
        else:
            await loading_msg.delete()
            
            if not results:
                await update.message.reply_text("❌ ТП не найдено. Попробуйте другой запрос.")
                return
        
        tp_list = list(set([r['Наименование ТП'] for r in results]))
        
        keyboard = []
        for tp in tp_list[:10]:
            keyboard.append([tp])
        keyboard.append(['⬅️ Назад'])
        
        user_states[user_id]['notification_results'] = results
        user_states[user_id]['action'] = 'select_notification_tp'
        
        await update.message.reply_text(
            f"✅ Найдено {len(tp_list)} ТП. Выберите нужную:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )
    
    # Выбор ТП для уведомления
    elif state == 'send_notification' and user_states[user_id].get('action') == 'select_notification_tp':
        results = user_states[user_id].get('notification_results', [])
        filtered_results = [r for r in results if r['Наименование ТП'] == text]
        
        if filtered_results:
            user_states[user_id]['selected_tp'] = text
            user_states[user_id]['tp_data'] = filtered_results[0]
            
            vl_list = list(set([r['Наименование ВЛ'] for r in filtered_results]))
            
            keyboard = []
            for vl in vl_list:
                keyboard.append([vl])
            keyboard.append(['⬅️ Назад'])
            
            user_states[user_id]['action'] = 'select_vl'
            
            await update.message.reply_text(
                f"📨 Отправка уведомления по ТП: {text}\n\n"
                f"Выберите ВЛ:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
    
    # Выбор ВЛ
    elif state == 'send_notification' and user_states[user_id].get('action') == 'select_vl':
        user_states[user_id]['selected_vl'] = text
        user_states[user_id]['action'] = 'send_location'
        
        keyboard = [[KeyboardButton("📍 Отправить местоположение", request_location=True)]]
        keyboard.append(['⬅️ Назад'])
        
        selected_tp = user_states[user_id].get('selected_tp', '')
        selected_vl = text
        
        await update.message.reply_text(
            f"✅ Выбрана ВЛ: {selected_vl}\n"
            f"📍 ТП: {selected_tp}\n\n"
            "Теперь отправьте ваше местоположение:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )
    
    # Обработка действий при отправке уведомления с фото
    elif state == 'send_notification':
        action = user_states[user_id].get('action')
        
        if action == 'request_photo' and text == '⏭ Пропустить и добавить комментарий':
            user_states[user_id]['action'] = 'add_comment'
            keyboard = [
                ['📤 Отправить без комментария'],
                ['⬅️ Назад']
            ]
            
            selected_tp = user_states[user_id].get('selected_tp')
            selected_vl = user_states[user_id].get('selected_vl')
            
            await update.message.reply_text(
                f"📍 ТП: {selected_tp}\n"
                f"⚡ ВЛ: {selected_vl}\n\n"
                "💬 Введите комментарий к уведомлению:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
        
        elif action == 'request_photo' and text == '📤 Отправить без фото и комментария':
            await send_notification(update, context)
        
        elif action == 'add_comment' and text == '📤 Отправить без комментария':
            await send_notification(update, context)
        
        elif action == 'add_comment' and text not in ['⬅️ Назад', '📤 Отправить без комментария']:
            user_states[user_id]['comment'] = text
            await send_notification(update, context)
    
    # Персональные настройки
    elif state == 'settings':
        if text == '📖 Руководство пользователя':
            if USER_GUIDE_URL:
                keyboard = [[InlineKeyboardButton("📖 Открыть руководство", url=USER_GUIDE_URL)]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await update.message.reply_text(
                    "📖 *Руководство пользователя ВОЛС Ассистент*\n\n"
                    f"Версия {BOT_VERSION} • Июль 2025\n\n"
                    "В руководстве вы найдете:\n"
                    "• Пошаговые инструкции по работе\n"
                    "• Описание всех функций\n"
                    "• Ответы на частые вопросы\n"
                    "• Контакты поддержки\n\n"
                    "Нажмите кнопку ниже для просмотра:",
                    parse_mode='Markdown',
                    reply_markup=reply_markup
                )
            else:
                await update.message.reply_text("❌ Ссылка на руководство не настроена в системе")
        
        elif text == 'ℹ️ Моя информация':
            user_data = users_cache.get(user_id, {})
            
            info_text = f"""ℹ️ Ваша информация:

👤 ФИО: {user_data.get('name', 'Не указано')}
📝 Имя (для приветствия): {user_data.get('name_without_surname', 'Не указано')}
🆔 Telegram ID: {user_id}
📧 Email: {user_data.get('email', 'Не указан')}

🔐 Права доступа:
• Видимость: {user_data.get('visibility', '-')}
• Филиал: {user_data.get('branch', '-')}
• РЭС: {user_data.get('res', '-')}
• Ответственность: {user_data.get('responsible', 'Не назначена')}"""
            
            await update.message.reply_text(info_text)
    
    # Действия с документами
    elif state == 'document_actions':
        if text == '📧 Отправить себе на почту':
            user_data = users_cache.get(user_id, {})
            user_email = user_data.get('email', '')
            
            if not user_email:
                await update.message.reply_text(
                    "❌ У вас не указан email в системе.\n"
                    "Обратитесь к администратору для добавления email.",
                    reply_markup=get_document_action_keyboard()
                )
                return
            
            doc_info = user_states[user_id].get('current_document')
            if not doc_info:
                await update.message.reply_text(
                    "❌ Документ не найден",
                    reply_markup=get_document_action_keyboard()
                )
                return
            
            sending_msg = await update.message.reply_text("📧 Отправляю документ на почту...")
            
            try:
                doc_data = doc_info.get('data')
                if not doc_data:
                    document = await get_cached_document(doc_info['name'], doc_info['url'])
                    if document:
                        doc_data = document.getvalue()
                
                if doc_data:
                    document_io = BytesIO(doc_data)
                    
                    subject = f"Документ: {doc_info['name']}"
                    body = f"""Добрый день, {user_data.get('name', 'Пользователь')}!

Вы запросили отправку документа "{doc_info['name']}" из бота ВОЛС Ассистент.

Документ прикреплен к данному письму.

С уважением,
Бот ВОЛС Ассистент"""
                    
                    success = await send_email(
                        user_email,
                        subject,
                        body,
                        document_io,
                        doc_info['filename']
                    )
                    
                    await sending_msg.delete()
                    
                    if success:
                        await update.message.reply_text(
                            f"✅ Документ успешно отправлен на {user_email}",
                            reply_markup=get_document_action_keyboard()
                        )
                    else:
                        await update.message.reply_text(
                            "❌ Не удалось отправить документ. Попробуйте позже.",
                            reply_markup=get_document_action_keyboard()
                        )
                else:
                    await sending_msg.delete()
                    await update.message.reply_text(
                        "❌ Не удалось получить документ",
                        reply_markup=get_document_action_keyboard()
                    )
                    
            except Exception as e:
                logger.error(f"Ошибка отправки документа на почту: {e}")
                await sending_msg.delete()
                await update.message.reply_text(
                    "❌ Ошибка при отправке документа",
                    reply_markup=get_document_action_keyboard()
                )
            return
    
    # Действия с отчетами
    elif state == 'report_actions':
        if text == '📧 Отправить себе на почту':
            user_data = users_cache.get(user_id, {})
            user_email = user_data.get('email', '')
            
            if not user_email:
                await update.message.reply_text(
                    "❌ У вас не указан email в системе.\n"
                    "Обратитесь к администратору для добавления email.",
                    reply_markup=get_report_action_keyboard()
                )
                return
            
            report_info = user_states[user_id].get('last_report')
            if not report_info:
                await update.message.reply_text(
                    "❌ Отчет не найден", 
                    reply_markup=get_report_action_keyboard()
                )
                return
            
            sending_msg = await update.message.reply_text("📧 Отправляю отчет на почту...")
            
            try:
                report_data = BytesIO(report_info['data'])
                
                subject = f"Отчет: {report_info['filename'].replace('.xlsx', '')}"
                body = f"""Добрый день, {user_data.get('name', 'Пользователь')}!

Вы запросили отправку отчета из бота ВОЛС Ассистент.

{report_info['caption']}

Отчет прикреплен к данному письму.

С уважением,
Бот ВОЛС Ассистент"""
                
                success = await send_email(
                    user_email,
                    subject,
                    body,
                    report_data,
                    report_info['filename']
                )
                
                await sending_msg.delete()
                
                if success:
                    await update.message.reply_text(
                        f"✅ Отчет успешно отправлен на {user_email}",
                        reply_markup=get_report_action_keyboard()
                    )
                else:
                    await update.message.reply_text(
                        "❌ Не удалось отправить отчет. Попробуйте позже.",
                        reply_markup=get_report_action_keyboard()
                    )
                    
            except Exception as e:
                logger.error(f"Ошибка отправки отчета на почту: {e}")
                await sending_msg.delete()
                await update.message.reply_text(
                    "❌ Ошибка при отправке отчета",
                    reply_markup=get_report_action_keyboard()
                )
            return
    
    # Отчеты
    elif state == 'reports':
        if text == '📊 Уведомления РОССЕТИ КУБАНЬ':
            await generate_report(update, context, 'RK', permissions)
        elif text == '📊 Уведомления РОССЕТИ ЮГ':
            await generate_report(update, context, 'UG', permissions)
        elif text == '📈 Активность РОССЕТИ КУБАНЬ':
            await generate_activity_report(update, context, 'RK', permissions)
        elif text == '📈 Активность РОССЕТИ ЮГ':
            await generate_activity_report(update, context, 'UG', permissions)
    
    # Справка
    elif state == 'reference':
        if text.startswith('📄 ') or text.startswith('📊 '):
            button_text = text[2:].strip()
            
            doc_mapping = {
                'План выручки ВОЛС 24-26': 'План по выручке ВОЛС на ВЛ 24-26 годы',
                'Акт инвентаризации': 'Форма акта инвентаризации',
                'Гарантийное письмо': 'Форма гарантийного письма',
                'Претензионное письмо': 'Форма претензионного письма',
                'Регламент ВОЛС': 'Регламент ВОЛС',
                'Отчет по контрагентам': 'Отчет по контрагентам'
            }
            
            doc_name = doc_mapping.get(button_text, button_text)
            
            if doc_name not in REFERENCE_DOCS:
                for full_name in REFERENCE_DOCS.keys():
                    if button_text in full_name or full_name in button_text:
                        doc_name = full_name
                        break
            
            doc_url = REFERENCE_DOCS.get(doc_name)
            
            if doc_url:
                loading_msg = await update.message.reply_text("⏳ Загружаю документ...")
                
                try:
                    document = await get_cached_document(doc_name, doc_url)
                    
                    if document:
                        if 'spreadsheet' in doc_url or 'xlsx' in doc_url:
                            extension = 'xlsx'
                        elif 'document' in doc_url or 'pdf' in doc_url:
                            extension = 'pdf'
                        else:
                            extension = 'pdf'
                        
                        filename = f"{doc_name}.{extension}"
                        
                        await update.message.reply_document(
                            document=InputFile(document, filename=filename),
                            caption=f"📄 {doc_name}"
                        )
                        
                        await loading_msg.delete()
                        
                        previous_state = user_states[user_id].get('previous_state')
                        branch = user_states[user_id].get('branch')
                        network = user_states[user_id].get('network')
                        
                        user_states[user_id]['state'] = 'document_actions'
                        user_states[user_id]['previous_state'] = previous_state
                        user_states[user_id]['branch'] = branch
                        user_states[user_id]['network'] = network
                        user_states[user_id]['current_document'] = {
                            'name': doc_name,
                            'url': doc_url,
                            'filename': filename,
                            'data': document.getvalue()
                        }
                        
                        await update.message.reply_text(
                            "Документ загружен",
                            reply_markup=get_document_action_keyboard()
                        )
                    else:
                        await loading_msg.delete()
                        await update.message.reply_text(
                            f"❌ Не удалось загрузить документ.\n\n"
                            f"Вы можете открыть его по ссылке:\n{doc_url}"
                        )
                        
                except Exception as e:
                    logger.error(f"Ошибка обработки документа {doc_name}: {e}")
                    await loading_msg.delete()
                    await update.message.reply_text(
                        f"❌ Ошибка загрузки документа.\n\n"
                        f"Вы можете открыть его по ссылке:\n{doc_url}"
                    )
            else:
                await update.message.reply_text(f"❌ Документ не найден")

async def show_tp_results(update: Update, results: List[Dict], tp_name: str):
    """Показать результаты поиска по ТП"""
    if not results:
        await update.message.reply_text("❌ Результаты не найдены")
        return
        
    # Сохраняем найденную ТП для возможности отправки уведомления
    user_id = str(update.effective_user.id)
    user_states[user_id]['last_search_tp'] = tp_name
    logger.info(f"[show_tp_results] Сохранена ТП для отправки уведомления: {tp_name}")
    logger.info(f"[show_tp_results] Текущий state: {user_states[user_id].get('state')}")
    logger.info(f"[show_tp_results] Текущий action: {user_states[user_id].get('action')}")
    
    res_name = results[0].get('РЭС', 'Неизвестный')
    
    message = f"📍 {res_name} РЭС, на {tp_name} найдено {len(results)} ВОЛС с договором аренды.\n\n"
    
    for result in results:
        vl = result.get('Наименование ВЛ', '-')
        supports = result.get('Опоры', '-')
        supports_count = result.get('Количество опор', '-')
        provider = result.get('Наименование Провайдера', '-')
        
        message += f"⚡ ВЛ: {vl}\n"
        message += f"Опоры: {supports}, Количество опор: {supports_count}\n"
        message += f"Контрагент: {provider}\n\n"
    
    if len(message) > 4000:
        parts = []
        current_part = f"📍 {res_name} РЭС, на {tp_name} найдено {len(results)} ВОЛС с договором аренды.\n\n"
        
        for result in results:
            result_text = f"⚡ ВЛ: {result.get('Наименование ВЛ', '-')}\n"
            result_text += f"Опоры: {result.get('Опоры', '-')}, Количество опор: {result.get('Количество опор', '-')}\n"
            result_text += f"Контрагент: {result.get('Наименование Провайдера', '-')}\n\n"
            
            if len(current_part + result_text) > 4000:
                parts.append(current_part)
                current_part = result_text
            else:
                current_part += result_text
        
        if current_part:
            parts.append(current_part)
        
        for part in parts:
            await update.message.reply_text(part)
    else:
        await update.message.reply_text(message)
    
    await update.message.reply_text(
        "Выберите действие:",
        reply_markup=get_after_search_keyboard(tp_name)
    )

async def handle_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка геолокации"""
    user_id = str(update.effective_user.id)
    state = user_states.get(user_id, {}).get('state')
    
    if state == 'send_notification' and user_states[user_id].get('action') == 'send_location':
        location = update.message.location
        selected_tp = user_states[user_id].get('selected_tp')
        selected_vl = user_states[user_id].get('selected_vl')
        
        # Сохраняем локацию
        user_states[user_id]['location'] = {
            'latitude': location.latitude,
            'longitude': location.longitude
        }
        
        # Переходим к запросу фото
        user_states[user_id]['action'] = 'request_photo'
        
        keyboard = [
            ['⏭ Пропустить и добавить комментарий'],
            ['📤 Отправить без фото и комментария'],
            ['⬅️ Назад']
        ]
        
        # Отправляем анимированную подсказку
        photo_tips = [
            "📸 Подготовьте камеру...",
            "📷 Сфотографируйте бездоговорной ВОЛС...",
            "💡 Совет: Снимите общий вид и детали"
        ]
        
        tip_msg = await update.message.reply_text(photo_tips[0])
        
        for tip in photo_tips[1:]:
            await asyncio.sleep(1.5)
            try:
                await tip_msg.edit_text(tip)
            except Exception:
                pass
        
        await asyncio.sleep(1.5)
        await tip_msg.delete()
        
        # Отправляем основное сообщение с информацией о выбранных ТП и ВЛ
        await update.message.reply_text(
            f"✅ Местоположение получено!\n\n"
            f"📍 ТП: {selected_tp}\n"
            f"⚡ ВЛ: {selected_vl}\n\n"
            "📸 Сделайте фото бездоговорного ВОЛС\n\n"
            "Как отправить фото:\n"
            "📱 **Мобильный**: нажмите 📎 → Камера\n"
            "Или выберите действие ниже:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
            parse_mode='Markdown'
        )

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка фотографий"""
    user_id = str(update.effective_user.id)
    state = user_states.get(user_id, {}).get('state')
    
    if state == 'send_notification' and user_states[user_id].get('action') == 'request_photo':
        # Сохраняем фото
        photo = update.message.photo[-1]  # Берем фото в максимальном качестве
        file_id = photo.file_id
        
        user_states[user_id]['photo_id'] = file_id
        user_states[user_id]['action'] = 'add_comment'
        
        keyboard = [
            ['📤 Отправить без комментария'],
            ['⬅️ Назад']
        ]
        
        selected_tp = user_states[user_id].get('selected_tp')
        selected_vl = user_states[user_id].get('selected_vl')
        
        await update.message.reply_text(
            f"✅ Фото получено!\n\n"
            f"📍 ТП: {selected_tp}\n"
            f"⚡ ВЛ: {selected_vl}\n\n"
            "💬 Добавьте комментарий к уведомлению или отправьте без комментария:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок"""
    logger.error(f"Exception while handling an update: {context.error}")
    
    try:
        if update and update.effective_message:
            await update.effective_message.reply_text(
                "⚠️ Произошла ошибка при обработке вашего запроса. Попробуйте еще раз."
            )
    except Exception:
        pass

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для проверки статуса бота"""
    user_id = str(update.effective_user.id)
    permissions = get_user_permissions(user_id)
    
    status_text = f"""🤖 Статус бота ВОЛС Ассистент v{BOT_VERSION}

👤 Ваш ID: {user_id}
📋 Ваши права: {permissions.get('visibility', 'Нет')}
👥 Загружено пользователей: {len(users_cache)}
💾 Резервная копия: {len(users_cache_backup)} пользователей
🟢 Активировали бота (текущая сессия): {len(bot_users)} пользователей
🕐 Время сервера: {get_moscow_time().strftime('%d.%m.%Y %H:%M:%S')} МСК

📊 Статистика:
• Уведомлений РК: {len(notifications_storage.get('RK', []))}
• Уведомлений ЮГ: {len(notifications_storage.get('UG', []))}
• Активных пользователей: {len(user_activity)}

🔧 Переменные окружения:
• BOT_TOKEN: {'✅ Задан' if BOT_TOKEN else '❌ Не задан'}
• ZONES_CSV_URL: {'✅ Задан' if ZONES_CSV_URL else '❌ Не задан'}
• WEBHOOK_URL: {'✅ Задан' if WEBHOOK_URL else '❌ Не задан'}

⚠️ Данные о запусках бота сбрасываются после перезапуска!"""
    
    await update.message.reply_text(status_text)

async def reload_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для принудительной перезагрузки данных пользователей"""
    user_id = str(update.effective_user.id)
    
    admin_ids = ['248207151', '1409325335']
    
    if user_id not in admin_ids:
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    loading_msg = await update.message.reply_text("🔄 Перезагружаю данные пользователей...")
    
    try:
        global users_cache, users_cache_backup
        old_count = len(users_cache)
        users_cache = {}
        
        load_users_data()
        
        new_count = len(users_cache)
        
        await loading_msg.edit_text(
            f"✅ Данные успешно перезагружены!\n"
            f"Было пользователей: {old_count}\n"
            f"Загружено пользователей: {new_count}\n"
            f"Резервная копия: {len(users_cache_backup)} пользователей\n"
            f"Активировали бота (текущая сессия): {len(bot_users)} пользователей"
        )
    except Exception as e:
        await loading_msg.edit_text(f"❌ Ошибка перезагрузки: {str(e)}")
        logger.error(f"Ошибка в команде reload: {e}", exc_info=True)

async def check_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверка доступности пользователя для отправки сообщений"""
    if len(context.args) == 0:
        await update.message.reply_text("Использование: /checkuser <telegram_id>")
        return
    
    target_id = context.args[0]
    
    try:
        chat = await context.bot.get_chat(chat_id=target_id)
        await update.message.reply_text(
            f"✅ Пользователь доступен\n"
            f"ID: {target_id}\n"
            f"Имя: {chat.first_name} {chat.last_name or ''}\n"
            f"Username: @{chat.username or 'нет'}"
        )
    except Exception as e:
        await update.message.reply_text(
            f"❌ Не могу отправить сообщения пользователю {target_id}\n"
            f"Ошибка: {str(e)}\n\n"
            f"Возможные причины:\n"
            f"• Пользователь не начал диалог с ботом\n"
            f"• Пользователь заблокировал бота\n"
            f"• Неверный ID"
        )

async def preload_documents():
    """Предзагрузка документов в кэш при старте"""
    logger.info("Начинаем предзагрузку документов...")
    
    for doc_name, doc_url in REFERENCE_DOCS.items():
        if doc_url:
            try:
                logger.info(f"Загружаем {doc_name}...")
                await get_cached_document(doc_name, doc_url)
                logger.info(f"✅ {doc_name} загружен в кэш")
            except Exception as e:
                logger.error(f"❌ Ошибка загрузки {doc_name}: {e}")
    
    logger.info("Предзагрузка документов завершена")

async def refresh_users_data():
    """Периодическое обновление данных пользователей"""
    while True:
        await asyncio.sleep(300)
        logger.info("Обновляем данные пользователей...")
        try:
            load_users_data()
            logger.info("✅ Данные пользователей обновлены")
        except Exception as e:
            logger.error(f"❌ Ошибка обновления данных пользователей: {e}")

async def save_bot_users_periodically():
    """Периодическое сохранение данных о пользователях бота"""
    while True:
        await asyncio.sleep(600)
        save_bot_users()
        logger.info("Автосохранение данных пользователей бота")

async def refresh_documents_cache():
    """Периодическое обновление кэша документов"""
    while True:
        await asyncio.sleep(3600)
        logger.info("Обновляем кэш документов...")
        
        for doc_name in list(documents_cache.keys()):
            doc_url = REFERENCE_DOCS.get(doc_name)
            if doc_url:
                try:
                    del documents_cache[doc_name]
                    del documents_cache_time[doc_name]
                    
                    await get_cached_document(doc_name, doc_url)
                    logger.info(f"✅ Обновлен кэш для {doc_name}")
                except Exception as e:
                    logger.error(f"❌ Ошибка обновления кэша {doc_name}: {e}")

if __name__ == '__main__':
    def signal_handler(sig, frame):
        logger.info("Получен сигнал остановки, сохраняем данные...")
        save_bot_users()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN не задан в переменных окружения!")
        exit(1)
        
    if not ZONES_CSV_URL:
        logger.error("ZONES_CSV_URL не задан в переменных окружения!")
        exit(1)
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("reload", reload_users))
    application.add_handler(CommandHandler("checkuser", check_user))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(MessageHandler(filters.LOCATION, handle_location))
    application.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    application.add_error_handler(error_handler)
    
    load_users_data()
    
    load_bot_users()
    
    async def init_and_start():
        """Инициализация и запуск"""
        await preload_documents()
        
        asyncio.create_task(refresh_documents_cache())
        asyncio.create_task(refresh_users_data())
        asyncio.create_task(save_bot_users_periodically())
    
    async def post_init(application: Application) -> None:
        """Вызывается после инициализации приложения"""
        await init_and_start()
    
    async def post_shutdown(application: Application) -> None:
        """Вызывается при остановке приложения"""
        logger.info("Сохраняем данные перед остановкой...")
        save_bot_users()
    
    application.post_init = post_init
    if hasattr(application, 'post_shutdown'):
        application.post_shutdown = post_shutdown
    
    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=BOT_TOKEN,
        webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}",
        drop_pending_updates=True
    )
