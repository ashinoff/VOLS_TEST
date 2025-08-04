"""
ВОЛС Ассистент - Telegram бот для управления уведомлениями о бездоговорных ВОЛС
Версия: 2.1.0 OPTIMIZED
"""
# ЧАСТЬ 1
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
import requests.adapters
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
MAX_BUTTONS_BEFORE_BACK = 40

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

# ==================== КЭШИРОВАНИЕ ====================
# Кэш для CSV файлов
csv_cache = {}
csv_cache_time = {}
CSV_CACHE_DURATION = timedelta(hours=2)  # Кэш на 2 часа

# Индексы для быстрого поиска
csv_index_cache = {}

# Пул соединений для requests (для загрузки пользователей)
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(
    pool_connections=10,
    pool_maxsize=20,
    max_retries=3
)
session.mount('http://', adapter)
session.mount('https://', adapter)

# ==================== СУЩЕСТВУЮЩИЕ ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ====================
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

# ЧАСТЬ 1 ==================== конец==================== ============================================================================================================
# ЧАСТЬ 2 ==================== УЛУЧШЕННЫЕ ФУНКЦИИ ПОИСКА ============================================================================================================


# ЧАСТЬ 2 ==================== УЛУЧШЕННЫЕ ФУНКЦИИ ПОИСКА ============================================================================================================

def normalize_tp_name_advanced(name: str) -> str:
    """Улучшенная нормализация имени ТП для поиска"""
    if not name:
        return ""
    
    # НОВОЕ: Убираем префиксы вида 1), 2) и т.д. в начале строки
    name = re.sub(r'^\d+\)\s*', '', name)
    
    # Приводим к верхнему регистру
    name = name.upper()
    
    # НОВОЕ: Заменяем точки с запятой на дефисы для унификации
    name = name.replace(';', '-')
    
    # НОВОЕ: Заменяем множественные дефисы на одинарные
    name = re.sub(r'-+', '-', name)
    
    # Оставляем буквы, цифры, дефисы, пробелы и слэши
    name = re.sub(r'[^\w\s\-/А-Яа-я]', '', name, flags=re.UNICODE)
    
    # Убираем лишние пробелы
    name = ' '.join(name.split())
    
    return name

def search_tp_in_data_advanced(tp_query: str, data: List[Dict], column: str) -> List[Dict]:
    """Гибкий поиск ТП с поддержкой частичных совпадений и сложных названий"""
    if not tp_query or not data:
        return []
    
    # Нормализуем запрос
    normalized_query = normalize_tp_name_advanced(tp_query)
    
    # Создаем версию запроса без дефисов и пробелов для гибкого поиска
    query_compact = normalized_query.replace('-', '').replace(' ', '').replace('/', '')
    
    # НОВОЕ: Создаем упрощенную версию запроса для кабельных линий
    query_simplified = simplify_cable_name(normalized_query)
    
    # Извлекаем все буквенные и цифровые части
    query_letter_parts = re.findall(r'[А-ЯA-Z]+', query_compact)
    query_digit_parts = re.findall(r'\d+', query_compact)
    
    results = []
    # ИСПРАВЛЕНО: Убрали seen_tp - больше не фильтруем дубликаты!
    
    for row in data:
        tp_name_original = row.get(column, '')
        if not tp_name_original:
            continue
        
        # НОВОЕ: Убираем префиксы перед нормализацией
        tp_name_clean = re.sub(r'^\d+\)\s*', '', tp_name_original)
        normalized_tp = normalize_tp_name_advanced(tp_name_clean)
        tp_compact = normalized_tp.replace('-', '').replace(' ', '').replace('/', '')
        
        # НОВОЕ: Упрощенная версия для кабельных линий
        tp_simplified = simplify_cable_name(normalized_tp)
        
        # Извлекаем буквенные и цифровые части из названия ТП
        tp_letter_parts = re.findall(r'[А-ЯA-Z]+', tp_compact)
        tp_digit_parts = re.findall(r'\d+', tp_compact)
        
        # 1. Точное совпадение (с учетом дефисов)
        if normalized_query == normalized_tp:
            # ИСПРАВЛЕНО: убрали проверку seen_tp
            results.append(row)
            continue
        
        # 2. Точное совпадение без дефисов
        if query_compact == tp_compact:
            results.append(row)
            continue
        
        # 3. НОВОЕ: Совпадение упрощенных версий для кабельных линий
        if query_simplified and tp_simplified and query_simplified in tp_simplified:
            results.append(row)
            continue
        
        # 4. Поиск вхождения компактной версии
        if len(query_compact) >= 4 and query_compact in tp_compact:
            results.append(row)
            continue
        
        # 5. НОВОЕ: Поиск ключевых слов для кабельных линий
        if is_cable_line_match(normalized_query, normalized_tp):
            results.append(row)
            continue
        
        # 6. Гибкий поиск по частям (существующая логика)
        match_found = False
        
        # Проверяем буквенные части
        if query_letter_parts:
            # Ищем все буквенные части запроса в названии ТП
            all_letters_found = True
            for query_letters in query_letter_parts:
                letter_found = False
                for tp_letters in tp_letter_parts:
                    if query_letters in tp_letters or tp_letters in query_letters:
                        letter_found = True
                        break
                if not letter_found:
                    all_letters_found = False
                    break
            
            if all_letters_found:
                # Теперь проверяем цифровые части
                if query_digit_parts:
                    # Объединяем цифры из запроса
                    query_digits_str = ''.join(query_digit_parts)
                    
                    # Ищем эту последовательность цифр
                    if len(query_digit_parts) == 1:
                        digit_found = False
                        for tp_digit in tp_digit_parts:
                            if tp_digit.startswith(query_digits_str):
                                digit_found = True
                                break
                        if digit_found:
                            match_found = True
                    else:
                        # Если в запросе несколько групп цифр
                        digits_match = True
                        for i, query_digit in enumerate(query_digit_parts):
                            if i < len(tp_digit_parts):
                                if not tp_digit_parts[i].startswith(query_digit):
                                    digits_match = False
                                    break
                            else:
                                digits_match = False
                                break
                        if digits_match:
                            match_found = True
                else:
                    # Если в запросе нет цифр, но все буквы найдены
                    match_found = True
        
        # 7. Если в запросе только цифры
        elif query_digit_parts and not query_letter_parts:
            query_digits_str = ''.join(query_digit_parts)
            
            # Проверяем каждую группу цифр в ТП
            digit_found = False
            for tp_digit in tp_digit_parts:
                if tp_digit.startswith(query_digits_str):
                    digit_found = True
                    break
            
            if digit_found:
                match_found = True
        
        # ИСПРАВЛЕНО: убрали проверку seen_tp
        if match_found:
            results.append(row)
    
    logger.info(f"[search_tp_in_data_advanced] Запрос: '{tp_query}', найдено записей: {len(results)}")
    return results

# НОВЫЕ вспомогательные функции для работы с кабельными линиями
def simplify_cable_name(name: str) -> str:
    """Упрощение названия кабельной линии для поиска"""
    if not name:
        return ""
    
    # Извлекаем ключевые компоненты кабельной линии
    # Например: КЛ-35-кВ -> КЛ35
    simplified = name.upper()
    
    # Убираем кВ, ПС и другие общие сокращения
    simplified = re.sub(r'\bКВ\b', '', simplified)
    simplified = re.sub(r'\bПС\b', '', simplified)
    simplified = re.sub(r'\bКРУ\b', '', simplified)
    simplified = re.sub(r'\bЯЧ\b', '', simplified)
    
    # Убираем все не-буквенно-цифровые символы
    simplified = re.sub(r'[^\w]', '', simplified)
    
    return simplified

def is_cable_line_match(query: str, tp_name: str) -> bool:
    """Проверка соответствия для кабельных линий"""
    # Проверяем, является ли это кабельной линией
    if not ('КЛ' in query.upper() or 'КЛ' in tp_name.upper()):
        return False
    
    # Извлекаем ключевые параметры кабельной линии
    query_params = extract_cable_params(query)
    tp_params = extract_cable_params(tp_name)
    
    # Сравниваем ключевые параметры
    if query_params and tp_params:
        # Проверяем напряжение
        if query_params.get('voltage') and tp_params.get('voltage'):
            if query_params['voltage'] != tp_params['voltage']:
                return False
        
        # Проверяем название подстанции/фидера
        if query_params.get('station') and tp_params.get('station'):
            if query_params['station'] in tp_params['station'] or tp_params['station'] in query_params['station']:
                return True
    
    return False

def extract_cable_params(name: str) -> Dict:
    """Извлечение параметров кабельной линии"""
    params = {}
    
    # Извлекаем напряжение (например, 35, 110)
    voltage_match = re.search(r'(\d+)\s*КВ', name.upper())
    if not voltage_match:
        voltage_match = re.search(r'КЛ\s*(\d+)', name.upper())
    if voltage_match:
        params['voltage'] = voltage_match.group(1)
    
    # Извлекаем название станции/подстанции
    # Ищем слова после ПС или в конце строки
    station_match = re.search(r'ПС[^\w]*([\w-]+)', name.upper())
    if station_match:
        params['station'] = station_match.group(1)
    else:
        # Берем последнее значимое слово
        words = re.findall(r'[А-Я][А-Я]+', name.upper())
        if words and len(words[-1]) > 3:  # Исключаем короткие аббревиатуры
            params['station'] = words[-1]
    
    return params

# Оставляем старые функции для совместимости
def normalize_tp_name(name: str) -> str:
    """Нормализовать название ТП для поиска (старая версия)"""
    return ''.join(filter(str.isdigit, name))

def search_tp_in_data(tp_query: str, data: List[Dict], column: str) -> List[Dict]:
    """Поиск ТП в данных (использует улучшенную версию)"""
    return search_tp_in_data_advanced(tp_query, data, column)

# Новая функция для двойного поиска (БЕЗ ИЗМЕНЕНИЙ)
async def search_tp_in_both_catalogs(tp_query: str, branch: str, network: str, user_res: str = None) -> Dict:
    """Поиск ТП одновременно в реестре договоров и структуре сети
    ВАЖНО: Возвращает ВСЕ записи для найденных ТП"""
    result = {
        'registry': [],  # ВСЕ результаты из реестра договоров
        'structure': [],  # ВСЕ результаты из структуры сети
        'registry_tp_names': [],  # Уникальные названия ТП из реестра
        'structure_tp_names': []  # Уникальные названия ТП из структуры
    }
    
    # Поиск в реестре договоров (без SP)
    registry_env_key = get_env_key_for_branch(branch, network, is_reference=False)
    registry_url = os.environ.get(registry_env_key)
    
    if registry_url:
        logger.info(f"Поиск в реестре договоров: {registry_env_key}")
        registry_data = await load_csv_from_url_async(registry_url)
        registry_results = search_tp_in_data(tp_query, registry_data, 'Наименование ТП')
        
        # Фильтруем по РЭС если нужно
        if user_res and user_res != 'All':
            registry_results = [r for r in registry_results if r.get('РЭС', '').strip() == user_res]
        
        result['registry'] = registry_results
        result['registry_tp_names'] = list(set([r['Наименование ТП'] for r in registry_results]))
        logger.info(f"[search_tp_in_both_catalogs] Реестр: найдено {len(registry_results)} записей, {len(result['registry_tp_names'])} уникальных ТП")
    
    # Поиск в структуре сети (с SP)
    structure_env_key = get_env_key_for_branch(branch, network, is_reference=True)
    structure_url = os.environ.get(structure_env_key)
    
    if structure_url:
        logger.info(f"Поиск в структуре сети: {structure_env_key}")
        structure_data = await load_csv_from_url_async(structure_url)
        structure_results = search_tp_in_data(tp_query, structure_data, 'Наименование ТП')
        
        # Фильтруем по РЭС если нужно
        if user_res and user_res != 'All':
            structure_results = [r for r in structure_results if r.get('РЭС', '').strip() == user_res]
        
        result['structure'] = structure_results
        result['structure_tp_names'] = list(set([r['Наименование ТП'] for r in structure_results]))
        logger.info(f"[search_tp_in_both_catalogs] Структура: найдено {len(structure_results)} записей, {len(result['structure_tp_names'])} уникальных ТП")
    
    return result

# ОСТАЛЬНЫЕ ФУНКЦИИ БЕЗ ИЗМЕНЕНИЙ (load_csv_from_url_async, load_csv_from_url, preload_csv_files)

# ==================== АСИНХРОННАЯ ЗАГРУЗКА CSV ====================

async def load_csv_from_url_async(url: str) -> List[Dict]:
    """Асинхронная загрузка CSV с кэшированием"""
    # Проверяем кэш
    if url in csv_cache:
        cache_time = csv_cache_time.get(url)
        if cache_time and (datetime.now() - cache_time) < CSV_CACHE_DURATION:
            logger.info(f"✅ Используем кэш для {url} ({len(csv_cache[url])} строк)")
            return csv_cache[url]
    
    try:
        logger.info(f"📥 Загружаем CSV из {url}")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                response.raise_for_status()
                text = await response.text()
                
                # Обрабатываем CSV
                csv_file = io.StringIO(text)
                reader = csv.DictReader(csv_file)
                
                data = []
                for row in reader:
                    normalized_row = {key.strip(): value.strip() if value else '' for key, value in row.items()}
                    data.append(normalized_row)
                
                # Сохраняем в кэш
                csv_cache[url] = data
                csv_cache_time[url] = datetime.now()
                
                logger.info(f"✅ Загружено и закэшировано {len(data)} строк")
                return data
                
    except asyncio.TimeoutError:
        logger.error(f"⏱️ Таймаут при загрузке CSV из {url}")
        # Если есть старый кэш - используем его
        if url in csv_cache:
            logger.warning("⚠️ Используем устаревший кэш из-за таймаута")
            return csv_cache[url]
        return []
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки CSV: {e}")
        if url in csv_cache:
            return csv_cache[url]
        return []

# Синхронная версия для обратной совместимости
def load_csv_from_url(url: str) -> List[Dict]:
    """Загрузить CSV файл по URL (проверяет кэш)"""
    # Сначала проверяем кэш
    if url in csv_cache:
        cache_time = csv_cache_time.get(url)
        if cache_time and (datetime.now() - cache_time) < CSV_CACHE_DURATION:
            logger.info(f"✅ Используем кэш для {url} ({len(csv_cache[url])} строк)")
            return csv_cache[url]
    
    # Если кэша нет - загружаем синхронно
    try:
        logger.info(f"Загружаем CSV из {url}")
        response = session.get(url, timeout=30)  # Используем session с пулом
        response.raise_for_status()
        response.encoding = 'utf-8-sig'
        
        csv_file = io.StringIO(response.text)
        reader = csv.DictReader(csv_file)
        
        data = []
        for row in reader:
            normalized_row = {key.strip(): value.strip() if value else '' for key, value in row.items()}
            data.append(normalized_row)
        
        # Сохраняем в кэш
        csv_cache[url] = data
        csv_cache_time[url] = datetime.now()
        
        logger.info(f"Успешно загружено {len(data)} строк из CSV")
        return data
    except requests.exceptions.Timeout:
        logger.error(f"Таймаут при загрузке CSV из {url}")
        if url in csv_cache:
            return csv_cache[url]
        return []
    except Exception as e:
        logger.error(f"Ошибка загрузки CSV: {e}", exc_info=True)
        if url in csv_cache:
            return csv_cache[url]
        return []

# ==================== ПРЕДЗАГРУЗКА CSV ====================

async def preload_csv_files():
    """Предзагрузка всех CSV файлов филиалов при старте"""
    logger.info("🚀 Начинаем предзагрузку CSV файлов...")
    
    tasks = []
    csv_urls = []
    
    # Собираем все URL из переменных окружения
    for key, value in os.environ.items():
        if 'URL' in key and value and value.startswith('http') and 'csv' in value.lower():
            # Исключаем ZONES_CSV_URL так как он загружается отдельно
            if key != 'ZONES_CSV_URL':
                csv_urls.append(value)
                tasks.append(load_csv_from_url_async(value))
    
    # Загружаем параллельно
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success_count = sum(1 for r in results if not isinstance(r, Exception) and r)
        logger.info(f"✅ Предзагружено {success_count}/{len(tasks)} CSV файлов")
        
        # Логируем ошибки
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"❌ Ошибка загрузки {csv_urls[i]}: {result}")

# чАСТЬ 2 КОНЕЦ    ==================================================================================================================================================



# чАСТЬ 2 КОНЕЦ    ==================================================================================================================================================    
# чАСТЬ 3== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =================================================================================================================================
# ЧАСТЬ 3== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =================================================================================================================================

def get_moscow_time():
    """Получить текущее время в Москве"""
    return datetime.now(MOSCOW_TZ)

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
        
        # Создаем временный файл для безопасного сохранения
        temp_file = BOT_USERS_FILE + '.tmp'
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(serializable_data, f, ensure_ascii=False, indent=2)
        
        # Переименовываем временный файл в основной
        if os.path.exists(temp_file):
            if os.path.exists(BOT_USERS_FILE):
                os.remove(BOT_USERS_FILE)
            os.rename(temp_file, BOT_USERS_FILE)
            
        logger.info(f"✅ Сохранено {len(bot_users)} пользователей бота в {BOT_USERS_FILE}")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения данных пользователей бота: {e}")
        return False

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
            
            logger.info(f"✅ Загружено {len(bot_users)} пользователей бота из файла")
        else:
            logger.info(f"📄 Файл {BOT_USERS_FILE} не найден, начинаем с пустого списка")
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки данных пользователей бота: {e}")
        bot_users = {}

def update_user_activity(user_id: str):
    """Обновить активность пользователя"""
    if user_id not in user_activity:
        user_activity[user_id] = {'last_activity': get_moscow_time(), 'count': 0}
    user_activity[user_id]['last_activity'] = get_moscow_time()

# ==================== РАБОТА С ДОКУМЕНТАМИ ====================

async def download_document(url: str) -> Optional[BytesIO]:
    """Скачать документ по URL (асинхронно)"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as response:
                if response.status == 200:
                    content = await response.read()
                    return BytesIO(content)
    except Exception as e:
        logger.error(f"Ошибка загрузки документа: {e}")
    return None

async def get_cached_document(doc_name: str, doc_url: str) -> Optional[BytesIO]:
    """Получить документ из кэша или загрузить"""
    now = datetime.now()
    
    # Проверяем кэш
    if doc_name in documents_cache:
        cache_time = documents_cache_time.get(doc_name)
        if cache_time and (now - cache_time) < timedelta(hours=1):
            cached_doc = documents_cache[doc_name]
            cached_doc.seek(0)
            return BytesIO(cached_doc.read())
    
    logger.info(f"Загружаем документ {doc_name} из {doc_url}")
    
    # Формируем правильный URL для скачивания
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

# ==================== ЗАГРУЗКА ДАННЫХ ПОЛЬЗОВАТЕЛЕЙ ====================

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
                logger.info(f"Пример пользователя: ID={uid}, visibility={udata.get('visibility')}, name={udata.get('name')}")
                
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

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ ФИЛИАЛОВ ====================

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
        'Юго-Западный': 'YUGO_ZAPADNYE',
        'Усть-Лабинские': 'UST_LABINSKIE', 
        'Усть-Лабинский': 'UST_LABINSKIE',
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

# ==================== ФУНКЦИИ ТЕЛЕФОННОГО СПРАВОЧНИКА ====================

def load_contractors_data() -> List[Dict]:
    """Загрузить данные контрагентов из CSV"""
    contractors_url = os.environ.get('CONTRACTORS_PHONE_BOOK_URL')
    if not contractors_url:
        logger.error("CONTRACTORS_PHONE_BOOK_URL не задан в переменных окружения!")
        return []
    
    try:
        data = load_csv_from_url(contractors_url)
        logger.info(f"Загружено {len(data)} контрагентов из справочника")
        return data
    except Exception as e:
        logger.error(f"Ошибка загрузки справочника контрагентов: {e}")
        return []

def search_contractors(query: str, data: List[Dict]) -> List[Dict]:
    """Поиск контрагентов по наименованию"""
    if not query or not data:
        return []
    
    # Нормализуем запрос
    query_lower = query.lower().strip()
    
    results = []
    seen_contractors = set()
    
    for row in data:
        contractor_name = row.get('Контрагент', '').strip()
        if not contractor_name:
            continue
        
        # Поиск по вхождению подстроки (регистронезависимый)
        if query_lower in contractor_name.lower():
            if contractor_name not in seen_contractors:
                results.append(row)
                seen_contractors.add(contractor_name)
    
    # Сортируем результаты по названию
    results.sort(key=lambda x: x.get('Контрагент', ''))
    
    logger.info(f"Поиск '{query}' нашел {len(results)} контрагентов")
    return results

def escape_markdown(text: str) -> str:
    """Экранирование специальных символов для Telegram Markdown"""
    if not text:
        return text
    
    # Символы, которые нужно экранировать в Markdown v1
    # Только эти символы влияют на форматирование в Telegram
    special_chars = ['*', '_', '[', ']', '(', ')', '`']
    
    escaped_text = text
    for char in special_chars:
        escaped_text = escaped_text.replace(char, f'\\{char}')
    
    return escaped_text

def format_contractor_info(contractor_data: Dict) -> str:
    """Форматирование информации о контрагенте для отображения"""
    lines = []
    
    # Название организации - ЭКРАНИРУЕМ!
    contractor_name = contractor_data.get('Контрагент', 'Не указано')
    contractor_name_escaped = escape_markdown(contractor_name)
    lines.append(f"🏢 **{contractor_name_escaped}**")
    lines.append("")
    
    # Email адреса
    email1 = contractor_data.get('Mail 1', '').strip()
    email2 = contractor_data.get('Mail 2', '').strip()
    
    if email1 or email2:
        lines.append("📧 **Email:**")
        if email1:
            lines.append(f"   • {escape_markdown(email1)}")
        if email2:
            lines.append(f"   • {escape_markdown(email2)}")
        lines.append("")
    
    # Первое контактное лицо
    position1 = contractor_data.get('Должность 1', '').strip()
    contact1 = contractor_data.get('Контактное лицо 1', '').strip()
    phone1 = contractor_data.get('Телефон 1', '').strip()
    
    if position1 or contact1 or phone1:
        lines.append("👤 **Контактное лицо 1:**")
        if position1:
            lines.append(f"   • Должность: {escape_markdown(position1)}")
        if contact1:
            lines.append(f"   • ФИО: {escape_markdown(contact1)}")
        if phone1:
            # Форматируем телефон для удобства
            phone1_formatted = format_phone_number(phone1)
            lines.append(f"   • Телефон: {phone1_formatted}")
        lines.append("")
    
    # Второе контактное лицо
    position2 = contractor_data.get('Должность 2', '').strip()
    contact2 = contractor_data.get('Контактное лицо 2', '').strip()
    phone2 = contractor_data.get('Телефон 2', '').strip()
    
    if position2 or contact2 or phone2:
        lines.append("👤 **Контактное лицо 2:**")
        if position2:
            lines.append(f"   • Должность: {escape_markdown(position2)}")
        if contact2:
            lines.append(f"   • ФИО: {escape_markdown(contact2)}")
        if phone2:
            # Форматируем телефон для удобства
            phone2_formatted = format_phone_number(phone2)
            lines.append(f"   • Телефон: {phone2_formatted}")
    
    # Если нет никакой информации кроме названия
    if len(lines) == 2:  # Только название и пустая строка
        lines.append("ℹ️ Контактная информация не указана")
    
    return "\n".join(lines)

def format_phone_number(phone: str) -> str:
    """Форматирование телефонного номера для красивого отображения"""
    # Убираем все не-цифры
    digits = ''.join(filter(str.isdigit, phone))
    
    # Если номер начинается с 8 или 7 и имеет 11 цифр - форматируем как российский
    if len(digits) == 11 and digits[0] in ['7', '8']:
        # +7 (XXX) XXX-XX-XX
        return f"+7 ({digits[1:4]}) {digits[4:7]}-{digits[7:9]}-{digits[9:11]}"
    elif len(digits) == 10:
        # Возможно номер без кода страны
        # (XXX) XXX-XX-XX
        return f"({digits[0:3]}) {digits[3:6]}-{digits[6:8]}-{digits[8:10]}"
    else:
        # Возвращаем как есть если не можем отформатировать
        return phone

def get_all_contractors_sorted(data: List[Dict]) -> List[str]:
    """Получить отсортированный список всех уникальных контрагентов"""
    contractors = set()
    
    for row in data:
        contractor_name = row.get('Контрагент', '').strip()
        if contractor_name:
            contractors.add(contractor_name)
    
    # Сортируем по алфавиту
    sorted_contractors = sorted(contractors)
    logger.info(f"Всего уникальных контрагентов: {len(sorted_contractors)}")
    
    return sorted_contractors

# ЧАСТЬ 3 КОНЕЦ==============================================================================================================================
# ЧАСТЬ 4 === ФУНКЦИИ КЛАВИАТУР ==================================================================================================================


# ВАЖНО: Максимальное количество кнопок перед кнопкой "Назад"
MAX_BUTTONS_BEFORE_BACK = 40  # Telegram позволяет до ~100 кнопок, но для удобства ограничим

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
    
    # НА ГЛАВНОМ ЭКРАНЕ НЕ ДОБАВЛЯЕМ НАВИГАЦИЮ
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
    
    # Навигационная строка
    keyboard.append(['⬅️ Назад', '🏠 Главная', '🔄 Рестарт'])
    
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_branch_menu_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура меню филиала"""
    keyboard = [
        ['🔍 Поиск по ТП'],
        ['📨 Отправить уведомление'],
        ['ℹ️ Справка'],
        ['⬅️ Назад', '🏠 Главная', '🔄 Рестарт']
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
    
    keyboard.append(['⬅️ Назад', '🏠 Главная', '🔄 Рестарт'])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_settings_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура персональных настроек"""
    keyboard = [
        ['📖 Руководство пользователя'],
        ['ℹ️ Моя информация'],
        ['⬅️ Назад', '🏠 Главная', '🔄 Рестарт']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_admin_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура администрирования"""
    keyboard = [
        ['📊 СТАТУС ПОЛЬЗОВАТЕЛЕЙ'],
        ['🔄 УВЕДОМИТЬ О ПЕРЕЗАПУСКЕ'],
        ['📢 МАССОВАЯ РАССЫЛКА'],
        ['⬅️ Назад', '🏠 Главная']  # В админке не нужен рестарт
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
    
    keyboard.append(['⬅️ Назад', '🏠 Главная', '🔄 Рестарт'])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_document_action_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура действий с документом"""
    keyboard = [
        ['📧 Отправить себе на почту'],
        ['⬅️ Назад', '🏠 Главная', '🔄 Рестарт']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_after_search_keyboard(tp_name: str = None, search_query: str = None) -> ReplyKeyboardMarkup:
    """Клавиатура после результатов поиска"""
    keyboard = [
        ['🔍 Новый поиск']
    ]
    
    # Используем оригинальный поисковый запрос для кнопки уведомления
    if search_query:
        # Обрезаем запрос если слишком длинный для кнопки
        display_query = search_query[:25] + '...' if len(search_query) > 25 else search_query
        keyboard.append([f'📨 Отправить уведомление по "{display_query}"'])
    elif tp_name:
        # Fallback на название ТП если запрос не сохранен
        display_tp = tp_name[:25] + '...' if len(tp_name) > 25 else tp_name
        keyboard.append([f'📨 Отправить уведомление по {display_tp}'])
    else:
        keyboard.append(['📨 Отправить уведомление'])
    
    keyboard.append(['⬅️ Назад', '🏠 Главная', '🔄 Рестарт'])
    
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_after_dual_search_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура после просмотра результатов из двойного поиска"""
    keyboard = [
        ['⬅️ Вернуться к результатам поиска'],
        ['🔍 Новый поиск'],
        ['⬅️ Назад в меню филиала'],
        ['🏠 Главная', '🔄 Рестарт']
    ]
    
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_report_action_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура действий с отчетом"""
    keyboard = [
        ['📧 Отправить себе на почту'],
        ['⬅️ Назад', '🏠 Главная', '🔄 Рестарт']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

# ВАЖНАЯ ФУНКЦИЯ! Клавиатура для двойного поиска - показывает ВСЕ найденные ТП
def get_dual_search_keyboard(registry_tp_names: List[str], structure_tp_names: List[str]) -> ReplyKeyboardMarkup:
    """Клавиатура с результатами поиска из двух справочников
    ВАЖНО: Показывает найденные ТП с учетом ограничения на количество"""
    keyboard = []
    
    # Ограничиваем количество отображаемых ТП (учитываем навигационную строку)
    max_items_per_column = (MAX_BUTTONS_BEFORE_BACK - 2) // 2  # -2 для навигации, делим на 2 колонки
    
    # Если слишком много результатов - обрезаем и информируем
    registry_truncated = len(registry_tp_names) > max_items_per_column
    structure_truncated = len(structure_tp_names) > max_items_per_column
    
    registry_tp_display = registry_tp_names[:max_items_per_column]
    structure_tp_display = structure_tp_names[:max_items_per_column]
    
    # Определяем максимальное количество строк
    max_rows = max(len(registry_tp_display), len(structure_tp_display), 1)
    
    logger.info(f"[get_dual_search_keyboard] Реестр ТП: {len(registry_tp_names)} (показано: {len(registry_tp_display)}), Структура ТП: {len(structure_tp_names)} (показано: {len(structure_tp_display)})")
    
    # Всегда добавляем заголовок с двумя колонками
    header_left = '📋 РЕЕСТР ДОГОВОРОВ'
    header_right = '🗂️ СТРУКТУРА СЕТИ'
    
    if registry_truncated:
        header_left += f' ({len(registry_tp_display)} из {len(registry_tp_names)})'
    if structure_truncated:
        header_right += f' ({len(structure_tp_display)} из {len(structure_tp_names)})'
        
    keyboard.append([header_left, header_right])
    
    # Формируем строки с кнопками для найденных ТП
    for i in range(max_rows):
        row = []
        
        # Кнопка из реестра договоров (слева)
        if i < len(registry_tp_display):
            tp_name = registry_tp_display[i]
            # Обрезаем название если слишком длинное
            display_name = tp_name[:20] + '...' if len(tp_name) > 20 else tp_name
            row.append(f'📄 {display_name}')
        else:
            row.append('➖')
        
        # Кнопка из структуры сети (справа)
        if i < len(structure_tp_display):
            tp_name = structure_tp_display[i]
            # Обрезаем название если слишком длинное
            display_name = tp_name[:20] + '...' if len(tp_name) > 20 else tp_name
            row.append(f'📍 {display_name}')
        else:
            row.append('➖')
        
        keyboard.append(row)
    
    # Если совсем нет результатов - добавляем строку с двумя тире
    if not registry_tp_names and not structure_tp_names:
        keyboard.append(['➖', '➖'])
    
    # Если были обрезаны результаты - добавляем информационное сообщение
    if registry_truncated or structure_truncated:
        keyboard.append(['⚠️ Показаны не все результаты'])
    
    # Кнопки действий
    keyboard.append(['🔍 Новый поиск'])
    keyboard.append(['⬅️ Назад', '🏠 Главная', '🔄 Рестарт'])
    
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

# ВАЖНАЯ ФУНКЦИЯ! Клавиатура для выбора ТП при уведомлении
def get_tp_selection_keyboard(tp_list: List[str]) -> ReplyKeyboardMarkup:
    """Клавиатура для выбора ТП с ограничением количества"""
    keyboard = []
    
    # Ограничиваем количество ТП (учитываем навигационную строку)
    tp_truncated = len(tp_list) > MAX_BUTTONS_BEFORE_BACK - 1  # -1 для навигации
    tp_display = tp_list[:MAX_BUTTONS_BEFORE_BACK - 1]
    
    logger.info(f"[get_tp_selection_keyboard] Создаю клавиатуру для {len(tp_list)} ТП (показано: {len(tp_display)})")
    
    # Если обрезали - добавляем информацию
    if tp_truncated:
        keyboard.append([f'⚠️ Показано {len(tp_display)} из {len(tp_list)} ТП'])
    
    # Добавляем ТП как кнопки
    for tp in tp_display:
        # ИЗМЕНЕНО: Обрезаем слишком длинные названия для отображения
        display_name = tp
        
        # Убираем префикс для отображения если название слишком длинное
        if len(tp) > 40:
            # Пробуем убрать префикс
            tp_no_prefix = re.sub(r'^\d+\)\s*', '', tp)
            if len(tp_no_prefix) < len(tp):
                display_name = tp_no_prefix[:37] + '...' if len(tp_no_prefix) > 40 else tp_no_prefix
            else:
                display_name = tp[:37] + '...'
        
        # ВАЖНО: В значение кнопки передаем ПОЛНОЕ название
        keyboard.append([tp])  # Не display_name, а полное tp!
    
    keyboard.append(['⬅️ Назад', '🏠 Главная', '🔄 Рестарт'])
    
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

# ВАЖНАЯ ФУНКЦИЯ! Клавиатура для выбора ВЛ - ОБНОВЛЕНА С ПАРАМЕТРОМ from_dual_search
def get_vl_selection_keyboard(vl_list: List[str], tp_name: str, from_dual_search: bool = False) -> ReplyKeyboardMarkup:
    """Клавиатура для выбора ВЛ с ограничением количества"""
    keyboard = []
    
    # Ограничиваем количество ВЛ 
    if from_dual_search:
        # Если есть кнопка возврата к результатам - учитываем её
        vl_truncated = len(vl_list) > MAX_BUTTONS_BEFORE_BACK - 3  # -3 для навигации, поиска и возврата
        vl_display = vl_list[:MAX_BUTTONS_BEFORE_BACK - 3]
    else:
        vl_truncated = len(vl_list) > MAX_BUTTONS_BEFORE_BACK - 2  # -2 для навигации и поиска
        vl_display = vl_list[:MAX_BUTTONS_BEFORE_BACK - 2]
    
    logger.info(f"[get_vl_selection_keyboard] Создаю клавиатуру для {len(vl_list)} ВЛ на ТП {tp_name} (показано: {len(vl_display)})")
    
    # Если обрезали - добавляем информацию
    if vl_truncated:
        keyboard.append([f'⚠️ Показано {len(vl_display)} из {len(vl_list)} ВЛ'])
    
    # Сортируем ВЛ для удобства
    vl_display_sorted = sorted(vl_display)
    
    # Добавляем ВЛ как кнопки
    for vl in vl_display_sorted:
        keyboard.append([vl])
    
    # НОВОЕ: Если пришли из двойного поиска - добавляем кнопку возврата
    if from_dual_search:
        keyboard.append(['⬅️ Вернуться к результатам поиска'])
    
    keyboard.append(['🔍 Новый поиск'])
    keyboard.append(['⬅️ Назад', '🏠 Главная', '🔄 Рестарт'])
    
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

# Специальные клавиатуры для процесса отправки уведомления
def get_location_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура для запроса геолокации"""
    keyboard = [
        [KeyboardButton("📍 Отправить местоположение", request_location=True)],
        ['⬅️ Назад', '🏠 Главная', '🔄 Рестарт']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_photo_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура при запросе фото"""
    keyboard = [
        ['⏭ Пропустить и добавить комментарий'],
        ['📤 Отправить без фото и комментария'],
        ['⬅️ Назад', '🏠 Главная', '🔄 Рестарт']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_comment_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура при вводе комментария"""
    keyboard = [
        ['📤 Отправить без комментария'],
        ['⬅️ Назад', '🏠 Главная', '🔄 Рестарт']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_search_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура при поиске"""
    keyboard = [
        ['⬅️ Назад', '🏠 Главная', '🔄 Рестарт']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

# Клавиатура для broadcast
def get_broadcast_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура при массовой рассылке"""
    keyboard = [
        ['❌ Отмена']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

# Клавиатуры для телефонного справочника
def get_phone_book_menu_keyboard() -> ReplyKeyboardMarkup:
    """Главное меню телефонного справочника"""
    keyboard = [
        ['🔍 Поиск по наименованию'],
        ['📋 Весь реестр'],
        ['⬅️ Назад', '🏠 Главная', '🔄 Рестарт']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_contractors_list_keyboard(contractors: List[str], page: int = 0) -> ReplyKeyboardMarkup:
    """Клавиатура со списком контрагентов с пагинацией"""
    keyboard = []
    
    # Параметры пагинации
    items_per_page = 20  # Контрагентов на страницу
    total_pages = (len(contractors) - 1) // items_per_page + 1
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(contractors))
    
    # Информация о странице
    if total_pages > 1:
        keyboard.append([f'📄 Страница {page + 1} из {total_pages}'])
    
    # Добавляем контрагентов текущей страницы
    for contractor in contractors[start_idx:end_idx]:
        # Обрезаем название если слишком длинное
        display_name = contractor[:40] + '...' if len(contractor) > 40 else contractor
        keyboard.append([f'🏢 {display_name}'])
    
    # Навигация по страницам
    nav_row = []
    if page > 0:
        nav_row.append('⬅️ Предыдущая')
    if page < total_pages - 1:
        nav_row.append('➡️ Следующая')
    if nav_row:
        keyboard.append(nav_row)
    
    keyboard.append(['🔍 Поиск'])
    keyboard.append(['⬅️ Назад', '🏠 Главная', '🔄 Рестарт'])
    
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_contractor_actions_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура действий с контрагентом"""
    keyboard = [
        ['🔍 Новый поиск'],
        ['📋 К списку контрагентов'],
        ['⬅️ Назад', '🏠 Главная', '🔄 Рестарт']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


 
    #чАСТЬ 4 КОНЕЦ ==============================================================================================================================================
    #чАСТЬ 4 КОНЕЦ ==============================================================================================================================================
 # ЧАСТЬ 5.1 ========= EMAIL ФУНКЦИИ ============================================================================================================================

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

# ==================== ОБРАБОТЧИКИ КОМАНД ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user_id = str(update.effective_user.id)
    
    logger.info(f"Команда /start от пользователя {user_id} ({update.effective_user.first_name})")
    logger.info(f"Размер users_cache: {len(users_cache)}")
    
    current_time = get_moscow_time()
    is_new_user = user_id not in bot_users
    
    if is_new_user:
        bot_users[user_id] = {
            'first_start': current_time,
            'last_start': current_time,
            'username': update.effective_user.username or '',
            'first_name': update.effective_user.first_name or ''
        }
        logger.info(f"🆕 Новый пользователь добавлен: {user_id}")
    else:
        bot_users[user_id]['last_start'] = current_time
        logger.info(f"🔄 Обновлен последний запуск для: {user_id}")
    
    # ВАЖНО: Сохраняем немедленно после обновления
    if save_bot_users():
        logger.info(f"✅ Данные пользователей сохранены немедленно (всего: {len(bot_users)})")
    else:
        logger.error("❌ Не удалось сохранить данные пользователей немедленно")
    
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
    
    # Приветственное сообщение
    welcome_text = f"👋 Добро пожаловать, {permissions.get('name_without_surname', permissions.get('name', 'Пользователь'))}!"
    
    # Если это новый пользователь - добавляем дополнительную информацию
    if is_new_user:
        welcome_text += "\n\n🎉 Вы успешно активировали бота!"
        
    
    await update.message.reply_text(
        welcome_text,
        reply_markup=get_main_keyboard(permissions)
    )

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
• CSV в кэше: {len(csv_cache)} файлов

🔧 Переменные окружения:
• BOT_TOKEN: {'✅ Задан' if BOT_TOKEN else '❌ Не задан'}
• ZONES_CSV_URL: {'✅ Задан' if ZONES_CSV_URL else '❌ Не задан'}
• WEBHOOK_URL: {'✅ Задан' if WEBHOOK_URL else '❌ Не задан'}

⚠️ Данные о запусках бота сохраняются в файл {BOT_USERS_FILE}"""
    
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
# ЧАСТЬ 5.1 КОНЕЦ =====================================================================================================
        # ЧАСТЬ 5.1 КОНЕЦ =====================================================================================================
# =ЧАСТЬ 5.2 ====== ОТПРАВКА УВЕДОМЛЕНИЙ ====================================================================================

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
    
    # Очищаем временные данные уведомления
    user_states[user_id]['location'] = None
    user_states[user_id]['photo_id'] = None
    user_states[user_id]['comment'] = ''
    
    # ВСЕГДА возвращаемся к выбору ВЛ после отправки уведомления
    user_states[user_id]['state'] = 'send_notification'
    user_states[user_id]['action'] = 'select_vl'
    
    # Перезагружаем данные из справочника для получения списка ВЛ
    env_key = get_env_key_for_branch(branch, network, is_reference=True)
    csv_url = os.environ.get(env_key)
    
    if csv_url:
        data = load_csv_from_url(csv_url)
        # ВАЖНО: используем ТОЧНОЕ название ТП для поиска
        results = [r for r in data if r.get('Наименование ТП', '') == selected_tp]
        
        logger.info(f"[send_notification] Перезагрузка данных для ТП '{selected_tp}'")
        logger.info(f"[send_notification] Найдено записей с точным совпадением: {len(results)}")
        
        # Фильтруем по РЭС если нужно
        user_permissions = get_user_permissions(user_id)
        user_res = user_permissions.get('res')
        if user_res and user_res != 'All':
            results = [r for r in results if r.get('РЭС', '').strip() == user_res]
            logger.info(f"[send_notification] После фильтрации по РЭС '{user_res}': {len(results)} записей")
        
        if results:
            # ВАЖНО: Получаем ВСЕ уникальные ВЛ
            vl_list = list(set([r['Наименование ВЛ'] for r in results]))
            vl_list.sort()
            
            logger.info(f"[send_notification] После отправки найдено {len(vl_list)} уникальных ВЛ")
            logger.info(f"[send_notification] ВЛ: {vl_list}")
            
            # Используем новую функцию для создания клавиатуры
            from_dual_search = user_states[user_id].get('from_dual_search', False) 
            reply_markup = get_vl_selection_keyboard(vl_list, selected_tp, from_dual_search)
            
            await update.message.reply_text(
                result_text + f"\n\n✨ Можете отправить еще уведомление по этой же ТП:\n📍 ТП: {selected_tp}\n\nВыберите ВЛ:",
                reply_markup=reply_markup
            )
            return
        else:
            # Если не удалось загрузить ВЛ - возвращаемся в меню филиала
            logger.warning(f"[send_notification] Не найдено ВЛ для ТП '{selected_tp}'")
            user_states[user_id]['state'] = f'branch_{branch}'
            user_states[user_id]['branch'] = branch
            user_states[user_id]['network'] = network
            
            await update.message.reply_text(
                result_text + "\n\n⚠️ Не удалось загрузить список ВЛ для повторной отправки",
                reply_markup=get_branch_menu_keyboard()
            )
    else:
        # Если нет справочника - возвращаемся в меню филиала
        user_states[user_id]['state'] = f'branch_{branch}'
        user_states[user_id]['branch'] = branch
        user_states[user_id]['network'] = network
        
        await update.message.reply_text(
            result_text,
            reply_markup=get_branch_menu_keyboard()
        )

# ==================== ПОКАЗ РЕЗУЛЬТАТОВ ПОИСКА ====================

async def show_tp_results(update: Update, results: List[Dict], tp_name: str, search_query: str = None):
    """Показать результаты поиска по ТП - ПОКАЗЫВАЕТ ВСЕ ЗАПИСИ БЕЗ ГРУППИРОВКИ"""
    if not results:
        await update.message.reply_text("❌ Результаты не найдены")
        return
        
    # Сохраняем найденную ТП для возможности отправки уведомления
    user_id = str(update.effective_user.id)
    user_states[user_id]['last_search_tp'] = tp_name
    user_states[user_id]['last_search_query'] = search_query or tp_name
    user_states[user_id]['action'] = 'after_results'
    
    logger.info(f"[show_tp_results] Сохранена ТП для отправки уведомления: {tp_name}")
    logger.info(f"[show_tp_results] Сохранен поисковый запрос: {search_query}")
    logger.info(f"[show_tp_results] Всего найдено записей: {len(results)}")
    
    res_name = results[0].get('РЭС', 'Неизвестный')
    
    # Формируем заголовок сообщения
    message = f"📍 {res_name} РЭС, на {tp_name} найдено {len(results)} ВОЛС с договором аренды.\n\n"
    
    # ИЗМЕНЕНО: Показываем ВСЕ записи без группировки
    for i, result in enumerate(results, 1):
        vl = result.get('Наименование ВЛ', '-')
        supports = result.get('Опоры', '-')
        supports_count = result.get('Количество опор', '-')
        provider = result.get('Наименование Провайдера', '-')
        
        message += f"{i}. ⚡ **ВЛ: {vl}**\n"
        message += f"   Опоры: {supports}, Кол-во: {supports_count}\n"
        message += f"   Контрагент: {provider}\n\n"
    
    # Разбиваем на части если сообщение слишком длинное
    if len(message) > 4000:
        # Первое сообщение с заголовком
        header = f"📍 {res_name} РЭС, на {tp_name} найдено {len(results)} ВОЛС с договором аренды.\n\n"
        
        parts = []
        current_part = ""
        
        for i, result in enumerate(results, 1):
            vl = result.get('Наименование ВЛ', '-')
            supports = result.get('Опоры', '-')
            supports_count = result.get('Количество опор', '-')
            provider = result.get('Наименование Провайдера', '-')
            
            record_text = f"{i}. ⚡ **ВЛ: {vl}**\n"
            record_text += f"   Опоры: {supports}, Кол-во: {supports_count}\n"
            record_text += f"   Контрагент: {provider}\n\n"
            
            # Проверяем, поместится ли в текущую часть
            if len(current_part + record_text) > 3500:
                parts.append(current_part)
                current_part = record_text
            else:
                current_part += record_text
        
        if current_part:
            parts.append(current_part)
        
        # Отправляем первую часть с заголовком
        await update.message.reply_text(header + parts[0], parse_mode='Markdown')
        
        # Отправляем остальные части
        for part in parts[1:]:
            await update.message.reply_text(part, parse_mode='Markdown')
    else:
        await update.message.reply_text(message, parse_mode='Markdown')
    
    # Проверяем, откуда пришли - из двойного поиска или обычного
    if 'dual_search_results' in user_states[user_id]:
        # Пришли из двойного поиска - показываем специальную клавиатуру
        await update.message.reply_text(
            "Выберите действие:",
            reply_markup=get_after_dual_search_keyboard()
        )
    else:
        # Обычный поиск - показываем стандартную клавиатуру
        await update.message.reply_text(
            "Выберите действие:",
            reply_markup=get_after_search_keyboard(tp_name, search_query)
        )
        #ЧАСТЬ 5.2 КОНЕЦ= ОБРАБОТЧИК СООБЩЕНИЙ ================================================================================================



# ===ЧАСТЬ 5.3=== ОБРАБОТЧИК СООБЩЕНИЙ ========================================================================================================

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
    action = user_states.get(user_id, {}).get('action')
    
    # Отладка для админа
    if user_id == '248207151':
        logger.info(f"[DEBUG] User {user_id}: state='{state}', action='{action}', text='{text}'")
    
    # ==================== ГЛОБАЛЬНЫЕ НАВИГАЦИОННЫЕ КНОПКИ ====================
    # Обработка кнопки "Главная" - работает из любого места
    if text == '🏠 Главная':
        # Очищаем все состояния
        user_states[user_id] = {'state': 'main'}
        
        # Возвращаемся на главный экран
        await update.message.reply_text(
            "🏠 Главное меню",
            reply_markup=get_main_keyboard(permissions)
        )
        return
    
    # Обработка кнопки "Рестарт" - перезапуск бота
    if text == '🔄 Рестарт':
        # Очищаем состояние
        user_states[user_id] = {'state': 'main'}
        
        # Обновляем время последнего запуска
        current_time = get_moscow_time()
        if user_id in bot_users:
            bot_users[user_id]['last_start'] = current_time
        else:
            bot_users[user_id] = {
                'first_start': current_time,
                'last_start': current_time,
                'username': update.effective_user.username or '',
                'first_name': update.effective_user.first_name or ''
            }
        
        # Сохраняем данные
        save_bot_users()
        
        # Показываем приветствие как при /start
        welcome_text = f"🔄 Перезапуск выполнен!\n\n"
        welcome_text += f"👋 Добро пожаловать, {permissions.get('name_without_surname', permissions.get('name', 'Пользователь'))}!"
        
        await update.message.reply_text(
            welcome_text,
            reply_markup=get_main_keyboard(permissions)
        )
        return
    
    # ==================== КОНЕЦ ГЛОБАЛЬНЫХ НАВИГАЦИОННЫХ КНОПОК ====================
    
    # Обработка пустых кнопок (➖) - игнорируем их
    if text == '➖':
        logger.info(f"Пользователь {user_id} нажал на пустую кнопку ➖, игнорируем")
        return
    
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
                    reply_markup=get_broadcast_keyboard()
                )
        return
    
    # Обработка массовой рассылки
    if state == 'broadcast_message':
        await handle_broadcast(update, context)
        return
    
    # Обработка кнопки Назад - ИСПРАВЛЕНО: убрал return в конце!
    if text == '⬅️ Назад':
        if state in ['rosseti_kuban', 'rosseti_yug', 'reports', 'phones', 'settings', 'broadcast_message', 'broadcast_choice', 'admin', 'phone_book', 'phone_book_search', 'contractor_view']:
            user_states[user_id] = {'state': 'main'}
            await update.message.reply_text("Главное меню", reply_markup=get_main_keyboard(permissions))
            return  # return только здесь
            
        elif state == 'phone_book_list':
            # Возвращаемся в меню справочника
            user_states[user_id] = {'state': 'phone_book'}
            await update.message.reply_text(
                "📞 Телефонный справочник контрагентов\n\n"
                "Выберите способ поиска:",
                reply_markup=get_phone_book_menu_keyboard()
            )
            return
            
        elif state == 'reference':
            previous_state = user_states[user_id].get('previous_state')
            if previous_state and previous_state.startswith('branch_'):
                branch = user_states[user_id].get('branch')
                user_states[user_id]['state'] = previous_state
                await update.message.reply_text(f"{branch}", reply_markup=get_branch_menu_keyboard())
            else:
                user_states[user_id] = {'state': 'main'}
                await update.message.reply_text("Главное меню", reply_markup=get_main_keyboard(permissions))
            return  # return только здесь
            
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
            await update.message.reply_text(
                "Выберите документ",
                reply_markup=get_reference_keyboard()
            )
            return  # return только здесь
            
        # ВАЖНО: НЕ ставим return здесь! Продолжаем обработку других состояний с кнопкой "Назад"
    
    # Поиск ТП с двойным поиском
    if state == 'search_tp':
        if text == '🔍 Новый поиск':
            user_states[user_id]['action'] = 'search'
            await update.message.reply_text(
                "🔍 Введите наименование ТП для поиска:",
                reply_markup=get_search_keyboard()
            )
        elif text.startswith('📨 Отправить уведомление по'):
            logger.info(f"Обработка кнопки отправки уведомления: '{text}'")
            # Переход к отправке уведомления с уже найденной ТП
            if 'last_search_query' in user_states[user_id]:
                search_query = user_states[user_id]['last_search_query']
                branch = user_states[user_id].get('branch')
                network = user_states[user_id].get('network')
                
                logger.info(f"Используем поисковый запрос для уведомления: {search_query}")
                logger.info(f"Branch: {branch}, Network: {network}")
                
                # Проверяем права пользователя
                user_permissions = get_user_permissions(user_id)
                user_branch = user_permissions.get('branch')
                
                if user_branch and user_branch != 'All':
                    branch = user_branch
                    logger.info(f"Используем филиал из прав пользователя: {branch}")
                
                # Ищем в структуре сети (справочник SP) для уведомлений
                env_key = get_env_key_for_branch(branch, network, is_reference=True)
                csv_url = os.environ.get(env_key)
                
                if not csv_url:
                    await update.message.reply_text(f"❌ Справочник для филиала {branch} не найден")
                    return
                
                loading_msg = await update.message.reply_text("🔍 Ищу в справочнике структуры сети...")
                
                data = load_csv_from_url(csv_url)
                results = search_tp_in_data(search_query, data, 'Наименование ТП')
                
                # Фильтруем по РЭС если у пользователя ограничения
                user_res = user_permissions.get('res')
                
                if user_res and user_res != 'All':
                    results = [r for r in results if r.get('РЭС', '').strip() == user_res]
                
                await loading_msg.delete()
                
                if not results:
                    await update.message.reply_text("❌ ТП не найдена в справочнике структуры сети")
                    return
                
                # ВАЖНО: Получаем ВСЕ уникальные ТП
                tp_list = list(set([r['Наименование ТП'] for r in results]))
                
                logger.info(f"[handle_message] Найдено {len(tp_list)} уникальных ТП для уведомления")
                
                if len(tp_list) == 1:
                    # Даже если найдена одна ТП - показываем список для выбора
                    user_states[user_id]['notification_results'] = results
                    user_states[user_id]['action'] = 'select_notification_tp'
                    user_states[user_id]['state'] = 'send_notification'
                    user_states[user_id]['branch'] = branch
                    user_states[user_id]['network'] = network
                    
                    reply_markup = get_tp_selection_keyboard(tp_list)
                    
                    await update.message.reply_text(
                        f"✅ Найдена 1 ТП в структуре сети. Подтвердите выбор:",
                        reply_markup=reply_markup
                    )
                else:
                    # Если найдено несколько ТП - показываем список для выбора
                    user_states[user_id]['notification_results'] = results
                    user_states[user_id]['action'] = 'select_notification_tp'
                    user_states[user_id]['state'] = 'send_notification'
                    
                    # Используем функцию для создания клавиатуры
                    reply_markup = get_tp_selection_keyboard(tp_list)
                    
                    await update.message.reply_text(
                        f"✅ Найдено {len(tp_list)} ТП в структуре сети. Выберите нужную:",
                        reply_markup=reply_markup
                    )
            else:
                await update.message.reply_text("❌ Сначала выполните поиск ТП")
                
        # Обработка кнопки "⬅️ Вернуться к результатам поиска"
        elif text == '⬅️ Вернуться к результатам поиска':
            dual_results = user_states[user_id].get('dual_search_results', {})
            if dual_results:
                registry_tp_names = dual_results['registry_tp_names']
                structure_tp_names = dual_results['structure_tp_names']
                search_query = user_states[user_id].get('last_search_query', '')
                
                message = f"🔍 Результаты поиска по запросу: **{search_query}**\n\n"
                
                if registry_tp_names:
                    message += f"📋 **Реестр договоров** (найдено {len(dual_results['registry'])} записей)\n"
                    message += f"   ТП: {len(registry_tp_names)} шт.\n\n"
                
                if structure_tp_names:
                    message += f"🗂️ **Структура сети** (найдено {len(dual_results['structure'])} записей)\n"
                    message += f"   ТП: {len(structure_tp_names)} шт.\n\n"
                
                message += "📌 Выберите ТП:\n"
                message += "• Слева (📄) - просмотр договоров\n"
                message += "• Справа (📍) - отправка уведомления"
                
                await update.message.reply_text(
                    message,
                    reply_markup=get_dual_search_keyboard(registry_tp_names, structure_tp_names),
                    parse_mode='Markdown'
                )
                
        # Обработка кнопки "⬅️ Назад в меню филиала"
        elif text == '⬅️ Назад в меню филиала':
            branch = user_states[user_id].get('branch')
            user_states[user_id]['state'] = f'branch_{branch}'
            await update.message.reply_text(f"{branch}", reply_markup=get_branch_menu_keyboard())
                
        # Обработка нажатий на кнопки из двойного поиска
        elif text.startswith('📄 ') or text.startswith('📍 '):
            # Извлекаем название ТП из кнопки
            tp_display_name = text[2:].strip()
            
            # Получаем сохраненные результаты поиска
            dual_results = user_states[user_id].get('dual_search_results', {})
            
            if text.startswith('📄 '):
                # Нажата кнопка из реестра договоров
                registry_results = dual_results.get('registry', [])
                registry_tp_names = dual_results.get('registry_tp_names', [])
                
                # Находим полное название ТП
                full_tp_name = None
                # ВАЖНО: Сначала ищем ТОЧНОЕ совпадение!
                for tp_name in registry_tp_names:
                    if tp_name == tp_display_name:
                        full_tp_name = tp_name
                        break
                
                # Если не нашли точное (название обрезано с ...) - ищем по началу
                if not full_tp_name:
                    for tp_name in registry_tp_names:
                        if tp_display_name.endswith('...') and tp_name.startswith(tp_display_name[:-3]):
                            full_tp_name = tp_name
                            break
                
                # Если всё еще не нашли - ищем вхождение
                if not full_tp_name:
                    for tp_name in registry_tp_names:
                        if tp_display_name in tp_name or tp_name.startswith(tp_display_name):
                            full_tp_name = tp_name
                            break
                
                if full_tp_name:
                    # Фильтруем результаты по найденной ТП
                    tp_results = [r for r in registry_results if r['Наименование ТП'] == full_tp_name]
                    if tp_results:
                        search_query = user_states[user_id].get('last_search_query')
                        await show_tp_results(update, tp_results, full_tp_name, search_query)
                        user_states[user_id]['action'] = 'after_results'
                    else:
                        await update.message.reply_text("❌ Результаты не найдены")
                        
            elif text.startswith('📍 '):
                # Нажата кнопка из структуры сети - переходим к выбору ВЛ для уведомления
                structure_results = dual_results.get('structure', [])
                structure_tp_names = dual_results.get('structure_tp_names', [])
                
                # Находим полное название ТП
                full_tp_name = None
                # ВАЖНО: Сначала ищем ТОЧНОЕ совпадение!
                for tp_name in structure_tp_names:
                    if tp_name == tp_display_name:
                        full_tp_name = tp_name
                        break
                
                # Если не нашли точное (название обрезано с ...) - ищем по началу
                if not full_tp_name:
                    for tp_name in structure_tp_names:
                        if tp_display_name.endswith('...') and tp_name.startswith(tp_display_name[:-3]):
                            full_tp_name = tp_name
                            break
                
                # Если всё еще не нашли - ищем вхождение
                if not full_tp_name:
                    for tp_name in structure_tp_names:
                        if tp_display_name in tp_name or tp_name.startswith(tp_display_name):
                            full_tp_name = tp_name
                            break
                
                if full_tp_name:
                    # ВАЖНО: Делаем точный поиск для получения ВСЕХ записей
                    branch = user_states[user_id].get('branch')
                    network = user_states[user_id].get('network')
                    
                    env_key = get_env_key_for_branch(branch, network, is_reference=True)
                    csv_url = os.environ.get(env_key)
                    
                    if csv_url:
                        data = load_csv_from_url(csv_url)
                        # Точный поиск по полному названию ТП
                        tp_results = [r for r in data if r.get('Наименование ТП', '') == full_tp_name]
                    
                    logger.info(f"[handle_message] Выбрана ТП из структуры: {full_tp_name}")
                    logger.info(f"[handle_message] Точный поиск нашел записей: {len(tp_results)}")
                    
                    if tp_results:
                        # Переходим к отправке уведомления
                        user_states[user_id]['state'] = 'send_notification'
                        user_states[user_id]['action'] = 'select_vl'
                        user_states[user_id]['selected_tp'] = full_tp_name
                        user_states[user_id]['tp_data'] = tp_results[0]
                        user_states[user_id]['branch'] = branch
                        user_states[user_id]['network'] = network
                        
                        # ВАЖНО: Получаем ВСЕ уникальные ВЛ
                        vl_list = list(set([r['Наименование ВЛ'] for r in tp_results]))
                        vl_list.sort()
                        
                        logger.info(f"[handle_message] Уникальных ВЛ найдено: {len(vl_list)}")
                        logger.info(f"[handle_message] ВЛ: {vl_list}")
                        user_states[user_id]['from_dual_search'] = True
                        
                        # Используем функцию для создания клавиатуры
                        reply_markup = get_vl_selection_keyboard(vl_list, full_tp_name, from_dual_search=True)
                        
                        await update.message.reply_text(
                            f"📨 Отправка уведомления по ТП: {full_tp_name}\n"
                            f"📊 Найдено ВЛ: {len(vl_list)}\n\n"
                            f"Выберите ВЛ:",
                            reply_markup=reply_markup
                        )
                    else:
                        await update.message.reply_text("❌ Результаты не найдены")
        
        elif user_states[user_id].get('action') == 'search':
            # Новый двойной поиск
            branch = user_states[user_id].get('branch')
            network = user_states[user_id].get('network')
            
            # Проверяем права пользователя
            user_permissions = get_user_permissions(user_id)
            user_branch = user_permissions.get('branch')
            user_res = user_permissions.get('res')
            
            # Если у пользователя указан конкретный филиал в правах - используем его
            if user_branch and user_branch != 'All':
                branch = user_branch
                logger.info(f"Используем филиал из прав пользователя: {branch}")
            else:
                # Только если выбрали из меню - нормализуем
                branch = normalize_branch_name(branch)
            
            logger.info(f"Двойной поиск ТП для филиала: {branch}, сеть: {network}")
            if user_res and user_res != 'All':
                logger.info(f"Пользователь имеет доступ только к РЭС: {user_res}")
            
            search_messages = [
                "🔍 Ищу информацию...",
                "📋 Проверяю реестр договоров...",
                "🗂️ Проверяю структуру сети...",
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
            
            # Выполняем двойной поиск
            dual_results = await search_tp_in_both_catalogs(text, branch, network, user_res)
            
            await loading_msg.delete()
            
            # Сохраняем результаты и оригинальный запрос
            user_states[user_id]['dual_search_results'] = dual_results
            user_states[user_id]['last_search_query'] = text
            user_states[user_id]['action'] = 'dual_search'
            
            registry_tp_names = dual_results['registry_tp_names']
            structure_tp_names = dual_results['structure_tp_names']
            
            if not registry_tp_names and not structure_tp_names:
                await update.message.reply_text(
                    "❌ ТП не найдено ни в одном справочнике.\n\n"
                    "Попробуйте другой запрос.",
                    reply_markup=get_after_search_keyboard(None, text)
                )
            elif len(registry_tp_names) == 1 and not structure_tp_names:
                # Если найдена только одна ТП в реестре договоров
                await show_tp_results(update, dual_results['registry'], registry_tp_names[0], text)
            elif not registry_tp_names and len(structure_tp_names) == 1:
                # Если найдена только одна ТП в структуре сети - показываем для выбора
                user_states[user_id]['notification_results'] = dual_results['structure']
                user_states[user_id]['action'] = 'select_notification_tp'
                user_states[user_id]['state'] = 'send_notification'
                user_states[user_id]['branch'] = branch
                user_states[user_id]['network'] = network
                
                reply_markup = get_tp_selection_keyboard(structure_tp_names)
                
                await update.message.reply_text(
                    f"✅ ТП найдена только в структуре сети\n\n"
                    f"Подтвердите выбор для отправки уведомления:",
                    reply_markup=reply_markup
                )
            else:
                # Показываем двойную клавиатуру
                registry_count = len(dual_results['registry'])
                structure_count = len(dual_results['structure'])
                
                message = f"🔍 Результаты поиска по запросу: **{text}**\n\n"
                
                if registry_tp_names:
                    message += f"📋 **Реестр договоров** (найдено {registry_count} записей)\n"
                    message += f"   ТП: {len(registry_tp_names)} шт.\n\n"
                
                if structure_tp_names:
                    message += f"🗂️ **Структура сети** (найдено {structure_count} записей)\n"
                    message += f"   ТП: {len(structure_tp_names)} шт.\n\n"
                
                message += "📌 Выберите ТП:\n"
                message += "• Слева (📄) - просмотр договоров\n"
                message += "• Справа (📍) - отправка уведомления"
                
                await update.message.reply_text(
                    message,
                    reply_markup=get_dual_search_keyboard(registry_tp_names, structure_tp_names),
                    parse_mode='Markdown'
                )
        
        elif user_states[user_id].get('action') == 'select_tp':
            results = user_states[user_id].get('search_results', [])
            filtered_results = [r for r in results if r['Наименование ТП'] == text]
            
            if filtered_results:
                search_query = user_states[user_id].get('last_search_query')
                await show_tp_results(update, filtered_results, text, search_query)
                user_states[user_id]['action'] = 'search'
            
        # Обработка кнопки "Назад" для search_tp и send_notification
        elif text == '⬅️ Назад':
            branch = user_states[user_id].get('branch')
            action = user_states[user_id].get('action')
            
            # Для search_tp с action 'after_results' (после показа результатов)
            if state == 'search_tp' and action == 'after_results':
                user_states[user_id]['state'] = f'branch_{branch}'
                user_states[user_id]['action'] = None
                await update.message.reply_text(f"{branch}", reply_markup=get_branch_menu_keyboard())
                return
            
            # Для search_tp с action 'search' (в процессе поиска)
            elif state == 'search_tp' and action == 'search':
                user_states[user_id]['state'] = f'branch_{branch}'
                user_states[user_id]['action'] = None
                await update.message.reply_text(f"{branch}", reply_markup=get_branch_menu_keyboard())
                return
                
            # Для search_tp с action 'dual_search' (после двойного поиска)
            elif state == 'search_tp' and action == 'dual_search':
                user_states[user_id]['state'] = f'branch_{branch}'
                user_states[user_id]['action'] = None
                await update.message.reply_text(f"{branch}", reply_markup=get_branch_menu_keyboard())
                return
            
        return  # Завершаем обработку search_tp
    
    # Обработка кнопки "Назад" для состояний с уведомлениями - ВЫНЕСЕНО НАРУЖУ!
    if state == 'send_notification' and text == '⬅️ Назад':
        action = user_states[user_id].get('action')
        branch = user_states[user_id].get('branch')
        
        # Если мы в процессе отправки уведомления, пришедшего из поиска
        if 'last_search_tp' in user_states[user_id]:
            if action == 'select_vl':
                # Возвращаемся к результатам поиска
                user_states[user_id]['state'] = 'search_tp'
                user_states[user_id]['action'] = 'after_results'
                tp_name = user_states[user_id].get('last_search_tp', '')
                search_query = user_states[user_id].get('last_search_query', tp_name)
                await update.message.reply_text(
                    "Вернулись к результатам поиска",
                    reply_markup=get_after_search_keyboard(tp_name, search_query)
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
                        # Используем ТОЧНОЕ совпадение для получения ВСЕХ ВЛ
                        results = [r for r in data if r.get('Наименование ТП', '') == selected_tp]
                        
                        # Фильтруем по РЭС если нужно
                        user_res = user_permissions.get('res')
                        if user_res and user_res != 'All':
                            results = [r for r in results if r.get('РЭС', '').strip() == user_res]
                        
                        if results:
                            # ВАЖНО: Получаем ВСЕ уникальные ВЛ
                            vl_list = list(set([r['Наименование ВЛ'] for r in results]))
                            vl_list.sort()
                            
                            logger.info(f"[handle_message] При возврате назад найдено {len(vl_list)} ВЛ")
                            
                            # Используем функцию для создания клавиатуры
                            from_dual_search = user_states[user_id].get('from_dual_search', False) 
                            reply_markup = get_vl_selection_keyboard(vl_list, selected_tp, from_dual_search)
                            
                            await update.message.reply_text(
                                f"📨 Отправка уведомления по ТП: {selected_tp}\n"
                                f"📊 Найдено ВЛ: {len(vl_list)}\n\n"
                                f"Выберите ВЛ:",
                                reply_markup=reply_markup
                            )
                        else:
                            await update.message.reply_text("❌ Не удалось загрузить список ВЛ")
                    else:
                        await update.message.reply_text("❌ Справочник не найден")
                elif action == 'request_photo':
                    # Возвращаемся к отправке локации
                    user_states[user_id]['action'] = 'send_location'
                    
                    selected_tp = user_states[user_id].get('selected_tp')
                    selected_vl = user_states[user_id].get('selected_vl')
                    
                    await update.message.reply_text(
                        f"✅ Выбрана ВЛ: {selected_vl}\n"
                        f"📍 ТП: {selected_tp}\n\n"
                        "Теперь отправьте ваше местоположение:",
                        reply_markup=get_location_keyboard()
                    )
                elif action == 'add_comment':
                    # Возвращаемся к запросу фото
                    user_states[user_id]['action'] = 'request_photo'
                    
                    selected_tp = user_states[user_id].get('selected_tp')
                    selected_vl = user_states[user_id].get('selected_vl')
                    
                    await update.message.reply_text(
                        f"📍 ТП: {selected_tp}\n"
                        f"⚡ ВЛ: {selected_vl}\n\n"
                        "📸 Сделайте фото бездоговорного ВОЛС\n\n"
                        "Как отправить фото:\n"
                        "📱 Мобильный: нажмите 📎 → Камера\n"
                        "Или выберите действие ниже:",
                        reply_markup=get_photo_keyboard()
                    )
        else:
            # Если пришли не из поиска
            if action == 'select_notification_tp':
                # Возвращаемся в меню филиала
                user_states[user_id]['state'] = f'branch_{branch}'
                await update.message.reply_text(f"{branch}", reply_markup=get_branch_menu_keyboard())
            elif action == 'select_vl':
                # Возвращаемся к поиску ТП для уведомления
                user_states[user_id]['action'] = 'notification_tp'
                await update.message.reply_text(
                    "📨 Введите наименование ТП для уведомления:",
                    reply_markup=get_search_keyboard()
                )
            elif action in ['send_location', 'request_photo', 'add_comment']:
                # Обрабатываем так же как и выше
                if action == 'send_location':
                    # Возвращаемся к выбору ВЛ
                    user_states[user_id]['action'] = 'select_vl'
                    selected_tp = user_states[user_id].get('selected_tp')
                    
                    # Перезагружаем данные
                    branch = user_states[user_id].get('branch')
                    network = user_states[user_id].get('network')
                    
                    user_permissions = get_user_permissions(user_id)
                    user_branch = user_permissions.get('branch')
                    if user_branch and user_branch != 'All':
                        branch = user_branch
                    
                    env_key = get_env_key_for_branch(branch, network, is_reference=True)
                    csv_url = os.environ.get(env_key)
                    
                    if csv_url:
                        data = load_csv_from_url(csv_url)
                        results = [r for r in data if r.get('Наименование ТП', '') == selected_tp]
                        
                        user_res = user_permissions.get('res')
                        if user_res and user_res != 'All':
                            results = [r for r in results if r.get('РЭС', '').strip() == user_res]
                        
                        if results:
                            vl_list = list(set([r['Наименование ВЛ'] for r in results]))
                            vl_list.sort()
                            
                            from_dual_search = user_states[user_id].get('from_dual_search', False) 
                            reply_markup = get_vl_selection_keyboard(vl_list, selected_tp, from_dual_search)
                            
                            await update.message.reply_text(
                                f"📨 Отправка уведомления по ТП: {selected_tp}\n"
                                f"📊 Найдено ВЛ: {len(vl_list)}\n\n"
                                f"Выберите ВЛ:",
                                reply_markup=reply_markup
                            )
                        else:
                            await update.message.reply_text("❌ Не удалось загрузить список ВЛ")
                    else:
                        await update.message.reply_text("❌ Справочник не найден")
                elif action == 'request_photo':
                    # Возвращаемся к отправке локации
                    user_states[user_id]['action'] = 'send_location'
                    
                    selected_tp = user_states[user_id].get('selected_tp')
                    selected_vl = user_states[user_id].get('selected_vl')
                    
                    await update.message.reply_text(
                        f"✅ Выбрана ВЛ: {selected_vl}\n"
                        f"📍 ТП: {selected_tp}\n\n"
                        "Теперь отправьте ваше местоположение:",
                        reply_markup=get_location_keyboard()
                    )
                elif action == 'add_comment':
                    # Возвращаемся к запросу фото
                    user_states[user_id]['action'] = 'request_photo'
                    
                    selected_tp = user_states[user_id].get('selected_tp')
                    selected_vl = user_states[user_id].get('selected_vl')
                    
                    await update.message.reply_text(
                        f"📍 ТП: {selected_tp}\n"
                        f"⚡ ВЛ: {selected_vl}\n\n"
                        "📸 Сделайте фото бездоговорного ВОЛС\n\n"
                        "Как отправить фото:\n"
                        "📱 Мобильный: нажмите 📎 → Камера\n"
                        "Или выберите действие ниже:",
                        reply_markup=get_photo_keyboard()
                    )
            else:
                # По умолчанию возвращаемся в меню филиала
                user_states[user_id]['state'] = f'branch_{branch}'
                await update.message.reply_text(f"{branch}", reply_markup=get_branch_menu_keyboard())
        return
    
    # ==================== ОБРАБОТКА ОТПРАВКИ УВЕДОМЛЕНИЙ ====================
    # Отправка уведомлений
    elif state == 'send_notification':
        if action == 'notification_tp':
            # Поиск ТП для уведомления
            branch = user_states[user_id].get('branch')
            network = user_states[user_id].get('network')
            
            # Ищем в структуре сети (справочник SP)
            env_key = get_env_key_for_branch(branch, network, is_reference=True)
            csv_url = os.environ.get(env_key)
            
            if not csv_url:
                await update.message.reply_text(f"❌ Справочник для филиала {branch} не найден")
                return
            
            loading_msg = await update.message.reply_text("🔍 Ищу ТП в структуре сети...")
            
            data = load_csv_from_url(csv_url)
            results = search_tp_in_data(text, data, 'Наименование ТП')
            
            # Фильтруем по РЭС если у пользователя ограничения
            user_permissions = get_user_permissions(user_id)
            user_res = user_permissions.get('res')
            
            if user_res and user_res != 'All':
                results = [r for r in results if r.get('РЭС', '').strip() == user_res]
            
            await loading_msg.delete()
            
            if not results:
                await update.message.reply_text("❌ ТП не найдена в справочнике структуры сети")
                return
            
            # ВАЖНО: Получаем ВСЕ уникальные ТП
            tp_list = list(set([r['Наименование ТП'] for r in results]))
            
            if len(tp_list) == 1:
                # Если найдена одна ТП
                user_states[user_id]['notification_results'] = results
                user_states[user_id]['action'] = 'select_notification_tp'
                
                reply_markup = get_tp_selection_keyboard(tp_list)
                
                await update.message.reply_text(
                    f"✅ Найдена 1 ТП в структуре сети. Подтвердите выбор:",
                    reply_markup=reply_markup
                )
            else:
                # Если найдено несколько ТП
                user_states[user_id]['notification_results'] = results
                user_states[user_id]['action'] = 'select_notification_tp'
                
                reply_markup = get_tp_selection_keyboard(tp_list)
                
                await update.message.reply_text(
                    f"✅ Найдено {len(tp_list)} ТП. Выберите нужную:",
                    reply_markup=reply_markup
                )
        
        elif action == 'select_notification_tp':
            # Выбор ТП из списка
            results = user_states[user_id].get('notification_results', [])
            
            # ВАЖНО: Делаем точный поиск для получения ВСЕХ записей с этой ТП
            branch = user_states[user_id].get('branch')
            network = user_states[user_id].get('network')
            
            env_key = get_env_key_for_branch(branch, network, is_reference=True)
            csv_url = os.environ.get(env_key)
            
            if csv_url:
                data = load_csv_from_url(csv_url)
                
                # ИЗМЕНЕНО: Ищем как по точному совпадению, так и по очищенному названию
                tp_results = []
                
                # Сначала пробуем точное совпадение
                tp_results = [r for r in data if r.get('Наименование ТП', '') == text]
                
                # Если не нашли - ищем без учета префиксов
                if not tp_results:
                    # Убираем префикс из выбранного текста
                    text_clean = re.sub(r'^\d+\)\s*', '', text)
                    
                    for row in data:
                        tp_name = row.get('Наименование ТП', '')
                        tp_name_clean = re.sub(r'^\d+\)\s*', '', tp_name)
                        
                        # Сравниваем очищенные версии
                        if tp_name_clean == text_clean or tp_name == text:
                            tp_results.append(row)
                
                # Фильтруем по РЭС если нужно
                user_permissions = get_user_permissions(user_id)
                user_res = user_permissions.get('res')
                if user_res and user_res != 'All':
                    # ИСПРАВЛЕНО: Фильтруем tp_results, а не results!
                    tp_results = [r for r in tp_results if r.get('РЭС', '').strip() == user_res]
                
                logger.info(f"[select_notification_tp] Точный поиск для '{text}' нашел {len(tp_results)} записей")
                
                # ДОБАВЛЕНО: Логирование для отладки
                if tp_results:
                    vl_names = [r.get('Наименование ВЛ', '') for r in tp_results]
                    unique_vl = list(set(vl_names))
                    logger.info(f"[select_notification_tp] Всего записей: {len(tp_results)}, уникальных ВЛ: {len(unique_vl)}")
                    logger.info(f"[select_notification_tp] Примеры ВЛ: {unique_vl[:5]}")
                    
            else:
                # Если не удалось загрузить - используем исходные результаты
                tp_results = [r for r in results if r['Наименование ТП'] == text]
            
            if tp_results:
                # Сохраняем оригинальное название ТП (с префиксом если есть)
                original_tp_name = tp_results[0].get('Наименование ТП', text)
                
                user_states[user_id]['selected_tp'] = original_tp_name
                user_states[user_id]['tp_data'] = tp_results[0]
                user_states[user_id]['action'] = 'select_vl'
                
                # ВАЖНО: Получаем ВСЕ уникальные ВЛ
                vl_list = list(set([r['Наименование ВЛ'] for r in tp_results]))
                vl_list.sort()
                
                logger.info(f"[select_notification_tp] Найдено {len(vl_list)} уникальных ВЛ")
                logger.info(f"[select_notification_tp] ВЛ: {vl_list[:10]}...")  # Показываем первые 10 для отладки
                
                # Используем функцию для создания клавиатуры
                reply_markup = get_vl_selection_keyboard(vl_list, original_tp_name)
                
                await update.message.reply_text(
                    f"📨 Отправка уведомления по ТП: {original_tp_name}\n"
                    f"📊 Найдено записей: {len(tp_results)}\n"
                    f"⚡ Уникальных ВЛ: {len(vl_list)}\n\n"
                    f"Выберите ВЛ:",
                    reply_markup=reply_markup
                )
            else:
                await update.message.reply_text("❌ Ошибка выбора ТП")
        
        elif action == 'select_vl':
            # ВАЖНЕЙШИЙ БЛОК - ОБРАБОТКА ВЫБОРА ВЛ!
            if text == '🔍 Новый поиск':
                user_states[user_id]['state'] = 'search_tp'
                user_states[user_id]['action'] = 'search'
                await update.message.reply_text(
                    "🔍 Введите наименование ТП для поиска:",
                    reply_markup=get_search_keyboard()
                )
            elif text == '⬅️ Вернуться к результатам поиска':
                dual_results = user_states[user_id].get('dual_search_results', {})
                if dual_results:
                    registry_tp_names = dual_results['registry_tp_names']
                    structure_tp_names = dual_results['structure_tp_names']
                    search_query = user_states[user_id].get('last_search_query', '')
                    
                    # Возвращаемся в состояние поиска
                    user_states[user_id]['state'] = 'search_tp'
                    user_states[user_id]['action'] = 'dual_search'
                    
                    message = f"🔍 Результаты поиска по запросу: **{search_query}**\n\n"
                    
                    if registry_tp_names:
                        message += f"📋 **Реестр договоров** (найдено {len(dual_results['registry'])} записей)\n"
                        message += f"   ТП: {len(registry_tp_names)} шт.\n\n"
                    
                    if structure_tp_names:
                        message += f"🗂️ **Структура сети** (найдено {len(dual_results['structure'])} записей)\n"
                        message += f"   ТП: {len(structure_tp_names)} шт.\n\n"
                    
                    message += "📌 Выберите ТП:\n"
                    message += "• Слева (📄) - просмотр договоров\n"
                    message += "• Справа (📍) - отправка уведомления"
                    
                    await update.message.reply_text(
                        message,
                        reply_markup=get_dual_search_keyboard(registry_tp_names, structure_tp_names),
                        parse_mode='Markdown'
                    )
                else:
                    await update.message.reply_text("❌ Результаты поиска не найдены")
            else:
                # Выбрана конкретная ВЛ
                selected_tp = user_states[user_id].get('selected_tp')
                
                # Проверяем, что текст - это ВЛ (не кнопка)
                if text not in ['⬅️ Назад', '🏠 Главная', '🔄 Рестарт', '⚠️ Показано']:
                    user_states[user_id]['selected_vl'] = text
                    user_states[user_id]['action'] = 'send_location'
                    
                    await update.message.reply_text(
                        f"✅ Выбрана ВЛ: {text}\n"
                        f"📍 ТП: {selected_tp}\n\n"
                        "Теперь отправьте ваше местоположение:",
                        reply_markup=get_location_keyboard()
                    )
        
        elif action == 'send_location':
            # Ожидаем геолокацию, но обрабатываем текстовые команды
            if text == '⏭ Пропустить и добавить комментарий':
                user_states[user_id]['action'] = 'add_comment'
                await update.message.reply_text(
                    "💬 Введите комментарий к уведомлению:",
                    reply_markup=get_comment_keyboard()
                )
            elif text == '📤 Отправить без фото и комментария':
                await send_notification(update, context)
        
        elif action == 'request_photo':
            # Обработка текста при запросе фото
            if text == '⏭ Пропустить и добавить комментарий':
                user_states[user_id]['action'] = 'add_comment'
                await update.message.reply_text(
                    "💬 Введите комментарий к уведомлению:",
                    reply_markup=get_comment_keyboard()
                )
            elif text == '📤 Отправить без фото и комментария':
                await send_notification(update, context)
        
        elif action == 'add_comment':
            # Обработка комментария
            if text == '📤 Отправить без комментария':
                await send_notification(update, context)
            else:
                # Сохраняем комментарий
                user_states[user_id]['comment'] = text
                await send_notification(update, context)
    
    # ==================== ОБРАБОТКА ОТЧЕТОВ ====================
    elif state == 'reports':
        if text == '📊 Уведомления РОССЕТИ КУБАНЬ':
            await generate_report(update, context, 'RK', permissions)
        elif text == '📊 Уведомления РОССЕТИ ЮГ':
            await generate_report(update, context, 'UG', permissions)
        elif text == '📈 Активность РОССЕТИ КУБАНЬ':
            await generate_activity_report(update, context, 'RK', permissions)
        elif text == '📈 Активность РОССЕТИ ЮГ':
            await generate_activity_report(update, context, 'UG', permissions)
    
    elif state == 'report_actions':
        if text == '📧 Отправить себе на почту':
            user_email = permissions.get('email')
            if not user_email:
                await update.message.reply_text("❌ Email не указан в вашем профиле")
                return
            
            last_report = user_states[user_id].get('last_report', {})
            if last_report:
                buffer = BytesIO(last_report['data'])
                buffer.seek(0)
                
                email_sent = await send_email(
                    user_email,
                    f"Отчет ВОЛС - {last_report['filename']}",
                    f"{last_report['caption']}\n\nОтчет во вложении.",
                    buffer,
                    last_report['filename']
                )
                
                if email_sent:
                    await update.message.reply_text(f"✅ Отчет отправлен на {user_email}")
                else:
                    await update.message.reply_text("❌ Ошибка отправки email")
            else:
                await update.message.reply_text("❌ Нет отчета для отправки")
    
    # ==================== ОБРАБОТКА СПРАВКИ ====================
    elif state == 'reference':
        # Обработка выбора документа
        doc_name = None
        doc_url = None
        
        # Проверяем соответствие кнопки документу
        for original_name, url in REFERENCE_DOCS.items():
            if url:
                # Проверяем сокращенные названия кнопок
                if ('План выручки ВОЛС 24-26' in text and 'План по выручке' in original_name) or \
                   ('Акт инвентаризации' in text and 'акта инвентаризации' in original_name) or \
                   ('Гарантийное письмо' in text and 'гарантийного письма' in original_name) or \
                   ('Претензионное письмо' in text and 'претензионного письма' in original_name) or \
                   (original_name in text) or \
                   (text.replace('📄 ', '') == original_name):
                    doc_name = original_name
                    doc_url = url
                    break
        
        if doc_name and doc_url:
            loading_msg = await update.message.reply_text("📥 Загружаю документ...")
            
            document = await get_cached_document(doc_name, doc_url)
            
            if document:
                await loading_msg.delete()
                
                # Определяем расширение файла
                if 'xlsx' in doc_url or 'spreadsheets' in doc_url:
                    extension = '.xlsx'
                elif 'pdf' in doc_url or doc_url.endswith('.pdf'):
                    extension = '.pdf'
                elif 'docx' in doc_url or doc_url.endswith('.docx'):
                    extension = '.docx'
                else:
                    extension = '.pdf'
                
                filename = f"{doc_name}{extension}"
                
                await update.message.reply_document(
                    document=InputFile(document, filename=filename),
                    caption=f"📄 {doc_name}"
                )
                
                # Сохраняем информацию о документе
                user_states[user_id]['state'] = 'document_actions'
                user_states[user_id]['last_document'] = {
                    'name': doc_name,
                    'filename': filename,
                    'data': document.getvalue()
                }
                
                await update.message.reply_text(
                    "Документ загружен",
                    reply_markup=get_document_action_keyboard()
                )
            else:
                await loading_msg.delete()
                await update.message.reply_text("❌ Не удалось загрузить документ")
    
    elif state == 'document_actions':
        if text == '📧 Отправить себе на почту':
            user_email = permissions.get('email')
            if not user_email:
                await update.message.reply_text("❌ Email не указан в вашем профиле")
                return
            
            last_document = user_states[user_id].get('last_document', {})
            if last_document:
                buffer = BytesIO(last_document['data'])
                buffer.seek(0)
                
                email_sent = await send_email(
                    user_email,
                    f"Документ ВОЛС - {last_document['name']}",
                    f"Документ '{last_document['name']}' во вложении.",
                    buffer,
                    last_document['filename']
                )
                
                if email_sent:
                    await update.message.reply_text(f"✅ Документ отправлен на {user_email}")
                else:
                    await update.message.reply_text("❌ Ошибка отправки email")
            else:
                await update.message.reply_text("❌ Нет документа для отправки")
    
    # ==================== МОИ НАСТРОЙКИ ====================
    elif state == 'settings':
        if text == '📖 Руководство пользователя':
            guide_text = f"""📖 **РУКОВОДСТВО ПОЛЬЗОВАТЕЛЯ**

🤖 ВОЛС Ассистент v{BOT_VERSION}

**Основные функции:**

🔍 **Поиск по ТП**
• Поиск информации о договорах ВОЛС
• Просмотр контрагентов и опор
• Двойной поиск по реестру и структуре

📨 **Отправка уведомлений**
• Уведомление ответственных лиц
• Прикрепление фото и геолокации
• Добавление комментариев

📊 **Отчеты** (для руководителей)
• Отчет по уведомлениям
• Отчет по активности пользователей
• Экспорт в Excel

📄 **Справочные документы**
• Регламенты и формы документов
• Отправка на email

**Подробная инструкция:**
{USER_GUIDE_URL}

По вопросам обращайтесь к администратору."""
            
            await update.message.reply_text(guide_text, parse_mode='Markdown')
        
        elif text == 'ℹ️ Моя информация':
            info_text = f"""👤 **Ваша информация**

🆔 ID: {user_id}
👤 ФИО: {permissions.get('name', 'Не указано')}
🏢 Филиал: {permissions.get('branch', 'Не указан')}
📍 РЭС: {permissions.get('res', 'Не указан')}
📧 Email: {permissions.get('email', 'Не указан')}
🔐 Уровень доступа: {permissions.get('visibility', 'Не указан')}
👥 Ответственный за: {permissions.get('responsible', 'Не указано')}"""
            
            await update.message.reply_text(info_text, parse_mode='Markdown')
    
    # Обработка кнопки "Назад" для остальных состояний
    if text == '⬅️ Назад':
        if state == 'report_actions':
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
            user_states[user_id] = {'state': 'phone_book'}
            await update.message.reply_text(
                "📞 Телефонный справочник контрагентов\n\n"
                "Выберите способ поиска:",
                reply_markup=get_phone_book_menu_keyboard()
            )
        
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
                "📨 Всем кто запускал бота - отправка только тем, кто использовал /start после последнего обновления\n\n"
                "📋 Всем из базы данных - отправка всем пользователям из зон доступа",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
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
            await update.message.reply_text(
                "🔍 Введите наименование ТП для поиска:",
                reply_markup=get_search_keyboard()
            )
        
        elif text == '📨 Отправить уведомление':
            user_states[user_id]['state'] = 'send_notification'
            user_states[user_id]['action'] = 'notification_tp'
            await update.message.reply_text(
                "📨 Введите наименование ТП для уведомления:",
                reply_markup=get_search_keyboard()
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
    
    # ==================== ТЕЛЕФОННЫЙ СПРАВОЧНИК ====================
    elif state == 'phone_book':
        if text == '🔍 Поиск по наименованию':
            user_states[user_id]['state'] = 'phone_book_search'
            await update.message.reply_text(
                "🔍 Введите название организации или его часть:",
                reply_markup=get_search_keyboard()
            )
        
        elif text == '📋 Весь реестр':
            # Загружаем данные контрагентов
            contractors_data = load_contractors_data()
            if not contractors_data:
                await update.message.reply_text(
                    "❌ Не удалось загрузить справочник контрагентов",
                    reply_markup=get_main_keyboard(permissions)
                )
                user_states[user_id] = {'state': 'main'}
                return
            
            # Получаем отсортированный список
            all_contractors = get_all_contractors_sorted(contractors_data)
            
            if not all_contractors:
                await update.message.reply_text(
                    "📋 Справочник пуст",
                    reply_markup=get_phone_book_menu_keyboard()
                )
                return
            
            # Сохраняем данные для навигации
            user_states[user_id]['state'] = 'phone_book_list'
            user_states[user_id]['contractors_list'] = all_contractors
            user_states[user_id]['contractors_data'] = contractors_data
            user_states[user_id]['current_page'] = 0
            
            await update.message.reply_text(
                f"📋 Всего контрагентов: {len(all_contractors)}\n"
                "Выберите контрагента:",
                reply_markup=get_contractors_list_keyboard(all_contractors, 0)
            )
    
    elif state == 'phone_book_search':
        # Поиск контрагента
        contractors_data = load_contractors_data()
        if not contractors_data:
            await update.message.reply_text(
                "❌ Не удалось загрузить справочник контрагентов",
                reply_markup=get_main_keyboard(permissions)
            )
            user_states[user_id] = {'state': 'main'}
            return
        
        # Ищем контрагентов
        results = search_contractors(text, contractors_data)
        
        if not results:
            await update.message.reply_text(
                f"❌ По запросу '{escape_markdown(text)}' ничего не найдено\n\n"
                "Попробуйте изменить запрос",
                reply_markup=get_phone_book_menu_keyboard()
            )
            user_states[user_id]['state'] = 'phone_book'
            return
        
        # Если найден один контрагент - сразу показываем
        if len(results) == 1:
            contractor_info = format_contractor_info(results[0])
            user_states[user_id]['state'] = 'contractor_view'
            user_states[user_id]['current_contractor'] = results[0]
            
            await update.message.reply_text(
                contractor_info,
                parse_mode='Markdown',
                reply_markup=get_contractor_actions_keyboard()
            )
        else:
            # Если найдено несколько - показываем список
            contractor_names = [r['Контрагент'] for r in results]
            
            user_states[user_id]['state'] = 'phone_book_list'
            user_states[user_id]['contractors_list'] = contractor_names
            user_states[user_id]['contractors_data'] = contractors_data
            user_states[user_id]['current_page'] = 0
            user_states[user_id]['search_query'] = text
            
            await update.message.reply_text(
                f"🔍 По запросу '{escape_markdown(text)}' найдено: {len(results)}\n"
                "Выберите контрагента:",
                reply_markup=get_contractors_list_keyboard(contractor_names, 0)
            )
    
    elif state == 'phone_book_list':
        if text == '⬅️ Предыдущая':
            current_page = user_states[user_id].get('current_page', 0)
            if current_page > 0:
                user_states[user_id]['current_page'] = current_page - 1
                contractors_list = user_states[user_id].get('contractors_list', [])
                
                await update.message.reply_text(
                    "Выберите контрагента:",
                    reply_markup=get_contractors_list_keyboard(contractors_list, current_page - 1)
                )
        
        elif text == '➡️ Следующая':
            current_page = user_states[user_id].get('current_page', 0)
            contractors_list = user_states[user_id].get('contractors_list', [])
            items_per_page = 20
            total_pages = (len(contractors_list) - 1) // items_per_page + 1
            
            if current_page < total_pages - 1:
                user_states[user_id]['current_page'] = current_page + 1
                
                await update.message.reply_text(
                    "Выберите контрагента:",
                    reply_markup=get_contractors_list_keyboard(contractors_list, current_page + 1)
                )
        
        elif text == '🔍 Поиск':
            user_states[user_id]['state'] = 'phone_book_search'
            await update.message.reply_text(
                "🔍 Введите название организации или его часть:",
                reply_markup=get_search_keyboard()
            )
        
        elif text.startswith('🏢 '):
            # Выбран контрагент
            contractor_name = text[2:].strip()
            
            # Если название было обрезано - ищем полное
            contractors_data = user_states[user_id].get('contractors_data', [])
            contractor_data = None
            
            for row in contractors_data:
                if row.get('Контрагент', '').strip() == contractor_name or \
                   row.get('Контрагент', '').strip().startswith(contractor_name.replace('...', '')):
                    contractor_data = row
                    break
            
            if contractor_data:
                contractor_info = format_contractor_info(contractor_data)
                user_states[user_id]['state'] = 'contractor_view'
                user_states[user_id]['current_contractor'] = contractor_data
                
                await update.message.reply_text(
                    contractor_info,
                    parse_mode='Markdown',
                    reply_markup=get_contractor_actions_keyboard()
                )
            else:
                await update.message.reply_text("❌ Информация о контрагенте не найдена")
    
    elif state == 'contractor_view':
        if text == '🔍 Новый поиск':
            user_states[user_id]['state'] = 'phone_book_search'
            await update.message.reply_text(
                "🔍 Введите название организации или его часть:",
                reply_markup=get_search_keyboard()
            )
        
        elif text == '📋 К списку контрагентов':
            # Возвращаемся к списку если он был
            if 'contractors_list' in user_states[user_id]:
                contractors_list = user_states[user_id]['contractors_list']
                current_page = user_states[user_id].get('current_page', 0)
                user_states[user_id]['state'] = 'phone_book_list'
                
                search_query = user_states[user_id].get('search_query')
                if search_query:
                    message = f"🔍 Результаты поиска '{escape_markdown(search_query)}':\n"
                else:
                    message = f"📋 Всего контрагентов: {len(contractors_list)}\n"
                
                await update.message.reply_text(
                    message + "Выберите контрагента:",
                    reply_markup=get_contractors_list_keyboard(contractors_list, current_page)
                )
            else:
                # Если списка не было - показываем главное меню справочника
                user_states[user_id]['state'] = 'phone_book'
                await update.message.reply_text(
                    "📞 Телефонный справочник контрагентов\n\n"
                    "Выберите способ поиска:",
                    reply_markup=get_phone_book_menu_keyboard()
                )
    # ЧАСТЬ 5.3 КОНЕЦ====================================================================================================================
  
    # ЧАСТЬ 5.3 КОНЕЦ====================================================================================================================
    
    
   
  
# ЧАСТЬ ФИНАЛ=======================================================================================================================

# ==================== ОБРАБОТЧИКИ ЛОКАЦИИ И ФОТО ====================

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

# ==================== ДОБАВЛЯЕМ НЕДОСТАЮЩИЕ ФУНКЦИИ ====================

async def generate_report(update: Update, context: ContextTypes.DEFAULT_TYPE, network: str, permissions: Dict):
    """Генерация отчета по уведомлениям"""
    loading_msg = await update.message.reply_text("📊 Генерирую отчет...")
    
    # Фильтруем уведомления
    notifications = notifications_storage.get(network, [])
    
    if not notifications:
        await loading_msg.delete()
        await update.message.reply_text(
            f"📊 Нет данных для отчета по {'РОССЕТИ КУБАНЬ' if network == 'RK' else 'РОССЕТИ ЮГ'}"
        )
        return
    
    # Создаем DataFrame - БЕЗ ID!
    report_data = []
    for notif in notifications:
        report_data.append({
            'Филиал': notif['branch'],
            'РЭС': notif['res'],
            'ТП': notif['tp'],
            'ВЛ': notif['vl'],
            'Отправитель': notif['sender_name'],
            'Получатель': notif['recipient_name'],
            'Дата и время': notif['datetime'],
            'Координаты': notif['coordinates'],
            'Комментарий': notif['comment'],
            'Фото': 'Да' if notif['has_photo'] else 'Нет'  # Преобразуем в Да/Нет
        })
    
    df = pd.DataFrame(report_data)
    
    # Создаем Excel файл
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Уведомления', index=False)
        
        workbook = writer.book
        worksheet = writer.sheets['Уведомления']
        
        # Форматирование
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#4472C4',
            'font_color': 'white',
            'border': 1
        })
        
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        
        # Автоподбор ширины колонок
        for i, col in enumerate(df.columns):
            # Безопасно вычисляем максимальную длину
            try:
                max_len = df[col].astype(str).str.len().max()
                # Проверяем на NaN
                if pd.isna(max_len):
                    max_len = 10
                column_len = max(int(max_len), len(col)) + 2
            except:
                column_len = len(col) + 2
            
            worksheet.set_column(i, i, column_len)
    
    buffer.seek(0)
    
    # Отправляем файл
    network_name = 'РОССЕТИ КУБАНЬ' if network == 'RK' else 'РОССЕТИ ЮГ'
    filename = f"Уведомления_{network_name}_{get_moscow_time().strftime('%d.%m.%Y_%H%M')}.xlsx"
    
    await loading_msg.delete()
    
    caption = f"📊 Отчет по уведомлениям {network_name}\n"
    caption += f"Период: все время\n"
    caption += f"Всего уведомлений: {len(notifications)}"
    
    await update.message.reply_document(
        document=InputFile(buffer, filename=filename),
        caption=caption
    )
    
    # Сохраняем информацию об отчете
    user_id = str(update.effective_user.id)
    user_states[user_id]['state'] = 'report_actions'
    user_states[user_id]['last_report'] = {
        'filename': filename,
        'caption': caption,
        'data': buffer.getvalue()
    }
    
    await update.message.reply_text(
        "Отчет сгенерирован",
        reply_markup=get_report_action_keyboard()
    )

async def generate_activity_report(update: Update, context: ContextTypes.DEFAULT_TYPE, network: str, permissions: Dict):
    """Генерация отчета по активности"""
    loading_msg = await update.message.reply_text("📊 Генерирую отчет активности...")
    
    # Собираем данные об активности - БЕЗ ID!
    activity_data = []
    for uid, activity in user_activity.items():
        user_data = users_cache.get(uid, {})
        if network == 'RK' and user_data.get('visibility') in ['All', 'RK']:
            activity_data.append({
                'ФИО': user_data.get('name', 'Неизвестный'),
                'Филиал': user_data.get('branch', '-'),
                'РЭС': user_data.get('res', '-'),
                'Последняя активность': activity['last_activity'].strftime('%d.%m.%Y %H:%M'),
                'Количество уведомлений': activity['count']
            })
        elif network == 'UG' and user_data.get('visibility') in ['All', 'UG']:
            activity_data.append({
                'ФИО': user_data.get('name', 'Неизвестный'),
                'Филиал': user_data.get('branch', '-'),
                'РЭС': user_data.get('res', '-'),
                'Последняя активность': activity['last_activity'].strftime('%d.%m.%Y %H:%M'),
                'Количество уведомлений': activity['count']
            })
    
    if not activity_data:
        await loading_msg.delete()
        await update.message.reply_text(
            f"📊 Нет данных по активности для {'РОССЕТИ КУБАНЬ' if network == 'RK' else 'РОССЕТИ ЮГ'}"
        )
        return
    
    # Создаем DataFrame
    df = pd.DataFrame(activity_data)
    
    # Создаем Excel файл
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Активность', index=False)
        
        workbook = writer.book
        worksheet = writer.sheets['Активность']
        
        # Форматирование
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#70AD47',
            'font_color': 'white',
            'border': 1
        })
        
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        
        # Автоподбор ширины колонок
        for i, col in enumerate(df.columns):
            # Безопасно вычисляем максимальную длину
            try:
                max_len = df[col].astype(str).str.len().max()
                # Проверяем на NaN
                if pd.isna(max_len):
                    max_len = 10
                column_len = max(int(max_len), len(col)) + 2
            except:
                column_len = len(col) + 2
            
            worksheet.set_column(i, i, column_len)
    
    buffer.seek(0)
    
    # Отправляем файл
    network_name = 'РОССЕТИ КУБАНЬ' if network == 'RK' else 'РОССЕТИ ЮГ'
    filename = f"Активность_{network_name}_{get_moscow_time().strftime('%d.%m.%Y_%H%M')}.xlsx"
    
    await loading_msg.delete()
    
    caption = f"📈 Отчет по активности пользователей {network_name}\n"
    caption += f"Всего активных пользователей: {len(activity_data)}"
    
    await update.message.reply_document(
        document=InputFile(buffer, filename=filename),
        caption=caption
    )
    
    # Сохраняем информацию об отчете
    user_id = str(update.effective_user.id)
    user_states[user_id]['state'] = 'report_actions'
    user_states[user_id]['last_report'] = {
        'filename': filename,
        'caption': caption,
        'data': buffer.getvalue()
    }
    
    await update.message.reply_text(
        "Отчет сгенерирован",
        reply_markup=get_report_action_keyboard()
    )

async def generate_ping_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Генерация отчета о статусе пользователей"""
    loading_msg = await update.message.reply_text("🔄 Проверяю статус пользователей...")
    
    ping_data = []
    total_users = len(users_cache)
    active_users = 0
    blocked_users = 0
    never_started = 0
    
    for uid, user_data in users_cache.items():
        status = "❓ Неизвестно"
        last_activity = "-"
        
        # Проверяем, запускал ли пользователь бота
        if uid in bot_users:
            last_start = bot_users[uid]['last_start']
            last_activity = last_start.strftime('%d.%m.%Y %H:%M')
            status = "✅ Активен"
            active_users += 1
        else:
            status = "⏸️ Не запускал"
            never_started += 1
        
        ping_data.append({
            'ID': uid,
            'ФИО': user_data.get('name', 'Неизвестный'),
            'Филиал': user_data.get('branch', '-'),
            'РЭС': user_data.get('res', '-'),
            'Статус': status,
            'Последний запуск': last_activity
        })
    
    # Создаем DataFrame
    df = pd.DataFrame(ping_data)
    
    # Создаем Excel файл
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Статус пользователей', index=False)
        
        workbook = writer.book
        worksheet = writer.sheets['Статус пользователей']
        
        # Форматирование
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#FFC000',
            'font_color': 'black',
            'border': 1
        })
        
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        
        # Автоподбор ширины колонок
        for i, col in enumerate(df.columns):
            # Безопасно вычисляем максимальную длину
            try:
                max_len = df[col].astype(str).str.len().max()
                # Проверяем на NaN
                if pd.isna(max_len):
                    max_len = 10
                column_len = max(int(max_len), len(col)) + 2
            except:
                column_len = len(col) + 2
            
            worksheet.set_column(i, i, column_len)
    
    buffer.seek(0)
    
    # Отправляем файл
    filename = f"Статус_пользователей_{get_moscow_time().strftime('%d.%m.%Y_%H%M')}.xlsx"
    
    await loading_msg.delete()
    
    caption = f"""📊 Статус пользователей бота

👥 Всего пользователей: {total_users}
✅ Активных (запускали бота): {active_users}
⏸️ Не запускали: {never_started}

💾 Данные сохранены в файле: {BOT_USERS_FILE}"""
    
    await update.message.reply_document(
        document=InputFile(buffer, filename=filename),
        caption=caption
    )

async def notify_restart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Уведомление о перезапуске бота"""
    # Проверяем, есть ли пользователи для уведомления
    if not bot_users:
        await update.message.reply_text(
            "⚠️ Нет пользователей для уведомления.\n"
            "Пока никто не активировал бота командой /start после последнего обновления."
        )
        return
        
    loading_msg = await update.message.reply_text(
        f"🔄 Отправляю уведомления о перезапуске...\n"
        f"Всего пользователей: {len(bot_users)}"
    )
    
    success_count = 0
    failed_count = 0
    
    message_text = """🔄 Бот ВОЛС Ассистент был обновлен!

✨ Что нового:
• Улучшена стабильность работы
• Оптимизирована скорость загрузки данных
• Исправлены мелкие ошибки

Для продолжения работы используйте команду /start"""
    
    # Отправляем по одному сообщению с задержкой
    for uid in bot_users.keys():
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=message_text
            )
            success_count += 1
            await asyncio.sleep(0.1)  # Защита от лимитов Telegram
            
            # Обновляем статус каждые 10 сообщений
            if success_count % 10 == 0:
                try:
                    await loading_msg.edit_text(
                        f"🔄 Отправляю уведомления...\n"
                        f"✅ Отправлено: {success_count}/{len(bot_users)}"
                    )
                except:
                    pass
                    
        except Exception as e:
            logger.error(f"Не удалось отправить уведомление пользователю {uid}: {e}")
            failed_count += 1
    
    await loading_msg.delete()
    
    result_text = f"""✅ Уведомление о перезапуске отправлено!

📊 Статистика:
📨 Успешно: {success_count}
❌ Не доставлено: {failed_count}
👥 Всего пользователей: {len(bot_users)}"""
    
    await update.message.reply_text(result_text)

async def handle_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка массовой рассылки"""
    user_id = str(update.effective_user.id)
    text = update.message.text
    
    if text == '❌ Отмена':
        user_states[user_id] = {'state': 'main'}
        permissions = get_user_permissions(user_id)
        await update.message.reply_text(
            "Рассылка отменена",
            reply_markup=get_main_keyboard(permissions)
        )
        return
    
    broadcast_type = user_states[user_id].get('broadcast_type', 'bot_users')
    
    # Определяем получателей
    if broadcast_type == 'bot_users':
        recipients = list(bot_users.keys())
        recipients_name = "пользователям, запускавшим бота"
    else:
        recipients = list(users_cache.keys())
        recipients_name = "всем пользователям из базы"
    
    loading_msg = await update.message.reply_text(
        f"📤 Начинаю рассылку {recipients_name}...\n"
        f"Всего получателей: {len(recipients)}"
    )
    
    success_count = 0
    failed_count = 0
    
    # Отправляем сообщения
    for i, uid in enumerate(recipients):
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=text,
                parse_mode='Markdown'
            )
            success_count += 1
            await asyncio.sleep(0.1)  # Защита от лимитов
            
            # Обновляем статус каждые 10 сообщений
            if (i + 1) % 10 == 0:
                try:
                    await loading_msg.edit_text(
                        f"📤 Отправляю сообщения...\n"
                        f"✅ Отправлено: {success_count}/{len(recipients)}"
                    )
                except:
                    pass
                    
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение пользователю {uid}: {e}")
            failed_count += 1
    
    await loading_msg.delete()
    
    # Результат
    result_text = f"""✅ Рассылка завершена!

📊 Статистика:
📨 Успешно отправлено: {success_count}
❌ Не доставлено: {failed_count}
👥 Всего получателей: {len(recipients)}
📝 Тип рассылки: {recipients_name}"""
    
    user_states[user_id] = {'state': 'main'}
    permissions = get_user_permissions(user_id)
    
    await update.message.reply_text(
        result_text,
        reply_markup=get_main_keyboard(permissions)
    )

# ==================== ФОНОВЫЕ ЗАДАЧИ ====================

async def preload_documents():
    """Предзагрузка документов в кэш при старте"""
    logger.info("📄 Начинаем предзагрузку документов...")
    
    for doc_name, doc_url in REFERENCE_DOCS.items():
        if doc_url:
            try:
                logger.info(f"Загружаем {doc_name}...")
                await get_cached_document(doc_name, doc_url)
                logger.info(f"✅ {doc_name} загружен в кэш")
            except Exception as e:
                logger.error(f"❌ Ошибка загрузки {doc_name}: {e}")
    
    logger.info("✅ Предзагрузка документов завершена")

async def refresh_users_data():
    """Периодическое обновление данных пользователей"""
    while True:
        await asyncio.sleep(300)  # Каждые 5 минут
        logger.info("🔄 Обновляем данные пользователей...")
        try:
            load_users_data()
            logger.info("✅ Данные пользователей обновлены")
        except Exception as e:
            logger.error(f"❌ Ошибка обновления данных пользователей: {e}")

async def save_bot_users_periodically():
    """Периодическое сохранение данных о пользователях бота"""
    while True:
        await asyncio.sleep(120)  # Каждые 2 минуты вместо 10
        
        # Сохраняем только если есть что сохранять
        if bot_users:
            if save_bot_users():
                logger.info(f"⏰ Автосохранение: сохранено {len(bot_users)} пользователей")
            else:
                logger.error("❌ Ошибка автосохранения данных пользователей")
        else:
            logger.debug("⏰ Автосохранение: нет данных для сохранения")

async def refresh_documents_cache():
    """Периодическое обновление кэша документов"""
    while True:
        await asyncio.sleep(3600)  # Каждый час
        logger.info("🔄 Обновляем кэш документов...")
        
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

# ==================== НАСТРОЙКА ВЕБХУКА ====================

async def setup_webhook(application: Application, webhook_url: str):
    """Настройка вебхука"""
    try:
        # Удаляем старый вебхук
        await application.bot.delete_webhook(drop_pending_updates=True)
        
        # Устанавливаем новый
        success = await application.bot.set_webhook(
            url=webhook_url,
            allowed_updates=Update.ALL_TYPES
        )
        
        if success:
            logger.info(f"✅ Вебхук установлен: {webhook_url}")
            
            # Проверяем информацию о вебхуке
            webhook_info = await application.bot.get_webhook_info()
            logger.info(f"📌 Webhook URL: {webhook_info.url}")
            logger.info(f"📌 Pending updates: {webhook_info.pending_update_count}")
        else:
            logger.error("❌ Не удалось установить вебхук")
            
    except Exception as e:
        logger.error(f"❌ Ошибка настройки вебхука: {e}")

# ==================== ИНИЦИАЛИЗАЦИЯ ====================

async def init_and_start():
    """Инициализация и запуск фоновых задач"""
    logger.info("=" * 60)
    logger.info(f"🚀 ЗАПУСК БОТА ВОЛС АССИСТЕНТ v{BOT_VERSION}")
    logger.info("=" * 60)
    
    # Загружаем документы
    logger.info("📄 Начинаем предзагрузку документов...")
    await preload_documents()
    
    # Загружаем CSV файлы
    logger.info("📊 Начинаем предзагрузку CSV файлов...")
    await preload_csv_files()
    
    # Выводим статистику
    logger.info("=" * 60)
    logger.info("📈 СТАТИСТИКА ПОСЛЕ ЗАГРУЗКИ:")
    logger.info(f"👥 Пользователей в базе (CSV): {len(users_cache)}")
    logger.info(f"🔄 Пользователей запускавших бота: {len(bot_users)}")
    logger.info(f"📁 CSV файлов в кэше: {len(csv_cache)}")
    logger.info(f"📄 Документов в кэше: {len(documents_cache)}")
    logger.info("=" * 60)
    
    # Запускаем фоновые задачи
    logger.info("⚙️ Запускаем фоновые задачи...")
    asyncio.create_task(refresh_documents_cache())
    asyncio.create_task(refresh_users_data())
    asyncio.create_task(save_bot_users_periodically())
    
    logger.info("✅ Инициализация завершена!")

# ==================== ГЛАВНАЯ ФУНКЦИЯ ====================

if __name__ == '__main__':
    def signal_handler(sig, frame):
        logger.info("🛑 Получен сигнал остановки, сохраняем данные...")
        save_bot_users()
        logger.info("💾 Данные сохранены. Завершение работы.")
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
    
    # Регистрируем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("reload", reload_users))
    application.add_handler(CommandHandler("checkuser", check_user))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(MessageHandler(filters.LOCATION, handle_location))
    application.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    application.add_error_handler(error_handler)
    
    # Загружаем данные
    logger.info("📊 Загружаем данные пользователей...")
    load_users_data()
    logger.info("💾 Загружаем историю запусков бота...")
    load_bot_users()
    
    async def post_init(application: Application) -> None:
        """Вызывается после инициализации приложения"""
        await init_and_start()
    
    async def post_shutdown(application: Application) -> None:
        """Вызывается при остановке приложения"""
        logger.info("🛑 Сохраняем данные перед остановкой...")
        save_bot_users()
        logger.info("✅ Данные сохранены")
    
    application.post_init = post_init
    if hasattr(application, 'post_shutdown'):
        application.post_shutdown = post_shutdown
    
    # Запуск бота
    if WEBHOOK_URL:
        logger.info(f"🌐 Запуск в режиме вебхука: {WEBHOOK_URL}")
        application.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=BOT_TOKEN,
            webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}",
            drop_pending_updates=True
        )
    else:
        logger.info("🤖 Запуск в режиме polling...")
        application.run_polling(drop_pending_updates=True)
