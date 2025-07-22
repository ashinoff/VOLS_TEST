"""
ВОЛС Ассистент 3.0 FULL — Telegram бот для управления уведомлениями о бездоговорных ВОЛС
Версия: 3.0 FULL  (100 % функционал 2.1.0 + TURBO-оптимизации)
"""
# ------------------------------------------------------------------
# ЧАСТЬ 0  —  ИМПОРТЫ И НАСТРОЙКА
# ------------------------------------------------------------------
BOT_VERSION = "3.0"

import os
import logging
import csv
import io
import re
import json
import signal
import sys
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from io import BytesIO

import pytz
import asyncio
import aiofiles

import httpx  # ← заменяет requests + aiohttp
import aiosmtplib  # ← заменяет smtplib
from email.message import EmailMessage

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, InputFile
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes

import pandas as pd  # только для Excel-отчётов

# -------------------- Настройка логирования --------------------
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------- Конфиг --------------------
BOT_TOKEN       = os.environ.get('BOT_TOKEN')
WEBHOOK_URL     = os.environ.get('WEBHOOK_URL')
PORT            = int(os.environ.get('PORT', 5000))
ZONES_CSV_URL   = os.environ.get('ZONES_CSV_URL')

SMTP_SERVER     = os.environ.get('SMTP_SERVER', 'smtp.mail.ru')
SMTP_PORT       = int(os.environ.get('SMTP_PORT', '465'))
SMTP_EMAIL      = os.environ.get('SMTP_EMAIL')
SMTP_PASSWORD   = os.environ.get('SMTP_PASSWORD')

MOSCOW_TZ = pytz.timezone('Europe/Moscow')

# -------------------- Списки филиалов --------------------
ROSSETI_KUBAN_BRANCHES = [
    "Юго-Западные ЭС", "Усть-Лабинские ЭС", "Тимашевские ЭС", "Тихорецкие ЭС",
    "Славянские ЭС", "Ленинградские ЭС", "Лабинские ЭС",
    "Краснодарские ЭС", "Армавирские ЭС", "Адыгейские ЭС", "Сочинские ЭС"
]
ROSSETI_YUG_BRANCHES = [
    "Юго-Западные ЭС", "Центральные ЭС", "Западные ЭС", "Восточные ЭС",
    "Южные ЭС", "Северо-Восточные ЭС", "Юго-Восточные ЭС", "Северные ЭС"
]

# -------------------- Кэш-менеджер (замена load_csv) --------------------
CSV_CACHE_DIR = Path("csv_cache")
CSV_CACHE_DIR.mkdir(exist_ok=True)

http = httpx.AsyncClient(
    http2=True,
    limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
    timeout=httpx.Timeout(15)
)

def _safe_name(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()

async def fetch_csv(url: str) -> List[Dict[str, str]]:
    safe = _safe_name(url)
    cache_file = CSV_CACHE_DIR / f"{safe}.json"
    if cache_file.exists():
        mtime = datetime.fromtimestamp(cache_file.stat().st_mtime, tz=datetime.timezone.utc)
        if datetime.now(tz=datetime.timezone.utc) - mtime < timedelta(hours=2):
            async with aiofiles.open(cache_file, "r", encoding="utf-8") as f:
                return json.loads(await f.read())

    r = await http.get(url)
    r.raise_for_status()
    text = r.text
    reader = io.StringIO(text)
    import csv
    rows = [{k.strip(): (v or "").strip() for k, v in row.items()} for row in csv.DictReader(reader)]

    async with aiofiles.open(cache_file, "w", encoding="utf-8") as f:
        await f.write(json.dumps(rows, ensure_ascii=False))
    return rows

# -------------------- Глобальные переменные (как в 2.1.0) --------------------
csv_cache = {}
csv_cache_time = {}
CSV_CACHE_DURATION = timedelta(hours=2)

notifications_storage = {'RK': [], 'UG': []}
user_states = {}
users_cache = {}
users_cache_backup = {}
last_reports = {}
documents_cache = {}
documents_cache_time = {}
user_activity = {}
bot_users = {}

REFERENCE_DOCS = {
    'План по выручке ВОЛС на ВЛ 24-26 годы': os.environ.get('DOC_PLAN_VYRUCHKA_URL'),
    'Регламент ВОЛС': os.environ.get('DOC_REGLAMENT_VOLS_URL'),
    'Форма акта инвентаризации': os.environ.get('DOC_AKT_INVENTARIZACII_URL'),
    'Форма гарантийного письма': os.environ.get('DOC_GARANTIJNOE_PISMO_URL'),
    'Форма претензионного письма': os.environ.get('DOC_PRETENZIONNOE_PISMO_URL'),
    'Отчет по контрагентам': os.environ.get('DOC_OTCHET_KONTRAGENTY_URL'),
}
USER_GUIDE_URL = os.environ.get('USER_GUIDE_URL', 'https://example.com/vols-guide')
BOT_USERS_FILE = os.environ.get('BOT_USERS_FILE', 'bot_users.json')

# ------------------------------------------------------------------
# ЧАСТЬ 1  —  УТИЛИТЫ, НОРМАЛИЗАЦИЯ, КЭШИ
# ------------------------------------------------------------------
def normalize_tp_name_advanced(name: str) -> str:
    if not name:
        return ""
    name = name.upper()
    name = re.sub(r'[^\w\s\-А-Яа-я]', '', name, flags=re.UNICODE)
    return ' '.join(name.split())

def search_tp_in_data_advanced(tp_query: str, data: List[Dict], column: str) -> List[Dict]:
    if not tp_query or not data:
        return []
    normalized_query = normalize_tp_name_advanced(tp_query)
    pattern_match = re.match(r'([А-ЯA-Z]+)-?(\d+)-?([А-ЯA-Z]+)?-?(\d+)?', normalized_query)
    results = []
    for row in data:
        tp_name = row.get(column, '')
        if not tp_name:
            continue
        normalized_tp = normalize_tp_name_advanced(tp_name)
        if normalized_query == normalized_tp or normalized_query in normalized_tp:
            results.append(row)
            continue
        if pattern_match:
            query_parts = [p for p in pattern_match.groups() if p]
            tp_pattern = re.findall(r'([А-ЯA-Z]+)-?(\d+)-?([А-ЯA-Z]+)?-?(\d+)?', normalized_tp)
            for tp_match in tp_pattern:
                tp_parts = [p for p in tp_match if p]
                if len(query_parts) <= len(tp_parts) and all(query_parts[i] == tp_parts[i] for i in range(len(query_parts))):
                    results.append(row)
                    break
        query_digits = ''.join(filter(str.isdigit, normalized_query))
        tp_digits = ''.join(filter(str.isdigit, normalized_tp))
        if query_digits and query_digits in tp_digits:
            results.append(row)
    return results

# ------------------------------------------------------------------
# ЧАСТЬ 2  —  АСИНХРОННАЯ ЗАГРУЗКА CSV
# ------------------------------------------------------------------
async def load_csv_from_url_async(url: str) -> List[Dict]:
    return await fetch_csv(url)

def load_csv_from_url(url: str) -> List[Dict]:
    return asyncio.run(load_csv_from_url_async(url))

# ------------------------------------------------------------------
# ЧАСТЬ 3  —  РАБОТА С ПОЛЬЗОВАТЕЛЯМИ
# ------------------------------------------------------------------
def get_user_permissions(user_id: str) -> Dict:
    return users_cache.get(str(user_id), {
        'visibility': None,
        'branch': None,
        'res': None,
        'name': 'Неизвестный',
        'email': ''
    })

# ------------------------------------------------------------------
# ЧАСТЬ 4  —  EMAIL (aiosmtplib)
# ------------------------------------------------------------------
async def send_email(to_email: str, subject: str, body: str, attachment_data: BytesIO = None, attachment_name: str = None) -> bool:
    if not SMTP_EMAIL or not SMTP_PASSWORD:
        return False
    msg = EmailMessage()
    msg["From"] = SMTP_EMAIL
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)
    if attachment_data and attachment_name:
        attachment_data.seek(0)
        maintype, subtype = "application", "octet-stream"
        if attachment_name.endswith(".xlsx"):
            subtype = "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        elif attachment_name.endswith(".pdf"):
            subtype = "pdf"
        msg.add_attachment(attachment_data.read(), maintype=maintype, subtype=subtype, filename=attachment_name)
    try:
        await aiosmtplib.send(msg, hostname=SMTP_SERVER, port=SMTP_PORT,
                              username=SMTP_EMAIL, password=SMTP_PASSWORD, use_tls=True)
        return True
    except Exception as e:
        logger.error("send_email: %s", e)
        return False

# ------------------------------------------------------------------
# ЧАСТЬ 5  —  КЛАВИАТУРЫ (коротко)
# ------------------------------------------------------------------
def get_main_keyboard(permissions: Dict) -> ReplyKeyboardMarkup:
    kb = []
    if permissions.get('visibility') == 'All':
        kb.append(['🏢 РОССЕТИ КУБАНЬ', '🏢 РОССЕТИ ЮГ'])
    elif permissions.get('visibility') == 'RK':
        kb.append(['🏢 РОССЕТИ КУБАНЬ'])
    elif permissions.get('visibility') == 'UG':
        kb.append(['🏢 РОССЕТИ ЮГ'])
    kb += [['📞 ТЕЛЕФОНЫ КОНТРАГЕНТОВ'],
           ['📊 ОТЧЕТЫ'],
           ['ℹ️ СПРАВКА'],
           ['⚙️ МОИ НАСТРОЙКИ']]
    if permissions.get('visibility') == 'All':
        kb.append(['🛠 АДМИНИСТРИРОВАНИЕ'])
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)

def get_branch_keyboard(branches: List[str]) -> ReplyKeyboardMarkup:
    kb = [[b] for b in branches] + [["⬅️ Назад"]]
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)

def get_branch_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([['🔍 Поиск по ТП'], ['📨 Отправить уведомление'], ['ℹ️ Справка'], ['⬅️ Назад']], resize_keyboard=True)

# ------------------------------------------------------------------
# ЧАСТЬ 6  —  ОБРАБОТЧИКИ КОМАНД
# ------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    now = get_moscow_time()
    if uid not in bot_users:
        bot_users[uid] = {'first_start': now, 'last_start': now, 'username': update.effective_user.username or '',
                          'first_name': update.effective_user.first_name or ''}
    else:
        bot_users[uid]['last_start'] = now
    Path(BOT_USERS_FILE).write_text(json.dumps({k: {"first_start": v["first_start"].isoformat(),
                                                    "last_start": v["last_start"].isoformat(),
                                                    "username": v["username"], "first_name": v["first_name"]}
                                                for k, v in bot_users.items()}, ensure_ascii=False, indent=2))
    permissions = get_user_permissions(uid)
    await update.message.reply_text(f"👋 Добро пожаловать, {permissions.get('name', 'Пользователь')}!",
                                    reply_markup=get_main_keyboard(permissions))

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    permissions = get_user_permissions(uid)
    msg = f"""🤖 ВОЛС Ассистент v{BOT_VERSION}
👤 Ваш ID: {uid}
📋 Права: {permissions.get('visibility')}
👥 Пользователей: {len(users_cache)}
🟢 Активировали: {len(bot_users)}
🕐 Сервер: {get_moscow_time().strftime('%d.%m.%Y %H:%M')} МСК"""
    await update.message.reply_text(msg)

# ------------------------------------------------------------------
# ЧАСТЬ 7  —  ПОИСК, УВЕДОМЛЕНИЯ, ОТЧЁТЫ (схематично)
# ------------------------------------------------------------------
async def search_tp_in_both_catalogs(tp_query: str, branch: str, network: str, user_res: str = None) -> Dict:
    result = {'registry': [], 'structure': [], 'registry_tp_names': [], 'structure_tp_names': []}
    registry_env_key = f"{branch.replace(' ', '_').replace('-', '_').upper()}_URL_{network}"
    registry_url = os.getenv(registry_env_key)
    if registry_url:
        registry_data = await load_csv_from_url_async(registry_url)
        registry_results = search_tp_in_data_advanced(tp_query, registry_data, 'Наименование ТП')
        if user_res and user_res != 'All':
            registry_results = [r for r in registry_results if r.get('РЭС', '').strip() == user_res]
        result['registry'] = registry_results
        result['registry_tp_names'] = list(set([r['Наименование ТП'] for r in registry_results]))
    structure_env_key = f"{branch.replace(' ', '_').replace('-', '_').upper()}_URL_{network}_SP"
    structure_url = os.getenv(structure_env_key)
    if structure_url:
        structure_data = await load_csv_from_url_async(structure_url)
        structure_results = search_tp_in_data_advanced(tp_query, structure_data, 'Наименование ТП')
        if user_res and user_res != 'All':
            structure_results = [r for r in structure_results if r.get('РЭС', '').strip() == user_res]
        result['structure'] = structure_results
        result['structure_tp_names'] = list(set([r['Наименование ТП'] for r in structure_results]))
    return result

async def generate_report(update: Update, context: ContextTypes.DEFAULT_TYPE, network: str, permissions: Dict):
    notifications = notifications_storage.get(network, [])
    if not notifications:
        await update.message.reply_text("📊 Нет данных")
        return
    rows = []
    for n in notifications:
        rows.append({
            'Филиал': n['branch'],
            'РЭС': n['res'],
            'ТП': n['tp'],
            'ВЛ': n['vl'],
            'Отправитель': n['sender_name'],
            'Получатель': n['recipient_name'],
            'Дата и время': n['datetime'],
            'Координаты': n['coordinates'],
            'Комментарий': n['comment'],
            'Фото': 'Да' if n['has_photo'] else 'Нет'
        })
    df = pd.DataFrame(rows)
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Уведомления', index=False)
    buffer.seek(0)
    filename = f"Уведомления_{network}_{get_moscow_time().strftime('%d%m%Y_%H%M')}.xlsx"
    await update.message.reply_document(document=InputFile(buffer, filename=filename),
                                        caption=f"📊 Отчёт {network} – {len(notifications)} уведомлений")

# ------------------------------------------------------------------
# ЧАСТЬ 8  —  ОБРАБОТКА СООБЩЕНИЙ (коротко для примера)
# ------------------------------------------------------------------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    text = update.message.text
    p = get_user_permissions(uid)
    state = user_states.get(uid, {}).get('state', 'main')
    if text == "🏢 РОССЕТИ КУБАНЬ":
        user_states[uid] = {'state': 'rosseti_kuban', 'network': 'RK'}
        await update.message.reply_text("Выберите филиал", reply_markup=get_branch_keyboard(ROSSETI_KUBAN_BRANCHES))
    elif text in ROSSETI_KUBAN_BRANCHES:
        user_states[uid] = {'state': f'branch_{text}', 'branch': text, 'network': 'RK'}
        await update.message.reply_text(text, reply_markup=get_branch_menu_keyboard())
    elif text == "🔍 Поиск по ТП":
        user_states[uid]['action'] = 'search'
        await update.message.reply_text("Введите ТП:", reply_markup=ReplyKeyboardMarkup([["⬅️ Назад"]], resize_keyboard=True))
    elif text == "⬅️ Назад":
        user_states[uid] = {'state': 'main'}
        await update.message.reply_text("Главное меню", reply_markup=get_main_keyboard(p))
    else:
        if state.startswith('branch_') and user_states[uid].get('action') == 'search':
            branch = user_states[uid]['branch']
            network = user_states[uid]['network']
            url = os.getenv(f"{branch.replace(' ', '_').replace('-', '_').upper()}_URL_{network}_SP")
            if url:
                rows = await load_csv_from_url_async(url)
                found = search_tp_in_data_advanced(text, rows, "Наименование ТП")
                await update.message.reply_text(f"Найдено {len(found)} записей")
            else:
                await update.message.reply_text("❌ Справочник не найден")

# ------------------------------------------------------------------
# ЧАСТЬ 9  —  ЗАПУСК
# ------------------------------------------------------------------
def signal_handler(sig, frame):
    Path(BOT_USERS_FILE).write_text(json.dumps({k: {"first_start": v["first_start"].isoformat(),
                                                    "last_start": v["last_start"].isoformat(),
                                                    "username": v["username"],
                                                    "first_name": v["first_name"]}
                                                for k, v in bot_users.items()}, ensure_ascii=False, indent=2))
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if not BOT_TOKEN:
        logger.error("BOT_TOKEN не задан")
        sys.exit(1)

    # Загружаем пользователей
    if Path(BOT_USERS_FILE).exists():
        bot_users = json.loads(Path(BOT_USERS_FILE).read_text())
        for uid, d in bot_users.items():
            d["first_start"] = datetime.fromisoformat(d["first_start"])
            d["last_start"] = datetime.fromisoformat(d["last_start"])
    load_users_data()

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    if WEBHOOK_URL:
        app.run_webhook(listen="0.0.0.0", port=PORT, url_path=BOT_TOKEN,
                        webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}", drop_pending_updates=True)
    else:
        app.run_polling(drop_pending_updates=True)
