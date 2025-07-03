from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ContextTypes, CommandHandler
from config import VISIBILITY_GROUPS

# Задайте реальные ID администраторов
ADMINS = {123456789, 987654321}

# Начальное меню с выбором группы, телефонов провайдеров и отчета
initial_buttons = [
    ["Россети ЮГ"],
    ["Россети Кубань"],
    ["Телефоны провайдеров"],
    ["Сформировать отчет"],
]
kb_initial = ReplyKeyboardMarkup(initial_buttons, resize_keyboard=True)

async def start_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return
    await update.message.reply_text(
        "👑 Здравствуйте, администратор! Выберите опцию:",
        reply_markup=kb_initial
    )

handler = CommandHandler("start", start_admin)("start", start_admin)
