from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ContextTypes, CommandHandler
from config import VISIBILITY_GROUPS

# Задайте реальные ID администраторов
ADMINS = {955536270, 987654321}

# Клавиатура выбора группы видимости
kb_visibility = ReplyKeyboardMarkup(
    [[name] for name in VISIBILITY_GROUPS.keys()],
    resize_keyboard=True
)

async def start_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return
    await update.message.reply_text(
        "👑 Здравствуйте, администратор! Выберите группу филиалов:",
        reply_markup=kb_visibility
    )

handler = CommandHandler("start", start_admin)
