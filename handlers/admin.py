from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ContextTypes, CommandHandler
from config import VISIBILITY_GROUPS

# ID админов
ADMINS = {123456789, 987654321}

# клавиатура выбора RK/RU
kb_visibility = ReplyKeyboardMarkup(
    [[k] for k in VISIBILITY_GROUPS.keys()],
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
