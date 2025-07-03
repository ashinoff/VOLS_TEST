from telegram import Update
from telegram.ext import ContextTypes, CommandHandler

# Здесь укажите реальные ID админов
ADMINS = {123456789, 987654321}

async def start_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return
    await update.message.reply_text("👑 Здравствуйте, администратор!")

handler = CommandHandler("start", start_admin)
