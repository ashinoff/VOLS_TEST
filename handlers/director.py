from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from zones import load_zones

async def start_director(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    bz, rz, names = load_zones()
    if uid not in bz:
        return
    branch, res, name = bz[uid], rz[uid], names[uid]
    # директор: branch != "All" и res == "All"
    if branch != "All" and res == "All":
        await update.message.reply_text(f"👔 Привет, директор филиала {branch}, {name}!")

handler = CommandHandler("start", start_director)
