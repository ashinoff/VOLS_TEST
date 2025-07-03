from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ContextTypes, CommandHandler
from zones import load_zones
from config import VISIBILITY_GROUPS

async def start_director(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    vis_map, bz, rz, names = load_zones()
    if uid not in bz:
        return
    branch, res, name = bz[uid], rz[uid], names[uid]
    if branch != "All" and res == "All":
        kb_visibility = ReplyKeyboardMarkup(
            [[name] for name in VISIBILITY_GROUPS.keys()],
            resize_keyboard=True
        )
        await update.message.reply_text(
            f"👔 Привет, директор филиала {branch}, {name}! Выберите группу филиалов:",
            reply_markup=kb_visibility
        )

handler = CommandHandler("start", start_director)
