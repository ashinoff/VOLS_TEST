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
    # директор: branch != "All" и res == "All"
    if branch != "All" and res == "All":
        # Клавиатура видимости
        kb_visibility = ReplyKeyboardMarkup(
            [[v] for v in VISIBILITY_GROUPS.keys()],
            resize_keyboard=True
        )
        await update.message.reply_text(
            f"👔 Привет, директор филиала {branch}, {name}! Выберите группу филиалов:",
            reply_markup=kb_visibility
        )

handler = CommandHandler("start", start_director)
```python
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler

ADMINS = {123456789, 987654321}

async def start_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return
    await update.message.reply_text("👑 Здравствуйте, администратор!")

handler = CommandHandler("start", start_admin)
