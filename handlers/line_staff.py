from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ContextTypes, MessageHandler, filters, CommandHandler
from zones import load_zones
from search import search_tp

# Клавиатуры

def kb_select_branch(branches):
    return ReplyKeyboardMarkup([[b] for b in branches], resize_keyboard=True)

def kb_search():
    return ReplyKeyboardMarkup([["Поиск по ТП"], ["Выбор филиала"]], resize_keyboard=True)

async def start_line(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    bz, rz, names = load_zones()
    if uid not in bz:
        await update.message.reply_text("🚫 У вас нет доступа.")
        return
    branch, res, name = bz[uid], rz[uid], names[uid]

    if res != "All":
        context.user_data["mode"] = 3
        context.user_data["current_branch"] = branch
        context.user_data["current_res"] = res
        await update.message.reply_text(
            f"👷 Привет, {name}! Вы — линейный персонал РЭС {res} филиала {branch}.",
            reply_markup=kb_search()
        )
    elif branch != "All":
        context.user_data["mode"] = 2
        context.user_data["current_branch"] = branch
        await update.message.reply_text(
            f"👷 Привет, {name}! Вы — линейный персонал филиала {branch}.",
            reply_markup=kb_search()
        )
    else:
        context.user_data["mode"] = 1
        await update.message.reply_text(
            f"👷 Привет, {name}! Вы можете искать по любому филиалу.",
            reply_markup=kb_select_branch(list(context.bot_data["branches"]))
        )

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    mode = context.user_data.get("mode", 1)

    if text == "Выбор филиала":
        await update.message.reply_text("Выберите филиал:", reply_markup=kb_select_branch(list(context.bot_data["branches"])))
        return

    if mode == 1:
        if text in context.bot_data["branches"]:
            context.user_data["current_branch"] = text
            await update.message.reply_text(f"Введите номер ТП для филиала {text}:", reply_markup=kb_search())
            return
        if text == "Поиск по ТП" and "current_branch" in context.user_data:
            await update.message.reply_text("Введите номер ТП:", reply_markup=kb_search())
            return

    branch = context.user_data.get("current_branch")
    res    = context.user_data.get("current_res")
    df = search_tp(branch, text, res_filter=res)
    if df.empty:
        await update.message.reply_text("🔍 Ничего не найдено.", reply_markup=kb_search())
        return

    for _, row in df.iterrows():
        await update.message.reply_text(
            f"📍 {row['Наименование ТП']}\n"
            f"ВЛ {row['Уровень напряжения']} {row['Наименование ВЛ']}\n"
            f"Опоры: {row['Опоры']} ({row['Количество опор']})\n"
            f"Провайдер: {row.get('Наименование Провайдера','')}"
            + (f", договор {row.get('Номер договора')}" if row.get('Номер договора') else ""),
            reply_markup=kb_search()
        )

handler_start = CommandHandler("start", start_line)
handler_text  = MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text)
