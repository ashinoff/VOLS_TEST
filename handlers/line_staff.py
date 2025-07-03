from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ContextTypes, CommandHandler, MessageHandler, filters
from zones import load_zones
from search import search_tp
from config import VISIBILITY_GROUPS

# Начальное меню: сети, телефоны, отчет
initial_buttons = [
    ["Россети ЮГ"],
    ["Россети Кубань"],
    ["Телефоны провайдеров"],
    ["Сформировать отчет"],
]
kb_initial = ReplyKeyboardMarkup(initial_buttons, resize_keyboard=True)

# Кнопка Назад
def kb_back():
    return ReplyKeyboardMarkup([["Назад"]], resize_keyboard=True)

# Клавиатура филиалов с кнопкой Назад
def kb_branches(branches):
    btns = [[b] for b in branches]
    btns.append(["Назад"])
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

# Клавиатура поиска
def kb_search():
    return ReplyKeyboardMarkup(
        [["Поиск по ТП"], ["Назад"]],
        resize_keyboard=True
    )

async def start_line(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.message.from_user.id
    vis_map, bz, rz, names = load_zones()
    if uid not in bz:
        await update.message.reply_text("🚫 У вас нет доступа.")
        return
    context.user_data.clear()
    await update.message.reply_text(
        f"👷 Привет, {names[uid]}! Выберите опцию:",
        reply_markup=kb_initial
    )

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    uid = update.message.from_user.id
    vis_map, bz, rz, names = load_zones()

    # Назад
    if text == "Назад":
        return await start_line(update, context)

    # Главные опции
    if text == "Телефоны провайдеров":
        await update.message.reply_text(
            "📞 Телефоны провайдеров: ...",
            reply_markup=kb_back()
        )
        return
    if text == "Сформировать отчет":
        await update.message.reply_text(
            "📝 Ваш отчет сформирован.",
            reply_markup=kb_back()
        )
        return

    # Выбор сети
    if text in ("Россети ЮГ", "Россети Кубань"):
        context.user_data["visibility"] = text
        branches = VISIBILITY_GROUPS[text]
        await update.message.reply_text(
            f"Вы выбрали {text}. Теперь выберите филиал:",
            reply_markup=kb_branches(branches)
        )
        return

    # Выбор филиала
    if "visibility" in context.user_data and "current_branch" not in context.user_data:
        branches = VISIBILITY_GROUPS[context.user_data["visibility"]]
        if text in branches:
            context.user_data["current_branch"] = text
            await update.message.reply_text(
                f"Филиал {text} выбран. Выберите действие или введите номер ТП:",
                reply_markup=kb_search()
            )
        return

    # Поиск по ТП
    if "current_branch" in context.user_data:
        if text == "Поиск по ТП":
            await update.message.reply_text(
                "Введите номер ТП:",
                reply_markup=kb_back()
            )
            return
        branch = context.user_data["current_branch"]
        res = context.user_data.get("res")
        df = search_tp(branch, text, res_filter=res)
        if df.empty:
            await update.message.reply_text(
                "🔍 Ничего не найдено.",
                reply_markup=kb_back()
            )
            return
        for _, r in df.iterrows():
            await update.message.reply_text(
                f"📍 {r['Наименование ТП']}\n"
                f"ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
                f"Опоры: {r['Опоры']} ({r['Количество опор']})\n"
                f"Провайдер: {r.get('Наименование Провайдера','')}"
                + (f", договор {r.get('Номер договора')}" if r.get('Номер договора') else ""),
                reply_markup=kb_back()
            )
        return

    # По умолчанию возвращаемся в начало
    return await start_line(update, context)

handler_start = CommandHandler("start", start_line)
handler_text = MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text)
