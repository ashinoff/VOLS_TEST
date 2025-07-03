from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ContextTypes, CommandHandler, MessageHandler, filters
from zones import load_zones
from search import search_tp
from config import VISIBILITY_GROUPS

# Клавиатуры
kb_visibility = ReplyKeyboardMarkup(
    [[name] for name in VISIBILITY_GROUPS.keys()],
    resize_keyboard=True
)

def kb_branches(branches):
    return ReplyKeyboardMarkup([[b] for b in branches], resize_keyboard=True)

kb_search = ReplyKeyboardMarkup(
    [["Поиск по ТП"], ["Выбор филиала"]],
    resize_keyboard=True
)

async def start_line(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.message.from_user.id
    vis_map, bz, rz, names = load_zones()
    if uid not in bz:
        await update.message.reply_text("🚫 У вас нет доступа.")
        return
    vis, branch, res, name = vis_map[uid], bz[uid], rz[uid], names[uid]
    context.user_data.clear()
    context.user_data['role'] = 'line_staff'
    context.user_data['res'] = res
    # Шаг 1: выбор видимости
    if vis != 'All':
        context.user_data['visibility'] = vis
        branches = VISIBILITY_GROUPS.get(vis, [])
        await update.message.reply_text(
            f"👷 Привет, {name}! Видимость: {vis}.",
            reply_markup=kb_branches(branches)
        )
    else:
        await update.message.reply_text(
            f"👷 Привет, {name}! Выберите группу филиалов:",
            reply_markup=kb_visibility
        )

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if context.user_data.get('role') != 'line_staff':
        return
    # Шаг 2: выбор филиала
    if 'visibility' in context.user_data and 'current_branch' not in context.user_data:
        branches = VISIBILITY_GROUPS[context.user_data['visibility']]
        if text in branches:
            context.user_data['current_branch'] = text
            await update.message.reply_text(
                f"Филиал {text} выбран. Введите номер ТП:",
                reply_markup=kb_search
            )
        elif text in VISIBILITY_GROUPS:
            # пользователь выбрал группу вместо филиала
            branches = VISIBILITY_GROUPS[text]
            context.user_data['visibility'] = text
            await update.message.reply_text(
                f"Вы выбрали {text}. Теперь выберите филиал:",
                reply_markup=kb_branches(branches)
            )
        return
    # Шаг 3: поиск или возврат к выбору филиала
    if text == 'Выбор филиала':
        await update.message.reply_text(
            "Выберите филиал:",
            reply_markup=kb_branches(VISIBILITY_GROUPS[context.user_data['visibility']])
        )
        return
    if text == 'Поиск по ТП':
        await update.message.reply_text(
            "Введите номер ТП:",
            reply_markup=kb_search
        )
        return
    # Шаг 4: выполняем поиск
    df = search_tp(
        context.user_data['current_branch'],
        text,
        res_filter=context.user_data.get('res')
    )
    if df.empty:
        await update.message.reply_text("🔍 Ничего не найдено.", reply_markup=kb_search)
        return
    for _, r in df.iterrows():
        await update.message.reply_text(
            f"📍 {r['Наименование ТП']}\n"
            f"ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
            f"Опоры: {r['Опоры']} ({r['Количество опор']})\n"
            f"Провайдер: {r.get('Наименование Провайдера','')}"
            + (f", договор {r.get('Номер договора')}" if r.get('Номер договора') else ""),
            reply_markup=kb_search
        )

handler_start = CommandHandler("start", start_line)
handler_text = MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text)
