from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ContextTypes, MessageHandler, filters, CommandHandler
from zones import load_zones
from search import search_tp
from config import VISIBILITY_GROUPS

# Клавиатуры для выбора группы и филиала

def kb_visibility():
    return ReplyKeyboardMarkup([[v] for v in VISIBILITY_GROUPS.keys()], resize_keyboard=True)

def kb_branches(branches):
    return ReplyKeyboardMarkup([[b] for b in branches], resize_keyboard=True)

def kb_search():
    return ReplyKeyboardMarkup([["Поиск по ТП"], ["Выбор филиала"]], resize_keyboard=True)

async def start_line(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    vis_map, bz, rz, names = load_zones()
    if uid not in bz:
        await update.message.reply_text("🚫 У вас нет доступа.")
        return
    vis, branch, res, name = vis_map[uid], bz[uid], rz[uid], names[uid]
    context.user_data.clear()
    context.user_data['role'] = 'line_staff'
    context.user_data['res']  = res
    # Если видимость задана явно, пропускаем выбор группы
    if vis != 'All':
        context.user_data['visibility'] = vis
        branches = VISIBILITY_GROUPS.get(vis, [])
        await update.message.reply_text(
            f"👷 Привет, {name}! Ваш уровень видимости: {vis}.",
            reply_markup=kb_branches(branches)
        )
    else:
        # Для All предложим выбрать группу
        await update.message.reply_text(
            f"👷 Привет, {name}! Выберите группу филиалов (RK или RU):",
            reply_markup=kb_visibility()
        )

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    role = context.user_data.get('role')
    if role != 'line_staff':
        return

    # 1) Выбор группы, если не задана
    if 'visibility' not in context.user_data:
        if text in VISIBILITY_GROUPS:
            context.user_data['visibility'] = text
            branches = VISIBILITY_GROUPS[text]
            await update.message.reply_text(
                f"Вы выбрали группу {text}. Теперь выберите филиал:",
                reply_markup=kb_branches(branches)
            )
        return

    # 2) Выбор филиала, если не задан
    if 'current_branch' not in context.user_data:
        branches = VISIBILITY_GROUPS[context.user_data['visibility']]
        if text in branches:
            context.user_data['current_branch'] = text
            await update.message.reply_text(
                f"Филиал {text} выбран. Введите номер ТП:",
                reply_markup=kb_search()
            )
        return

    # 3) Обработка поиска по ТП
    if text == 'Выбор филиала':
        branches = VISIBILITY_GROUPS[context.user_data['visibility']]
        await update.message.reply_text(
            "Выберите филиал:", reply_markup=kb_branches(branches)
        )
        return

    if text == 'Поиск по ТП':
        await update.message.reply_text(
            "Введите номер ТП:", reply_markup=kb_search()
        )
        return

    # Сам поиск
    branch = context.user_data['current_branch']
    res    = context.user_data.get('res')
    df     = search_tp(branch, text, res_filter=res)
    if df.empty:
        await update.message.reply_text(
            "🔍 Ничего не найдено.", reply_markup=kb_search()
        )
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
