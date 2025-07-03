# handlers/line_staff.py  (изменен)

from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ContextTypes, CommandHandler, MessageHandler, filters
from zones import load_zones
from search import search_tp
from config import VISIBILITY_GROUPS

# === Клавиатуры ===

# Стартовое меню
kb_initial = ReplyKeyboardMarkup(
    [
        ["Россети ЮГ"],
        ["Россети Кубань"],
        ["Телефоны провайдеров"],
        ["Сформировать отчет"],
    ],
    resize_keyboard=True
)

# «Назад»
kb_back = ReplyKeyboardMarkup([["Назад"]], resize_keyboard=True)

# Филиалы + кнопка «Назад»
def kb_branches(branches):
    btns = [[b] for b in branches]
    btns.append(["Назад"])
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

# Меню поиска внутри филиала
kb_search = ReplyKeyboardMarkup(
    [["Поиск по ТП"], ["Назад"]],
    resize_keyboard=True
)

# === Хендлеры ===

async def start_line(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Стартовое меню."""
    uid = update.effective_user.id
    vis_map, bz, rz, names = load_zones()
    if uid not in bz:
        await update.message.reply_text("🚫 У вас нет доступа.")
        return

    # сбросим всё состояние
    context.user_data.clear()
    context.user_data['step'] = 'INIT'

    await update.message.reply_text(
        f"👷 Привет, {names[uid]}! Выберите опцию:",
        reply_markup=kb_initial
    )

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    uid = update.effective_user.id
    vis_map, bz, rz, names = load_zones()

    # Если нет доступа - выход
    if uid not in bz:
        return

    step = context.user_data.get('step', 'INIT')

    # ======= ОБРАБОТКА «НАЗАД» =======
    if text == "Назад":
        # По уровню шага возвращаем предыдущий экран
        if step == 'DISAMBIGUOUS':
            # возвращаем в ожидание ввода ТП
            context.user_data['step'] = 'AWAIT_TP_INPUT'
            await update.message.reply_text("Введите номер ТП:", reply_markup=kb_back)
            return

        if step == 'AWAIT_TP_INPUT' or step == 'BRANCH_SELECTED':
            # возвращаем на выбор филиала
            vis = context.user_data.get('visibility')
            branches = VISIBILITY_GROUPS.get(vis, [])
            context.user_data['step'] = 'NETWORK_SELECTED'
            await update.message.reply_text("Выберите филиал:", reply_markup=kb_branches(branches))
            return

        if step == 'NETWORK_SELECTED':
            # возвращаем на начальный экран
            await start_line(update, context)
            return

        # в INIT — игнорируем «Назад»
        return

    # ======= ШАГ INIT: главное меню =======
    if step == 'INIT':
        if text == "Телефоны провайдеров":
            await update.message.reply_text("📞 Список телефонов провайдеров:\n...", reply_markup=kb_back)
            return
        if text == "Сформировать отчет":
            await update.message.reply_text("📝 Отчёт сформирован.", reply_markup=kb_back)
            return
        if text in VISIBILITY_GROUPS:
            # выбрали сеть
            context.user_data['visibility'] = text
            context.user_data['step'] = 'NETWORK_SELECTED'
            branches = VISIBILITY_GROUPS[text]
            await update.message.reply_text(f"Вы выбрали {text}. Теперь — филиал:", reply_markup=kb_branches(branches))
            return
        # любое другое сообщение игнорируем на этом шаге
        return

    # ======= ШАГ NETWORK_SELECTED: выбор филиала =======
    if step == 'NETWORK_SELECTED':
        branches = VISIBILITY_GROUPS.get(context.user_data['visibility'], [])
        if text in branches:
            # выбрали филиал
            context.user_data['current_branch'] = text
            context.user_data['step'] = 'BRANCH_SELECTED'
            await update.message.reply_text(
                f"Филиал {text} выбран. Что дальше?",
                reply_markup=kb_search
            )
        return

    # ======= ШАГ BRANCH_SELECTED: меню поиска =======
    if step == 'BRANCH_SELECTED':
        if text == "Поиск по ТП":
            # готовимся к вводу
            context.user_data['step'] = 'AWAIT_TP_INPUT'
            await update.message.reply_text("Введите номер ТП:", reply_markup=kb_back)
        return

    # ======= ШАГ AWAIT_TP_INPUT: ввод ТП и поиск =======
    if step == 'AWAIT_TP_INPUT':
        # Выполняем поиск
        branch = context.user_data['current_branch']
        res    = context.user_data.get('res')
        df = search_tp(branch, text, res_filter=res)

        if df.empty:
            await update.message.reply_text("🔍 Ничего не найдено.", reply_markup=kb_search)
            context.user_data['step'] = 'BRANCH_SELECTED'
            return

        # проверим неоднозначность
        unique_tp = df['Наименование ТП'].unique().tolist()
        if len(unique_tp) > 1:
            # предложим выбрать конкретную ТП
            context.user_data['ambiguous_list'] = unique_tp
            context.user_data['ambiguous_df']   = df
            context.user_data['step'] = 'DISAMBIGUOUS'
            kb = ReplyKeyboardMarkup([[tp] for tp in unique_tp] + [["Назад"]], resize_keyboard=True)
            await update.message.reply_text("Найдены несколько ТП, выберите:", reply_markup=kb)
            return

        # точно одна ТП
        tp_name = unique_tp[0]
        found = df[df['Наименование ТП'] == tp_name]
        for _, r in found.iterrows():
            await update.message.reply_text(
                f"📍 {tp_name}\n"
                f"ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
                f"Опоры: {r['Опоры']} ({r['Количество опор']})\n"
                f"Провайдер: {r.get('Наименование Провайдера','')}"
                + (f", договор {r.get('Номер договора')}" if r.get('Номер договора') else ""),
                reply_markup=kb_search
            )
        context.user_data['step'] = 'BRANCH_SELECTED'
        return

    # ======= ШАГ DISAMBIGUOUS: выбор из списка ТП =======
    if step == 'DISAMBIGUOUS':
        options = context.user_data.get('ambiguous_list', [])
        if text in options:
            df = context.user_data['ambiguous_df']
            found = df[df['Наименование ТП'] == text]
            for _, r in found.iterrows():
                await update.message.reply_text(
                    f"📍 {text}\n"
                    f"ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
                    f"Опоры: {r['Опоры']} ({r['Количество опор']})\n"
                    f"Провайдер: {r.get('Наименование Провайдера','')}"
                    + (f", договор {r.get('Номер договора')}" if r.get('Номер договора') else ""),
                    reply_markup=kb_search
                )
            context.user_data['step'] = 'BRANCH_SELECTED'
        return

# Регистрация хендлеров в main.py:
handler_start = CommandHandler("start", start_line)
handler_text  = MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text)
