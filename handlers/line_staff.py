from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ContextTypes, CommandHandler, MessageHandler, filters
from zones import load_zones
from search import search_tp
from config import VISIBILITY_GROUPS

# --- Keyboards ---
kb_initial = ReplyKeyboardMarkup(
    [["Россети ЮГ"], ["Россети Кубань"], ["Телефоны провайдеров"], ["Сформировать отчет"]],
    resize_keyboard=True
)

def kb_back():
    return ReplyKeyboardMarkup([["Назад"]], resize_keyboard=True)

def kb_branches(branches):
    btns = [[b] for b in branches]
    btns.append(["Назад"])
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

def kb_search():
    return ReplyKeyboardMarkup(
        [["Поиск по ТП"], ["Назад"]],
        resize_keyboard=True
    )

# --- Handlers ---
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

    # Статические опции
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

    # Выбор сети (видимости)
    if text in VISIBILITY_GROUPS and text not in ("Телефоны провайдеров", "Сформировать отчет"):
        context.user_data.clear()
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
                f"Филиал {text} выбран. Введите номер ТП или нажмите 'Поиск по ТП':",
                reply_markup=kb_search()
            )
        return

    # Поиск по ТП и алгоритм неоднозначности
    if "current_branch" in context.user_data:
        branch = context.user_data["current_branch"]
        res = context.user_data.get("res")

        # Сброс предыдущей неоднозначности
        if "ambiguous" in context.user_data and text == context.user_data.get("ambiguous_prompt"):
            # выбор конкретного наименования
            tp_selected = text
            found_df = context.user_data["ambiguous_df"]
            matched = found_df[found_df['Наименование ТП'] == tp_selected]
            # вывод одной TP
            lines = []
            for _, r in matched.iterrows():
                lines.append(f"📍 {r['Наименование ТП']}")
                lines.append(f"ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}")
                lines.append(f"Опоры: {r['Опоры']} ({r['Количество опор']})")
                prov = r.get('Наименование Провайдера','')
                num  = r.get('Номер договора','')
                tail = f", договор {num}" if num else ""
                lines.append(f"Провайдер: {prov}{tail}")
                lines.append("")
            await update.message.reply_text("
".join(lines).strip(), reply_markup=kb_search())
            # очистка
            context.user_data.pop('ambiguous')
            context.user_data.pop('ambiguous_df')
            context.user_data.pop('ambiguous_prompt')
            return

        # Если нажали Поиск по ТП или ввели текст впервые
        if text == "Поиск по ТП":
            await update.message.reply_text(
                "Введите наименование ТП:",
                reply_markup=kb_back()
            )
            return

        # сам поиск
        df = search_tp(branch, text, res_filter=res)
        if df.empty:
            await update.message.reply_text(
                "🔍 Ничего не найдено.",
                reply_markup=kb_search()
            )
            return
        # уникальные наименования
        unique_tp = df['Наименование ТП'].unique().tolist()
        if len(unique_tp) > 1:
            # неоднозначность
            context.user_data['ambiguous'] = unique_tp
            context.user_data['ambiguous_df'] = df
            context.user_data['ambiguous_prompt'] = None  # will set to chosen
            kb = ReplyKeyboardMarkup([[tp] for tp in unique_tp] + [["Назад"]], resize_keyboard=True)
            await update.message.reply_text(
                "Найдено несколько совпадений. Выберите точное наименование ТП:",
                reply_markup=kb
            )
            return
        # точное совпадение одного
        tp_name = unique_tp[0]
        matched = df[df['Наименование ТП'] == tp_name]
        lines = []
        for _, r in matched.iterrows():
            lines.append(f"📍 {r['Наименование ТП']}")
            lines.append(f"ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}")
            lines.append(f"Опоры: {r['Опоры']} ({r['Количество опор']})")
            prov = r.get('Наименование Провайдера','')
            num  = r.get('Номер договора','')
            tail = f", договор {num}" if num else ""
            lines.append(f"Провайдер: {prov}{tail}")
            lines.append("")
        await update.message.reply_text("
".join(lines).strip(), reply_markup=kb_search())
        return

# Регистрируем хендлеры
handler_start = CommandHandler("start", start_line)
handler_text = MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text)
