import threading
import re
import requests
import pandas as pd
from io import StringIO
from flask import Flask
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

from config import (
    TOKEN,
    SELF_URL,
    PORT,
    BRANCH_URLS,
    VISIBILITY_GROUPS,
    NOTIFY_SHEET_URL,
)
from zones import normalize_sheet_url, load_zones

app = Flask(__name__)
application = ApplicationBuilder().token(TOKEN).build()

# === Клавиатуры ===
kb_initial = ReplyKeyboardMarkup(
    [["Россети ЮГ"], ["Россети Кубань"], ["Телефоны провайдеров"], ["Сформировать отчёт"]],
    resize_keyboard=True
)
kb_back = ReplyKeyboardMarkup([["🔙 Назад"]], resize_keyboard=True)

def kb_branches(branches):
    btns = [[b] for b in branches] + [["🔙 Назад"]]
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

kb_actions = ReplyKeyboardMarkup(
    [["🔍 Поиск по ТП"], ["🔔 Отправить уведомление"], ["🔙 Назад"]],
    resize_keyboard=True
)

kb_request_location = ReplyKeyboardMarkup(
    [[KeyboardButton("📍 Отправить геолокацию", request_location=True)], ["🔙 Назад"]],
    resize_keyboard=True
)

# === /start ===
async def start_line(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    vis_map, bz, rz, names, resp_map = load_zones()
    if uid not in bz:
        await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)
        return

    context.user_data.clear()
    context.user_data["step"] = "INIT"
    context.user_data["res"] = rz[uid]
    context.user_data["name"] = names[uid]
    context.user_data["resp_map"] = resp_map  # для удобства

    await update.message.reply_text(
        f"Привет, {names[uid]}! Выберите опцию:",
        reply_markup=kb_initial
    )

# === текстовые сообщения ===
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    uid  = update.effective_user.id
    vis_map, bz, rz, names, resp_map = load_zones()
    if uid not in bz:
        await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)
        return

    step = context.user_data.get("step", "INIT")

    # --- Назад ---
    if text == "🔙 Назад":
        # из уведомлений — вернуть выбор филиала
        if step in ("NOTIFY_WAIT_GEO", "NOTIFY_AWAIT_VL", "NOTIFY_DISAMBIGUOUS", "NOTIFY_AWAIT_TP"):
            context.user_data["step"] = "BRANCH_SELECTED"
            await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)
            return
        # из поиска ТП — вернуть выбор филиала
        if step in ("AWAIT_TP_INPUT", "DISAMBIGUOUS", "SEARCH_DONE"):
            context.user_data["step"] = "BRANCH_SELECTED"
            await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)
            return
        # из выбора филиала — вернуть выбор сети
        if step == "NETWORK_SELECTED":
            context.user_data["step"] = "INIT"
            await update.message.reply_text("Выберите опцию:", reply_markup=kb_initial)
            return
        # из телефонов/отчёта — тоже в INIT
        context.user_data["step"] = "INIT"
        await update.message.reply_text("Выберите опцию:", reply_markup=kb_initial)
        return

    # --- INIT: главное меню ---
    if step == "INIT":
        if text == "Телефоны провайдеров":
            context.user_data["step"] = "VIEW_PHONES"
            await update.message.reply_text("📞 Телефоны провайдеров:\n…", reply_markup=kb_back)
            return
        if text == "Сформировать отчёт":
            context.user_data["step"] = "VIEW_REPORT"
            await update.message.reply_text("📝 Отчёт сформирован.", reply_markup=kb_back)
            return
        if text in ("Россети ЮГ", "Россети Кубань"):
            context.user_data["step"] = "NETWORK_SELECTED"
            context.user_data["visibility"] = text
            await update.message.reply_text(
                "Выберите филиал:",
                reply_markup=kb_branches(VISIBILITY_GROUPS[text])
            )
        return

    # --- NETWORK_SELECTED: выбор филиала ---
    if step == "NETWORK_SELECTED":
        branches = VISIBILITY_GROUPS[context.user_data["visibility"]]
        if text in branches:
            context.user_data["step"] = "BRANCH_SELECTED"
            context.user_data["current_branch"] = text
            await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)
        return

    # --- BRANCH_SELECTED: действия в филиале ---
    if step == "BRANCH_SELECTED":
        # Поиск по ТП
        if text == "🔍 Поиск по ТП":
            context.user_data["step"] = "AWAIT_TP_INPUT"
            await update.message.reply_text("Введите номер ТП:", reply_markup=kb_back)
            return
        # Уведомление — только для тех, у кого конкретный РЭС (mode 3)
        if text == "🔔 Отправить уведомление":
            # доступно, если у пользователя есть res != "All"
            if context.user_data["res"] == "All":
                await update.message.reply_text("❌ Доступно только для конкретных РЭС.", reply_markup=kb_actions)
                return
            context.user_data["step"] = "NOTIFY_AWAIT_TP"
            await update.message.reply_text("Введите номер ТП для уведомления:", reply_markup=kb_back)
            return
        return

    # --- AWAIT_TP_INPUT: ввод ТП и поиск обычный ---
    if step == "AWAIT_TP_INPUT":
        branch = context.user_data["current_branch"]
        res    = context.user_data["res"]

        df = pd.read_csv(normalize_sheet_url(BRANCH_URLS[branch]))
        if res != "All":
            df = df[df["РЭС"] == res]
        df.columns = df.columns.str.strip()
        df["D_UP"] = df["Наименование ТП"].str.upper().str.replace(r'\W', '', regex=True)

        q = re.sub(r'\W','', text.upper())
        found = df[df["D_UP"].str.contains(q, na=False)]

        if found.empty:
            await update.message.reply_text("🔍 Ничего не найдено. Повторите ввод:", reply_markup=kb_back)
            return

        unique_tp = found["Наименование ТП"].unique().tolist()
        if len(unique_tp) > 1:
            context.user_data["step"] = "DISAMBIGUOUS"
            context.user_data["ambiguous_df"] = found
            kb = ReplyKeyboardMarkup([[tp] for tp in unique_tp] + [["🔙 Назад"]], resize_keyboard=True)
            await update.message.reply_text("Найдены несколько ТП, выберите:", reply_markup=kb)
            return

        # одна ТП
        tp = unique_tp[0]
        details = found[found["Наименование ТП"] == tp]
        count = len(details)
        res_name = details.iloc[0]["РЭС"]
        await update.message.reply_text(
            f"{res_name}, {tp} ({count}) ВОЛС с договором аренды.\nВыберите действие:",
            reply_markup=kb_actions
        )
        for _, r in details.iterrows():
            await update.message.reply_text(
                f"📍 ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
                f"Опоры: {r['Опоры']}\n"
                f"Провайдер: {r.get('Наименование Провайдера','')}",
                reply_markup=kb_actions
            )
        context.user_data["step"] = "BRANCH_SELECTED"
        return

    # --- DISAMBIGUOUS: выбор из обычного поиска ---
    if step == "DISAMBIGUOUS":
        df = context.user_data["ambiguous_df"]
        if text in df["Наименование ТП"].unique():
            details = df[df["Наименование ТП"] == text]
            count = len(details)
            res_name = details.iloc[0]["РЭС"]
            await update.message.reply_text(
                f"{res_name}, {text} ({count}) ВОЛС с договором аренды.\nВыберите действие:",
                reply_markup=kb_actions
            )
            for _, r in details.iterrows():
                await update.message.reply_text(
                    f"📍 ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
                    f"Опоры: {r['Опоры']}\n"
                    f"Провайдер: {r.get('Наименование Провайдера','')}",
                    reply_markup=kb_actions
                )
            context.user_data["step"] = "BRANCH_SELECTED"
        return

    # --- NOTIFY_AWAIT_TP: ввод ТП для уведомления ---
    if step == "NOTIFY_AWAIT_TP":
        # читаем отдельный лист для уведомлений
        dfn = pd.read_csv(normalize_sheet_url(NOTIFY_SHEET_URL))
        dfn.columns = dfn.columns.str.strip()
        dfn["D_UP"] = dfn["Наименование ТП"].str.upper().str.replace(r'\W','', regex=True)

        q = re.sub(r'\W','', text.upper())
        found = dfn[dfn["D_UP"].str.contains(q, na=False)]

        if found.empty:
            await update.message.reply_text("🔍 Не найдено. Повторите ввод:", reply_markup=kb_back)
            return

        unique_tp = found["Наименование ТП"].unique().tolist()
        if len(unique_tp) > 1:
            context.user_data["step"] = "NOTIFY_DISAMBIGUOUS"
            context.user_data["notify_df"] = found
            kb = ReplyKeyboardMarkup([[tp] for tp in unique_tp] + [["🔙 Назад"]], resize_keyboard=True)
            await update.message.reply_text("Несколько ТП—выберите:", reply_markup=kb)
            return

        # одна ТП
        tp = unique_tp[0]
        context.user_data["step"] = "NOTIFY_AWAIT_VL"
        context.user_data["notify_df"] = found
        context.user_data["notify_tp"] = tp
        vls = found[found["Наименование ТП"] == tp]["Наименование ВЛ"].unique().tolist()
        kb = ReplyKeyboardMarkup([[vl] for vl in vls] + [["🔙 Назад"]], resize_keyboard=True)
        await update.message.reply_text("Выберите ВЛ:", reply_markup=kb)
        return

    # --- NOTIFY_DISAMBIGUOUS: выбор ТП для уведомления ---
    if step == "NOTIFY_DISAMBIGUOUS":
        dfn = context.user_data["notify_df"]
        if text in dfn["Наименование ТП"].unique():
            context.user_data["step"] = "NOTIFY_AWAIT_VL"
            context.user_data["notify_tp"] = text
            vls = dfn[dfn["Наименование ТП"] == text]["Наименование ВЛ"].unique().tolist()
            kb = ReplyKeyboardMarkup([[vl] for vl in vls] + [["🔙 Назад"]], resize_keyboard=True)
            await update.message.reply_text("Выберите ВЛ:", reply_markup=kb)
        return

    # --- NOTIFY_AWAIT_VL: выбор ВЛ для уведомления ---
    if step == "NOTIFY_AWAIT_VL":
        dfn = context.user_data["notify_df"]
        if text in dfn["Наименование ВЛ"].unique():
            context.user_data["step"] = "NOTIFY_WAIT_GEO"
            context.user_data["notify_vl"] = text
            await update.message.reply_text(
                "📍 Пожалуйста, отправьте свою геолокацию:",
                reply_markup=kb_request_location
            )
        return

# === Ловим геолокацию для уведомлений ===
async def location_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("step") != "NOTIFY_WAIT_GEO":
        return
    loc = update.message.location
    uid = update.effective_user.id
    _, _, _, _, resp_map = load_zones()
    # получаем текущего ответственного по res_map
    current_resp = resp_map.get(uid)
    # список получателей — все с таким же responsible, кроме себя
    recipients = [
        uid2 for uid2, r in resp_map.items()
        if r == current_resp and uid2 != uid
    ]
    tp = context.user_data["notify_tp"]
    vl = context.user_data["notify_vl"]
    branch = context.user_data["current_branch"]
    # шлём уведомление всем ответственным
    for chat_id in recipients:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"🔔 Уведомление по {branch} / {tp} / {vl}"
        )
        await context.bot.send_location(
            chat_id=chat_id,
            latitude=loc.latitude,
            longitude=loc.longitude
        )
    # подтверждаем отправителю
    await update.message.reply_text(
        f"✅ Уведомление отправлено ответственным: {', '.join(map(str, recipients))}",
        reply_markup=kb_actions
    )
    context.user_data["step"] = "BRANCH_SELECTED"

# Регистрируем хендлеры
application.add_handler(CommandHandler("start", start_line))
application.add_handler(MessageHandler(filters.LOCATION, location_handler))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

# Запуск webhook
if __name__ == "__main__":
    threading.Thread(target=lambda: requests.get(f"{SELF_URL}/webhook"), daemon=True).start()
    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=f"{SELF_URL}/webhook"
    )
