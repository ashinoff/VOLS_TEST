import threading
from flask import Flask
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters
from telegram import Update, ReplyKeyboardMarkup
from config import TOKEN, SELF_URL, PORT, VISIBILITY_GROUPS, BRANCH_URLS
from zones import normalize_sheet_url, load_zones
import pandas as pd
import re
import requests
from io import StringIO

app = Flask(__name__)

# === Клавиатуры ===
kb_initial = ReplyKeyboardMarkup(
    [["Россети ЮГ"], ["Россети Кубань"], ["Телефоны провайдеров"], ["Сформировать отчёт"]],
    resize_keyboard=True
)
kb_back = ReplyKeyboardMarkup([["Назад"]], resize_keyboard=True)
def kb_branches(branches):
    btns = [[b] for b in branches] + [["Назад"]]
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)
kb_search = ReplyKeyboardMarkup([["Поиск по ТП"], ["Назад"]], resize_keyboard=True)

# === Handlers ===
async def start_line(update: Update, context):
    uid = update.effective_user.id
    vis_map, bz, rz, names = load_zones()
    if uid not in bz:
        await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)
        return
    context.user_data.clear()
    context.user_data["step"] = "INIT"
    context.user_data["res"] = rz[uid]
    context.user_data["name"] = names[uid]
    await update.message.reply_text(f"Привет, {names[uid]}! Выберите опцию:", reply_markup=kb_initial)

async def handle_text(update: Update, context):
    text = update.message.text.strip()
    uid = update.effective_user.id
    vis_map, bz, rz, names = load_zones()
    if uid not in bz:
        await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)
        return
    step = context.user_data.get("step", "INIT")
    # Назад
    if text == "Назад":
        if step in ("AWAIT_TP_INPUT","DISAMBIGUOUS"):
            context.user_data["step"] = "BRANCH_SELECTED"
            await update.message.reply_text("Выберите действие:", reply_markup=kb_search)
            return
        if step == "BRANCH_SELECTED":
            context.user_data["step"] = "INIT"
            await update.message.reply_text("Выберите опцию:", reply_markup=kb_initial)
            return
        return
    # INIT
    if step == "INIT":
        if text == "Телефоны провайдеров":
            context.user_data["step"] = "VIEW_PHONES"
            await update.message.reply_text("📞 Телефоны провайдеров:\n…", reply_markup=kb_back)
            return
        if text == "Сформировать отчёт":
            context.user_data["step"] = "VIEW_REPORT"
            await update.message.reply_text("📝 Отчёт сформирован.", reply_markup=kb_back)
            return
        if text in VISIBILITY_GROUPS:
            context.user_data["step"] = "NETWORK_SELECTED"
            context.user_data["visibility"] = text
            branches = VISIBILITY_GROUPS[text]
            await update.message.reply_text(f"Вы выбрали {text}. Теперь — филиал:", reply_markup=kb_branches(branches))
        return
    # NETWORK_SELECTED
    if step == "NETWORK_SELECTED":
        branches = VISIBILITY_GROUPS[context.user_data["visibility"]]
        if text in branches:
            context.user_data["step"] = "BRANCH_SELECTED"
            context.user_data["current_branch"] = text
            await update.message.reply_text(f"Филиал {text} выбран. Что дальше?", reply_markup=kb_search)
        return
    # BRANCH_SELECTED
    if step == "BRANCH_SELECTED":
        if text == "Поиск по ТП":
            context.user_data["step"] = "AWAIT_TP_INPUT"
            await update.message.reply_text("Введите номер ТП:", reply_markup=kb_back)
        return
    # AWAIT_TP_INPUT
    if step == "AWAIT_TP_INPUT":
        branch = context.user_data["current_branch"]
        res    = context.user_data["res"]
        # загрузка данных
        url = normalize_sheet_url(BRANCH_URLS[branch])
        df  = pd.read_csv(url)
        if res != "All":
            df = df[df["РЭС"] == res]
        df.columns = df.columns.str.strip()
        df["D_UP"] = df["Наименование ТП"].str.upper().str.replace(r'\W','', regex=True)
        q = re.sub(r'\W','', text.upper())
        found = df[df["D_UP"].str.contains(q, na=False)]
        if found.empty:
            context.user_data["step"] = "BRANCH_SELECTED"
            await update.message.reply_text("🔍 Ничего не найдено.", reply_markup=kb_search)
            return
        unique_tp = found["Наименование ТП"].unique().tolist()
        if len(unique_tp) > 1:
            context.user_data["step"] = "DISAMBIGUOUS"
            context.user_data["ambiguous_df"] = found
            kb = ReplyKeyboardMarkup([[tp] for tp in unique_tp] + [["Назад"]], resize_keyboard=True)
            await update.message.reply_text("Найдено несколько ТП, выберите:", reply_markup=kb)
            return
        # одна TP
        tp = unique_tp[0]
        for _, r in found[found["Наименование ТП"] == tp].iterrows():
            await update.message.reply_text(
                f"📍 {tp}\nВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
                f"Опоры: {r['Опоры']} ({r['Количество опор']})\n"
                f"Провайдер: {r.get('Наименование Провайдера','')}"
                + (f", договор {r.get('Номер договора')}" if r.get('Номер договора') else ""),
                reply_markup=kb_search
            )
        context.user_data["step"] = "BRANCH_SELECTED"
        return
    # DISAMBIGUOUS
    if step == "DISAMBIGUOUS":
        df = context.user_data["ambiguous_df"]
        if text in df["Наименование ТП"].unique():
            for _, r in df[df["Наименование ТП"] == text].iterrows():
                await update.message.reply_text(
                    f"📍 {text}\nВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
                    f"Опоры: {r['Опоры']} ({r['Количество опор']})\n"
                    f"Провайдер: {r.get('Наименование Провайдера','')}"
                    + (f", договор {r.get('Номер договора')}" if r.get('Номер договора') else ""),
                    reply_markup=kb_search
                )
            context.user_data["step"] = "BRANCH_SELECTED"
        return

# Регистрируем handlers
application = ApplicationBuilder().token(TOKEN).build()
application.add_handler(CommandHandler("start", start_line))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

# Запуск webhook
if __name__ == "__main__":
    threading.Thread(target=lambda: requests.get(f"{SELF_URL}/webhook"), daemon=True).start()
    application.run_webhook(
        listen="0.0.0.0", port=PORT, url_path="webhook", webhook_url=f"{SELF_URL}/webhook"
    )
