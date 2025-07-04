import os
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

from config import TOKEN, SELF_URL, PORT, BRANCH_URLS, NOTIFY_SHEET_URL, ZONES_CSV_URL
from zones import normalize_sheet_url, load_zones

app = Flask(__name__)
application = ApplicationBuilder().token(TOKEN).build()

# — Клавиатуры —
kb_back = ReplyKeyboardMarkup([["🔙 Назад"]], resize_keyboard=True)

def build_initial_kb(vis_flag: str) -> ReplyKeyboardMarkup:
    """Главное меню: сети с ⚡, телефон и отчёт с иконками."""
    flag = vis_flag.strip().upper()
    if flag == "ALL":
        nets = ["⚡ Россети ЮГ", "⚡ Россети Кубань"]
    elif flag == "UG":
        nets = ["⚡ Россети ЮГ"]
    elif flag == "RK":
        nets = ["⚡ Россети Кубань"]
    else:
        nets = []
    buttons = [[n] for n in nets] + [
        ["📞 Телефоны провайдеров"],
        ["📝 Сформировать отчёт"]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def build_branch_kb(uid: int, selected_net: str, branch_map: dict) -> ReplyKeyboardMarkup:
    """Меню филиалов: либо только свой, либо все из BRANCH_URLS[selected_net]."""
    user_branch = branch_map.get(uid, "All")
    if user_branch != "All":
        branches = [user_branch]
    else:
        branches = list(BRANCH_URLS[selected_net].keys())
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

# — /start —
async def start_line(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    vis_map, branch_map_raw, res_map, names, resp_map = load_zones()

    # Нужно, чтобы branch_map совпадал с ключами в BRANCH_URLS
    branch_map = {
        u: b for u, b in branch_map_raw.items()
        if b in sum((list(d.keys()) for d in BRANCH_URLS.values()), [])
    }

    if uid not in branch_map:
        return await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)

    context.user_data.clear()
    context.user_data.update({
        "step":        "INIT",
        "vis_flag":    vis_map[uid],
        "branch_user": branch_map[uid],
        "res_user":    res_map[uid],
        "resp_map":    resp_map,
        "name":        names[uid],
    })

    kb = build_initial_kb(vis_map[uid])
    await update.message.reply_text(
        f"👋 Приветствую Вас, {names[uid]}! Выберите опцию:",
        reply_markup=kb
    )

# — Обработка текстовых сообщений —
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    uid  = update.effective_user.id

    vis_map, branch_map_raw, res_map, names, _ = load_zones()
    branch_map = {
        u: b for u, b in branch_map_raw.items()
        if b in sum((list(d.keys()) for d in BRANCH_URLS.values()), [])
    }
    if uid not in branch_map:
        return await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)

    step        = context.user_data.get("step", "INIT")
    vis_flag    = context.user_data["vis_flag"]
    branch_user = context.user_data["branch_user"]
    res_user    = context.user_data["res_user"]
    resp_flag   = context.user_data["resp_map"].get(uid, "").strip()
    user_name   = context.user_data["name"]

    # — Назад —
    if text == "🔙 Назад":
        # ... (как раньше) ...
        return

    # — INIT: главное меню —
    if step == "INIT":
        # Телефоны/отчёт
        if text == "📞 Телефоны провайдеров":
            context.user_data["step"] = "VIEW_PHONES"
            return await update.message.reply_text("📞 Телефоны провайдеров:\n…", reply_markup=kb_back)
        if text == "📝 Сформировать отчёт":
            context.user_data["step"] = "VIEW_REPORT"
            return await update.message.reply_text("📝 Отчёт сформирован.", reply_markup=kb_back)

        # Сети с ⚡
        flag = vis_flag.strip().upper()
        if flag == "ALL":
            allowed = ["⚡ Россети ЮГ", "⚡ Россети Кубань"]
        elif flag == "UG":
            allowed = ["⚡ Россети ЮГ"]
        else:
            allowed = ["⚡ Россети Кубань"]

        if text in allowed:
            selected_net = text.replace("⚡ ", "")
            context.user_data.update({
                "step":         "NETWORK_SELECTED",
                "selected_net": selected_net
            })
            kb = build_branch_kb(uid, selected_net, branch_map)
            return await update.message.reply_text("Выберите филиал:", reply_markup=kb)

        return await update.message.reply_text(
            f"{user_name}, можете просматривать только: {', '.join(allowed)} 😕",
            reply_markup=build_initial_kb(vis_flag)
        )

    # — NETWORK_SELECTED: выбор филиала —
    if step == "NETWORK_SELECTED":
        selected_net = context.user_data["selected_net"]
        allowed = [branch_user] if branch_user != "All" else list(BRANCH_URLS[selected_net].keys())
        if text in allowed:
            context.user_data["step"]           = "BRANCH_SELECTED"
            context.user_data["current_branch"] = text
            return await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)
        return await update.message.reply_text(
            f"{user_name}, можете просматривать только: {', '.join(allowed)} 😕",
            reply_markup=build_branch_kb(uid, selected_net, branch_map)
        )

    # — BRANCH_SELECTED: …
    # остальная логика поиска и уведомлений без изменений,
    # только при загрузке CSV теперь:
    # df = pd.read_csv(
    #     normalize_sheet_url(BRANCH_URLS[selected_net][current_branch])
    # )
    # вместо прежнего BRANCH_URLS[current_branch]

# — Геолокация для уведомлений — (как было)
async def location_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # …

# — Регистрация —
application.add_handler(CommandHandler("start", start_line))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
application.add_handler(MessageHandler(filters.LOCATION, location_handler))

if __name__ == "__main__":
    threading.Thread(target=lambda: requests.get(f"{SELF_URL}/webhook"), daemon=True).start()
    application.run_webhook(
        listen="0.0.0.0", port=PORT,
        url_path="webhook", webhook_url=f"{SELF_URL}/webhook"
    )
