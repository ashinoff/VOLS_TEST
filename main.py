# main.py

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

from config import (
    TOKEN, SELF_URL, PORT,
    BRANCH_URLS, VISIBILITY_GROUPS,
    NOTIFY_SHEET_URL
)
from zones import normalize_sheet_url, load_zones

app = Flask(__name__)
application = ApplicationBuilder().token(TOKEN).build()

# — общие клавиатуры —
kb_back = ReplyKeyboardMarkup([["🔙 Назад"]], resize_keyboard=True)

def build_initial_kb(vis_flag: str) -> ReplyKeyboardMarkup:
    # обновлённая версия: RU → UG, явная ветка для RK
    if vis_flag == "All":
        nets = ["Россети ЮГ", "Россети Кубань"]
    elif vis_flag == "UG":       # вместо RU
        nets = ["Россети ЮГ"]
    elif vis_flag == "RK":
        nets = ["Россети Кубань"]
    else:
        nets = []
    buttons = [[n] for n in nets] + [["Телефоны провайдеров"], ["Сформировать отчёт"]]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def build_branch_kb(uid: int, selected_net: str, branch_map: dict) -> ReplyKeyboardMarkup:
    user_branch = branch_map[uid]
    if user_branch != "All":
        branches = [user_branch]
    else:
        branches = VISIBILITY_GROUPS[selected_net]
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
    vis_map, branch_map, res_map, names, resp_map = load_zones()
    if uid not in branch_map:
        await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)
        return

    context.user_data.clear()
    context.user_data.update({
        "step":       "INIT",
        "vis_flag":   vis_map[uid],      # All/UG/RK
        "branch_user":branch_map[uid],
        "res_user":   res_map[uid],      # конкретный РЭС или All
        "name":       names[uid],
        "resp_map":   resp_map
    })

    kb = build_initial_kb(vis_map[uid])
    await update.message.reply_text(f"Привет, {names[uid]}! Выберите опцию:", reply_markup=kb)

# — Обработка текстовых сообщений —
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    uid  = update.effective_user.id
    vis_map, branch_map, res_map, names, resp_map = load_zones()
    if uid not in branch_map:
        return await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)

    step        = context.user_data.get("step", "INIT")
    vis_flag    = context.user_data["vis_flag"]
    branch_user = context.user_data["branch_user"]
    res_user    = context.user_data["res_user"]
    user_name   = context.user_data["name"]

    # — Назад —
    if text == "🔙 Назад":
        if step in (
            "AWAIT_TP_INPUT","DISAMBIGUOUS",
            "NOTIFY_AWAIT_TP","NOTIFY_DISAMBIGUOUS","NOTIFY_AWAIT_VL","NOTIFY_WAIT_GEO"
        ):
            context.user_data["step"] = "BRANCH_SELECTED"
            return await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)
        if step == "NETWORK_SELECTED":
            context.user_data["step"] = "INIT"
            return await update.message.reply_text(
                "Выберите опцию:", reply_markup=build_initial_kb(vis_flag)
            )
        context.user_data["step"] = "INIT"
        return await update.message.reply_text(
            "Выберите опцию:", reply_markup=build_initial_kb(vis_flag)
        )

    # — INIT: главное меню —
    if step == "INIT":
        if text == "Телефоны провайдеров":
            context.user_data["step"] = "VIEW_PHONES"
            return await update.message.reply_text("📞 Телефоны провайдеров:\n…", reply_markup=kb_back)
        if text == "Сформировать отчёт":
            context.user_data["step"] = "VIEW_REPORT"
            return await update.message.reply_text("📝 Отчёт сформирован.", reply_markup=kb_back)

        # обновлённая проверка allowed_nets
        if vis_flag == "All":
            allowed_nets = ["Россети ЮГ", "Россети Кубань"]
        elif vis_flag == "UG":
            allowed_nets = ["Россети ЮГ"]
        elif vis_flag == "RK":
            allowed_nets = ["Россети Кубань"]
        else:
            allowed_nets = []

        if text in allowed_nets:
            context.user_data["step"]        = "NETWORK_SELECTED"
            context.user_data["selected_net"] = text
            kb = build_branch_kb(uid, text, branch_map)
            return await update.message.reply_text("Выберите филиал:", reply_markup=kb)

        return await update.message.reply_text(
            f"{user_name}, можете просматривать только: {', '.join(allowed_nets)} 😕",
            reply_markup=build_initial_kb(vis_flag)
        )

    # — NETWORK_SELECTED: выбор филиала —
    if step == "NETWORK_SELECTED":
        selected_net = context.user_data["selected_net"]
        if branch_user != "All":
            allowed_branches = [branch_user]
        else:
            allowed_branches = VISIBILITY_GROUPS[selected_net]

        if text in allowed_branches:
            context.user_data["step"] = "BRANCH_SELECTED"
            context.user_data["current_branch"] = text
            return await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)

        return await update.message.reply_text(
            f"{user_name}, можете просматривать только филиал(ы): {', '.join(allowed_branches)} 😕",
            reply_markup=build_branch_kb(uid, selected_net, branch_map)
        )

    # — BRANCH_SELECTED и далее оставляем без изменений —
    # … ваш код AWAIT_TP_INPUT, DISAMBIGUOUS, NOTIFY_… и location_handler …

# — Регистрируем хендлеры —
application.add_handler(CommandHandler("start", start_line))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
application.add_handler(MessageHandler(filters.LOCATION, location_handler))

if __name__ == "__main__":
    threading.Thread(target=lambda: requests.get(f"{SELF_URL}/webhook"), daemon=True).start()
    application.run_webhook(
        listen="0.0.0.0", port=PORT,
        url_path="webhook", webhook_url=f"{SELF_URL}/webhook"
    )
