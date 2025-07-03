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

# — клавиатуры с «Назад» и действиями филиала остаются неизменными —
kb_back = ReplyKeyboardMarkup([["🔙 Назад"]], resize_keyboard=True)
kb_actions = ReplyKeyboardMarkup(
    [["🔍 Поиск по ТП"], ["🔔 Отправить уведомление"], ["🔙 Назад"]],
    resize_keyboard=True
)
kb_request_location = ReplyKeyboardMarkup(
    [[KeyboardButton("📍 Отправить геолокацию", request_location=True)], ["🔙 Назад"]],
    resize_keyboard=True
)

def build_initial_kb(vis_flag: str) -> ReplyKeyboardMarkup:
    """
    Если vis_flag == 'All' — две сети,
    'RU' — только Россети ЮГ,
    'RK' — только Россети Кубань.
    Всегда добавляем Телефоны и Отчёт.
    """
    if vis_flag == "All":
        nets = ["Россети ЮГ", "Россети Кубань"]
    elif vis_flag == "RU":
        nets = ["Россети ЮГ"]
    else:  # 'RK'
        nets = ["Россети Кубань"]
    buttons = [[n] for n in nets] + [["Телефоны провайдеров"], ["Сформировать отчёт"]]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def build_branch_kb(uid: int, selected_net: str, branch_map: dict) -> ReplyKeyboardMarkup:
    """
    Если пользователь привязан к одному филиалу (branch_map[uid] != 'All'),
    показываем только его. Иначе — все филиалы выбранной сети.
    """
    user_branch = branch_map[uid]
    if user_branch != "All":
        branches = [user_branch]
    else:
        branches = VISIBILITY_GROUPS[selected_net]
    buttons = [[b] for b in branches] + [["🔙 Назад"]]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

# — /start —
async def start_line(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    vis_map, branch_map, res_map, names, resp_map = load_zones()
    if uid not in branch_map:
        await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)
        return

    # сохраняем в user_data
    context.user_data.clear()
    context.user_data.update({
        "step": "INIT",
        "vis_flag": vis_map[uid],      # All, RU или RK
        "branch_user": branch_map[uid],
        "res_user": res_map[uid],      # конкретный РЭС или All
        "name": names[uid],
        "resp_map": resp_map,
    })

    kb = build_initial_kb(vis_map[uid])
    await update.message.reply_text(
        f"Привет, {names[uid]}! Выберите опцию:",
        reply_markup=kb
    )

# — Обработка текста —
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
        if step in ("AWAIT_TP_INPUT","DISAMBIGUOUS","SEARCH_DONE",
                    "NOTIFY_AWAIT_TP","NOTIFY_DISAMBIGUOUS","NOTIFY_AWAIT_VL","NOTIFY_WAIT_GEO"):
            # возврат к меню действий в филиале
            context.user_data["step"] = "BRANCH_SELECTED"
            return await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)
        if step == "NETWORK_SELECTED":
            context.user_data["step"] = "INIT"
            kb = build_initial_kb(vis_flag)
            return await update.message.reply_text("Выберите опцию:", reply_markup=kb)
        # INIT, VIEW_PHONES, VIEW_REPORT
        context.user_data["step"] = "INIT"
        kb = build_initial_kb(vis_flag)
        return await update.message.reply_text("Выберите опцию:", reply_markup=kb)

    # — INIT: главное меню —
    if step == "INIT":
        # Телефоны и отчёт всегда доступны
        if text == "Телефоны провайдеров":
            context.user_data["step"] = "VIEW_PHONES"
            return await update.message.reply_text("📞 Телефоны провайдеров:\n…", reply_markup=kb_back)
        if text == "Сформировать отчёт":
            context.user_data["step"] = "VIEW_REPORT"
            return await update.message.reply_text("📝 Отчёт сформирован.", reply_markup=kb_back)

        # Сети по vis_flag
        allowed_nets = []
        if vis_flag == "All":
            allowed_nets = ["Россети ЮГ", "Россети Кубань"]
        elif vis_flag == "RU":
            allowed_nets = ["Россети ЮГ"]
        else:
            allowed_nets = ["Россети Кубань"]

        if text in allowed_nets:
            # переходим к выбору филиала
            context.user_data["step"]       = "NETWORK_SELECTED"
            context.user_data["selected_net"] = text
            kb = build_branch_kb(uid, text, branch_map)
            return await update.message.reply_text("Выберите филиал:", reply_markup=kb)

        # кликнул по недоступному
        return await update.message.reply_text(
            f"{user_name}, вы можете просматривать только: {', '.join(allowed_nets)} 😕",
            reply_markup=build_initial_kb(vis_flag)
        )

    # — NETWORK_SELECTED: выбор филиала —
    if step == "NETWORK_SELECTED":
        selected_net = context.user_data["selected_net"]
        # допустимые филиалы
        if branch_user != "All":
            allowed_branches = [branch_user]
        else:
            allowed_branches = VISIBILITY_GROUPS[selected_net]

        if text in allowed_branches:
            context.user_data["step"] = "BRANCH_SELECTED"
            context.user_data["current_branch"] = text
            return await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)
        # кликнул чужой филиал
        allowed = allowed_branches[0] if len(allowed_branches)==1 else ", ".join(allowed_branches)
        return await update.message.reply_text(
            f"{user_name}, вы можете просматривать только филиал(ы): {allowed} 😕",
            reply_markup=build_branch_kb(uid, selected_net, branch_map)
        )

    # — BRANCH_SELECTED: действия филиала —
    if step == "BRANCH_SELECTED":
        if text == "🔍 Поиск по ТП":
            context.user_data["step"] = "AWAIT_TP_INPUT"
            return await update.message.reply_text("Введите номер ТП:", reply_markup=kb_back)

        if text == "🔔 Отправить уведомление":
            if res_user == "All":
                return await update.message.reply_text(
                    f"{user_name}, уведомления доступны только при конкретном РЭС 😕",
                    reply_markup=kb_actions
                )
            context.user_data["step"] = "NOTIFY_AWAIT_TP"
            return await update.message.reply_text("Введите номер ТП для уведомления:", reply_markup=kb_back)

        # любое другое — игнор
        return

    # (далее можно оставить вашу ранее настроенную логику поиска и уведомлений,
    #  она уже фильтрует по res_user и uses context.user_data["step"])

    # … ваш код обработки AWAIT_TP_INPUT, DISAMBIGUOUS, NOTIFY_AWAIT_TP, NOTIFY_DISAMBIGUOUS, NOTIFY_AWAIT_VL, location_handler …
    # он остаётся без изменений, т.к. уже учитывает res_user и работает внутри филиала.

# Регистрируем handlers
application.add_handler(CommandHandler("start", start_line))
application.add_handler(MessageHandler(filters.LOCATION, location_handler))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

if __name__ == "__main__":
    threading.Thread(target=lambda: requests.get(f"{SELF_URL}/webhook"), daemon=True).start()
    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=f"{SELF_URL}/webhook"
    )
