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

from config import TOKEN, SELF_URL, PORT, BRANCH_URLS, VISIBILITY_GROUPS, NOTIFY_SHEET_URL
from zones import normalize_sheet_url, load_zones

app = Flask(__name__)
application = ApplicationBuilder().token(TOKEN).build()

# — КЛАВИАТУРЫ —
kb_back = ReplyKeyboardMarkup([["🔙 Назад"]], resize_keyboard=True)

def build_initial_kb(vis_flag: str) -> ReplyKeyboardMarkup:
    flag = vis_flag.strip().upper()
    if flag == "ALL":
        nets = ["Россети ЮГ", "Россети Кубань"]
    elif flag == "UG":
        nets = ["Россети ЮГ"]
    elif flag == "RK":
        nets = ["Россети Кубань"]
    else:
        nets = []
    buttons = [[n] for n in nets] + [["Телефоны провайдеров"], ["Сформировать отчёт"]]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def build_branch_kb(uid: int, selected_net: str, branch_map: dict) -> ReplyKeyboardMarkup:
    user_branch = branch_map.get(uid, "All")
    if user_branch != "All":
        branches = [user_branch]
    else:
        branches = VISIBILITY_GROUPS.get(selected_net, [])
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
        "step":        "INIT",
        "vis_flag":    vis_map[uid],
        "branch_user": branch_map[uid],
        "res_user":    res_map[uid],
        "resp_map":    resp_map,      # добавлено
        "name":        names[uid],
    })

    kb = build_initial_kb(vis_map[uid])
    await update.message.reply_text(f"Привет, {names[uid]}! Выберите опцию:", reply_markup=kb)


# — Обработка текста —
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    uid  = update.effective_user.id
    vis_map, branch_map, res_map, names, resp_map = load_zones()
    if uid not in branch_map:
        await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)
        return

    step        = context.user_data.get("step", "INIT")
    vis_flag    = context.user_data["vis_flag"]
    branch_user = context.user_data["branch_user"]
    res_user    = context.user_data["res_user"]
    resp_map_u  = context.user_data["resp_map"].get(uid, "")  # изменено: признак ответственного
    user_name   = context.user_data["name"]

    # — Назад —
    if text == "🔙 Назад":
        # ... (как ранее) ...
        return

    # — INIT: главное меню —
    if step == "INIT":
        # ... (телефоны/отчёт) ...

        # составляем allowed_nets по vis_flag
        # (без изменений)

        if text in allowed_nets:
            # ... переходим к NETWORK_SELECTED ...
        else:
            # ... блокировка ...

        return

    # — NETWORK_SELECTED: выбор филиала —
    if step == "NETWORK_SELECTED":
        # ... как ранее ...
        return

    # — BRANCH_SELECTED: действия филиала —
    if step == "BRANCH_SELECTED":
        if text == "🔍 Поиск по ТП":
            # ... как ранее ...
            return

        if text == "🔔 Отправить уведомление":
            # ИЗМЕНЕНИЕ: доступно ВСЕМ, кроме тех, у кого есть resp_map
            if resp_map_u != "":
                await update.message.reply_text(
                    f"{user_name}, вы не можете отправлять уведомления 😕",
                    reply_markup=kb_actions
                )
                return
            # дальше — ввод ТП для уведомления
            context.user_data["step"] = "NOTIFY_AWAIT_TP"
            await update.message.reply_text(
                "Введите номер ТП для уведомления:", reply_markup=kb_back
            )
            return
        return

    # — AWAIT_TP_INPUT, DISAMBIGUOUS — по-прежнему без изменений —

    # — NOTIFY_AWAIT_TP: ввод ТП для уведомления —
    if step == "NOTIFY_AWAIT_TP":
        # ... загрузка dfn, поиск ...
        # при однозначном TP:
        tp2 = unique_tp[0]
        context.user_data.update({
            "step":        "NOTIFY_AWAIT_VL",
            "notify_df":   found,
            "notify_tp":   tp2,
            "notify_res":  found.iloc[0]["РЭС"],  # изменено: сохраняем RES из справочника
        })
        # дальше — выбор ВЛ
        return

    # — NOTIFY_AWAIT_VL: выбор ВЛ —
    # ... как ранее ...

    return


# — Ловим геолокацию и рассылаем уведомление —
async def location_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("step") != "NOTIFY_WAIT_GEO":
        return
    loc = update.message.location

    notify_res = context.user_data["notify_res"]      # изменено: берем из user_data
    sender_name = context.user_data["name"]
    tp = context.user_data["notify_tp"]
    vl = context.user_data["notify_vl"]

    _, _, _, _, resp_map2 = load_zones()
    # получатели: те, у кого resp_map == notify_res
    recipients = [u for u, r in resp_map2.items() if r == notify_res]

    notif_text = f"{sender_name}, {notify_res}, {tp}, {vl} — Найден бездоговорной ВОЛС"
    for cid in recipients:
        await context.bot.send_message(chat_id=cid, text=notif_text)
        await context.bot.send_location(chat_id=cid, latitude=loc.latitude, longitude=loc.longitude)

    await update.message.reply_text(
        f"✅ Уведомление отправлено: {', '.join(map(str, recipients))}",
        reply_markup=kb_actions
    )
    context.user_data["step"] = "BRANCH_SELECTED"


# — РЕГИСТРАЦИЯ —
application.add_handler(CommandHandler("start", start_line))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
application.add_handler(MessageHandler(filters.LOCATION, location_handler))

if __name__ == "__main__":
    threading.Thread(target=lambda: requests.get(f"{SELF_URL}/webhook"), daemon=True).start()
    application.run_webhook(
        listen="0.0.0.0", port=PORT,
        url_path="webhook", webhook_url=f"{SELF_URL}/webhook"
    )
