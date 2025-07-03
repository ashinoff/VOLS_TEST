import os
import threading
import re
import requests
import pandas as pd
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

# === Клавиатуры ===
kb_initial = ReplyKeyboardMarkup(
    [["Россети ЮГ"], ["Россети Кубань"], ["Телефоны провайдеров"], ["Сформировать отчёт"]],
    resize_keyboard=True
)
kb_back = ReplyKeyboardMarkup([["🔙 Назад"]], resize_keyboard=True)

def kb_branches(branches):
    return ReplyKeyboardMarkup([[b] for b in branches] + [["🔙 Назад"]], resize_keyboard=True)

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
    context.user_data.update({
        "step": "INIT",
        "res":   rz[uid],
        "name":  names[uid],
        "resp":  resp_map
    })
    await update.message.reply_text(
        f"Привет, {names[uid]}! Выберите опцию:",
        reply_markup=kb_initial
    )

# === Обработка текстовых сообщений ===
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    uid  = update.effective_user.id
    vis_map, bz, rz, names, resp_map = load_zones()
    if uid not in bz:
        return await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)
    step = context.user_data.get("step", "INIT")

    # — Назад —
    if text == "🔙 Назад":
        # из уведомлений и поиска возвращаем к действиям филиала
        if step in ("NOTIFY_WAIT_GEO", "NOTIFY_AWAIT_VL", "NOTIFY_DISAMBIGUOUS",
                    "NOTIFY_AWAIT_TP", "AWAIT_TP_INPUT", "DISAMBIGUOUS", "SEARCH_DONE"):
            context.user_data["step"] = "BRANCH_SELECTED"
            return await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)
        # из выбора филиала — в INIT
        if step == "NETWORK_SELECTED":
            context.user_data["step"] = "INIT"
            return await update.message.reply_text("Выберите опцию:", reply_markup=kb_initial)
        # INIT, VIEW_PHONES, VIEW_REPORT — остаёмся в INIT
        context.user_data["step"] = "INIT"
        return await update.message.reply_text("Выберите опцию:", reply_markup=kb_initial)

    # — INIT: главное меню —
    if step == "INIT":
        if text == "Телефоны провайдеров":
            context.user_data["step"] = "VIEW_PHONES"
            return await update.message.reply_text("📞 Телефоны провайдеров:\n…", reply_markup=kb_back)
        if text == "Сформировать отчёт":
            context.user_data["step"] = "VIEW_REPORT"
            return await update.message.reply_text("📝 Отчёт сформирован.", reply_markup=kb_back)
        if text in VISIBILITY_GROUPS:
            context.user_data["step"]       = "NETWORK_SELECTED"
            context.user_data["visibility"] = text
            return await update.message.reply_text(
                "Выберите филиал:",
                reply_markup=kb_branches(VISIBILITY_GROUPS[text])
            )
        return

    # — NETWORK_SELECTED: выбор филиала —
    if step == "NETWORK_SELECTED":
        branches = VISIBILITY_GROUPS.get(context.user_data["visibility"], [])
        if text in branches:
            context.user_data["step"] = "BRANCH_SELECTED"
            context.user_data["current_branch"] = text
            return await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)
        return

    # — BRANCH_SELECTED: действия в филиале —
    if step == "BRANCH_SELECTED":
        if text == "🔍 Поиск по ТП":
            context.user_data["step"] = "AWAIT_TP_INPUT"
            return await update.message.reply_text("Введите номер ТП:", reply_markup=kb_back)
        if text == "🔔 Отправить уведомление":
            if context.user_data["res"] == "All":
                return await update.message.reply_text("❌ Только для конкретных РЭС.", reply_markup=kb_actions)
            context.user_data["step"] = "NOTIFY_AWAIT_TP"
            return await update.message.reply_text("Введите номер ТП для уведомления:", reply_markup=kb_back)
        return

    # — Обычный поиск ТП —
    if step == "AWAIT_TP_INPUT":
        branch = context.user_data["current_branch"]
        res    = context.user_data["res"]
        df = pd.read_csv(normalize_sheet_url(BRANCH_URLS[branch]))
        if res != "All":
            df = df[df["РЭС"] == res]
        df.columns = df.columns.str.strip()
        df["D_UP"] = df["Наименование ТП"].str.upper().str.replace(r'\W','', regex=True)
        q = re.sub(r'\W','', text.upper())
        found = df[df["D_UP"].str.contains(q, na=False)]
        if found.empty:
            return await update.message.reply_text("🔍 Не найдено. Повторите ввод:", reply_markup=kb_back)
        unique_tp = found["Наименование ТП"].unique().tolist()
        if len(unique_tp) > 1:
            context.user_data.update({"step":"DISAMBIGUOUS","ambiguous_df":found})
            kb = ReplyKeyboardMarkup([[tp] for tp in unique_tp]+[["🔙 Назад"]], resize_keyboard=True)
            return await update.message.reply_text("Найдены несколько ТП, выберите:", reply_markup=kb)
        tp = unique_tp[0]
        details = found[found["Наименование ТП"] == tp]
        cnt = len(details); res_name = details.iloc[0]["РЭС"]
        await update.message.reply_text(
            f"{res_name}, {tp} ({cnt}) ВОЛС с договором аренды.\nВыберите действие:",
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

    # — DISAMBIGUOUS: выбор из обычного поиска —
    if step == "DISAMBIGUOUS":
        df = context.user_data["ambiguous_df"]
        if text in df["Наименование ТП"].unique():
            details = df[df["Наименование ТП"] == text]
            cnt = len(details); res_name = details.iloc[0]["РЭС"]
            await update.message.reply_text(
                f"{res_name}, {text} ({cnt}) ВОЛС с договором аренды.\nВыберите действие:",
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

    # — NOTIFY_AWAIT_TP: поиск ТП для уведомления —
    if step == "NOTIFY_AWAIT_TP":
        dfn = pd.read_csv(normalize_sheet_url(NOTIFY_SHEET_URL))
        dfn.columns = dfn.columns.str.strip()
        dfn["D_UP"] = dfn["Наименование ТП"].str.upper().str.replace(r'\W','',regex=True)
        q = re.sub(r'\W','', text.upper()); found = dfn[dfn["D_UP"].str.contains(q,na=False)]
        if found.empty:
            return await update.message.reply_text("🔍 Не найдено. Повторите:", reply_markup=kb_back)
        u = found["Наименование ТП"].unique().tolist()
        if len(u)>1:
            context.user_data.update({"step":"NOTIFY_DISAMBIGUOUS","notify_df":found})
            kb = ReplyKeyboardMarkup([[tp] for tp in u]+[["🔙 Назад"]], resize_keyboard=True)
            return await update.message.reply_text("Несколько ТП—выберите:", reply_markup=kb)
        tp = u[0]
        context.user_data.update({"step":"NOTIFY_AWAIT_VL","notify_df":found,"notify_tp":tp})
        vls = found[found["Наименование ТП"]==tp]["Наименование ВЛ"].unique().tolist()
        kb = ReplyKeyboardMarkup([[vl] for vl in vls]+[["🔙 Назад"]], resize_keyboard=True)
        return await update.message.reply_text("Выберите ВЛ:", reply_markup=kb)

    # — NOTIFY_DISAMBIGUOUS: выбор ТП для уведомления —
    if step == "NOTIFY_DISAMBIGUOUS":
        dfn = context.user_data["notify_df"]
        if text in dfn["Наименование ТП"].unique():
            tp = text
            context.user_data.update({"step":"NOTIFY_AWAIT_VL","notify_tp":tp})
            vls = dfn[dfn["Наименование ТП"]==tp]["Наименование ВЛ"].unique().tolist()
            kb = ReplyKeyboardMarkup([[vl] for vl in vls]+[["🔙 Назад"]], resize_keyboard=True)
            return await update.message.reply_text("Выберите ВЛ:", reply_markup=kb)

    # — NOTIFY_AWAIT_VL: запрос геолокации для уведомления —
    if step == "NOTIFY_AWAIT_VL" and text:
        dfn = context.user_data["notify_df"]
        if text in dfn["Наименование ВЛ"].unique():
            context.user_data["step"] = "NOTIFY_WAIT_GEO"
            context.user_data["notify_vl"] = text
            return await update.message.reply_text(
                "📍 Отправьте геолокацию для уведомления:",
                reply_markup=kb_request_location
            )

# === Ловим геолокацию и рассылаем уведомление ===
async def location_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("step") != "NOTIFY_WAIT_GEO":
        return
    loc = update.message.location
    sender_id = update.effective_user.id

    # пересоздаём словарь ответственных
    _, _, rz_map, _, resp_map = load_zones()
    sender_res = context.user_data["res"]
    sender_name = context.user_data["name"]
    tp = context.user_data["notify_tp"]
    vl = context.user_data["notify_vl"]

    # получатели: все, у кого resp_map == sender_res
    recipients = [uid2 for uid2, resp in resp_map.items() if resp == sender_res]

    if not recipients:
        return await update.message.reply_text(
            "❌ Нет назначенных ответственных для Вашего РЭС.",
            reply_markup=kb_actions
        )

    notif_text = f"{sender_name}, {sender_res}, {tp}, {vl} — Найден бездоговорной ВОЛС"
    for cid in recipients:
        await context.bot.send_message(chat_id=cid, text=notif_text)
        await context.bot.send_location(chat_id=cid, latitude=loc.latitude, longitude=loc.longitude)

    await update.message.reply_text(
        f"✅ Уведомление отправлено: {', '.join(map(str,recipients))}",
        reply_markup=kb_actions
    )
    context.user_data["step"] = "BRANCH_SELECTED"

# — Регистрируем и запускаем —
application.add_handler(CommandHandler("start", start_line))
application.add_handler(MessageHandler(filters.LOCATION, location_handler))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

if __name__ == "__main__":
    threading.Thread(target=lambda: requests.get(f"{SELF_URL}/webhook"), daemon=True).start()
    application.run_webhook(
        listen="0.0.0.0", port=PORT,
        url_path="webhook", webhook_url=f"{SELF_URL}/webhook"
    )
