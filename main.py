import os
import threading
import re
import requests
import pandas as pd
import csv
from datetime import datetime, timezone
from io import BytesIO

from flask import Flask
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    ApplicationBuilder, CommandHandler,
    MessageHandler, filters, ContextTypes,
)

from config import (
    TOKEN, SELF_URL, PORT,
    BRANCH_URLS, NOTIFY_URLS,
    NOTIFY_LOG_FILE_UG, NOTIFY_LOG_FILE_RK,
    HELP_FOLDER
)
from zones import normalize_sheet_url, load_zones

# Маппинг «сырого» имени филиала в ключ BRANCH_URLS
BRANCH_KEY_MAP = {
    "Тимашевский":      "Тимашевские ЭС",
    "Усть-Лабинский":   "Усть-Лабинские ЭС",
    "Тихорецкий":       "Тихорецкие ЭС",
    "Сочинский":        "Сочинские ЭС",
    "Славянский":       "Славянские ЭС",
    "Ленинградский":    "Ленинградские ЭС",
    "Лабинский":        "Лабинские ЭС",
    "Краснодарский":    "Краснодарские ЭС",
    "Армавирский":      "Армавирские ЭС",
    "Адыгейский":       "Адыгейские ЭС",
    "Центральный":      "Центральные ЭС",
    "Западный":         "Западные ЭС",
    "Восточный":        "Восточные ЭС",
    "Южный":            "Южные ЭС",
    "Северо-Восточный": "Северо-Восточные ЭС",
    "Юго-Восточный":    "Юго-Восточные ЭС",
    "Северный":         "Северные ЭС",
}

app = Flask(__name__)
application = ApplicationBuilder().token(TOKEN).build()

# Создаём папку HELP_FOLDER, если нужно
if HELP_FOLDER and not os.path.isdir(HELP_FOLDER):
    os.makedirs(HELP_FOLDER, exist_ok=True)

# Инициализация CSV-логов уведомлений
for lf in (NOTIFY_LOG_FILE_UG, NOTIFY_LOG_FILE_RK):
    if not os.path.exists(lf):
        with open(lf, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "Filial","РЭС","SenderID","SenderName",
                "RecipientID","RecipientName","Timestamp","Coordinates"
            ])

# Клавиатуры
kb_back = ReplyKeyboardMarkup([["🔙 Назад"]], resize_keyboard=True)
kb_actions = ReplyKeyboardMarkup(
    [["🔍 Поиск по ТП"], ["🔔 Отправить уведомление"], ["ℹ️ Справка"], ["🔙 Назад"]],
    resize_keyboard=True
)
kb_request_location = ReplyKeyboardMarkup(
    [[KeyboardButton("📍 Отправить геолокацию", request_location=True)],
     ["ℹ️ Справка"], ["🔙 Назад"]],
    resize_keyboard=True
)

def build_initial_kb(vis_flag: str, res_flag: str) -> ReplyKeyboardMarkup:
    f = vis_flag.strip().upper()
    nets = (["⚡ Россети ЮГ","⚡ Россети Кубань"] if f=="ALL"
            else ["⚡ Россети ЮГ"] if f=="UG"
            else ["⚡ Россети Кубань"])
    btns = [[n] for n in nets] + [["📞 Телефоны провайдеров"]]
    if res_flag.strip().upper()=="ALL":
        btns += [["📝 Сформировать отчёт"]]
    btns += [["ℹ️ Справка"]]
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

def build_report_kb(vis_flag: str) -> ReplyKeyboardMarkup:
    f = vis_flag.strip().upper()
    rows = []
    if f in ("ALL","UG"):
        rows.append(["📊 Уведомления о бездоговорных ВОЛС ЮГ"])
    if f in ("ALL","RK"):
        rows.append(["📊 Уведомления о бездоговорных ВОЛС Кубань"])
    rows += [["📋 Выгрузить информацию по контрагентам"],
             ["ℹ️ Справка"], ["🔙 Назад"]]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

# /start
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    vis_map, branch_map, res_map, names, resp_map = load_zones()
    if uid not in branch_map:
        return await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)

    raw = branch_map[uid]
    branch_key = "All" if raw=="All" else BRANCH_KEY_MAP.get(raw, raw)

    context.user_data.clear()
    context.user_data.update({
        "step":        "INIT",
        "vis_flag":    vis_map[uid],
        "res_user":    res_map[uid],
        "branch_user": branch_key,
        "name":        names[uid],
        "resp_map":    resp_map
    })

    await update.message.reply_text(
        f"👋 Приветствую Вас, {names[uid]}! Выберите опцию:",
        reply_markup=build_initial_kb(vis_map[uid], res_map[uid])
    )

# TEXT
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = context.user_data

    # если справка
    if text=="ℹ️ Справка" and data.get("step")!="HELP_LIST":
        data["prev_step"] = data.get("step","INIT")
        try:
            files = sorted(os.listdir(HELP_FOLDER))
        except:
            return await update.message.reply_text(
                "❌ Ошибка чтения папки справки.", reply_markup=kb_back
            )
        data["help_files"] = files
        data["step"] = "HELP_LIST"
        kb = ReplyKeyboardMarkup([[f] for f in files]+[["🔙 Назад"]], resize_keyboard=True)
        return await update.message.reply_text("Выберите файл:", reply_markup=kb)

    # в меню справки
    if data.get("step")=="HELP_LIST":
        if text=="🔙 Назад":
            data["step"] = data.get("prev_step","INIT")
            return await restore_menu(update,context)
        if text in data.get("help_files",[]):
            path = os.path.join(HELP_FOLDER,text)
            if text.lower().endswith((".png",".jpg",".jpeg")):
                await update.message.reply_photo(open(path,"rb"))
            else:
                await update.message.reply_document(open(path,"rb"))
            data["step"] = data.get("prev_step","INIT")
            return await restore_menu(update,context)

    # иначе — ваш остальной код (INIT, REPORT_MENU, NET, BRANCH,…)
    # просто вставьте сюда всё, что было до этого, без изменений,
    # но убедитесь, что на всех ветках в reply_markup есть кнопка “ℹ️ Справка”,
    # и что в конце есть переход в restore_menu при возврате из справки.

# Обработчик локации (для уведомлений)
async def location_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # … ваша логика отправки уведомлений …
    pass  # <— вставьте сюда уже отлаженный вами код уведомлений

# Вспомогательный возврат из справки
async def restore_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    step = context.user_data.get("step","INIT")
    v = context.user_data["vis_flag"]
    r = context.user_data["res_user"]
    if step=="INIT":
        return await update.message.reply_text(
            "Выберите опцию:", reply_markup=build_initial_kb(v,r)
        )
    if step=="REPORT_MENU":
        return await update.message.reply_text(
            "📝 Выберите тип отчёта:", reply_markup=build_report_kb(v)
        )
    if step=="BRANCH":
        return await update.message.reply_text(
            "Выберите действие:", reply_markup=kb_actions
        )
    return await update.message.reply_text("🔙 Назад", reply_markup=kb_back)

# Регистрируем
application.add_handler(CommandHandler("start", start_cmd))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
application.add_handler(MessageHandler(filters.LOCATION, location_handler))

if __name__=="__main__":
    if SELF_URL:
        threading.Thread(
            target=lambda: requests.get(f"{SELF_URL}/webhook"),
            daemon=True
        ).start()
    application.run_webhook(
        listen="0.0.0.0", port=PORT,
        url_path="webhook", webhook_url=f"{SELF_URL}/webhook"
    )
