# main.py

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

# Сопоставление «сырого» названия филиала (из CSV) в ключ BRANCH_URLS
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

# Инициализация папки справки
if HELP_FOLDER:
    os.makedirs(HELP_FOLDER, exist_ok=True)

# Инициализация CSV-файлов логов уведомлений
for lf in (NOTIFY_LOG_FILE_UG, NOTIFY_LOG_FILE_RK):
    if not os.path.exists(lf):
        with open(lf, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "Filial","РЭС",
                "SenderID","SenderName",
                "RecipientID","RecipientName",
                "Timestamp","Coordinates"
            ])

# --- Клавиатуры ---
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
    if f == "ALL":
        nets = ["⚡ Россети ЮГ", "⚡ Россети Кубань"]
    elif f == "UG":
        nets = ["⚡ Россети ЮГ"]
    else:
        nets = ["⚡ Россети Кубань"]
    buttons = [[n] for n in nets] + [["📞 Телефоны провайдеров"]]
    if res_flag.strip().upper() == "ALL":
        buttons += [["📝 Сформировать отчёт"]]
    buttons += [["ℹ️ Справка"]]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def build_report_kb(vis_flag: str) -> ReplyKeyboardMarkup:
    f = vis_flag.strip().upper()
    rows = []
    if f in ("ALL", "UG"):
        rows.append(["📊 Уведомления о бездоговорных ВОЛС ЮГ"])
    if f in ("ALL", "RK"):
        rows.append(["📊 Уведомления о бездоговорных ВОЛС Кубань"])
    rows += [["📋 Выгрузить информацию по контрагентам"], ["ℹ️ Справка"], ["🔙 Назад"]]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

# --- /start ---
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    vis_map, branch_map, res_map, names, resp_map = load_zones()
    if uid not in branch_map:
        return await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)

    raw_branch = branch_map[uid]
    branch_key = "All" if raw_branch == "All" else BRANCH_KEY_MAP.get(raw_branch, raw_branch)

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

# --- TEXT handler ---
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = context.user_data

    # 1) Справка: список файлов
    if text == "ℹ️ Справка" and data.get("step") != "HELP_LIST":
        data["prev_step"] = data.get("step", "INIT")
        try:
            files = sorted(os.listdir(HELP_FOLDER))
        except:
            return await update.message.reply_text("❌ Ошибка доступа к справке.", reply_markup=kb_back)
        data["help_files"] = files
        data["step"] = "HELP_LIST"
        kb = ReplyKeyboardMarkup([[f] for f in files] + [["🔙 Назад"]], resize_keyboard=True)
        return await update.message.reply_text("Выберите файл справки:", reply_markup=kb)

    # 2) Меню справки: выдаем файл
    if data.get("step") == "HELP_LIST":
        if text == "🔙 Назад":
            data["step"] = data.get("prev_step", "INIT")
            return await restore_menu(update, context)
        if text in data.get("help_files", []):
            path = os.path.join(HELP_FOLDER, text)
            if text.lower().endswith((".jpg", ".jpeg", ".png", ".gif")):
                await update.message.reply_photo(open(path, "rb"))
            else:
                await update.message.reply_document(open(path, "rb"))
            data["step"] = data.get("prev_step", "INIT")
            return await restore_menu(update, context)

    # Далее — весь ваш прежний код, с точными шагами INIT, REPORT_MENU, NET, BRANCH, AWAIT_TP_INPUT, DISAMB, NOTIFY_…
    # Я привожу его здесь целиком:

    vis_map, branch_map, res_map, names, resp_map = load_zones()
    step        = data.get("step", "INIT")
    vis_flag    = data["vis_flag"]
    res_user    = data["res_user"]
    branch_user = data["branch_user"]
    name        = data["name"]

    # «Назад» во всех режимах
    if text == "🔙 Назад":
        if step in ("AWAIT_TP_INPUT","DISAMB","NOTIFY_AWAIT_TP","NOTIFY_DISAMB","NOTIFY_VL"):
            data["step"] = "BRANCH"
            return await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)
        if step == "REPORT_MENU":
            data["step"] = "INIT"
            return await update.message.reply_text(
                f"👋 Приветствую Вас, {name}! Выберите опцию:",
                reply_markup=build_initial_kb(vis_flag, res_user)
            )
        data["step"] = "INIT"
        return await update.message.reply_text(
            "Выберите опцию:", reply_markup=build_initial_kb(vis_flag, res_user)
        )

    # INIT
    if step == "INIT":
        if text == "📞 Телефоны провайдеров":
            data["step"] = "VIEW_PHONES"
            return await update.message.reply_text("📞 Телефоны провайдеров:\n…", reply_markup=kb_back)
        if text == "📝 Сформировать отчёт":
            data["step"] = "REPORT_MENU"
            return await update.message.reply_text("📝 Выберите тип отчёта:", reply_markup=build_report_kb(vis_flag))
        allowed = (["⚡ Россети ЮГ","⚡ Россети Кубань"] if vis_flag=="ALL"
                   else ["⚡ Россети ЮГ"] if vis_flag=="UG"
                   else ["⚡ Россети Кубань"])
        if text not in allowed:
            return await update.message.reply_text(
                f"{name}, доступны: {', '.join(allowed)}",
                reply_markup=build_initial_kb(vis_flag, res_user)
            )
        selected_net = text.replace("⚡ ","")
        data.update({"step":"NET","net":selected_net})
        if branch_user != "All":
            branches = [branch_user]
        else:
            branches = list(BRANCH_URLS[selected_net].keys())
        kb = ReplyKeyboardMarkup([[b] for b in branches] + [["🔙 Назад"]], resize_keyboard=True)
        return await update.message.reply_text("Выберите филиал:", reply_markup=kb)

    # REPORT_MENU
    if step == "REPORT_MENU":
        if text == "📊 Уведомления о бездоговорных ВОЛС ЮГ":
            df = pd.read_csv(NOTIFY_LOG_FILE_UG)
            bio = BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as w:
                df.to_excel(w, index=False, sheet_name="UG")
            bio.seek(0)
            await update.message.reply_document(bio, filename="log_ug.xlsx")
        elif text == "📊 Уведомления о бездоговорных ВОЛС Кубань":
            df = pd.read_csv(NOTIFY_LOG_FILE_RK)
            bio = BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as w:
                df.to_excel(w, index=False, sheet_name="RK")
            bio.seek(0)
            await update.message.reply_document(bio, filename="log_rk.xlsx")
        elif text == "📋 Выгрузить информацию по контрагентам":
            return await update.message.reply_text(
                "📋 Справочник контрагентов — скоро будет!",
                reply_markup=build_report_kb(vis_flag)
            )
        return await update.message.reply_text("📝 Выберите тип отчёта:", reply_markup=build_report_kb(vis_flag))

    # NET → филиал
    if step == "NET":
        selected_net = data["net"]
        if branch_user!="All" and text!=branch_user:
            return await update.message.reply_text(
                f"{name}, доступен только филиал «{branch_user}».",
                reply_markup=kb_back
            )
        if text not in BRANCH_URLS[selected_net]:
            return await update.message.reply_text(
                f"⚠ Филиал «{text}» не найден.",
                reply_markup=kb_back
            )
        data.update({"step":"BRANCH","branch":text})
        return await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)

    # BRANCH → действие
    if step == "BRANCH":
        if text == "🔍 Поиск по ТП":
            data["step"] = "AWAIT_TP_INPUT"
            return await update.message.reply_text("Введите номер ТП:", reply_markup=kb_back)
        if text == "🔔 Отправить уведомление":
            data["step"] = "NOTIFY_AWAIT_TP"
            return await update.message.reply_text("Введите номер ТП для уведомления:", reply_markup=kb_back)

    # AWAIT_TP_INPUT
    if step == "AWAIT_TP_INPUT":
        net    = data["net"]
        branch = data["branch"]
        url    = BRANCH_URLS[net].get(branch,"").strip()
        if not url:
            data["step"] = "BRANCH"
            return await update.message.reply_text(f"⚠️ URL для «{branch}» не настроен.", reply_markup=kb_back)
        try:
            df = pd.read_csv(normalize_sheet_url(url))
        except Exception as e:
            data["step"] = "BRANCH"
            return await update.message.reply_text(f"❌ Ошибка загрузки: {e}", reply_markup=kb_back)

        if res_user!="ALL":
            df = df[df["РЭС"].str.upper()==res_user.upper()]
        df["D_UP"] = df["Наименование ТП"].str.upper().str.replace(r"\W","",regex=True)
        q = re.sub(r"\W","", text.upper())
        found = df[df["D_UP"].str.contains(q, na=False)]
        if found.empty:
            data["step"] = "BRANCH"
            return await update.message.reply_text(
                f"⚠ В {res_user} РЭС отсутствуют ТП удовлетворяющие параметры поиска.",
                reply_markup=kb_back
            )

        ulist = found["Наименование ТП"].unique().tolist()
        if len(ulist)>1:
            data.update({"step":"DISAMB","amb_df":found})
            kb = ReplyKeyboardMarkup([[tp] for tp in ulist] + [["🔙 Назад"]], resize_keyboard=True)
            return await update.message.reply_text("Выберите ТП:", reply_markup=kb)

        tp = ulist[0]
        det=found[found["Наименование ТП"]==tp]
        resname=det.iloc[0]["РЭС"]
        await update.message.reply_text(f"{resname}, {tp} ({len(det)}) ВОЛС с договором аренды:", reply_markup=kb_actions)
        for _,r in det.iterrows():
            await update.message.reply_text(
                f"📍 ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
                f"Опоры: {r['Опоры']}\n"
                f"Провайдер: {r.get('Наименование Провайдера','')}",
                reply_markup=kb_actions
            )
        data["step"]="BRANCH"
        return

    # DISAMB
    if step == "DISAMB":
        found = data["amb_df"]
        if text == "🔙 Назад":
            data["step"]="AWAIT_TP_INPUT"
            return await update.message.reply_text("Введите номер ТП:", reply_markup=kb_back)
        if text in found["Наименование ТП"].unique():
            det=found[found["Наименование ТП"]==text]
            resname=det.iloc[0]["РЭС"]
            await update.message.reply_text(f"{resname}, {text} ({len(det)}) ВОЛС с договором аренды:", reply_markup=kb_actions)
            for _,r in det.iterrows():
                await update.message.reply_text(
                    f"📍 ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
                    f"Опоры: {r['Опоры']}\n"
                    f"Провайдер: {r.get('Наименование Провайдера','')}",
                    reply_markup=kb_actions
                )
            data["step"]="BRANCH"
            return

    # NOTIFY_AWAIT_TP
    if step == "NOTIFY_AWAIT_TP":
        net    = data["net"]
        branch = data["branch"]
        url    = NOTIFY_URLS[net].get(branch,"").strip()
        if not url:
            data["step"]="BRANCH"
            return await update.message.reply_text(f"⚠️ URL уведомлений для «{branch}» не настроен.", reply_markup=kb_back)
        try:
            df = pd.read_csv(normalize_sheet_url(url))
        except Exception as e:
            data["step"]="BRANCH"
            return await update.message.reply_text(f"❌ Ошибка загрузки уведомлений: {e}", reply_markup=kb_back)
        df["D_UP"] = df["Наименование ТП"].str.upper().str.replace(r"\W","",regex=True)
        q = re.sub(r"\W","", text.upper())
        found = df[df["D_UP"].str.contains(q, na=False)]
        if found.empty:
            data["step"]="BRANCH"
            return await update.message.reply_text("🔔 ТП не найдено в справочнике.", reply_markup=kb_back)
        ulist = found["Наименование ТП"].unique().tolist()
        if len(ulist)>1:
            data.update({"step":"NOTIFY_DISAMB","amb_df_notify":found})
            kb = ReplyKeyboardMarkup([[tp] for tp in ulist] + [["🔙 Назад"]], resize_keyboard=True)
            return await update.message.reply_text("Выберите ТП для уведомления:", reply_markup=kb)
        tp = ulist[0]
        data["tp"] = tp
        subset = found[found["Наименование ТП"]==tp]
        data["vl_df"]      = subset
        data["notify_res"] = subset.iloc[0]["РЭС"]
        data["step"]       = "NOTIFY_VL"
        vls = subset["Наименование ВЛ"].unique().tolist()
        kb  = ReplyKeyboardMarkup([[vl] for vl in vls] + [["🔙 Назад"]], resize_keyboard=True)
        return await update.message.reply_text("Выберите ВЛ для уведомления:", reply_markup=kb)

    # NOTIFY_DISAMB
    if step == "NOTIFY_DISAMB":
        found = data["amb_df_notify"]
        if text == "🔙 Назад":
            data["step"] = "NOTIFY_AWAIT_TP"
            return await update.message.reply_text("Введите номер ТП для уведомления:", reply_markup=kb_back)
        if text in found["Наименование ТП"].unique():
            data["tp"] = text
            subset = found[found["Наименование ТП"]==text]
            data["vl_df"]      = subset
            data["notify_res"] = subset.iloc[0]["РЭС"]
            data["step"]       = "NOTIFY_VL"
            vls = subset["Наименование ВЛ"].unique().tolist()
            kb  = ReplyKeyboardMarkup([[vl] for vl in vls] + [["🔙 Назад"]], resize_keyboard=True)
            return await update.message.reply_text("Выберите ВЛ для уведомления:", reply_markup=kb)

    # NOTIFY_VL
    if step == "NOTIFY_VL":
        subset = data["vl_df"]
        if text == "🔙 Назад":
            data["step"] = "NOTIFY_AWAIT_TP"
            return await update.message.reply_text("Введите номер ТП для уведомления:", reply_markup=kb_back)
        if text in subset["Наименование ВЛ"].unique():
            data["vl"]   = text
            data["step"] = "NOTIFY_GEO"
            return await update.message.reply_text("Пожалуйста, отправьте геолокацию:", reply_markup=kb_request_location)

    # если ничего не подошло, сброс в старт
    return await start_cmd(update, context)

# --- Обработчик геолокации для уведомлений ---
async def location_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("step") != "NOTIFY_GEO":
        return
    loc       = update.message.location
    tp        = context.user_data["tp"]
    vl        = context.user_data["vl"]
    res_tp    = context.user_data["notify_res"]
    sender    = context.user_data["name"]
    _,_,_,names,resp_map = load_zones()

    # принимающие — у кого в колонке F совпадает с res_tp
    recipients = [
        uid for uid, r in resp_map.items()
        if r.strip().lower() == res_tp.strip().lower()
    ]

    msg   = f"🔔 Уведомление от {sender}, {res_tp} РЭС, {tp}, {vl} – Найден бездоговорной ВОЛС"
    log_f = NOTIFY_LOG_FILE_UG if context.user_data["net"]=="Россети ЮГ" else NOTIFY_LOG_FILE_RK

    for cid in recipients:
        await context.bot.send_message(cid, msg)
        await context.bot.send_location(cid, loc.latitude, loc.longitude)
        await context.bot.send_message(
            cid,
            f"📍 Широта: {loc.latitude:.6f}, Долгота: {loc.longitude:.6f}"
        )
        with open(log_f, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                context.user_data["branch_user"],
                res_tp,
                update.effective_user.id,
                sender,
                cid,
                names.get(cid, ""),
                datetime.now(timezone.utc).isoformat(),
                f"{loc.latitude:.6f},{loc.longitude:.6f}"
            ])

    if recipients:
        names_list = [names[c] for c in recipients]
        await update.message.reply_text(
            f"✅ Уведомление отправлено: {', '.join(names_list)}",
            reply_markup=kb_actions
        )
    else:
        await update.message.reply_text(
            f"⚠ Ответственный на {res_tp} РЭС не назначен.",
            reply_markup=kb_actions
        )

    context.user_data["step"] = "BRANCH"

# --- Вспомогательная функция для возврата в меню после справки ---
async def restore_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    step     = context.user_data.get("step", "INIT")
    v        = context.user_data["vis_flag"]
    r        = context.user_data["res_user"]
    if step == "INIT":
        return await update.message.reply_text(
            "Выберите опцию:", reply_markup=build_initial_kb(v, r)
        )
    if step == "REPORT_MENU":
        return await update.message.reply_text(
            "📝 Выберите тип отчёта:", reply_markup=build_report_kb(v)
        )
    if step == "BRANCH":
        return await update.message.reply_text(
            "Выберите действие:", reply_markup=kb_actions
        )
    return await update.message.reply_text("🔙 Назад", reply_markup=kb_back)

# --- Регистрация хендлеров ---
application.add_handler(CommandHandler("start", start_cmd))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
application.add_handler(MessageHandler(filters.LOCATION, location_handler))

# --- Запуск вебхука ---
if __name__ == "__main__":
    if SELF_URL:
        threading.Thread(
            target=lambda: requests.get(f"{SELF_URL}/webhook"),
            daemon=True
        ).start()
    application.run_webhook(
        listen="0.0.0.0", port=PORT,
        url_path="webhook", webhook_url=f"{SELF_URL}/webhook"
    )
