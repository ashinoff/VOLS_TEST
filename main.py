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
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from config import (
    TOKEN, SELF_URL, PORT,
    ZONES_CSV_URL, BRANCH_URLS, NOTIFY_URLS,
    NOTIFY_LOG_FILE_UG, NOTIFY_LOG_FILE_RK
)
from zones import normalize_sheet_url, load_zones

# === Flask & Bot ===
app = Flask(__name__)
application = ApplicationBuilder().token(TOKEN).build()

# === Инициализация логов уведомлений ===
for lf in (NOTIFY_LOG_FILE_UG, NOTIFY_LOG_FILE_RK):
    if not os.path.exists(lf):
        with open(lf, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "Filial","РЭС",
                "SenderID","SenderName",
                "RecipientID","RecipientName",
                "Timestamp","Coordinates"
            ])

# === Клавиатуры ===
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
    f = vis_flag.strip().upper()
    if f == "ALL":
        nets = ["⚡ Россети ЮГ", "⚡ Россети Кубань"]
    elif f == "UG":
        nets = ["⚡ Россети ЮГ"]
    else:
        nets = ["⚡ Россети Кубань"]
    rows = [[n] for n in nets] + [["📞 Телефоны провайдеров"], ["📝 Сформировать отчёт"]]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def build_report_kb(vis_flag: str) -> ReplyKeyboardMarkup:
    f = vis_flag.strip().upper()
    rows = []
    if f in ("ALL", "UG"):
        rows.append(["📊 Логи Россети ЮГ"])
    if f in ("ALL", "RK"):
        rows.append(["📊 Логи Россети Кубань"])
    rows += [["📋 Выгрузить информацию по контрагентам"], ["🔙 Назад"]]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

# === /start ===
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    vis_map, raw_branch_map, res_map, names, resp_map = load_zones()
    if uid not in raw_branch_map:
        return await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)

    context.user_data.clear()
    context.user_data.update({
        "step":        "INIT",
        "vis_flag":    vis_map[uid],
        "branch_user": raw_branch_map[uid],  # либо "All", либо конкретный "Тимашевские ЭС" и т.п.
        "res_user":    res_map[uid],
        "name":        names[uid],
        "resp_map":    resp_map
    })
    await update.message.reply_text(
        f"👋 Приветствую Вас, {names[uid]}! Выберите опцию:",
        reply_markup=build_initial_kb(vis_map[uid])
    )

# === TEXT handler ===
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    uid  = update.effective_user.id

    # Если /start не было — отправляем туда
    if "step" not in context.user_data:
        return await start_cmd(update, context)

    # Перезагрузим зоны (на случай, если изменили CSV)
    vis_map, raw_branch_map, res_map, names, resp_map = load_zones()
    if uid not in raw_branch_map:
        return await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)

    step        = context.user_data["step"]
    vis_flag    = context.user_data["vis_flag"]
    branch_user = context.user_data["branch_user"]
    res_user    = context.user_data["res_user"]
    name        = context.user_data["name"]

    # ---- BACK ----
    if text == "🔙 Назад":
        if step in ("AWAIT_TP_INPUT","DISAMB","NOTIFY_AWAIT_TP","NOTIFY_DISAMB","NOTIFY_VL"):
            # возвращаем на уровень «BRANCH»
            context.user_data["step"] = "BRANCH"
            return await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)
        if step == "INIT":
            return
        if step == "REPORT_MENU":
            context.user_data["step"] = "INIT"
            return await update.message.reply_text(
                f"👋 Приветствую Вас, {name}! Выберите опцию:",
                reply_markup=build_initial_kb(vis_flag)
            )
        context.user_data["step"] = "INIT"
        return await update.message.reply_text("Выберите опцию:", reply_markup=build_initial_kb(vis_flag))

    # ---- INIT ----
    if step == "INIT":
        if text == "📞 Телефоны провайдеров":
            context.user_data["step"] = "VIEW_PHONES"
            return await update.message.reply_text("📞 Телефоны провайдеров:\n…", reply_markup=kb_back)
        if text == "📝 Сформировать отчёт":
            context.user_data["step"] = "REPORT_MENU"
            return await update.message.reply_text(
                "📝 Выберите тип отчёта:", reply_markup=build_report_kb(vis_flag)
            )
        # выбор сети
        allowed = (["⚡ Россети ЮГ","⚡ Россети Кубань"] if vis_flag=="All"
                   else ["⚡ Россети ЮГ"] if vis_flag=="UG"
                   else ["⚡ Россети Кубань"])
        if text not in allowed:
            return await update.message.reply_text(
                f"{name}, доступны: {', '.join(allowed)}", reply_markup=build_initial_kb(vis_flag)
            )
        selected_net = text.replace("⚡ ","")
        context.user_data.update({"step":"NET","net":selected_net})
        # список филиалов, но ограниченный если нужно
        if branch_user != "All":
            branches = [branch_user]
        else:
            branches = list(BRANCH_URLS[selected_net].keys())
        kb = ReplyKeyboardMarkup([[b] for b in branches]+[["🔙 Назад"]], resize_keyboard=True)
        return await update.message.reply_text("Выберите филиал:", reply_markup=kb)

    # ---- REPORT_MENU ----
    if step == "REPORT_MENU":
        if text == "📊 Логи Россети ЮГ":
            df = pd.read_csv(NOTIFY_LOG_FILE_UG)
            bio = BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="UG")
            bio.seek(0)
            await update.message.reply_document(bio, filename="log_ug.xlsx")
            return await update.message.reply_text("📝 Выберите тип отчёта:", reply_markup=build_report_kb(vis_flag))
        if text == "📊 Логи Россети Кубань":
            df = pd.read_csv(NOTIFY_LOG_FILE_RK)
            bio = BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="RK")
            bio.seek(0)
            await update.message.reply_document(bio, filename="log_rk.xlsx")
            return await update.message.reply_text("📝 Выберите тип отчёта:", reply_markup=build_report_kb(vis_flag))
        if text == "📋 Выгрузить информацию по контрагентам":
            return await update.message.reply_text("📋 Контрагенты…", reply_markup=build_report_kb(vis_flag))
        return

    # ---- NET: выбор филиала ----
    if step == "NET":
        selected_net = context.user_data["net"]
        # снова проверим доступ
        if branch_user!="All" and text!=branch_user:
            return await update.message.reply_text(
                f"{name}, можете работать только с {branch_user}", reply_markup=kb_back
            )
        if text not in BRANCH_URLS[selected_net]:
            return await update.message.reply_text(
                f"⚠ Филиал «{text}» не найден в конфиге.", reply_markup=kb_back
            )
        context.user_data.update({"step":"BRANCH","branch":text})
        return await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)

    # ---- BRANCH: поиск / уведомление ----
    if step == "BRANCH":
        if text == "🔍 Поиск по ТП":
            context.user_data["step"] = "AWAIT_TP_INPUT"
            return await update.message.reply_text("Введите номер ТП:", reply_markup=kb_back)
        if text == "🔔 Отправить уведомление":
            context.user_data["step"] = "NOTIFY_AWAIT_TP"
            return await update.message.reply_text("Введите номер ТП для уведомления:", reply_markup=kb_back)
        return

    # ---- AWAIT_TP_INPUT: классический поиск ----
    if step == "AWAIT_TP_INPUT":
        net    = context.user_data["net"]
        branch = context.user_data["branch"].strip()
        url    = BRANCH_URLS[net].get(branch, "").strip()
        if not url:
            return await update.message.reply_text(
                f"⚠️ URL для филиала «{branch}» не настроен.", reply_markup=kb_back
            )
        try:
            df = pd.read_csv(normalize_sheet_url(url))
        except Exception as e:
            return await update.message.reply_text(f"❌ Ошибка загрузки данных: {e}", reply_markup=kb_back)

        if res_user != "All":
            df = df[df["РЭС"].str.upper() == res_user.upper()]

        df["D_UP"] = df["Наименование ТП"].str.upper().str.replace(r"\W","",regex=True)
        q = re.sub(r"\W","", text.upper())
        found = df[df["D_UP"].str.contains(q, na=False)]
        if found.empty:
            return await update.message.reply_text("🔍 Ничего не найдено.", reply_markup=kb_back)

        ulist = found["Наименование ТП"].unique().tolist()
        if len(ulist) > 1:
            context.user_data.update({"step":"DISAMB","amb_df":found})
            kb = ReplyKeyboardMarkup([[tp] for tp in ulist]+[["🔙 Назад"]], resize_keyboard=True)
            return await update.message.reply_text("Выберите ТП:", reply_markup=kb)

        # единственное совпадение
        tp = ulist[0]
        det = found[found["Наименование ТП"]==tp]
        resname = det.iloc[0]["РЭС"]
        await update.message.reply_text(
            f"{resname}, {tp} ({len(det)}) ВОЛС с договором аренды:",
            reply_markup=kb_actions
        )
        for _, r in det.iterrows():
            await update.message.reply_text(
                f"📍 ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
                f"Опоры: {r['Опоры']}\n"
                f"Провайдер: {r.get('Наименование Провайдера','')}",
                reply_markup=kb_actions
            )
        context.user_data["step"] = "BRANCH"
        return

    # ---- DISAMB: уточнение ТП ----
    if step == "DISAMB":
        if text == "🔙 Назад":
            context.user_data["step"] = "AWAIT_TP_INPUT"
            return await update.message.reply_text("Введите номер ТП:", reply_markup=kb_back)
        found = context.user_data["amb_df"]
        if text not in found["Наименование ТП"].unique():
            return
        det = found[found["Наименование ТП"]==text]
        resname = det.iloc[0]["РЭС"]
        await update.message.reply_text(
            f"{resname}, {text} ({len(det)}) ВОЛС с договором аренды:",
            reply_markup=kb_actions
        )
        for _, r in det.iterrows():
            await update.message.reply_text(
                f"📍 ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
                f"Опоры: {r['Опоры']}\n"
                f"Провайдер: {r.get('Наименование Провайдера','')}",
                reply_markup=kb_actions
            )
        context.user_data["step"] = "BRANCH"
        return

    # ---- NOTIFY_AWAIT_TP ----
    if step == "NOTIFY_AWAIT_TP":
        net    = context.user_data["net"]
        branch = context.user_data["branch"].strip()
        url    = NOTIFY_URLS.get(net, {}).get(branch, "").strip()
        if not url:
            return await update.message.reply_text(
                f"⚠️ URL-справочник для «{branch}» не настроен.", reply_markup=kb_back
            )
        try:
            dfn = pd.read_csv(normalize_sheet_url(url))
        except Exception as e:
            return await update.message.reply_text(f"❌ Ошибка загрузки справочника: {e}", reply_markup=kb_back)

        dfn["D_UP"] = dfn["Наименование ТП"].str.upper().str.replace(r"\W","",regex=True)
        q = re.sub(r"\W","", text.upper())
        found = dfn[dfn["D_UP"].str.contains(q, na=False)]
        if found.empty:
            return await update.message.reply_text("🔔 ТП не найдено.", reply_markup=kb_back)

        ulist = found["Наименование ТП"].unique().tolist()
        if len(ulist) > 1:
            context.user_data.update({"step":"NOTIFY_DISAMB","dfn":found})
            kb = ReplyKeyboardMarkup([[tp] for tp in ulist]+[["🔙 Назад"]], resize_keyboard=True)
            return await update.message.reply_text("Выберите ТП для уведомления:", reply_markup=kb)

        tp = ulist[0]
        df0 = found[found["Наименование ТП"]==tp]
        context.user_data.update({"step":"NOTIFY_VL","df0":df0,"tp":tp})
        vls = df0["Наименование ВЛ"].unique().tolist()
        kb = ReplyKeyboardMarkup([[vl] for vl in vls]+[["🔙 Назад"]], resize_keyboard=True)
        return await update.message.reply_text("Выберите ВЛ:", reply_markup=kb)

    # ---- NOTIFY_DISAMB ----
    if step == "NOTIFY_DISAMB":
        if text == "🔙 Назад":
            context.user_data["step"] = "NOTIFY_AWAIT_TP"
            return await update.message.reply_text("Введите номер ТП для уведомления:", reply_markup=kb_back)
        dfn = context.user_data["dfn"]
        if text not in dfn["Наименование ТП"].unique():
            return
        df0 = dfn[dfn["Наименование ТП"]==text]
        context.user_data.update({"step":"NOTIFY_VL","df0":df0,"tp":text})
        vls = df0["Наименование ВЛ"].unique().tolist()
        kb = ReplyKeyboardMarkup([[vl] for vl in vls]+[["🔙 Назад"]], resize_keyboard=True)
        return await update.message.reply_text("Выберите ВЛ:", reply_markup=kb)

    # ---- NOTIFY_VL ----
    if step == "NOTIFY_VL":
        if text == "🔙 Назад":
            context.user_data["step"] = "NOTIFY_AWAIT_TP"
            return await update.message.reply_text("Введите номер ТП для уведомления:", reply_markup=kb_back)
        df0 = context.user_data["df0"]
        if text not in df0["Наименование ВЛ"].unique():
            return await update.message.reply_text("Выберите ВЛ из списка:", reply_markup=kb_back)
        context.user_data["vl"]   = text
        context.user_data["step"] = "NOTIFY_GEO"
        return await update.message.reply_text("📍 Отправьте геолокацию:", reply_markup=kb_request_location)

# === LOCATION handler ===
async def location_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("step") != "NOTIFY_GEO":
        return
    loc     = update.message.location
    tp      = context.user_data["tp"]
    vl      = context.user_data["vl"]
    resname = context.user_data["res_user"]
    sender  = context.user_data["name"]
    _,_,_,names,resp_map = load_zones()
    recips  = [uid for uid,r in resp_map.items() if r==resname]
    msg     = f"🔔 Уведомление от {sender}, {resname} РЭС, {tp}, {vl} – Найден бездоговорной ВОЛС"

    fpath = NOTIFY_LOG_FILE_UG if context.user_data["net"]=="Россети ЮГ" else NOTIFY_LOG_FILE_RK

    for cid in recips:
        await context.bot.send_message(cid, msg)
        await context.bot.send_location(cid, loc.latitude, loc.longitude)
        await context.bot.send_message(cid, f"📍 {loc.latitude:.6f},{loc.longitude:.6f}")
        with open(fpath, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                context.user_data["branch"],
                resname,
                update.effective_user.id,
                sender,
                cid,
                names.get(cid,""),
                datetime.now(timezone.utc).isoformat(),
                f"{loc.latitude:.6f},{loc.longitude:.6f}"
            ])

    if recips:
        fio_list = [names[c] for c in recips]
        await update.message.reply_text(
            f"✅ Уведомление отправлено ответственному: {', '.join(fio_list)}",
            reply_markup=kb_actions
        )
    else:
        await update.message.reply_text(
            f"⚠ Ответственный на {resname} РЭС не назначен.",
            reply_markup=kb_actions
        )
    context.user_data["step"] = "BRANCH"

# === Регистрируем хендлеры ===
application.add_handler(CommandHandler("start", start_cmd))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
application.add_handler(MessageHandler(filters.LOCATION, location_handler))

if __name__ == "__main__":
    threading.Thread(target=lambda: requests.get(f"{SELF_URL}/webhook"), daemon=True).start()
    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=f"{SELF_URL}/webhook"
    )
