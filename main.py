# main.py

import os
import threading
import re
import requests
import pandas as pd
import csv
from datetime import datetime, timezone
from io import StringIO, BytesIO

from flask import Flask
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

from config import TOKEN, SELF_URL, PORT, ZONES_CSV_URL, BRANCH_URLS, NOTIFY_URLS
from zones import normalize_sheet_url, load_zones

# Flask & Bot setup
app = Flask(__name__)
application = ApplicationBuilder().token(TOKEN).build()

# Лог уведомлений
LOG_NOTIFY_FILE = os.getenv("NOTIFY_LOG_FILE", "notifications_log.csv")
if not os.path.exists(LOG_NOTIFY_FILE):
    with open(LOG_NOTIFY_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Filial", "РЭС",
            "SenderID", "SenderName",
            "RecipientID", "RecipientName",
            "Timestamp", "Coordinates"
        ])

# Сырые имена филиалов → ключи BRANCH_URLS
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

# Клавиатуры
kb_back = ReplyKeyboardMarkup([["🔙 Назад"]], resize_keyboard=True)
kb_report = ReplyKeyboardMarkup(
    [
        ["📊 Выгрузить уведомления"],
        ["📋 Выгрузить информацию по контрагентам"],
        ["🔙 Назад"],
    ],
    resize_keyboard=True
)

def build_initial_kb(vis_flag: str) -> ReplyKeyboardMarkup:
    flag = vis_flag.strip().upper()
    if flag == "ALL":
        nets = ["⚡ Россети ЮГ", "⚡ Россети Кубань"]
    elif flag == "UG":
        nets = ["⚡ Россети ЮГ"]
    else:
        nets = ["⚡ Россети Кубань"]
    buttons = [[n] for n in nets] + [
        ["📞 Телефоны провайдеров"],
        ["📝 Сформировать отчёт"],
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def build_branch_kb(uid: int, selected_net: str, branch_map: dict) -> ReplyKeyboardMarkup:
    raw = branch_map.get(uid, "All")
    if raw != "All":
        branches = [raw]
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

# /start
async def start_line(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    vis_map, raw_branch_map, res_map, names, resp_map = load_zones()

    branch_map = {
        u: ("All" if raw == "All" else BRANCH_KEY_MAP.get(raw, raw))
        for u, raw in raw_branch_map.items()
    }

    if uid not in branch_map:
        await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)
        return

    context.user_data.clear()
    context.user_data.update({
        "step":        "INIT",
        "vis_flag":    vis_map[uid],
        "branch_user": branch_map[uid],
        "res_user":    res_map[uid],
        "resp_map":    resp_map,
        "name":        names[uid],
    })

    await update.message.reply_text(
        f"👋 Приветствую Вас, {names[uid]}! Выберите опцию:",
        reply_markup=build_initial_kb(vis_map[uid])
    )

# Обработка текстовых сообщений
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # если /start не вызывали — перенаправляем
    if "step" not in context.user_data:
        return await start_line(update, context)

    text = update.message.text.strip()
    uid  = update.effective_user.id

    vis_map, raw_branch_map, res_map, names, _ = load_zones()
    branch_map = {
        u: ("All" if raw == "All" else BRANCH_KEY_MAP.get(raw, raw))
        for u, raw in raw_branch_map.items()
    }

    if uid not in branch_map:
        await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)
        return

    step        = context.user_data["step"]
    vis_flag    = context.user_data["vis_flag"]
    branch_user = context.user_data["branch_user"]
    res_user    = context.user_data["res_user"]
    resp_flag   = context.user_data["resp_map"].get(uid, "").strip()
    user_name   = context.user_data["name"]

    # Назад
    if text == "🔙 Назад":
        if step in ("AWAIT_TP_INPUT","DISAMBIGUOUS",
                    "NOTIFY_AWAIT_TP","NOTIFY_DISAMBIGUOUS",
                    "NOTIFY_AWAIT_VL","NOTIFY_WAIT_GEO"):
            context.user_data["step"] = "BRANCH_SELECTED"
            await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)
            return
        if step == "NETWORK_SELECTED":
            context.user_data["step"] = "INIT"
            await update.message.reply_text("Выберите опцию:", reply_markup=build_initial_kb(vis_flag))
            return
        if step == "REPORT_MENU":
            context.user_data["step"] = "INIT"
            await update.message.reply_text(
                f"👋 Приветствую Вас, {user_name}! Выберите опцию:",
                reply_markup=build_initial_kb(vis_flag)
            )
            return
        context.user_data["step"] = "INIT"
        await update.message.reply_text("Выберите опцию:", reply_markup=build_initial_kb(vis_flag))
        return

    # INIT
    if step == "INIT":
        if text == "📞 Телефоны провайдеров":
            context.user_data["step"] = "VIEW_PHONES"
            await update.message.reply_text("📞 Телефоны провайдеров:\n…", reply_markup=kb_back)
            return

        if text == "📝 Сформировать отчёт":
            context.user_data["step"] = "REPORT_MENU"
            await update.message.reply_text("📝 Выберите тип отчёта:", reply_markup=kb_report)
            return

        flag = vis_flag.strip().upper()
        allowed = (["⚡ Россети ЮГ","⚡ Россети Кубань"] if flag=="ALL"
                   else ["⚡ Россети ЮГ"] if flag=="UG"
                   else ["⚡ Россети Кубань"])
        if text in allowed:
            selected_net = text.replace("⚡ ","")
            context.user_data.update({
                "step":         "NETWORK_SELECTED",
                "selected_net": selected_net
            })
            await update.message.reply_text(
                "Выберите филиал:",
                reply_markup=build_branch_kb(uid, selected_net, branch_map)
            )
        else:
            await update.message.reply_text(
                f"{user_name}, можете просматривать только: {', '.join(allowed)} 😕",
                reply_markup=build_initial_kb(vis_flag)
            )
        return

    # REPORT_MENU
    if step == "REPORT_MENU":
        if text == "📊 Выгрузить уведомления":
            df = pd.read_csv(LOG_NOTIFY_FILE)
            bio = BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Notifications")
                wb  = writer.book
                ws  = writer.sheets["Notifications"]
                from openpyxl.styles import PatternFill
                from openpyxl.utils import get_column_letter
                # pink header
                pink = PatternFill(start_color="FFF2DCDB", end_color="FFF2DCDB", fill_type="solid")
                for cell in ws[1]:
                    cell.fill = pink
                # auto-width
                for col in ws.columns:
                    max_length = max(len(str(c.value)) for c in col if c.value)
                    ws.column_dimensions[get_column_letter(col[0].column)].width = max_length + 2
            bio.seek(0)
            await update.message.reply_document(
                document=bio,
                filename="notifications_log.xlsx",
                caption="📊 Лог уведомлений (Excel)"
            )
            await update.message.reply_text("📝 Выберите тип отчёта:", reply_markup=kb_report)
            return

        if text == "📋 Выгрузить информацию по контрагентам":
            await update.message.reply_text("📋 Справочник контрагентов — скоро будет!", reply_markup=kb_report)
            return

        return

    # NETWORK_SELECTED
    if step == "NETWORK_SELECTED":
        selected_net = context.user_data["selected_net"]
        allowed = [branch_user] if branch_user!="All" else list(BRANCH_URLS[selected_net].keys())
        if text in allowed:
            context.user_data.update({
                "step":           "BRANCH_SELECTED",
                "current_branch": text
            })
            await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)
        else:
            await update.message.reply_text(
                f"{user_name}, можете просматривать только: {', '.join(allowed)} 😕",
                reply_markup=build_branch_kb(uid, selected_net, branch_map)
            )
        return

    # BRANCH_SELECTED
    if step == "BRANCH_SELECTED":
        if text == "🔍 Поиск по ТП":
            context.user_data["step"] = "AWAIT_TP_INPUT"
            await update.message.reply_text("Введите номер ТП:", reply_markup=kb_back)
            return
        if text == "🔔 Отправить уведомление":
            if resp_flag:
                await update.message.reply_text(f"{user_name}, не можете отправлять уведомления 😕", reply_markup=kb_actions)
                return
            context.user_data["step"] = "NOTIFY_AWAIT_TP"
            await update.message.reply_text("Введите номер ТП для уведомления:", reply_markup=kb_back)
            return
        return

    # AWAIT_TP_INPUT (с отладкой)
    if step == "AWAIT_TP_INPUT":
        selected_net   = context.user_data.get("selected_net")
        current_branch = context.user_data.get("current_branch")
        await update.message.reply_text(f"🔍 DEBUG: Step={step}, net={selected_net}, branch={current_branch}")
        url = BRANCH_URLS[selected_net][current_branch]
        await update.message.reply_text(f"🔍 DEBUG: Loading URL {url}")
        df = pd.read_csv(normalize_sheet_url(url))
        if res_user!="All":
            norm=lambda s:re.sub(r'\W','',str(s).upper())
            df = df[df["РЭС"].apply(norm)==norm(res_user)]
        df.columns=df.columns.str.strip()
        df["D_UP"]=df["Наименование ТП"].str.upper().str.replace(r'\W','',regex=True)

        q=re.sub(r'\W','',text.upper())
        found=df[df["D_UP"].str.contains(q,na=False)]
        if found.empty:
            await update.message.reply_text("🔍 Нет данных. Повторите:", reply_markup=kb_back)
            return

        # далее как обычно…
        unique_tp=found["Наименование ТП"].unique().tolist()
        if len(unique_tp)>1:
            context.user_data.update({"step":"DISAMBIGUOUS","ambiguous_df":found})
            kb=ReplyKeyboardMarkup([[tp] for tp in unique_tp]+[["🔙 Назад"]],resize_keyboard=True)
            await update.message.reply_text("Выберите ТП:",reply_markup=kb)
            return

        tp_sel=unique_tp[0]
        details=found[found["Наименование ТП"]==tp_sel]
        cnt=len(details); rname=details.iloc[0]["РЭС"]
        await update.message.reply_text(
            f"{rname}, {tp_sel} ({cnt}) ВОЛС с договором аренды.\nВыберите действие:",
            reply_markup=kb_actions
        )
        for _,r in details.iterrows():
            await update.message.reply_text(
                f"📍 ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
                f"Опоры: {r['Опоры']}\n"
                f"Провайдер: {r.get('Наименование Провайдера','')}",
                reply_markup=kb_actions
            )
        context.user_data["step"]="BRANCH_SELECTED"
        return

    # NOTIFY_AWAIT_TP (с отладкой)
    if step == "NOTIFY_AWAIT_TP":
        current_branch = context.user_data.get("current_branch")
        await update.message.reply_text(f"🔔 DEBUG: Notify step={step}, branch={current_branch}")
        sheet_url = NOTIFY_URLS.get(current_branch, "")
        await update.message.reply_text(f"🔔 DEBUG: Notify URL {sheet_url}")
        dfn = pd.read_csv(normalize_sheet_url(sheet_url))
        dfn.columns = dfn.columns.str.strip()
        dfn["D_UP_TP"] = dfn["Наименование ТП"].str.upper().str.replace(r"\W","",regex=True)
        # далее как обычно…
        q = re.sub(r"\W","",text.upper())
        found = dfn[dfn["D_UP_TP"].str.contains(q,na=False)]
        if found.empty:
            await update.message.reply_text("🔔 Не найдено ТП. Повторите:",reply_markup=kb_back)
            return
        ulist = found["Наименование ТП"].unique().tolist()
        if len(ulist)>1:
            context.user_data.update({"step":"NOTIFY_DISAMBIGUOUS","notify_df":found})
            kb = ReplyKeyboardMarkup([[tp] for tp in ulist]+[["🔙 Назад"]],resize_keyboard=True)
            await update.message.reply_text("Выберите ТП для уведомления:",reply_markup=kb)
            return
        tp_sel = ulist[0]
        context.user_data["notify_tp"] = tp_sel
        df_tp = found[found["Наименование ТП"]==tp_sel]
        context.user_data["notify_df"] = df_tp
        vls = df_tp["Наименование ВЛ"].unique().tolist()
        context.user_data["step"] = "NOTIFY_AWAIT_VL"
        kb = ReplyKeyboardMarkup([[vl] for vl in vls]+[["🔙 Назад"]],resize_keyboard=True)
        await update.message.reply_text("Выберите ВЛ для уведомления:",reply_markup=kb)
        return

    # … остальные шаги уведомлений и geo handler …

# Геолокация
async def location_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("step") != "NOTIFY_WAIT_GEO":
        return
    # реализация как раньше…

# Регистрируем хендлеры
application.add_handler(CommandHandler("start", start_line))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
application.add_handler(MessageHandler(filters.LOCATION, location_handler))

# Запуск webhook
if __name__ == "__main__":
    threading.Thread(target=lambda: requests.get(f"{SELF_URL}/webhook"), daemon=True).start()
    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=f"{SELF_URL}/webhook"
    )
