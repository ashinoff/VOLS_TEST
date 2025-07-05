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

app = Flask(__name__)
application = ApplicationBuilder().token(TOKEN).build()

# Создаем файлы-логи, если их нет
for lf in (NOTIFY_LOG_FILE_UG, NOTIFY_LOG_FILE_RK):
    if not os.path.exists(lf):
        with open(lf, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "Filial","РЭС",
                "SenderID","SenderName",
                "RecipientID","RecipientName",
                "Timestamp","Coordinates"
            ])

# Клавиатуры
kb_back = ReplyKeyboardMarkup([["🔙 Назад"]], resize_keyboard=True)

def build_initial_kb(vis_flag: str):
    f = vis_flag.upper()
    nets = (["⚡ Россети ЮГ","⚡ Россети Кубань"] if f=="ALL"
            else ["⚡ Россети ЮГ"] if f=="UG"
            else ["⚡ Россети Кубань"])
    rows = [[n] for n in nets] + [["📞 Телефоны провайдеров"],["📝 Сформировать отчёт"]]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

kb_actions = ReplyKeyboardMarkup(
    [["🔍 Поиск по ТП"],["🔔 Отправить уведомление"],["🔙 Назад"]],
    resize_keyboard=True
)

kb_request_location = ReplyKeyboardMarkup(
    [[KeyboardButton("📍 Отправить геолокацию", request_location=True)],["🔙 Назад"]],
    resize_keyboard=True
)

def build_report_kb(f: str):
    f = f.upper()
    rows = []
    if f in ("ALL","UG"): rows.append(["📊 Логи Россети ЮГ"])
    if f in ("ALL","RK"): rows.append(["📊 Логи Россети Кубань"])
    rows += [["📋 Выгрузить информацию по контрагентам"],["🔙 Назад"]]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

# /start
async def start_cmd(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id
    vis, br_raw, res_map, names, resp_map = load_zones()
    if uid not in br_raw:
        return await u.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)

    # Сохраняем в user_data
    ctx.user_data.clear()
    ctx.user_data.update({
        "step":       "INIT",
        "vis_flag":   vis[uid],
        "res_user":   res_map[uid],
        "name":       names[uid],
        "resp_map":   resp_map
    })
    await u.message.reply_text(
        f"👋 Приветствую Вас, {names[uid]}! Выберите опцию:",
        reply_markup=build_initial_kb(vis[uid])
    )

# Обработчик текстовых команд
async def handle_text(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if "step" not in ctx.user_data:
        return await start_cmd(u, ctx)

    text = u.message.text.strip()
    uid  = u.effective_user.id
    step = ctx.user_data["step"]
    vis  = ctx.user_data["vis_flag"]
    resu = ctx.user_data["res_user"]
    name = ctx.user_data["name"]

    # BACK
    if text=="🔙 Назад":
        # Возврат по уровням
        if step in ("AWAIT_TP_INPUT","DISAMBIGUOUS"):
            ctx.user_data["step"]="INIT"
            return await u.message.reply_text("Выберите опцию:",reply_markup=build_initial_kb(vis))
        if step=="INIT":
            return
        ctx.user_data["step"]="INIT"
        return await u.message.reply_text("Выберите опцию:",reply_markup=build_initial_kb(vis))

    # INIT: выбор сети / телефоны / отчёт
    if step=="INIT":
        if text=="📞 Телефоны провайдеров":
            ctx.user_data["step"]="VIEW_PHONES"
            return await u.message.reply_text("📞 Телефоны …",reply_markup=kb_back)
        if text=="📝 Сформировать отчёт":
            ctx.user_data["step"]="REPORT_MENU"
            return await u.message.reply_text("📝 Выберите отчёт:",reply_markup=build_report_kb(vis))
        # Сеть
        allowed = (["⚡ Россети ЮГ","⚡ Россети Кубань"] if vis=="ALL"
                   else ["⚡ Россети ЮГ"] if vis=="UG"
                   else ["⚡ Россети Кубань"])
        if text not in allowed:
            return await u.message.reply_text(f"{name}, доступны: {', '.join(allowed)}",reply_markup=build_initial_kb(vis))
        net = text.replace("⚡ ","")
        ctx.user_data.update({"step":"NET","net":net})
        # Показать филиалы
        rows = [[b] for b in BRANCH_URLS[net].keys()]+[["🔙 Назад"]]
        return await u.message.reply_text("Выберите филиал:",reply_markup=ReplyKeyboardMarkup(rows,resize_keyboard=True))

    # REPORT_MENU
    if step=="REPORT_MENU":
        if text=="📊 Логи Россети ЮГ":
            df = pd.read_csv(NOTIFY_LOG_FILE_UG)
            bio=BytesIO()
            with pd.ExcelWriter(bio,engine="openpyxl") as w:
                df.to_excel(w,index=False,sheet_name="UG")
            bio.seek(0)
            await ctx.bot.send_document(uid,document=bio,filename="log_ug.xlsx")
            return await u.message.reply_text("📝 Выберите отчёт:",reply_markup=build_report_kb(vis))
        if text=="📊 Логи Россети Кубань":
            df = pd.read_csv(NOTIFY_LOG_FILE_RK)
            bio=BytesIO()
            with pd.ExcelWriter(bio,engine="openpyxl") as w:
                df.to_excel(w,index=False,sheet_name="RK")
            bio.seek(0)
            await ctx.bot.send_document(uid,document=bio,filename="log_rk.xlsx")
            return await u.message.reply_text("📝 Выберите отчёт:",reply_markup=build_report_kb(vis))
        if text=="📋 Выгрузить информацию по контрагентам":
            return await u.message.reply_text("📋 Контрагенты…",reply_markup=build_report_kb(vis))

    # NET: выбор филиала
    if step=="NET":
        net = ctx.user_data["net"]
        if text not in BRANCH_URLS[net]:
            return await u.message.reply_text(f"{name}, недоступен филиал {text}",reply_markup=kb_back)
        ctx.user_data.update({"step":"BRANCH","branch":text})
        return await u.message.reply_text("Выберите действие:",reply_markup=kb_actions)

    # BRANCH: поиск / уведомление
    if step=="BRANCH":
        if text=="🔍 Поиск по ТП":
            ctx.user_data["step"]="AWAIT_TP_INPUT"
            return await u.message.reply_text("Введите номер ТП:",reply_markup=kb_back)
        if text=="🔔 Отправить уведомление":
            ctx.user_data["step"]="NOTIFY_AWAIT_TP"
            return await u.message.reply_text("Введите номер ТП для уведомления:",reply_markup=kb_back)

    # AWAIT_TP_INPUT: классический поиск
    if step=="AWAIT_TP_INPUT":
        net    = ctx.user_data["net"]
        branch = ctx.user_data["branch"]
        url    = BRANCH_URLS[net].get(branch,"")
        if not url:
            return await u.message.reply_text(f"⚠ URL для филиала {branch} не настроен.",reply_markup=kb_back)
        df = pd.read_csv(normalize_sheet_url(url))
        # фильтрация по РЭС
        if resu!="All":
            df = df[df["РЭС"].str.upper()==resu.upper()]
        df["D_UP"] = df["Наименование ТП"].str.upper().str.replace(r"\W","",regex=True)
        q = re.sub(r"\W","",text.upper())
        found = df[df["D_UP"].str.contains(q,na=False)]
        if found.empty:
            return await u.message.reply_text("🔍 Ничего не найдено.",reply_markup=kb_back)
        # неоднозначность
        ulist = found["Наименование ТП"].unique().tolist()
        if len(ulist)>1:
            ctx.user_data.update({"step":"DISAMB","amb":found})
            kb = ReplyKeyboardMarkup([[x] for x in ulist]+[["🔙 Назад"]],resize_keyboard=True)
            return await u.message.reply_text("Выберите ТП:",reply_markup=kb)
        # вывод единственного
        tp = ulist[0]
        det=found[found["Наименование ТП"]==tp]
        cnt=len(det)
        resname=det.iloc[0]["РЭС"]
        await u.message.reply_text(f"{resname}, {tp} ({cnt}) ВОЛС:",reply_markup=kb_actions)
        for _,r in det.iterrows():
            await u.message.reply_text(
                f"📍 ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
                f"Опоры: {r['Опоры']}\nПровайдер: {r.get('Наименование Провайдера','')}",
                reply_markup=kb_actions
            )
        ctx.user_data["step"]="BRANCH"
        return

    # DISAMB: уточнение ТП
    if step=="DISAMB":
        if text=="🔙 Назад":
            ctx.user_data["step"]="AWAIT_TP_INPUT"
            return await u.message.reply_text("Введите номер ТП:",reply_markup=kb_back)
        df0 = ctx.user_data["amb"]
        if text not in df0["Наименование ТП"].unique():
            return
        det=df0[df0["Наименование ТП"]==text]
        cnt=len(det)
        rn=det.iloc[0]["РЭС"]
        await u.message.reply_text(f"{rn}, {text} ({cnt}) ВОЛС:",reply_markup=kb_actions)
        for _,r in det.iterrows():
            await u.message.reply_text(
                f"📍 ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
                f"Опоры: {r['Опоры']}\nПровайдер: {r.get('Наименование Провайдера','')}",
                reply_markup=kb_actions
            )
        ctx.user_data["step"]="BRANCH"
        return

    # NOTIFY_AWAIT_TP: выбор ТП для уведомления
    if step=="NOTIFY_AWAIT_TP":
        net    = ctx.user_data["net"]
        branch = ctx.user_data["branch"]
        url    = NOTIFY_URLS[net].get(branch,"")
        if not url:
            return await u.message.reply_text(f"⚠ URL-справочник для {branch} не настроен.",reply_markup=kb_back)
        dfn = pd.read_csv(normalize_sheet_url(url))
        dfn["D_UP"] = dfn["Наименование ТП"].str.upper().str.replace(r"\W","",regex=True)
        q = re.sub(r"\W","",text.upper())
        found= dfn[dfn["D_UP"].str.contains(q,na=False)]
        if found.empty:
            return await u.message.reply_text("🔔 ТП не найдено.",reply_markup=kb_back)
        ulist=found["Наименование ТП"].unique().tolist()
        if len(ulist)>1:
            ctx.user_data.update({"step":"NOTIFY_DISAMB","dfn":found})
            kb = ReplyKeyboardMarkup([[x] for x in ulist]+[["🔙 Назад"]],resize_keyboard=True)
            return await u.message.reply_text("Выберите ТП:",reply_markup=kb)
        tp=ulist[0]; df0=found[found["Наименование ТП"]==tp]
        ctx.user_data.update({"step":"NOTIFY_VL","df0":df0,"tp":tp})
        vls=df0["Наименование ВЛ"].unique().tolist()
        kb=[[v] for v in vls]+[["🔙 Назад"]]
        return await u.message.reply_text("Выберите ВЛ:",reply_markup=ReplyKeyboardMarkup(kb,resize_keyboard=True))

    # NOTIFY_DISAMB: уточнение ТП
    if step=="NOTIFY_DISAMB":
        if text=="🔙 Назад":
            ctx.user_data["step"]="NOTIFY_AWAIT_TP"
            return await u.message.reply_text("Введите номер ТП:",reply_markup=kb_back)
        df0=ctx.user_data["dfn"]
        if text not in df0["Наименование ТП"].unique(): return
        df0=df0[df0["Наименование ТП"]==text]
        ctx.user_data.update({"step":"NOTIFY_VL","df0":df0,"tp":text})
        vls=df0["Наименование ВЛ"].unique().tolist()
        kb=[[v] for v in vls]+[["🔙 Назад"]]
        return await u.message.reply_text("Выберите ВЛ:",reply_markup=ReplyKeyboardMarkup(kb,resize_keyboard=True))

    # NOTIFY_VL: выбор ВЛ
    if step=="NOTIFY_VL":
        if text=="🔙 Назад":
            ctx.user_data["step"]="NOTIFY_AWAIT_TP"
            return await u.message.reply_text("Введите номер ТП:",reply_markup=kb_back)
        df0=ctx.user_data["df0"]
        if text not in df0["Наименование ВЛ"].unique(): return
        ctx.user_data["vl"]=text
        ctx.user_data["step"]="NOTIFY_GEO"
        return await u.message.reply_text("📍 Отправьте геолокацию:",reply_markup=kb_request_location)

# Обработка геолокации для уведомления
async def location_handler(u:Update,ctx:ContextTypes.DEFAULT_TYPE):
    if ctx.user_data.get("step")!="NOTIFY_GEO": return
    loc=u.message.location
    tp  =ctx.user_data["tp"]
    vl  =ctx.user_data["vl"]
    res =ctx.user_data["res_user"]
    sender=ctx.user_data["name"]
    _,_,_,names,resp_map=load_zones()
    recips=[i for i,r in resp_map.items() if r==res]
    msg=f"🔔 Уведомление от {sender}, {res} РЭС, {tp}, {vl} – Найден бездоговорной ВОЛС"
    # логируем и шлем
    fpath = NOTIFY_LOG_FILE_UG if ctx.user_data["net"]=="Россети ЮГ" else NOTIFY_LOG_FILE_RK
    for cid in recips:
        await ctx.bot.send_message(cid,msg)
        await ctx.bot.send_location(cid,loc.latitude,loc.longitude)
        await ctx.bot.send_message(cid,f"📍 {loc.latitude:.6f},{loc.longitude:.6f}")
        with open(fpath,"a",newline="",encoding="utf-8") as f:
            csv.writer(f).writerow([
                ctx.user_data["branch"],res,
                u.effective_user.id,sender,
                cid,names.get(cid,""),
                datetime.now(timezone.utc).isoformat(),
                f"{loc.latitude:.6f},{loc.longitude:.6f}"
            ])
    text = (f"✅ Уведомление отправлено: {', '.join(names[c] for c in recips)}"
            if recips else f"⚠ Ответственный на {res} РЭС не назначен.")
    await u.message.reply_text(text,reply_markup=kb_actions)
    ctx.user_data["step"]="BRANCH"

application.add_handler(CommandHandler("start",start_cmd))
application.add_handler(MessageHandler(filters.TEXT&~filters.COMMAND,handle_text))
application.add_handler(MessageHandler(filters.LOCATION,location_handler))

if __name__=="__main__":
    threading.Thread(target=lambda:requests.get(f"{SELF_URL}/webhook"),daemon=True).start()
    application.run_webhook(
        listen="0.0.0.0",port=PORT,
        url_path="webhook",webhook_url=f"{SELF_URL}/webhook"
    )
