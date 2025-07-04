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

from config import TOKEN, SELF_URL, PORT, BRANCH_URLS, NOTIFY_SHEET_URL
from zones import normalize_sheet_url, load_zones

# — Flask & PTB setup —
app = Flask(__name__)
application = ApplicationBuilder().token(TOKEN).build()

# — Общие клавиатуры —
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
    Главное меню: ⚡ Россети ЮГ / ⚡ Россети Кубань по vis_flag,
    плюс телефоны и отчёт.
    """
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
    """
    Меню филиалов: если у пользователя один филиал — только он,
    иначе — все ключи BRANCH_URLS[selected_net].
    """
    user_branch = branch_map.get(uid, "All")
    if user_branch != "All":
        branches = [user_branch]
    else:
        branches = list(BRANCH_URLS[selected_net].keys())

    btns = [[b] for b in branches] + [["🔙 Назад"]]
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

# — /start —
async def start_line(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    vis_map, raw_branch_map, res_map, names, resp_map = load_zones()

    # Фильтруем raw_branch_map только по тем, что есть в BRANCH_URLS
    all_branches = sum((list(v.keys()) for v in BRANCH_URLS.values()), [])
    branch_map = {u: b for u, b in raw_branch_map.items() if b in all_branches}

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

    kb = build_initial_kb(vis_map[uid])
    await update.message.reply_text(
        f"👋 Приветствую Вас, {names[uid]}! Выберите опцию:",
        reply_markup=kb
    )

# — Обработка текстовых сообщений —
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    uid  = update.effective_user.id

    # Подгружаем свежие зоны
    vis_map, raw_branch_map, res_map, names, _ = load_zones()
    all_branches = sum((list(v.keys()) for v in BRANCH_URLS.values()), [])
    branch_map = {u: b for u, b in raw_branch_map.items() if b in all_branches}

    if uid not in branch_map:
        await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)
        return

    step        = context.user_data.get("step", "INIT")
    vis_flag    = context.user_data["vis_flag"]
    branch_user = context.user_data["branch_user"]
    res_user    = context.user_data["res_user"]
    resp_flag   = context.user_data["resp_map"].get(uid, "").strip()
    user_name   = context.user_data["name"]

    # — Назад —
    if text == "🔙 Назад":
        if step in (
            "AWAIT_TP_INPUT", "DISAMBIGUOUS",
            "NOTIFY_AWAIT_TP", "NOTIFY_DISAMBIGUOUS",
            "NOTIFY_AWAIT_VL", "NOTIFY_WAIT_GEO"
        ):
            context.user_data["step"] = "BRANCH_SELECTED"
            await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)
            return

        if step == "NETWORK_SELECTED":
            context.user_data["step"] = "INIT"
            kb = build_initial_kb(vis_flag)
            await update.message.reply_text("Выберите опцию:", reply_markup=kb)
            return

        context.user_data["step"] = "INIT"
        kb = build_initial_kb(vis_flag)
        await update.message.reply_text("Выберите опцию:", reply_markup=kb)
        return

    # — INIT: главное меню —
    if step == "INIT":
        if text == "📞 Телефоны провайдеров":
            context.user_data["step"] = "VIEW_PHONES"
            await update.message.reply_text("📞 Телефоны провайдеров:\n…", reply_markup=kb_back)
            return

        if text == "📝 Сформировать отчёт":
            context.user_data["step"] = "VIEW_REPORT"
            await update.message.reply_text("📝 Отчёт сформирован.", reply_markup=kb_back)
            return

        flag = vis_flag.strip().upper()
        if flag == "ALL":
            allowed = ["⚡ Россети ЮГ", "⚡ Россети Кубань"]
        elif flag == "UG":
            allowed = ["⚡ Россети ЮГ"]
        else:
            allowed = ["⚡ Россети Кубань"]

        if text in allowed:
            selected_net = text.replace("⚡ ", "")
            context.user_data["step"]         = "NETWORK_SELECTED"
            context.user_data["selected_net"] = selected_net
            kb = build_branch_kb(uid, selected_net, branch_map)
            await update.message.reply_text("Выберите филиал:", reply_markup=kb)
        else:
            await update.message.reply_text(
                f"{user_name}, можете просматривать только: {', '.join(allowed)} 😕",
                reply_markup=build_initial_kb(vis_flag)
            )
        return

    # — NETWORK_SELECTED: выбор филиала —
    if step == "NETWORK_SELECTED":
        selected_net = context.user_data["selected_net"]
        allowed = [branch_user] if branch_user != "All" else list(BRANCH_URLS[selected_net].keys())

        if text in allowed:
            context.user_data["step"]           = "BRANCH_SELECTED"
            context.user_data["current_branch"] = text
            await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)
        else:
            await update.message.reply_text(
                f"{user_name}, можете просматривать только: {', '.join(allowed)} 😕",
                reply_markup=build_branch_kb(uid, selected_net, branch_map)
            )
        return

    # — BRANCH_SELECTED: действия в филиале —
    if step == "BRANCH_SELECTED":
        if text == "🔍 Поиск по ТП":
            context.user_data["step"] = "AWAIT_TP_INPUT"
            await update.message.reply_text("Введите номер ТП:", reply_markup=kb_back)
            return

        if text == "🔔 Отправить уведомление":
            if resp_flag:
                await update.message.reply_text(
                    f"{user_name}, вы не можете отправлять уведомления 😕",
                    reply_markup=kb_actions
                )
                return
            context.user_data["step"] = "NOTIFY_AWAIT_TP"
            await update.message.reply_text("Введите номер ТП для уведомления:", reply_markup=kb_back)
            return

        return

    # — AWAIT_TP_INPUT: простой поиск по ТП —
    if step == "AWAIT_TP_INPUT":
        selected_net    = context.user_data["selected_net"]
        current_branch  = context.user_data["current_branch"]
        # подбираем URL по сети и филиалу
        url = BRANCH_URLS[selected_net][current_branch]
        df  = pd.read_csv(normalize_sheet_url(url))

        if res_user != "All":
            norm = lambda s: re.sub(r'\W','', str(s).upper())
            df = df[df["РЭС"].apply(norm) == norm(res_user)]

        df.columns = df.columns.str.strip()
        df["D_UP"] = df["Наименование ТП"].str.upper().str.replace(r'\W','', regex=True)

        q     = re.sub(r'\W','', text.upper())
        found = df[df["D_UP"].str.contains(q, na=False)]
        if found.empty:
            await update.message.reply_text(
                "🔍 Нет договоров или неверное название. Введите снова:",
                reply_markup=kb_back
            )
            return

        unique_tp = found["Наименование ТП"].unique().tolist()
        if len(unique_tp) > 1:
            context.user_data["step"] = "DISAMBIGUOUS"
            context.user_data["ambiguous_df"] = found
            kb = ReplyKeyboardMarkup([[tp] for tp in unique_tp] + [["🔙 Назад"]],
                                     resize_keyboard=True)
            await update.message.reply_text("Найдено несколько ТП – выберите:", reply_markup=kb)
            return

        tp_sel  = unique_tp[0]
        details = found[found["Наименование ТП"] == tp_sel]
        cnt     = len(details)
        res_name= details.iloc[0]["РЭС"]

        await update.message.reply_text(
            f"{res_name}, {tp_sel} ({cnt}) ВОЛС с договором аренды.\nВыберите действие:",
            reply_markup=kb_actions
        )
        for _, r in details.iterrows():
            await update.message.reply_text(
                f"📍 ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
                f"Опоры: {r['Опоры']}\nПровайдер: {r.get('Наименование Провайдера','')}",
                reply_markup=kb_actions
            )

        context.user_data["step"] = "BRANCH_SELECTED"
        return

    # — DISAMBIGUOUS: уточнение ТП —
    if step == "DISAMBIGUOUS":
        if text == "🔙 Назад":
            context.user_data["step"] = "AWAIT_TP_INPUT"
            await update.message.reply_text("Введите номер ТП:", reply_markup=kb_back)
            return

        df_amb = context.user_data["ambiguous_df"]
        if text in df_amb["Наименование ТП"].unique():
            details = df_amb[df_amb["Наименование ТП"] == text]
            cnt     = len(details)
            res_name= details.iloc[0]["РЭС"]

            await update.message.reply_text(
                f"{res_name}, {text} ({cnt}) ВОЛС с договором аренды.\nВыберите действие:",
                reply_markup=kb_actions
            )
            for _, r in details.iterrows():
                await update.message.reply_text(
                    f"📍 ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
                    f"Опоры: {r['Опоры']}\nПровайдер: {r.get('Наименование Провайдера','')}",
                    reply_markup=kb_actions
                )
            context.user_data["step"] = "BRANCH_SELECTED"
        return

    # — NOTIFY_AWAIT_TP / NOTIFY_DISAMBIGUOUS / NOTIFY_AWAIT_VL —  
    # (оставляем как было)

# — Геолокация для уведомлений —
async def location_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("step") != "NOTIFY_WAIT_GEO":
        return

    loc = update.message.location
    sender_name = context.user_data["name"]
    tp          = context.user_data["notify_tp"]
    vl          = context.user_data["notify_vl"]
    notify_res  = context.user_data["notify_res"]

    _, _, _, _, resp_map2 = load_zones()
    recipients = [u for u, r in resp_map2.items() if r == notify_res]

    notif_text = (
        f"🔔 Уведомление от {sender_name}, {notify_res} РЭС, {tp}, {vl} – "
        "Найден бездоговорной ВОЛС"
    )
    for cid in recipients:
        await context.bot.send_message(chat_id=cid, text=notif_text)
        await context.bot.send_location(chat_id=cid,
                                        latitude=loc.latitude,
                                        longitude=loc.longitude)
        await context.bot.send_message(
            chat_id=cid,
            text=f"📍 Широта: {loc.latitude:.6f}, Долгота: {loc.longitude:.6f}"
        )

    await update.message.reply_text(
        f"✅ Уведомление отправлено: {', '.join(map(str, recipients))}",
        reply_markup=kb_actions
    )
    context.user_data["step"] = "BRANCH_SELECTED"

# — Регистрируем хендлеры —
application.add_handler(CommandHandler("start", start_line))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
application.add_handler(MessageHandler(filters.LOCATION, location_handler))

# — Запуск вебхука —
if __name__ == "__main__":
    threading.Thread(target=lambda: requests.get(f"{SELF_URL}/webhook"), daemon=True).start()
    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=f"{SELF_URL}/webhook"
    )
