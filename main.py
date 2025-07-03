# main.py
import threading
from flask import Flask
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters
from telegram import Update, ReplyKeyboardMarkup
import re
import requests
import pandas as pd
from io import StringIO

from config import TOKEN, SELF_URL, PORT, BRANCH_URLS, VISIBILITY_GROUPS
from zones import normalize_sheet_url, load_zones

app = Flask(__name__)
application = ApplicationBuilder().token(TOKEN).build()

# === Клавиатуры ===
kb_initial = ReplyKeyboardMarkup(
    [["Россети ЮГ"], ["Россети Кубань"], ["Телефоны провайдеров"], ["Сформировать отчёт"]],
    resize_keyboard=True
)
kb_back = ReplyKeyboardMarkup([["Назад"]], resize_keyboard=True)
def kb_branches(branches):
    btns = [[b] for b in branches] + [["Назад"]]
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

# === Хендлеры ===
async def start_line(update: Update, context):
    uid = update.effective_user.id
    vis_map, bz, rz, names = load_zones()
    if uid not in bz:
        await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)
        return
    # Сброс состояния
    context.user_data.clear()
    context.user_data["step"] = "INIT"
    context.user_data["res"] = rz[uid]
    context.user_data["name"] = names[uid]
    # Приветствие и главное меню
    await update.message.reply_text(
        f"Привет, {names[uid]}! Выберите опцию:",
        reply_markup=kb_initial
    )

async def handle_text(update: Update, context):
    text = update.message.text.strip()
    uid = update.effective_user.id
    vis_map, bz, rz, names = load_zones()
    if uid not in bz:
        await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)
        return

    step = context.user_data.get("step", "INIT")

    # --- «Назад» ---
    if text == "Назад":
        if step in ("AWAIT_TP_INPUT", "DISAMBIGUOUS"):
            # возвращаем выбор филиала
            context.user_data["step"] = "NETWORK_SELECTED"
            vis = context.user_data["visibility"]
            await update.message.reply_text(
                "Выберите филиал:",
                reply_markup=kb_branches(VISIBILITY_GROUPS[vis])
            )
            return
        # из выбора филиала или просмотра телефонов/отчёта возвращаем на меню
        context.user_data["step"] = "INIT"
        await update.message.reply_text(
            "Выберите опцию:",
            reply_markup=kb_initial
        )
        return

    # --- Шаг INIT: главное меню ---
    if step == "INIT":
        if text == "Телефоны провайдеров":
            context.user_data["step"] = "VIEW_PHONES"
            await update.message.reply_text("📞 Телефоны провайдеров:\n…", reply_markup=kb_back)
            return
        if text == "Сформировать отчёт":
            context.user_data["step"] = "VIEW_REPORT"
            await update.message.reply_text("📝 Отчёт сформирован.", reply_markup=kb_back)
            return
        if text in ("Россети ЮГ", "Россети Кубань"):
            context.user_data["step"] = "NETWORK_SELECTED"
            context.user_data["visibility"] = text
            # выбор филиала сразу
            await update.message.reply_text(
                "Выберите филиал:",
                reply_markup=kb_branches(VISIBILITY_GROUPS[text])
            )
        return

    # --- Шаг NETWORK_SELECTED: выбор филиала ---
    if step == "NETWORK_SELECTED":
        branches = VISIBILITY_GROUPS[context.user_data["visibility"]]
        if text in branches:
            context.user_data["step"] = "AWAIT_TP_INPUT"
            context.user_data["current_branch"] = text
            await update.message.reply_text(
                "Введите номер ТП:",
                reply_markup=kb_back
            )
        return

    # --- Шаг AWAIT_TP_INPUT: ввод ТП и поиск ---
    if step == "AWAIT_TP_INPUT":
        branch = context.user_data["current_branch"]
        # загрузка данных
        df = pd.read_csv(normalize_sheet_url(BRANCH_URLS[branch]))
        res = context.user_data.get("res")
        if res != "All":
            df = df[df["РЭС"] == res]
        df.columns = df.columns.str.strip()
        df["D_UP"] = df["Наименование ТП"].str.upper().str.replace(r'\W','', regex=True)

        q = re.sub(r'\W','', text.upper())
        found = df[df["D_UP"].str.contains(q, na=False)]

        if found.empty:
            context.user_data["step"] = "AWAIT_TP_INPUT"
            await update.message.reply_text("🔍 Ничего не найдено. Повторите ввод:", reply_markup=kb_back)
            return

        unique_tp = found["Наименование ТП"].unique().tolist()
        if len(unique_tp) > 1:
            context.user_data["step"] = "DISAMBIGUOUS"
            context.user_data["ambiguous_df"] = found
            kb = ReplyKeyboardMarkup([[tp] for tp in unique_tp] + [["Назад"]], resize_keyboard=True)
            await update.message.reply_text("Найдены несколько ТП, выберите:", reply_markup=kb)
            return

        # Одна ТП — выводим детали
        tp = unique_tp[0]
        details = found[found["Наименование ТП"] == tp]
        count = len(details)
        # подтягиваем название РЭС из данных
        res_name = details.iloc[0]["РЭС"]
        # заголовок
        await update.message.reply_text(
            f"{res_name}, {tp} ({count}) ВОЛС с договором аренды.",
            reply_markup=kb_back
        )
        # подробности
        for _, r in details.iterrows():
            await update.message.reply_text(
                f"📍 ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
                f"Опоры: {r['Опоры']}\n"
                f"Провайдер: {r.get('Наименование Провайдера','')}",
                reply_markup=kb_back
            )
        # Задание выполнено
        await update.message.reply_text(
            f"✅ Задание выполнено, {context.user_data['name']}.",
            reply_markup=kb_back
        )
        context.user_data["step"] = "NETWORK_SELECTED"
        return

    # --- Шаг DISAMBIGUOUS: выбор из нескольких ТП ---
    if step == "DISAMBIGUOUS":
        df = context.user_data["ambiguous_df"]
        if text in df["Наименование ТП"].unique():
            details = df[df["Наименование ТП"] == text]
            count = len(details)
            res_name = details.iloc[0]["РЭС"]
            await update.message.reply_text(
                f"{res_name}, {text} ({count}) ВОЛС с договором аренды.",
                reply_markup=kb_back
            )
            for _, r in details.iterrows():
                await update.message.reply_text(
                    f"📍 ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
                    f"Опоры: {r['Опоры']}\n"
                    f"Провайдер: {r.get('Наименование Провайдера','')}",
                    reply_markup=kb_back
                )
            await update.message.reply_text(
                f"✅ Задание выполнено, {context.user_data['name']}.",
                reply_markup=kb_back
            )
            context.user_data["step"] = "NETWORK_SELECTED"
        return

# Регистрируем handlers
application.add_handler(CommandHandler("start", start_line))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

# Запуск webhook
if __name__ == "__main__":
    threading.Thread(target=lambda: requests.get(f"{SELF_URL}/webhook"), daemon=True).start()
    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=f"{SELF_URL}/webhook"
    )
