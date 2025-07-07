import os
import asyncio
import time
import aiohttp
import aiofiles
import pandas as pd
import csv
import re
from datetime import datetime, timezone
from io import BytesIO
from enum import Enum
from flask import Flask
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from openpyxl.styles import PatternFill
from openpyxl.utils import get_column_letter
from config import (
    TOKEN, SELF_URL, PORT,
    BRANCH_URLS, NOTIFY_URLS,
    NOTIFY_LOG_FILE_UG, NOTIFY_LOG_FILE_RK
)
from zones import normalize_sheet_url, load_zones_cached

# Перечисление для шагов
class BotStep(Enum):
    INIT = "INIT"
    NET = "NET"
    BRANCH = "BRANCH"
    AWAIT_TP_INPUT = "AWAIT_TP_INPUT"
    DISAMB = "DISAMB"
    NOTIFY_AWAIT_TP = "NOTIFY_AWAIT_TP"
    NOTIFY_DISAMB = "NOTIFY_DISAMB"
    NOTIFY_VL = "NOTIFY_VL"
    NOTIFY_GEO = "NOTIFY_GEO"
    VIEW_PHONES = "VIEW_PHONES"
    REPORT_MENU = "REPORT_MENU"

# Клавиатуры
kb_back = ReplyKeyboardMarkup([["🔙 Назад"]], resize_keyboard=True)
kb_actions = ReplyKeyboardMarkup(
    [["🔍 Поиск по ТП"], ["🔔 Отправить уведомление"], ["🔙 Назад"]],
    resize_keyboard=True
)
kb_request_location = ReplyKeyboardMarkup(
    [[KeyboardButton("📍 Отправить геолокацию", request_location=True)], ["🔙 Назад"]],
    resize_keyboard=True
)

def build_initial_kb(vis_flag: str, res_flag: str) -> ReplyKeyboardMarkup:
    f = vis_flag.strip().upper()
    nets = ["⚡ Россети ЮГ", "⚡ Россети Кубань"] if f == "ALL" else \
           ["⚡ Россети ЮГ"] if f == "UG" else ["⚡ Россети Кубань"]
    buttons = [[n] for n in nets]
    buttons.append(["📞 Телефоны провайдеров"])
    if res_flag.strip().upper() == "ALL":
        buttons.append(["📝 Сформировать отчёт"])
    buttons.append(["📖 Помощь"])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def build_report_kb(vis_flag: str) -> ReplyKeyboardMarkup:
    f = vis_flag.strip().upper()
    rows = []
    if f in ("ALL", "UG"):
        rows.append(["📊 Уведомления ЮГ"])
    if f in ("ALL", "RK"):
        rows.append(["📊 Уведомления Кубань"])
    rows += [["📋 Выгрузить контрагентов"], ["🔙 Назад"]]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

app = Flask(__name__)
application = ApplicationBuilder().token(TOKEN).build()

# Инициализация CSV-файлов для логов уведомлений
for lf in (NOTIFY_LOG_FILE_UG, NOTIFY_LOG_FILE_RK):
    if not os.path.exists(lf):
        with open(lf, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "Filial", "РЭС", "SenderID", "SenderName",
                "RecipientID", "RecipientName", "Timestamp", "Coordinates"
            ])

async def get_cached_csv(context, url, cache_key, ttl=3600):
    """Кэширует CSV-файлы с данными, чтобы минимизировать HTTP-запросы."""
    if cache_key not in context.bot_data or context.bot_data[cache_key]["expires"] < time.time():
        async with aiohttp.ClientSession() as session:
            async with session.get(normalize_sheet_url(url), timeout=10) as response:
                response.raise_for_status()
                df = pd.read_csv(BytesIO(await response.read()))
        context.bot_data[cache_key] = {"data": df, "expires": time.time() + ttl}
    return context.bot_data[cache_key]["data"]

async def log_notification(log_file, data):
    """Асинхронно записывает уведомления в лог-файл."""
    async with aiofiles.open(log_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        await f.write(writer.writerow(data))

# === /start ===
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запускает бота, проверяет доступ и показывает начальное меню."""
    uid = update.effective_user.id
    try:
        vis_map, raw_branch_map, res_map, names, resp_map = await load_zones_cached(context)
    except Exception as e:
        await update.message.reply_text(f"⚠️ Ошибка загрузки зон доступа: {e}", reply_markup=kb_back)
        return
    if uid not in raw_branch_map:
        await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=kb_back)
        return

    raw = raw_branch_map[uid]
    branch_key = "All" if raw == "All" else raw  # Используем ключ из config.py
    context.user_data.clear()
    context.user_data.update({
        "step": BotStep.BRANCH.value if branch_key != "All" else BotStep.INIT.value,
        "vis_flag": vis_map[uid],
        "branch_user": branch_key,
        "res_user": res_map[uid],
        "name": names[uid],
        "resp_map": resp_map
    })

    if branch_key != "All":
        await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)
    else:
        await update.message.reply_text(
            f"👋 Приветствую Вас, {names[uid]}! Выберите опцию:",
            reply_markup=build_initial_kb(vis_map[uid], res_map[uid])
        )

# === /help ===
async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает инструкции по использованию бота."""
    await update.message.reply_text(
        "📖 Инструкции:\n"
        "1. Используйте /start для начала.\n"
        "2. Выберите сеть и филиал, затем действие.\n"
        "3. Для поиска введите номер ТП (например, ТП-123).\n"
        "4. Для уведомлений выберите ТП, ВЛ и отправьте геолокацию.\n"
        "5. Для отчётов выберите тип отчёта (доступно при общем доступе).",
        reply_markup=kb_back
    )

# === Обработчики шагов ===
async def handle_init_step(update, context, text, vis_flag, res_user, name):
    """Обрабатывает начальный шаг (выбор сети или действия)."""
    if text == "🔙 Назад":
        context.user_data["step"] = BotStep.INIT.value
        await update.message.reply_text(
            f"👋 Приветствую Вас, {name}! Выберите опцию:",
            reply_markup=build_initial_kb(vis_flag, res_user)
        )
        return
    if text == "📞 Телефоны провайдеров":
        context.user_data["step"] = BotStep.VIEW_PHONES.value
        await update.message.reply_text("📞 Телефоны провайдеров:\n…", reply_markup=kb_back)
        return
    if text == "📝 Сформировать отчёт":
        if res_user.strip().upper() != "ALL":
            await update.message.reply_text(
                f"{name}, выгрузка отчётов доступна только при общем доступе по РЭС.",
                reply_markup=build_initial_kb(vis_flag, res_user)
            )
            return
        context.user_data["step"] = BotStep.REPORT_MENU.value
        await update.message.reply_text(
            "📝 Выберите тип отчёта:", reply_markup=build_report_kb(vis_flag)
        )
        return
    if text == "📖 Помощь":
        await help_cmd(update, context)
        return

    allowed = ["⚡ Россети ЮГ", "⚡ Россети Кубань"] if vis_flag == "All" else \
              ["⚡ Россети ЮГ"] if vis_flag == "UG" else ["⚡ Россети Кубань"]
    if text not in allowed:
        await update.message.reply_text(
            f"{name}, доступны: {', '.join(allowed)}",
            reply_markup=build_initial_kb(vis_flag, res_user)
        )
        return

    selected_net = text.replace("⚡ ", "")
    context.user_data.update({"step": BotStep.NET.value, "net": selected_net})
    branches = [context.user_data["branch_user"]] if context.user_data["branch_user"] != "All" else \
               list(BRANCH_URLS[selected_net].keys())
    kb = ReplyKeyboardMarkup([[b] for b in branches] + [["🔙 Назад"]], resize_keyboard=True)
    await update.message.reply_text("Выберите филиал:", reply_markup=kb)

async def handle_net_step(update, context, text, vis_flag, res_user, name):
    """Обрабатывает выбор филиала."""
    if text == "🔙 Назад":
        context.user_data["step"] = BotStep.INIT.value
        await update.message.reply_text(
            f"👋 Приветствую Вас, {name}! Выберите опцию:",
            reply_markup=build_initial_kb(vis_flag, res_user)
        )
        return
    selected_net = context.user_data["net"]
    branch_user = context.user_data["branch_user"]
    if branch_user != "All" and text != branch_user:
        await update.message.reply_text(
            f"{name}, доступен только филиал «{branch_user}».", reply_markup=kb_back
        )
        return
    if text not in BRANCH_URLS[selected_net]:
        await update.message.reply_text(
            f"⚠ Филиал «{text}» не найден.", reply_markup=kb_back
        )
        return
    context.user_data.update({"step": BotStep.BRANCH.value, "branch": text})
    await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)

async def handle_branch_step(update, context, text, vis_flag, res_user, name):
    """Обрабатывает выбор действия (поиск или уведомление)."""
    if text == "🔙 Назад":
        context.user_data["step"] = BotStep.NET.value if context.user_data["branch_user"] == "All" else BotStep.INIT.value
        if context.user_data["branch_user"] == "All":
            branches = list(BRANCH_URLS[context.user_data["net"]].keys())
            kb = ReplyKeyboardMarkup([[b] for b in branches] + [["🔙 Назад"]], resize_keyboard=True)
            await update.message.reply_text("Выберите филиал:", reply_markup=kb)
        else:
            await update.message.reply_text(
                f"👋 Приветствую Вас, {name}! Выберите опцию:",
                reply_markup=build_initial_kb(vis_flag, res_user)
            )
        return
    if text == "🔍 Поиск по ТП":
        context.user_data["step"] = BotStep.AWAIT_TP_INPUT.value
        await update.message.reply_text("Введите номер ТП (например, ТП-123):", reply_markup=kb_back)
        return
    if text == "🔔 Отправить уведомление":
        context.user_data["step"] = BotStep.NOTIFY_AWAIT_TP.value
        await update.message.reply_text("Введите номер ТП для уведомления (например, ТП-123):", reply_markup=kb_back)
        return

async def handle_await_tp_input_step(update, context, text, vis_flag, res_user, name):
    """Обрабатывает ввод номера ТП для поиска."""
    if text == "🔙 Назад":
        context.user_data["step"] = BotStep.BRANCH.value
        await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)
        return
    net = context.user_data["net"]
    branch = context.user_data["branch"]
    url = BRANCH_URLS[net].get(branch, "")
    if not url:
        await update.message.reply_text(
            f"⚠️ URL для «{branch}» не настроен.", reply_markup=kb_back
        )
        context.user_data["step"] = BotStep.BRANCH.value
        return
    try:
        df = await get_cached_csv(context, url, f"{net}_{branch}")
        required_columns = ["Наименование ТП", "РЭС", "Наименование ВЛ", "Опоры", "Уровень напряжения"]
        if not all(col in df.columns for col in required_columns):
            await update.message.reply_text(
                f"⚠️ CSV не содержит обязательные столбцы: {', '.join(required_columns)}",
                reply_markup=kb_back
            )
            return
    except aiohttp.ClientError as e:
        await update.message.reply_text(f"⚠️ Ошибка сети: {e}", reply_markup=kb_back)
        return
    except pd.errors.EmptyDataError:
        await update.message.reply_text(f"⚠️ CSV-файл пуст", reply_markup=kb_back)
        return
    except Exception as e:
        await update.message.reply_text(f"⚠️ Ошибка загрузки данных: {e}", reply_markup=kb_back)
        return

    if res_user.upper() != "ALL":
        df = df[df["РЭС"].str.upper() == res_user.upper()]
    df["D_UP"] = df["Наименование ТП"].str.upper().str.replace(r"\W", "", regex=True)
    q = re.sub(r"\W", "", text.upper())
    found = df[df["D_UP"].str.contains(q, na=False)]
    if found.empty:
        await update.message.reply_text(
            f"В {res_user} отсутствуют ТП, удовлетворяющие параметры поиска.",
            reply_markup=kb_back
        )
        context.user_data["step"] = BotStep.BRANCH.value
        return

    ulist = found["Наименование ТП"].unique().tolist()
    if len(ulist) > 1:
        context.user_data.update({"step": BotStep.DISAMB.value, "amb_df": found})
        kb = ReplyKeyboardMarkup([[tp] for tp in ulist] + [["🔙 Назад"]], resize_keyboard=True)
        await update.message.reply_text("Выберите ТП:", reply_markup=kb)
        return

    tp = ulist[0]
    det = found[found["Наименование ТП"] == tp]
    resname = det.iloc[0]["РЭС"]
    await update.message.reply_text(
        f"{resname}, {tp} ({len(det)}) ВОЛС с договором аренды:", reply_markup=kb_actions
    )
    for _, r in det.iterrows():
        await update.message.reply_text(
            f"📍 ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
            f"Опоры: {r['Опоры']}\n"
            f"Провайдер: {r.get('Наименование Провайдера', '')}",
            reply_markup=kb_actions
        )
    context.user_data["step"] = BotStep.BRANCH.value

async def handle_disamb_step(update, context, text, vis_flag, res_user, name):
    """Разрешает неоднозначность при выборе ТП."""
    if text == "🔙 Назад":
        context.user_data["step"] = BotStep.AWAIT_TP_INPUT.value
        await update.message.reply_text("Введите номер ТП (например, ТП-123):", reply_markup=kb_back)
        return
    found = context.user_data["amb_df"]
    if text not in found["Наименование ТП"].unique():
        return
    det = found[found["Наименование ТП"] == text]
    resname = det.iloc[0]["РЭС"]
    await update.message.reply_text(
        f"{resname}, {text} ({len(det)}) ВОЛС с договором аренды:", reply_markup=kb_actions
    )
    for _, r in det.iterrows():
        await update.message.reply_text(
            f"📍 ВЛ {r['Уровень напряжения']} {r['Наименование ВЛ']}\n"
            f"Опоры: {r['Опоры']}\n"
            f"Провайдер: {r.get('Наименование Провайдера', '')}",
            reply_markup=kb_actions
        )
    context.user_data["step"] = BotStep.BRANCH.value

async def handle_notify_await_tp_step(update, context, text, vis_flag, res_user, name):
    """Обрабатывает ввод номера ТП для уведомления."""
    if text == "🔙 Назад":
        context.user_data["step"] = BotStep.BRANCH.value
        await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)
        return
    net = context.user_data["net"]
    branch = context.user_data["branch"]
    url = NOTIFY_URLS[net].get(branch, "")
    if not url:
        await update.message.reply_text(
            f"⚠️ URL уведомлений для «{branch}» не настроен.", reply_markup=kb_back
        )
        context.user_data["step"] = BotStep.BRANCH.value
        return
    try:
        df = await get_cached_csv(context, url, f"notify_{net}_{branch}")
        required_columns = ["Наименование ТП", "РЭС", "Наименование ВЛ"]
        if not all(col in df.columns for col in required_columns):
            await update.message.reply_text(
                f"⚠️ CSV уведомлений не содержит обязательные столбцы: {', '.join(required_columns)}",
                reply_markup=kb_back
            )
            return
    except aiohttp.ClientError as e:
        await update.message.reply_text(f"⚠️ Ошибка сети: {e}", reply_markup=kb_back)
        return
    except pd.errors.EmptyDataError:
        await update.message.reply_text(f"⚠️ CSV-файл пуст", reply_markup=kb_back)
        return
    except Exception as e:
        await update.message.reply_text(f"⚠️ Ошибка загрузки уведомлений: {e}", reply_markup=kb_back)
        return

    df["D_UP"] = df["Наименование ТП"].str.upper().str.replace(r"\W", "", regex=True)
    q = re.sub(r"\W", "", text.upper())
    found = df[df["D_UP"].str.contains(q, na=False)]
    if found.empty:
        await update.message.reply_text(
            f"🔔 ТП не найдено в справочнике «{branch}».", reply_markup=kb_back
        )
        context.user_data["step"] = BotStep.BRANCH.value
        return
    ulist = found["Наименование ТП"].unique().tolist()
    if len(ulist) > 1:
        context.user_data.update({"step": BotStep.NOTIFY_DISAMB.value, "amb_df_notify": found})
        kb = ReplyKeyboardMarkup([[tp] for tp in ulist] + [["🔙 Назад"]], resize_keyboard=True)
        await update.message.reply_text("Выберите ТП для уведомления:", reply_markup=kb)
        return
    tp = ulist[0]
    context.user_data["tp"] = tp
    subset = found[found["Наименование ТП"] == tp]
    context.user_data["vl_df"] = subset
    context.user_data["notify_res"] = subset.iloc[0]["РЭС"]
    context.user_data["step"] = BotStep.NOTIFY_VL.value
    vls = subset["Наименование ВЛ"].unique().tolist()
    kb = ReplyKeyboardMarkup([[vl] for vl in vls] + [["🔙 Назад"]], resize_keyboard=True)
    await update.message.reply_text("Выберите ВЛ для уведомления:", reply_markup=kb)

async def handle_notify_disamb_step(update, context, text, vis_flag, res_user, name):
    """Разрешает неоднозначность при выборе ТП для уведомления."""
    if text == "🔙 Назад":
        context.user_data["step"] = BotStep.NOTIFY_AWAIT_TP.value
        await update.message.reply_text("Введите номер ТП для уведомления (например, ТП-123):", reply_markup=kb_back)
        return
    found = context.user_data["amb_df_notify"]
    if text not in found["Наименование ТП"].unique():
        return
    context.user_data["tp"] = text
    subset = found[found["Наименование ТП"] == text]
    context.user_data["vl_df"] = subset
    context.user_data["notify_res"] = subset.iloc[0]["РЭС"]
    context.user_data["step"] = BotStep.NOTIFY_VL.value
    vls = subset["Наименование ВЛ"].unique().tolist()
    kb = ReplyKeyboardMarkup([[vl] for vl in vls] + [["🔙 Назад"]], resize_keyboard=True)
    await update.message.reply_text("Выберите ВЛ для уведомления:", reply_markup=kb)

async def handle_notify_vl_step(update, context, text, vis_flag, res_user, name):
    """Обрабатывает выбор ВЛ для уведомления."""
    if text == "🔙 Назад":
        context.user_data["step"] = BotStep.NOTIFY_AWAIT_TP.value
        await update.message.reply_text("Введите номер ТП для уведомления (например, ТП-123):", reply_markup=kb_back)
        return
    subset = context.user_data["vl_df"]
    if text not in subset["Наименование ВЛ"].unique():
        return
    context.user_data["vl"] = text
    context.user_data["step"] = BotStep.NOTIFY_GEO.value
    await update.message.reply_text("Пожалуйста, отправьте геолокацию:", reply_markup=kb_request_location)

async def handle_report_menu_step(update, context, text, vis_flag, res_user, name):
    """Обрабатывает меню отчётов."""
    if text == "🔙 Назад":
        context.user_data["step"] = BotStep.INIT.value
        await update.message.reply_text(
            f"👋 Приветствую Вас, {name}! Выберите опцию:",
            reply_markup=build_initial_kb(vis_flag, res_user)
        )
        return
    if text == "📋 Выгрузить контрагентов":
        net = context.user_data["net"]
        branch = context.user_data["branch"]
        url = BRANCH_URLS[net].get(branch, "")
        if not url:
            await update.message.reply_text(
                f"⚠️ URL для «{branch}» не настроен.", reply_markup=build_report_kb(vis_flag)
            )
            return
        try:
            df = await get_cached_csv(context, url, f"{net}_{branch}")
            contractors = df[["Наименование Провайдера"]].dropna().unique().tolist()
            if not contractors:
                await update.message.reply_text(
                    "⚠️ Данные о провайдерах отсутствуют.", reply_markup=build_report_kb(vis_flag)
                )
                return
            bio = BytesIO()
            pd.DataFrame({"Провайдер": contractors}).to_excel(bio, index=False)
            bio.seek(0)
            await update.message.reply_document(bio, filename="contractors.xlsx")
        except Exception as e:
            await update.message.reply_text(
                f"⚠️ Ошибка выгрузки контрагентов: {e}", reply_markup=build_report_kb(vis_flag)
            )
        return
    if text in ("📊 Уведомления ЮГ", "📊 Уведомления Кубань"):
        log_file = NOTIFY_LOG_FILE_UG if text == "📊 Уведомления ЮГ" else NOTIFY_LOG_FILE_RK
        try:
            df = pd.read_csv(log_file)
            if df.empty:
                await update.message.reply_text(
                    f"⚠️ Лог {log_file} пуст.", reply_markup=build_report_kb(vis_flag)
                )
                return
            bio = BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Логи")
                ws = writer.sheets["Логи"]
                pink = PatternFill(fill_type="solid", start_color="FFF4F4", end_color="FFF4F4")
                for col_idx in range(1, len(df.columns) + 1):
                    ws.cell(row=1, column=col_idx).fill = pink
                for idx, col in enumerate(df.columns, start=1):
                    max_len = max(df[col].astype(str).map(len).max(), len(col))
                    ws.column_dimensions[get_column_letter(idx)].width = max_len + 2
            bio.seek(0)
            fname = "log_ug.xlsx" if text == "📊 Уведомления ЮГ" else "log_rk.xlsx"
            await update.message.reply_document(bio, filename=fname)
        except FileNotFoundError:
            await update.message.reply_text(
                f"⚠️ Лог-файл {log_file} не найден.", reply_markup=build_report_kb(vis_flag)
            )
        except Exception as e:
            await update.message.reply_text(
                f"⚠️ Ошибка формирования отчёта: {e}", reply_markup=build_report_kb(vis_flag)
            )
        return
    await update.message.reply_text(
        "📝 Выберите тип отчёта:", reply_markup=build_report_kb(vis_flag)
    )

# === TEXT handler ===
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Маршрутизирует текстовые сообщения по шагам."""
    text = update.message.text.strip()
    if "step" not in context.user_data:
        await start_cmd(update, context)
        return
    try:
        vis_map, raw_branch_map, res_map, names, resp_map = await load_zones_cached(context)
    except Exception as e:
        await update.message.reply_text(f"⚠️ Ошибка загрузки зон доступа: {e}", reply_markup=kb_back)
        return
    step = context.user_data["step"]
    vis_flag = context.user_data["vis_flag"]
    res_user = context.user_data["res_user"]
    name = context.user_data["name"]

    handlers = {
        BotStep.INIT.value: handle_init_step,
        BotStep.NET.value: handle_net_step,
        BotStep.BRANCH.value: handle_branch_step,
        BotStep.AWAIT_TP_INPUT.value: handle_await_tp_input_step,
        BotStep.DISAMB.value: handle_disamb_step,
        BotStep.NOTIFY_AWAIT_TP.value: handle_notify_await_tp_step,
        BotStep.NOTIFY_DISAMB.value: handle_notify_disamb_step,
        BotStep.NOTIFY_VL.value: handle_notify_vl_step,
        BotStep.REPORT_MENU.value: handle_report_menu_step,
    }
    handler = handlers.get(step, lambda *args: None)
    await handler(update, context, text, vis_flag, res_user, name)

# === Обработчик геолокации ===
async def location_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает геолокацию для уведомлений."""
    if context.user_data.get("step") != BotStep.NOTIFY_GEO.value:
        return
    loc = update.message.location
    tp = context.user_data["tp"]
    vl = context.user_data["vl"]
    res_tp = context.user_data["notify_res"]
    sender = context.user_data["name"]
    net = context.user_data["net"]
    branch = context.user_data["branch"]
    recipients = [
        uid for uid, r in context.user_data["resp_map"].items()
        if r and r.strip().lower() == res_tp.strip().lower()
    ]

    msg = f"🔔 Уведомление от {sender}, {res_tp} РЭС, {tp}, {vl} – Найден бездоговорной ВОЛС"
    log_f = NOTIFY_LOG_FILE_UG if net == "Россети ЮГ" else NOTIFY_LOG_FILE_RK
    for cid in recipients:
        await context.bot.send_message(cid, msg)
        await context.bot.send_location(cid, loc.latitude, loc.longitude)
        await context.bot.send_message(
            cid, f"📍 Широта: {loc.latitude:.6f}, Долгота: {loc.longitude:.6f}"
        )
        await log_notification(log_f, [
            branch, res_tp, update.effective_user.id, sender,
            cid, context.user_data["resp_map"].get(cid, ""),
            datetime.now(timezone.utc).isoformat(),
            f"{loc.latitude:.6f},{loc.longitude:.6f}"
        ])

    if recipients:
        names_list = [context.user_data["resp_map"].get(c, "") for c in recipients]
        await update.message.reply_text(
            f"✅ Уведомление отправлено: {', '.join(names_list)}",
            reply_markup=kb_actions
        )
    else:
        await update.message.reply_text(
            f"⚠ Ответственный на {res_tp} РЭС не назначен.",
            reply_markup=kb_actions
        )
    context.user_data["step"] = BotStep.BRANCH.value

# Регистрируем хендлеры
application.add_handler(CommandHandler("start", start_cmd))
application.add_handler(CommandHandler("help", help_cmd))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
application.add_handler(MessageHandler(filters.LOCATION, location_handler))

if __name__ == "__main__":
    if SELF_URL:
        async def start_webhook():
            """Инициирует вебхук для Render."""
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{SELF_URL}/webhook", timeout=10) as response:
                    response.raise_for_status()
        asyncio.run(start_webhook())
    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=f"{SELF_URL}/webhook"
    )
