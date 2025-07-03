# bot.py
import re
import pandas as pd
from flask import jsonify
from telegram import Bot, Update
from telegram.ext import Dispatcher, CommandHandler, MessageHandler, Filters, CallbackContext

from config import TOKEN, BRANCH_URLS
from utils import (
    normalize_sheet_url,
    load_zones,
    kb_select_branch,
    kb_search_select,
    kb_only_select,
    send_long,
    get_update_json,
)

bot = Bot(token=TOKEN)
dispatcher = Dispatcher(bot, None, use_context=True)

def start(update: Update, context: CallbackContext):
    uid = update.message.from_user.id
    try:
        bz, rz, names = load_zones()
    except Exception as e:
        return update.message.reply_text(f"Ошибка загрузки прав доступа: {e}")
    if uid not in bz:
        return update.message.reply_text("К сожалению, у вас нет доступа, обратитесь к администратору.")
    branch, res, name = bz[uid], rz[uid], names[uid]

    # режимы доступа
    if branch == "All" and res == "All":
        context.user_data['mode'] = 1
        update.message.reply_text(
            f"Приветствую Вас, {name}! Вы можете осуществлять поиск в любом филиале.\n"
            f"Нажмите «Выбор филиала».",
            reply_markup=kb_only_select()
        )
    elif branch != "All" and res == "All":
        context.user_data['mode'] = 2
        context.user_data['current_branch'] = branch
        update.message.reply_text(
            f"Приветствую Вас, {name}! Вы можете просматривать только филиал {branch}.",
            reply_markup=kb_search_select()
        )
    else:
        context.user_data['mode'] = 3
        context.user_data['current_branch'] = branch
        context.user_data['current_res']    = res
        update.message.reply_text(
            f"Приветствую Вас, {name}! Вы можете просматривать только {res} РЭС филиала {branch}.",
            reply_markup=kb_search_select()
        )
    context.user_data.pop('ambiguous', None)
    context.user_data.pop('ambiguous_df', None)

def handle_text(update: Update, context: CallbackContext):
    text = update.message.text.strip()
    uid  = update.message.from_user.id
    bz, rz, names = load_zones()
    if uid not in bz:
        return update.message.reply_text("К сожалению, у вас нет доступа, обратитесь к администратору.")
    branch, res, name = bz[uid], rz[uid], names[uid]
    mode = context.user_data.get('mode', 1)

    # возврат из неоднозначности
    if context.user_data.get('ambiguous'):
        if text == "Назад":
            context.user_data.pop('ambiguous'); context.user_data.pop('ambiguous_df')
            return update.message.reply_text(f"{name}, введите номер ТП.", reply_markup=kb_search_select())

        options = context.user_data['ambiguous']
        if text in options:
            amb_df = context.user_data['ambiguous_df']
            found = amb_df[amb_df['Наименование ТП'] == text]
            lines = [f"На {text} {len(found)} ВОЛС с договором аренды.", ""]
            for _, r0 in found.iterrows():
                lines += [
                    f"ВЛ {r0['Уровень напряжения']} {r0['Наименование ВЛ']}:",
                    f"Опоры: {r0['Опоры']}",
                    f"Кол-во опор: {r0['Количество опор']}",
                    f"Провайдер: {r0.get('Наименование Провайдера','')}"
                        + (f", {r0.get('Номер договора')}" if r0.get('Номер договора') else ""),
                    ""
                ]
            send_long(update.message, "\n".join(lines).strip(), reply_markup=kb_search_select())
            update.message.reply_text(f"{name}, задание выполнено!", reply_markup=kb_search_select())
            context.user_data.pop('ambiguous'); context.user_data.pop('ambiguous_df')
            return

    # выбор филиала / поиск
    if text == "Выбор филиала":
        if mode == 1:
            return update.message.reply_text("Выберите филиал:", reply_markup=kb_select_branch())
        else:
            msg = f"{name}, вы можете просматривать только филиал {context.user_data.get('current_branch')}"
            return update.message.reply_text(msg, reply_markup=kb_search_select())

    if mode == 1:
        if text in BRANCH_URLS:
            context.user_data['current_branch'] = text
            return update.message.reply_text(f"{name}, введите номер ТП.", reply_markup=kb_search_select())
        if text == "Поиск по ТП":
            if 'current_branch' not in context.user_data:
                return update.message.reply_text("Сначала выберите филиал:", reply_markup=kb_select_branch())
            return update.message.reply_text(f"{name}, введите номер ТП.", reply_markup=kb_search_select())
        branch_search = context.user_data.get('current_branch')
        if not branch_search:
            return

    else:
        branch_search = context.user_data['current_branch']

    # загрузка и фильтрация
    try:
        df = pd.read_csv(normalize_sheet_url(BRANCH_URLS[branch_search]))
    except Exception as e:
        return update.message.reply_text(f"Ошибка загрузки таблицы: {e}", reply_markup=kb_search_select())

    if mode == 3:
        df = df[df["РЭС"] == res]

    df.columns = df.columns.str.strip()
    norm = lambda s: re.sub(r'\W','', str(s).upper())
    tp_input = norm(text)
    df['D_UP'] = df['Наименование ТП'].apply(norm)
    found = df[df['D_UP'].str.contains(tp_input, na=False)]

    # неоднозначность / пусто / вывод
    unique_tp = found['Наименование ТП'].unique().tolist()
    if len(unique_tp) > 1:
        kb = ReplyKeyboardMarkup([[tp] for tp in unique_tp] + [["Назад"]], resize_keyboard=True)
        context.user_data['ambiguous'], context.user_data['ambiguous_df'] = unique_tp, found
        return update.message.reply_text("Возможно, вы искали другое ТП:", reply_markup=kb)

    if found.empty:
        return update.message.reply_text(
            "Договоров ВОЛС на данной ТП нет либо имя введено некорректно.",
            reply_markup=kb_search_select()
        )

    tp_name = unique_tp[0]
    lines = [f"На {tp_name} {len(found)} ВОЛС с договором аренды.", ""]
    for _, r0 in found.iterrows():
        lines += [
            f"ВЛ {r0['Уровень напряжения']} {r0['Наименование ВЛ']}:",
            f"Опоры: {r0['Опоры']}",
            f"Кол-во опор: {r0['Количество опор']}",
            f"Провайдер: {r0.get('Наименование Провайдера','')}"
                + (f", {r0.get('Номер договора')}" if r0.get('Номер договора') else ""),
            ""
        ]
    send_long(update.message, "\n".join(lines).strip(), reply_markup=kb_search_select())
    update.message.reply_text(f"{name}, задание выполнено!", reply_markup=kb_search_select())

# регистрацию хендлеров
dispatcher.add_handler(CommandHandler('start', start))
dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_text))
