import re
import pandas as pd
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import CallbackContext
from utils.zones import load_zones, normalize_sheet_url
from utils.keyboards import kb_search_select, kb_select_branch


def send_long(update: Update, text: str, reply_markup=None):
    MAX = 4000
    lines = text.split('\n')
    chunk = ""
    for line in lines:
        if len(chunk) + len(line) + 1 > MAX:
            update.message.reply_text(chunk.strip(), reply_markup=reply_markup)
            chunk = ""
        chunk += line + "\n"
    if chunk:
        update.message.reply_text(chunk.strip(), reply_markup=reply_markup)


def handle_text(update: Update, context: CallbackContext):
    text = update.message.text.strip()
    uid  = update.message.from_user.id
    bz, rz, names = load_zones()
    if uid not in bz:
        return update.message.reply_text(
            "К сожалению, у вас нет доступа, обратитесь к администратору."
        )
    branch, res, name = bz[uid], rz[uid], names[uid]
    mode = context.user_data.get('mode', 1)

    # возврат из неоднозначности
    if context.user_data.get('ambiguous'):
        if text == "Назад":
            context.user_data.pop('ambiguous')
            context.user_data.pop('ambiguous_df')
            return update.message.reply_text(
                f"{name}, введите номер ТП.",
                reply_markup=kb_search_select()
            )
        options = context.user_data['ambiguous']
        if text in options:
            amb_df = context.user_data['ambiguous_df']
            # оставляем только выбранный
            found = amb_df[amb_df['Наименование ТП'] == text]
            lines = [f"На {text} {len(found)} ВОЛС с договором аренды.", ""]
            for _, r0 in found.iterrows():
                lines.append(f"ВЛ {r0['Уровень напряжения']} {r0['Наименование ВЛ']}:" )
                lines.append(f"Опоры: {r0['Опоры']}")
                lines.append(f"Кол-во опор: {r0['Количество опор']}")
                prov = r0.get('Наименование Провайдера', '')
                num  = r0.get('Номер договора', '')
                tail = f", {num}" if num else ""
                lines.append(f"Провайдер: {prov}{tail}")
                lines.append("")
            resp = "\n".join(lines).strip()
            send_long(update, resp, reply_markup=kb_search_select())
            update.message.reply_text(f"{name}, задание выполнено!", reply_markup=kb_search_select())
            context.user_data.pop('ambiguous')
            context.user_data.pop('ambiguous_df')
            return

    # кнопка «Выбор филиала»
    if text == "Выбор филиала":
        if mode == 1:
            return update.message.reply_text(
                "Выберите филиал:", reply_markup=kb_select_branch()
            )
        elif mode == 2:
            return update.message.reply_text(
                f"{name}, Вы можете просматривать только филиал {branch}.",
                reply_markup=kb_search_select()
            )
        else:
            return update.message.reply_text(
                f"{name}, Вы можете просматривать только {res} РЭС филиала {branch}.",
                reply_markup=kb_search_select()
            )

    # выбор ветки поиска
    if mode == 1:
        if text in bz.values() or text in BRANCH_URLS.keys():
            context.user_data['current_branch'] = text
            return update.message.reply_text(
                f"{name}, введите номер ТП.",
                reply_markup=kb_search_select()
            )
        if text == "Поиск по ТП":
            if 'current_branch' not in context.user_data:
                return update.message.reply_text(
                    "Сначала выберите филиал:", reply_markup=kb_select_branch()
                )
            return update.message.reply_text(
                f"{name}, введите номер ТП.",
                reply_markup=kb_search_select()
            )
        branch_search = context.user_data.get('current_branch')
    else:
        branch_search = branch

    # загрузка и фильтрация таблицы
    try:
        df = pd.read_csv(normalize_sheet_url(config.BRANCH_URLS[branch_search]))
    except Exception as e:
        return update.message.reply_text(
            f"Ошибка загрузки таблицы: {e}", reply_markup=kb_search_select()
        )
    if mode == 3:
        df = df[df['РЭС'] == res]
    df.columns = df.columns.str.strip()

    def norm(s): return re.sub(r'\W','', str(s).upper())
    tp_input = norm(text)
    df['D_UP'] = df['Наименование ТП'].apply(norm)
    found = df[df['D_UP'].str.contains(tp_input, na=False)]

    # неоднозначность
    unique_tp = found['Наименование ТП'].unique().tolist()
    if len(unique_tp) > 1:
        kb = ReplyKeyboardMarkup([[tp] for tp in unique_tp] + [["Назад"]], resize_keyboard=True)
        context.user_data['ambiguous']    = unique_tp
        context.user_data['ambiguous_df'] = found
        return update.message.reply_text(
            "Возможно, вы искали другое ТП, выберите из списка ниже:",
            reply_markup=kb
        )

    if found.empty:
        return update.message.reply_text(
            "Договоров ВОЛС на данной ТП нет, либо название ТП введено некорректно.",
            reply_markup=kb_search_select()
        )

    # вывод результата
    tp_name = unique_tp[0]
    lines = [f"На {tp_name} {len(found)} ВОЛС с договором аренды.", ""]
    for _, r0 in found.iterrows():
        lines.append(f"ВЛ {r0['Уровень напряжения']} {r0['Наименование ВЛ']}:" )
        lines.append(f"Опоры: {r0['Опоры']}")
        lines.append(f"Кол-во опор: {r0['Количество опор']}")
        prov = r0.get('Наименование Провайдера', '')
        num  = r0.get('Номер договора', '')
        tail = f", {num}" if num else ""
        lines.append(f"Провайдер: {prov}{tail}")
        lines.append("")
    resp = "\n".join(lines).strip()
    send_long(update, resp, reply_markup=kb_search_select())
    update.message.reply_text(f"{name}, задание выполнено!", reply_markup=kb_search_select())
