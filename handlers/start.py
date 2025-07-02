from telegram import Update
from telegram.ext import CallbackContext
from utils.zones import load_zones
from utils.keyboards import kb_only_select, kb_search_select


def start(update: Update, context: CallbackContext):
    uid = update.message.from_user.id
    try:
        bz, rz, names = load_zones()
    except Exception as e:
        return update.message.reply_text(f"Ошибка загрузки прав доступа: {e}")
    if uid not in bz:
        return update.message.reply_text(
            "К сожалению, у вас нет доступа, обратитесь к администратору."
        )
    branch, res, name = bz[uid], rz[uid], names[uid]

    # определяем режим
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
    # сброс
    context.user_data.pop('ambiguous', None)
    context.user_data.pop('ambiguous_df', None)
