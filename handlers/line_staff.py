from telegram import ReplyKeyboardMarkup
from config import VISIBILITY_GROUPS

def kb_visibility():
    return ReplyKeyboardMarkup(
        [[vis] for vis in VISIBILITY_GROUPS.keys()],
        resize_keyboard=True
    )

async def handle_line(update, context):
    text = update.message.text.strip()

    # 1) Если ещё не выбрана группа видимости — предлагаем выбрать её
    if "visibility" not in context.user_data:
        await update.message.reply_text(
            "Выберите группу филиалов:",
            reply_markup=kb_visibility()
        )
        if text in VISIBILITY_GROUPS:
            context.user_data["visibility"] = text
        return

    # 2) После того, как выбрали visibility, переходим к выбору филиала
    if "current_branch" not in context.user_data:
        vis = context.user_data["visibility"]
        branches = VISIBILITY_GROUPS[vis]
        keyboard = ReplyKeyboardMarkup([[b] for b in branches], resize_keyboard=True)
        if text in branches:
            context.user_data["current_branch"] = text
        else:
            await update.message.reply_text(
                f"Теперь выберите филиал из группы «{vis}»:",
                reply_markup=keyboard
            )
            return

    # 3) И только теперь идёт классический поиск по ТП
    branch = context.user_data["current_branch"]
    res    = context.user_data.get("current_res")
    df     = search_tp(branch, text, res_filter=res)
    # … остальной код вывода …
