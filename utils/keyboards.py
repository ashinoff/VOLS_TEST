from telegram import ReplyKeyboardMarkup
from config import BRANCH_URLS

BRANCHES = list(BRANCH_URLS.keys())


def kb_select_branch():
    return ReplyKeyboardMarkup([[b] for b in BRANCHES], resize_keyboard=True)


def kb_search_select():
    # поиск по ТП и смена филиала
    return ReplyKeyboardMarkup([
        ["Поиск по ТП"],
        ["Выбор филиала"]
    ], resize_keyboard=True)


def kb_only_select():
    # только выбор филиала
    return ReplyKeyboardMarkup([["Выбор филиала"]], resize_keyboard=True)
