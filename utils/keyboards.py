from telegram import ReplyKeyboardMarkup
from config import BRANCH_URLS, REGIONS

BRANCHES = list(BRANCH_URLS.keys())


def kb_select_region():
    # выбор региона (Россети Кубань / Россети ЮГ)
    return ReplyKeyboardMarkup(
        [[r] for r in REGIONS],
        resize_keyboard=True
    )


def kb_select_branch():
    # выбор филиала из доступных
    return ReplyKeyboardMarkup(
        [[b] for b in BRANCHES],
        resize_keyboard=True
    )


def kb_search_select():
    # поиск по ТП и смена филиала
    return ReplyKeyboardMarkup([
        ["Поиск по ТП"],
        ["Выбор филиала"]
    ], resize_keyboard=True)


def kb_only_select():
    # только выбор филиала
    return ReplyKeyboardMarkup(
        [["Выбор филиала"]],
        resize_keyboard=True
    )
```python
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
