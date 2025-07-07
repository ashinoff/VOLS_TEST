import os
from urllib.parse import urlparse

# — Основные настройки —
TOKEN = os.getenv("TOKEN")
if not TOKEN:
    raise ValueError("Переменная окружения TOKEN не задана")
SELF_URL = os.getenv("SELF_URL", "").rstrip("/")
PORT = int(os.getenv("PORT", "5000"))
ZONES_CSV_URL = os.getenv("ZONES_CSV_URL")
if not ZONES_CSV_URL:
    raise ValueError("Переменная окружения ZONES_CSV_URL не задана")

# — Проверка URL —
def validate_url(url):
    if not url:
        return False
    parsed = urlparse(url)
    return parsed.scheme in ("http", "https") and parsed.netloc

# — Нормализация названий филиалов —
BRANCH_KEY_MAP = {
    "Тимашевский": "Тимашевские ЭС",
    "Усть-Лабинский": "Усть-Лабинские ЭС",
    "Тихорецкий": "Тихорецкие ЭС",
    "Сочинский": "Сочинские ЭС",
    "Славянский": "Славянские ЭС",
    "Ленинградский": "Ленинградские ЭС",
    "Лабинский": "Лабинские ЭС",
    "Краснодарский": "Краснодарские ЭС",
    "Армавирский": "Армавирские ЭС",
    "Адыгейский": "Адыгейские ЭС",
    "Центральный": "Центральные ЭС",
    "Западный": "Западные ЭС",
    "Восточный": "Восточные ЭС",
    "Южный": "Южные ЭС",
    "Северо-Восточный": "Северо-Восточные ЭС",
    "Юго-Восточный": "Юго-Восточные ЭС",
    "Северный": "Северные ЭС",
}

# — URL-ы таблиц с данными по филиалам —
BRANCH_URLS = {
    "Россети Кубань": {
        BRANCH_KEY_MAP.get(raw_branch, raw_branch): os.getenv(f"{raw_branch.replace('-', '_').upper()}_ES_URL_RK", "")
        for raw_branch in [
            "Юго-Западный", "Усть-Лабинский", "Тимашевский", "Тихорецкий",
            "Сочинский", "Славянский", "Ленинградский", "Лабинский",
            "Краснодарский", "Армавирский", "Адыгейский"
        ]
    },
    "Россети ЮГ": {
        BRANCH_KEY_MAP.get(raw_branch, raw_branch): os.getenv(f"{raw_branch.replace('-', '_').upper()}_ES_URL_UG", "")
        for raw_branch in [
            "Центральный", "Западный", "Восточный", "Южный",
            "Юго-Западный", "Северо-Восточный", "Юго-Восточный", "Северный"
        ]
    },
}

# — Проверка URL в BRANCH_URLS —
for net, branches in BRANCH_URLS.items():
    for branch, url in branches.items():
        if not validate_url(url):
            print(f"Предупреждение: Неверный URL для {net} - {branch}: {url}")

# — Справочники для уведомлений (только для теста) —
NOTIFY_URLS = {
    "Россети Кубань": {
        "Тимашевские ЭС": os.getenv("TIMASHEV_ES_URL_RK_SP", ""),
    },
    "Россети ЮГ": {
        "Тимашевские ЭС": "",  # Заглушка для ЮГ, так как используется только RK
    },
}

# — Проверка URL в NOTIFY_URLS —
for net, branches in NOTIFY_URLS.items():
    for branch, url in branches.items():
        if url and not validate_url(url):
            print(f"Предупреждение: Неверный URL уведомлений для {net} - {branch}: {url}")

# — Логи уведомлений —
NOTIFY_LOG_FILE_UG = os.getenv("NOTIFY_LOG_FILE_UG", "notify_log_ug.csv")
NOTIFY_LOG_FILE_RK = os.getenv("NOTIFY_LOG_FILE_RK", "notify_log_rk.csv")
