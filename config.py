# config.py

import os

# — Telegram Bot settings —
TOKEN    = os.getenv("TOKEN")                       # Ваш бот-токен
SELF_URL = os.getenv("SELF_URL", "").rstrip("/")    # Ваш HTTPS URL, на который настроен webhook
PORT     = int(os.getenv("PORT", "5000"))           # Порт для Flask

# — Google Sheets / CSV URLs —
ZONES_CSV_URL    = os.getenv("ZONES_CSV_URL", "")    # Таблица зон (видимость, филиал, РЭС, ID, ФИО, Ответственный)
NOTIFY_SHEET_URL = os.getenv("NOTIFY_SHEET_URL", "") # Справочник ТП → ВЛ для уведомлений

# — Структура филиалов по сетям —
# BRANCH_URLS[network][branch_name] = ссылка на CSV/Google-sheet с данными этого филиала
BRANCH_URLS = {
    "Россети ЮГ": {
        "Центральные ЭС":      os.getenv("CENTRAL_ES_URL_UG", ""),
        "Западные ЭС":         os.getenv("WEST_ES_URL_UG", ""),
        "Восточные ЭС":        os.getenv("EAST_ES_URL_UG", ""),
        "Южные ЭС":            os.getenv("SOUTH_ES_URL_UG", ""),
        "Юго-Западные ЭС":     os.getenv("YUGO_ZAPAD_ES_UG_URL", ""),
        "Северо-Восточные ЭС": os.getenv("NE_ES_URL_UG", ""),
        "Юго-Восточные ЭС":    os.getenv("SE_ES_URL_UG", ""),
        "Северные ЭС":         os.getenv("NORTH_ES_URL_UG", ""),
    },
    "Россети Кубань": {
        "Юго-Западные ЭС":     os.getenv("YUGO_ZAPAD_ES_RK_URL", ""),
        "Усть-Лабинские ЭС":   os.getenv("UST_LAB_ES_URL_RK", ""),
        "Тимашевские ЭС":      os.getenv("TIMASHEV_ES_URL_RK", ""),
        "Тихорецкие ЭС":       os.getenv("TIKHORETS_ES_URL_RK", ""),
        "Сочинские ЭС":        os.getenv("SOCH_ES_URL_RK", ""),
        "Славянские ЭС":       os.getenv("SLAV_ES_URL_RK", ""),
        "Ленинградские ЭС":    os.getenv("LENINGRAD_ES_URL_RK", ""),
        "Лабинские ЭС":        os.getenv("LABIN_ES_URL_RK", ""),
        "Краснодарские ЭС":    os.getenv("KRASN_ES_URL_RK", ""),
        "Армавирские ЭС":      os.getenv("ARMAVIR_ES_URL_RK", ""),
        "Адыгейские ЭС":       os.getenv("ADYGEA_ES_URL_RK", ""),
    },
}

# Проверка на обязательные переменные
required = ["TOKEN", "SELF_URL", "ZONES_CSV_URL", "NOTIFY_SHEET_URL"]
missing = [v for v in required if not globals().get(v)]
if missing:
    raise RuntimeError(f"Не заданы ENV-переменные: {', '.join(missing)}")
