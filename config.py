# config.py

import os

# === Основные настройки ===
TOKEN         = os.getenv("TOKEN", "")
SELF_URL      = os.getenv("SELF_URL", "").rstrip("/")
PORT          = int(os.getenv("PORT", 5000))
ZONES_CSV_URL = os.getenv("ZONES_CSV_URL", "")

# === URL-ы таблиц с данными по филиалам ===
# для каждой сети свой набор переменных окружения
BRANCH_URLS = {
    "Россети Кубань": {
        "Юго-Западные ЭС": os.getenv("YUGO_ZAPAD_ES_RK_URL", ""),
        "Усть-Лабинские ЭС": os.getenv("UST_LAB_ES_RK_URL", ""),
        "Тимашевские ЭС":    os.getenv("TIMASHEV_ES_RK_URL", ""),
        "Тихорецкие ЭС":     os.getenv("TIKHORETS_ES_RK_URL", ""),
        "Сочинские ЭС":      os.getenv("SOCH_ES_RK_URL", ""),
        "Славянские ЭС":     os.getenv("SLAV_ES_RK_URL", ""),
        "Ленинградские ЭС":  os.getenv("LENINGRAD_ES_RK_URL", ""),
        "Лабинские ЭС":      os.getenv("LABIN_ES_RK_URL", ""),
        "Краснодарские ЭС":  os.getenv("KRASN_ES_RK_URL", ""),
        "Армавирские ЭС":    os.getenv("ARMAVIR_ES_RK_URL", ""),
        "Адыгейские ЭС":     os.getenv("ADYGEA_ES_RK_URL", ""),
    },
    "Россети ЮГ": {
        "Центральные ЭС":      os.getenv("CENTRAL_ES_UG_URL", ""),
        "Западные ЭС":         os.getenv("WEST_ES_UG_URL", ""),
        "Восточные ЭС":        os.getenv("EAST_ES_UG_URL", ""),
        "Южные ЭС":            os.getenv("SOUTH_ES_UG_URL", ""),
        "Юго-Западные ЭС":     os.getenv("YUGO_ZAPAD_ES_UG_URL", ""),
        "Северо-Восточные ЭС": os.getenv("NE_ES_UG_URL", ""),
        "Юго-Восточные ЭС":    os.getenv("SE_ES_UG_URL", ""),
        "Северные ЭС":         os.getenv("NORTH_ES_UG_URL", ""),
    },
}

# === URL-ы справочников для уведомлений ===
NOTIFY_URLS = {
    "Россети Кубань": {
        "Тимашевские ЭС": os.getenv("TIMASHEV_ES_URL_RK_SP", ""),
        # ... при необходимости другие справочники
    },
    "Россети ЮГ": {
        "Тимашевские ЭС": os.getenv("TIMASHEV_ES_URL_UG_SP", ""),
        # ... и т.п.
    },
}

# === Файлы-логи уведомлений ===
NOTIFY_LOG_FILE_UG = os.getenv("NOTIFY_LOG_FILE_UG", "notify_log_ug.csv")
NOTIFY_LOG_FILE_RK = os.getenv("NOTIFY_LOG_FILE_RK", "notify_log_rk.csv")
