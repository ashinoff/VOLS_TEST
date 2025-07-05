# config.py

import os

# — Основные настройки —
TOKEN         = os.getenv("TOKEN", "")
SELF_URL      = os.getenv("SELF_URL", "").rstrip("/")
PORT          = int(os.getenv("PORT", "5000"))
ZONES_CSV_URL = os.getenv("ZONES_CSV_URL", "")

# — URL-ы таблиц с данными по филиалам —
BRANCH_URLS = {
    "Россети Кубань": {
        "Юго-Западные ЭС":   os.getenv("YUGO_ZAPAD_ES_URL_RK",    ""),
        "Усть-Лабинские ЭС": os.getenv("UST_LAB_ES_URL_RK",        ""),
        "Тимашевские ЭС":    os.getenv("TIMASHEV_ES_URL_RK",       ""),
        "Тихорецкие ЭС":     os.getenv("TIKHORETS_ES_URL_RK",      ""),
        "Сочинские ЭС":      os.getenv("SOCH_ES_URL_RK",           ""),
        "Славянские ЭС":     os.getenv("SLAV_ES_URL_RK",           ""),
        "Ленинградские ЭС":  os.getenv("LENINGRAD_ES_URL_RK",      ""),
        "Лабинские ЭС":      os.getenv("LABIN_ES_URL_RK",          ""),
        "Краснодарские ЭС":  os.getenv("KRASN_ES_URL_RK",          ""),
        "Армавирские ЭС":    os.getenv("ARMAVIR_ES_URL_RK",        ""),
        "Адыгейские ЭС":     os.getenv("ADYGEA_ES_URL_RK",         ""),
    },
    "Россети ЮГ": {
        "Центральные ЭС":      os.getenv("CENTRAL_ES_URL_UG",       ""),
        "Западные ЭС":         os.getenv("WEST_ES_URL_UG",          ""),
        "Восточные ЭС":        os.getenv("EAST_ES_URL_UG",          ""),
        "Южные ЭС":            os.getenv("SOUTH_ES_URL_UG",         ""),
        "Юго-Западные ЭС":     os.getenv("YUGO_ZAPAD_ES_URL_UG",    ""),
        "Северо-Восточные ЭС": os.getenv("NE_ES_URL_UG",            ""),
        "Юго-Восточные ЭС":    os.getenv("SE_ES_URL_UG",            ""),
        "Северные ЭС":         os.getenv("NORTH_ES_URL_UG",         ""),
    },
}

# — Справочники для уведомлений —
NOTIFY_URLS = {
    "Россети Кубань": {
        "Тимашевские ЭС": os.getenv("TIMASHEV_ES_URL_RK_SP", ""),
        # ... при необходимости добавьте другие
    },
    "Россети ЮГ": {
        "Тимашевские ЭС": os.getenv("TIMASHEV_ES_URL_UG_SP", ""),
        # ... и т.д.
    },
}

# — Логи уведомлений —
NOTIFY_LOG_FILE_UG = os.getenv("NOTIFY_LOG_FILE_UG", "notify_log_ug.csv")
NOTIFY_LOG_FILE_RK = os.getenv("NOTIFY_LOG_FILE_RK", "notify_log_rk.csv")
