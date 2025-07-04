# config.py

import os

# — Telegram Bot —
TOKEN    = os.getenv("TOKEN")
SELF_URL = os.getenv("SELF_URL", "").rstrip("/")
PORT     = int(os.getenv("PORT", "5000"))

# — Зоны доступа —
ZONES_CSV_URL = os.getenv("ZONES_CSV_URL", "")

# — Поиск ТП: BRANCH_URLS[сеть][филиал] = URL CSV/Google-sheet —
BRANCH_URLS = {
    "Россети ЮГ": {
        "Центральные ЭС":      os.getenv("CENTRAL_ES_URL_UG", ""),
        "Западные ЭС":         os.getenv("WEST_ES_URL_UG", ""),
        "Восточные ЭС":        os.getenv("EAST_ES_URL_UG", ""),
        "Южные ЭС":            os.getenv("SOUTH_ES_URL_UG", ""),
        "Юго-Западные ЭС":     os.getenv("YUGO_ZAPAD_ES_URL_UG", ""),
        "Северо-Восточные ЭС": os.getenv("NE_ES_URL_UG", ""),
        "Юго-Восточные ЭС":    os.getenv("SE_ES_URL_UG", ""),
        "Северные ЭС":         os.getenv("NORTH_ES_URL_UG", ""),
    },
    "Россети Кубань": {
        "Юго-Западные ЭС":     os.getenv("YUGO_ZAPAD_ES_URL_RK", ""),
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

# — Справочники для уведомлений: те же филиалы, но ENV с суффиксом _SP —
NOTIFY_URLS = {
    # Россети ЮГ
    "Центральные ЭС":      os.getenv("CENTRAL_ES_URL_UG_SP",     ""),
    "Западные ЭС":         os.getenv("WEST_ES_URL_UG_SP",        ""),
    "Восточные ЭС":        os.getenv("EAST_ES_URL_UG_SP",        ""),
    "Южные ЭС":            os.getenv("SOUTH_ES_URL_UG_SP",       ""),
    "Юго-Западные ЭС":     os.getenv("YUGO_ZAPAD_ES_URL_UG_SP",  ""),
    "Северо-Восточные ЭС": os.getenv("NE_ES_URL_UG_SP",          ""),
    "Юго-Восточные ЭС":    os.getenv("SE_ES_URL_UG_SP",          ""),
    "Северные ЭС":         os.getenv("NORTH_ES_URL_UG_SP",       ""),

    # Россети Кубань
    "Юго-Западные ЭС":     os.getenv("YUGO_ZAPAD_ES_URL_RK_SP",  ""),
    "Усть-Лабинские ЭС":   os.getenv("UST_LAB_ES_URL_RK_SP",     ""),
    "Тимашевские ЭС":      os.getenv("TIMASHEV_ES_URL_RK_SP",    ""),
    "Тихорецкие ЭС":       os.getenv("TIKHORETS_ES_URL_RK_SP",   ""),
    "Сочинские ЭС":        os.getenv("SOCH_ES_URL_RK_SP",        ""),
    "Славянские ЭС":       os.getenv("SLAV_ES_URL_RK_SP",        ""),
    "Ленинградские ЭС":    os.getenv("LENINGRAD_ES_URL_RK_SP",   ""),
    "Лабинские ЭС":        os.getenv("LABIN_ES_URL_RK_SP",       ""),
    "Краснодарские ЭС":    os.getenv("KRASN_ES_URL_RK_SP",       ""),
    "Армавирские ЭС":      os.getenv("ARMAVIR_ES_URL_RK_SP",     ""),
    "Адыгейские ЭС":       os.getenv("ADYGEA_ES_URL_RK_SP",      ""),
}

# — Проверка обязательных переменных —
required = ["TOKEN", "SELF_URL", "ZONES_CSV_URL"]
missing = [v for v in required if not globals().get(v)]
if missing:
    raise RuntimeError(f"Не заданы ENV-переменные: {', '.join(missing)}")
