import os

# Токен и хостинг
TOKEN      = os.getenv("TOKEN")
SELF_URL   = os.getenv("SELF_URL", "").rstrip('/')
PORT       = int(os.getenv("PORT", 5000))

# Таблица зон (колонки: Видимость | Филиал | РЭС | ID | ФИО | Ответственный)
ZONES_CSV_URL = os.getenv("ZONES_CSV_URL", "").strip()

# Отдельная таблица ТП–ВЛ для уведомлений
NOTIFY_SHEET_URL = os.getenv("TIMASHEV_ES_URL_SP", "").strip()

# URL-ы филиальных CSV для каждой сети
BRANCH_URLS = {
    "Россети ЮГ": {
        "Центральные ЭС":        os.getenv("CENTRAL_ES_URL", ""),
        "Западные ЭС":           os.getenv("WEST_ES_URL", ""),
        "Восточные ЭС":          os.getenv("EAST_ES_URL", ""),
        "Южные ЭС":              os.getenv("SOUTH_ES_URL", ""),
        "Юго-Западные ЭС":       os.getenv("YUGO_ZAPAD_ES_UG_URL", ""),
        "Северо-Восточные ЭС":   os.getenv("NE_ES_URL", ""),
        "Юго-Восточные ЭС":      os.getenv("SE_ES_URL", ""),
        "Северные ЭС":           os.getenv("NORTH_ES_URL", ""),
    },
    "Россети Кубань": {
        "Юго-Западные ЭС":       os.getenv("YUGO_ZAPAD_ES_RK_URL", ""),
        "Усть-Лабинские ЭС":     os.getenv("UST_LAB_ES_URL", ""),
        "Тимашевские ЭС":        os.getenv("TIMASHEV_ES_URL", ""),
        "Тихорецкие ЭС":         os.getenv("TIKHORETS_ES_URL", ""),
        "Сочинские ЭС":          os.getenv("SOCH_ES_URL", ""),
        "Славянские ЭС":         os.getenv("SLAV_ES_URL", ""),
        "Ленинградские ЭС":      os.getenv("LENINGRAD_ES_URL", ""),
        "Лабинские ЭС":          os.getenv("LABIN_ES_URL", ""),
        "Краснодарские ЭС":      os.getenv("KRASN_ES_URL", ""),
        "Армавирские ЭС":        os.getenv("ARMAVIR_ES_URL", ""),
        "Адыгейские ЭС":         os.getenv("ADYGEA_ES_URL", ""),
    },
}
