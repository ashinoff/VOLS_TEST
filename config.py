import os

TOKEN         = os.getenv("TOKEN")
SELF_URL      = os.getenv("SELF_URL", "").rstrip('/')
PORT          = int(os.getenv("PORT", 5000))

# URL таблиц
ZONES_CSV_URL = os.getenv("ZONES_CSV_URL", "").strip()

BRANCH_URLS = {
    "Юго-Западные ЭС": os.getenv("YUGO_ZAPAD_ES_URL", ""),
    "Усть-Лабинские ЭС": os.getenv("UST_LAB_ES_URL", ""),
    "Тимашевские ЭС":    os.getenv("TIMASHEV_ES_URL", ""),
    "Тихорецкие ЭС":     os.getenv("TIKHORETS_ES_URL", ""),
    "Сочинские ЭС":      os.getenv("SOCH_ES_URL", ""),
    "Славянские ЭС":     os.getenv("SLAV_ES_URL", ""),
    "Ленинградские ЭС":  os.getenv("LENINGRAD_ES_URL", ""),
    "Лабинские ЭС":      os.getenv("LABIN_ES_URL", ""),
    "Краснодарские ЭС":  os.getenv("KRASN_ES_URL", ""),
    "Армавирские ЭС":    os.getenv("ARMAVIR_ES_URL", ""),
    "Адыгейские ЭС":     os.getenv("ADYGEA_ES_URL", ""),
}

# Группировка филиалов по видимости: RK — Россети ЮГ, RU — Россети Кубань
VISIBILITY_GROUPS = {
    "RK": [  # Россети ЮГ
        "Юго-Западные ЭС",
        "Усть-Лабинские ЭС",
        "Сочинские ЭС",
        "Славянские ЭС",
        "Лабинские ЭС",
        "Армавирские ЭС",
        "Адыгейские ЭС",
    ],
    "RU": [  # Россети Кубань
        "Тимашевские ЭС",
        "Тихорецкие ЭС",
        "Ленинградские ЭС",
        "Краснодарские ЭС",
    ],
    # "All" будет обрабатываться динамически (предложим выбор)
}
