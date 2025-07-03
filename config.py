import os

# Токен бота
TOKEN = os.getenv("TOKEN")

# Публичный URL приложения без конечного слэша
SELF_URL = os.getenv("SELF_URL", "").rstrip('/')

# Порт (Render задаёт автоматически)
# По умолчанию 5000
PORT = int(os.getenv("PORT", 5000))

# Ссылка на Google Sheet с колонками:
# Видимость | Филиал | РЭС | ID | ФИО | Ответственный
ZONES_CSV_URL = os.getenv("ZONES_CSV_URL", "").strip()

# Отдельный лист для уведомлений по ТП/ВЛ
NOTIFY_SHEET_URL = os.getenv("TIMASHEV_ES_URL_SP", "").strip()

# CSV URL-ы филиальных таблиц (ВОЛС)
BRANCH_URLS = {
    "Юго-Западные ЭС":   os.getenv("YUGO_ZAPAD_ES_URL", ""),
    "Усть-Лабинские ЭС":  os.getenv("UST_LAB_ES_URL", ""),
    "Тимашевские ЭС":     os.getenv("TIMASHEV_ES_URL", ""),
    "Тихорецкие ЭС":      os.getenv("TIKHORETS_ES_URL", ""),
    "Сочинские ЭС":       os.getenv("SOCH_ES_URL", ""),
    "Славянские ЭС":      os.getenv("SLAV_ES_URL", ""),
    "Ленинградские ЭС":   os.getenv("LENINGRAD_ES_URL", ""),
    "Лабинские ЭС":       os.getenv("LABIN_ES_URL", ""),
    "Краснодарские ЭС":   os.getenv("KRASN_ES_URL", ""),
    "Армавирские ЭС":     os.getenv("ARMAVIR_ES_URL", ""),
    "Адыгейские ЭС":      os.getenv("ADYGEA_ES_URL", ""),
}

# Разбиение филиалов по видимости
VISIBILITY_GROUPS = {
    "Россети ЮГ": [
        "Юго-Западные ЭС",
        "Усть-Лабинские ЭС",
        "Сочинские ЭС",
        "Славянские ЭС",
        "Лабинские ЭС",
        "Армавирские ЭС",
        "Адыгейские ЭС",
    ],
    "Россети Кубань": [
        "Тимашевские ЭС",
        "Тихорецкие ЭС",
        "Ленинградские ЭС",
        "Краснодарские ЭС",
    ],
}
