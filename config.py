# config.py

import os

# === Telegram & Webhook ===
TOKEN         = os.getenv("TOKEN")
SELF_URL      = os.getenv("SELF_URL", "").rstrip('/')
PORT          = int(os.getenv("PORT", "5000"))

# === Основной файл зон ===
ZONES_CSV_URL = os.getenv("ZONES_CSV_URL", "").strip()

# === Таблицы филиалов ===
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

# === Справочники для уведомлений (по тому же списку филиалов) ===
NOTIFY_URLS = {
    "Юго-Западные ЭС": os.getenv("YUGO_ZAPAD_ES_SP_URL", ""),
    "Усть-Лабинские ЭС": os.getenv("UST_LAB_ES_SP_URL", ""),
    "Тимашевские ЭС":    os.getenv("TIMASHEV_ES_SP_URL", ""),
    "Тихорецкие ЭС":     os.getenv("TIKHORETS_ES_SP_URL", ""),
    "Сочинские ЭС":      os.getenv("SOCH_ES_SP_URL", ""),
    "Славянские ЭС":     os.getenv("SLAV_ES_SP_URL", ""),
    "Ленинградские ЭС":  os.getenv("LENINGRAD_ES_SP_URL", ""),
    "Лабинские ЭС":      os.getenv("LABIN_ES_SP_URL", ""),
    "Краснодарские ЭС":  os.getenv("KRASN_ES_SP_URL", ""),
    "Армавирские ЭС":    os.getenv("ARMAVIR_ES_SP_URL", ""),
    "Адыгейские ЭС":     os.getenv("ADYGEA_ES_SP_URL", ""),
}

# === Два отдельных CSV-лога уведомлений ===
NOTIFY_LOG_FILE_UG = os.getenv("NOTIFY_LOG_FILE_UG", "notifications_log_UG.csv")
NOTIFY_LOG_FILE_RK = os.getenv("NOTIFY_LOG_FILE_RK", "notifications_log_RK.csv")
