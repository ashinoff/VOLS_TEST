import os

# === ENVIRONMENT VARIABLES ===
TOKEN         = os.getenv("TOKEN")
SELF_URL      = os.getenv("SELF_URL", "").rstrip('/')
ZONES_CSV_URL = os.getenv("ZONES_CSV_URL", "").strip()

# === Филиальные таблицы ===
# Пока один тестовый филиал
BRANCH_URLS = {"Тимашевские ЭС": os.getenv("TIMASHEV_ES_URL", ""),}
