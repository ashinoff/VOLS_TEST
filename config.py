# config.py (изменён)

import os

# ... те же TOKEN, SELF_URL, PORT, ZONES_CSV_URL, BRANCH_URLS ...

# — Справочники ТП→ВЛ для уведомлений, по филиалам —
NOTIFY_URLS = {
    # для Россети Кубань
    "Юго-Западные ЭС":   os.getenv("YUGO_ZАПАД_ES_URL_SP",   ""),
    "Усть-Лабинские ЭС": os.getenv("UST_LAB_ES_URL_SP",       ""),
    "Тимашевские ЭС":    os.getenv("TIMASHEV_ES_URL_SP",      ""),
    "Тихорецкие ЭС":     os.getenv("TIKHORETS_ES_URL_SP",     ""),
    "Сочинские ЭС":      os.getenv("SOCH_ES_URL_SP",          ""),
    "Славянские ЭС":     os.getenv("SLAV_ES_URL_SP",          ""),
    "Ленинградские ЭС":  os.getenv("LENINGRAD_ES_URL_SP",     ""),
    "Лабинские ЭС":      os.getenv("LABIN_ES_URL_SP",         ""),
    "Краснодарские ЭС":  os.getenv("KRASN_ES_URL_SP",         ""),
    "Армавирские ЭС":    os.getenv("ARMAVIR_ES_URL_SP",       ""),
    "Адыгейские ЭС":     os.getenv("ADYGEA_ES_URL_SP",        ""),

    # для Россети ЮГ — аналогично, если есть тестовые справочники:
    "Центральные ЭС":      os.getenv("CENTRAL_ES_URL_SP",     ""),
    "Западные ЭС":         os.getenv("WEST_ES_URL_SP",        ""),
    "Восточные ЭС":        os.getenv("EAST_ES_URL_SP",        ""),
    "Южные ЭС":            os.getenv("SOUTH_ES_URL_SP",       ""),
    "Юго-Западные ЭС":     os.getenv("YUGO_ZAPAD_ES_URL_SP",  ""),
    "Северо-Восточные ЭС": os.getenv("NE_ES_URL_SP",          ""),
    "Юго-Восточные ЭС":    os.getenv("SE_ES_URL_SP",          ""),
    "Северные ЭС":         os.getenv("NORTH_ES_URL_SP",       ""),
}

# проверка
miss = [b for b,u in NOTIFY_URLS.items() if not u]
if miss:
    print(f"⚠️ Warning: no NOTIFY_URLS for branches: {', '.join(miss)}")
