import os
TOKEN         = os.getenv("TOKEN")
SELF_URL      = os.getenv("SELF_URL","").rstrip('/')
PORT          = int(os.getenv("PORT", 5000))
ZONES_CSV_URL = os.getenv("ZONES_CSV_URL","")
BRANCH_URLS   = {
    "Юго-Западные ЭС": os.getenv("YUGO_ZAPAD_ES_URL",""),
    "Усть-Лабинские ЭС": os.getenv("UST_LAB_ES_URL",""),
    "Тимашевские ЭС":    os.getenv("TIMASHEV_ES_URL",""),
    "Тихорецкие ЭС":     os.getenv("TIKHORETS_ES_URL",""),
    "Сочинские ЭС":      os.getenv("SOCH_ES_URL",""),
    "Славянские ЭС":     os.getenv("SLAV_ES_URL",""),
    "Ленинградские ЭС":  os.getenv("LENINGRAD_ES_URL",""),
    "Лабинские ЭС":      os.getenv("LABIN_ES_URL",""),
    "Краснодарские ЭС":  os.getenv("KRASN_ES_URL",""),
    "Армавирские ЭС":    os.getenv("ARMAVIR_ES_URL",""),
    "Адыгейские ЭС":     os.getenv("ADYGEA_ES_URL",""),
}
VISIBILITY_GROUPS = {
    "Россети ЮГ": list(BRANCH_URLS.keys())[:7],
    "Россети Кубань": list(BRANCH_URLS.keys())[7:],
}
