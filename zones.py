# zones.py
import re
import requests
import pandas as pd
from io import StringIO
from config import ZONES_CSV_URL

def normalize_sheet_url(url: str) -> str:
    if 'output=csv' in url or '/export' in url or url.endswith('.csv'):
        return url
    m = re.search(r'/d/e/([\w-]+)/', url)
    if m:
        return f'https://docs.google.com/spreadsheets/d/e/{m.group(1)}/export?format=csv&gid=0'
    m = re.search(r'/d/([\w-]+)', url)
    if m:
        return f'https://docs.google.com/spreadsheets/d/{m.group(1)}/export?format=csv&gid=0'
    m2 = re.search(r'/file/d/([\w-]+)', url)
    if m2:
        return f'https://drive.google.com/uc?export=download&id={m2.group(1)}'
    return url

def load_zones():
    """
    Возвращает четыре словаря:
      vis_map[uid] = "RK"/"UG"/"All"
      bz[uid]      = филиал
      rz[uid]      = РЭС
      names[uid]   = ФИО
      resp_map[uid]= ответственный (из 6-й колонки), или "" если пусто
    """
    resp = requests.get(normalize_sheet_url(ZONES_CSV_URL), timeout=10)
    resp.raise_for_status()
    df = pd.read_csv(
        StringIO(resp.content.decode('utf-8-sig')),
        header=None, skiprows=1
    )

    vis_map, bz, rz, names, resp_map = {}, {}, {}, {}, {}
    for _, row in df.iterrows():
        try:
            uid = int(row[3])  # ID в четвёртом столбце
        except:
            continue

        vis_map[uid] = str(row[0]).strip()  # RK/UG/All
        bz[uid]      = str(row[1]).strip()  # Филиал
        rz[uid]      = str(row[2]).strip()  # РЭС
        names[uid]   = str(row[4]).strip()  # ФИО

        # Ответственный — шестая колонка, может быть NaN
        val = row[5]
        if pd.isna(val):
            resp_map[uid] = ""
        else:
            resp_map[uid] = str(val).strip()

    return vis_map, bz, rz, names, resp_map
