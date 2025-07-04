# zones.py

import re
import pandas as pd
import requests
from io import StringIO
from config import ZONES_CSV_URL

def normalize_sheet_url(url: str) -> str:
    # (ваш существующий код нормализации URL)
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
    Возвращает 5 словарей:
      vis_map[uid]       = видимость (All/UG/RK)
      raw_branch_map[uid]= «сырое» имя филиала из колонки B
      res_map[uid]       = РЭС из колонки C
      names[uid]         = ФИО из колонки E
      resp_map[uid]      = ответственный из колонки F (или "")
    """
    url = normalize_sheet_url(ZONES_CSV_URL)
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.content.decode('utf-8-sig')), header=None, skiprows=1)

    vis_map = {}
    raw_branch_map = {}
    res_map = {}
    names = {}
    resp_map = {}

    for _, row in df.iterrows():
        try:
            # в колонке D у вас ID пользователя
            uid = int(row[3])
        except Exception:
            continue

        vis_map[uid]        = str(row[0]).strip()
        raw_branch_map[uid] = str(row[1]).strip()
        res_map[uid]        = str(row[2]).strip()
        names[uid]          = str(row[4]).strip()

        # колонка F может быть пустой или NaN
        raw_resp = row[5] if 5 in row.index else None
        if pd.isna(raw_resp):
            resp_map[uid] = ""
        else:
            resp_map[uid] = str(raw_resp).strip()

    return vis_map, raw_branch_map, res_map, names, resp_map
