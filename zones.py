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
    """Возвращает четыре словаря: visibility_by_uid, branch_by_uid, res_by_uid, name_by_uid"""
    url = normalize_sheet_url(ZONES_CSV_URL)
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    df = pd.read_csv(
        StringIO(resp.content.decode('utf-8-sig')),
        header=None, skiprows=1
    )
    vis_map, bz, rz, names = {}, {}, {}, {}
    for _, row in df.iterrows():
        try:
            uid = int(row[3])  # ID теперь в 4-й колонке (индекс 3)
        except:
            continue
        vis_map[uid] = row[0].strip()   # RK/RU/All
        bz[uid]      = row[1].strip()   # Филиал
        rz[uid]      = row[2].strip()   # РЭС
        names[uid]   = row[4].strip()   # ФИО
    return vis_map, bz, rz, names
