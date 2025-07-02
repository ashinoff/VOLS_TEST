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
    Возвращает три словаря: регионы (uid->branch), рес (uid->res), имена (uid->name)
    """
    url = normalize_sheet_url(ZONES_CSV_URL)
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    text = r.content.decode('utf-8-sig')
    # читаем без заголовка, со второго ряда
    df = pd.read_csv(StringIO(text), header=None, skiprows=1)
    bz, rz, names = {}, {}, {}
    for _, row in df.iterrows():
        try:
            uid = int(row[2])  # столбец C
        except:
            continue
        bz[uid]    = str(row[0]).strip()  # столбец A: филиал или All
        rz[uid]    = str(row[1]).strip()  # столбец B: РЭС или All
        names[uid] = str(row[3]).strip()  # столбец D: ФИО
    return bz, rz, names
