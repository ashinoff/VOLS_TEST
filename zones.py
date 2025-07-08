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
    """Возвращает пять словарей по user_id."""
    url = normalize_sheet_url(ZONES_CSV_URL)
    r = requests.get(url, timeout=10); r.raise_for_status()
    df = pd.read_csv(StringIO(r.content.decode('utf-8-sig')), header=None, skiprows=1)

    vis_map, branch_map, res_map, names, resp_map = {}, {}, {}, {}, {}
    for _, row in df.iterrows():
        try:
            uid = int(row[3])
        except:
            continue
        vis_map[uid]    = str(row[0]).strip()
        branch_map[uid] = str(row[1]).strip()
        res_map[uid]    = str(row[2]).strip()
        names[uid]      = str(row[4]).strip()
        resp_map[uid]   = str(row[5]).strip()
    return vis_map, branch_map, res_map, names, resp_map
