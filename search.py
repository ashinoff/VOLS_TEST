import re
import pandas as pd
from zones import normalize_sheet_url
from config import BRANCH_URLS


def normalize_text(s: str) -> str:
    return re.sub(r'\W', '', str(s)).upper()


def search_tp(branch: str, tp_query: str, res_filter: str = None) -> pd.DataFrame:
    """
    Возвращает DataFrame строк из CSV по branch,
    отфильтрованных по res_filter и содержащих tp_query.
    """
    url = normalize_sheet_url(BRANCH_URLS[branch])
    df  = pd.read_csv(url)
    if res_filter:
        df = df[df["РЭС"] == res_filter]
    df.columns = df.columns.str.strip()
    df['D_UP'] = df['Наименование ТП'].apply(normalize_text)
    q = normalize_text(tp_query)
    return df[df['D_UP'].str.contains(q, na=False)]
