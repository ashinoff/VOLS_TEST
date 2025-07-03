# utils.py
import re
import time
import threading
import requests
import pandas as pd
from io import StringIO
from flask import request
from telegram import ReplyKeyboardMarkup

from config import SELF_URL, ZONES_CSV_URL, BRANCH_URLS, BRANCHES

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
    url = normalize_sheet_url(ZONES_CSV_URL)
    r   = requests.get(url, timeout=10); r.raise_for_status()
    df  = pd.read_csv(StringIO(r.content.decode('utf-8-sig')), header=None, skiprows=1)
    bz, rz, names = {}, {}, {}
    for _, row in df.iterrows():
        try:
            uid = int(row[2])
        except:
            continue
        bz[uid]    = row[0].strip()
        rz[uid]    = row[1].strip()
        names[uid] = row[3].strip()
    return bz, rz, names

def kb_select_branch():
    return ReplyKeyboardMarkup([[b] for b in BRANCHES], resize_keyboard=True)

def kb_search_select():
    return ReplyKeyboardMarkup([["Поиск по ТП"], ["Выбор филиала"]], resize_keyboard=True)

def kb_only_select():
    return ReplyKeyboardMarkup([["Выбор филиала"]], resize_keyboard=True)

def send_long(message, text: str, reply_markup=None):
    MAX = 4000
    lines = text.split('\n')
    chunk = ""
    for line in lines:
        if len(chunk) + len(line) + 1 > MAX:
            message.reply_text(chunk.strip(), reply_markup=reply_markup)
            chunk = ""
        chunk += line + "\n"
    if chunk:
        message.reply_text(chunk.strip(), reply_markup=reply_markup)

def ping_self():
    if not SELF_URL:
        return
    while True:
        try:
            requests.get(f"{SELF_URL}/webhook")
        except:
            pass
        time.sleep(300)

# Flask helper to extract JSON body
def get_update_json():
    return request.get_json(force=True)
