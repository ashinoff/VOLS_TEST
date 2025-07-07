import re
import aiohttp
import pandas as pd
from io import BytesIO
from config import ZONES_CSV_URL

def normalize_sheet_url(url: str) -> str:
    """Нормализует URL для Google Sheets или Drive."""
    if not url:
        raise ValueError("URL не указан")
    if url.startswith("file://"):
        return url.replace("file://", "")
    if 'output=csv' in url or '/export' in url or url.endswith('.csv'):
        return url
    if 'docs.google.com' in url:
        m = re.search(r'/d/e/([\w-]+)/', url) or re.search(r'/d/([\w-]+)', url)
        if m:
            return f'https://docs.google.com/spreadsheets/d/{m.group(1)}/export?format=csv&gid=0'
    if 'drive.google.com' in url:
        m = re.search(r'/file/d/([\w-]+)', url)
        if m:
            return f'https://drive.google.com/uc?export=download&id={m.group(1)}'
    raise ValueError(f"Неподдерживаемый формат URL: {url}")

async def load_zones():
    """
    Загружает данные о зонах доступа из CSV.
    Возвращает 5 словарей:
      vis_map[uid]    = 'All'/'UG'/'RK'
      branch_map[uid] = название филиала (или 'All')
      res_map[uid]    = название РЭС  (или 'All')
      names[uid]      = ФИО
      resp_map[uid]   = признак ответственного (строка из колонки F или "")
    Ожидаем CSV со столбцами:
      A: видимость (All/UG/RK), B: Филиал, C: РЭС, D: Telegram ID, E: ФИО, F: Ответственный
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(normalize_sheet_url(ZONES_CSV_URL), timeout=10) as response:
                response.raise_for_status()
                df = pd.read_csv(BytesIO(await response.read()), header=None)
    except aiohttp.ClientConnectionError:
        raise ValueError("Ошибка соединения с сервером зон")
    except pd.errors.ParserError:
        raise ValueError("Ошибка формата CSV-файла зон")
    except pd.errors.EmptyDataError:
        raise ValueError("CSV-файл зон пуст")
    except Exception as e:
        raise ValueError(f"Неизвестная ошибка при загрузке зон: {e}")

    vis_map, branch_map, res_map, names, resp_map = {}, {}, {}, {}, {}
    for _, row in df.iterrows():
        try:
            uid = int(row[3])
            if uid <= 0:
                raise ValueError("Некорректный Telegram ID")
            vis_map[uid] = str(row[0]).strip()
            branch_map[uid] = str(row[1]).strip()
            res_map[uid] = str(row[2]).strip()
            names[uid] = str(row[4]).strip()
            raw_resp = row[5]
            resp_map[uid] = str(raw_resp).strip() if pd.notna(raw_resp) else ""
        except (ValueError, TypeError):
            continue
    if not vis_map:
        raise ValueError("CSV-файл зон не содержит валидных записей")
    return vis_map, branch_map, res_map, names, resp_map

async def load_zones_cached(context, ttl=3600):
    """Кэширует данные зон с TTL 1 час."""
    cache_key = "zones_data"
    if cache_key not in context.bot_data or context.bot_data[cache_key]["expires"] < time.time():
        context.bot_data[cache_key] = {
            "data": await load_zones(),
            "expires": time.time() + ttl
        }
    return context.bot_data[cache_key]["data"]
