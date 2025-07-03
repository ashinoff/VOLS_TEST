import time
import threading
import requests
from config import SELF_URL


def ping_self():
    """Периодически дергает SELF_URL/webhook, чтобы Render не засыпал."""
    if not SELF_URL:
        return
    while True:
        try:
            requests.get(f"{SELF_URL}/webhook", timeout=5)
        except:
            pass
        time.sleep(300)
