import threading
from telegram.ext import ApplicationBuilder
from config import TOKEN, SELF_URL, PORT
from utils import ping_self

# Единый хендлер интерфейса
from handlers.line_staff import handler_start, handler_text

application = ApplicationBuilder().token(TOKEN).build()

# Загружаем конфиги
from config import BRANCH_URLS, VISIBILITY_GROUPS
application.bot_data['branches'] = list(BRANCH_URLS.keys())
application.bot_data['visibility_groups'] = VISIBILITY_GROUPS

# Регистрируем только два хендлера
application.add_handler(handler_start)
application.add_handler(handler_text)

if __name__ == '__main__':
    threading.Thread(target=ping_self, daemon=True).start()
    application.run_webhook(
        listen='0.0.0.0',
        port=PORT,
        url_path='webhook',
        webhook_url=f'{SELF_URL}/webhook'
    )
