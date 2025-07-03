import threading
from telegram.ext import ApplicationBuilder
from config import TOKEN, SELF_URL, PORT
from utils import ping_self

# Хендлеры
from handlers.admin import handler as admin_start
from handlers.director import handler as director_start
from handlers.line_staff import handler_start, handler_text

# Создаём Telegram Application на PTB 22+
application = ApplicationBuilder()\
    .token(TOKEN)\
    .build()

# Загружаем список филиалов и групп видимости в память бота
from config import BRANCH_URLS, VISIBILITY_GROUPS
application.bot_data["branches"] = list(BRANCH_URLS.keys())
application.bot_data["visibility_groups"] = VISIBILITY_GROUPS

# Регистрируем хендлеры
application.add_handler(admin_start)
application.add_handler(director_start)
application.add_handler(handler_start)
application.add_handler(handler_text)

if __name__ == '__main__':
    # Пинги для Render, чтобы приложение не засыпало
    threading.Thread(target=ping_self, daemon=True).start()
    # Запускаем встроенный Webhook-сервер PTB
    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=f"{SELF_URL}/webhook"
    )
