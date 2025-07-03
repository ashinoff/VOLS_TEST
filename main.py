import threading
from flask import Flask, request, jsonify
from telegram import Update
from telegram.ext import ApplicationBuilder

from config import TOKEN, SELF_URL, PORT
from utils import ping_self
from zones import load_zones
from search import search_tp

# Импорт хендлеров
from handlers.admin import handler as admin_start
from handlers.director import handler as director_start
from handlers.line_staff import handler_start, handler_text

app = Flask(__name__)

# Создаём Telegram Application
application = ApplicationBuilder()\
    .token(TOKEN)\
    .build()

# Загружаем список филиалов в память бота
from config import BRANCH_URLS
application.bot_data["branches"] = list(BRANCH_URLS.keys())

# Регистрируем хендлеры
application.add_handler(admin_start)
application.add_handler(director_start)
application.add_handler(handler_start)
application.add_handler(handler_text)

# Точка входа для Webhook
@app.route('/webhook', methods=['POST'])
def webhook():
    upd = Update.de_json(request.get_json(force=True), application.bot)
    application.update_queue.put(upd)
    return jsonify({"ok": True})

if __name__ == '__main__':
    # Пинги для Render
    threading.Thread(target=ping_self, daemon=True).start()
    # Запуск Flask
    app.run(host="0.0.0.0", port=PORT)
