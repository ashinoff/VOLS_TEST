import os
import threading
import time
import requests
from flask import Flask, request, jsonify
from telegram import Bot, Update
from telegram.ext import Dispatcher, CommandHandler, MessageHandler, Filters

import config
from handlers.start import start
from handlers.tp_search import handle_text

app = Flask(__name__)
bot = Bot(token=config.TOKEN)
dispatcher = Dispatcher(bot, None, use_context=True)

# регистрация хэндлеров
dispatcher.add_handler(CommandHandler('start', start))
dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_text))

@app.route('/webhook', methods=['POST'])
def webhook():
    upd = Update.de_json(request.get_json(force=True), bot)
    dispatcher.process_update(upd)
    return jsonify({'ok': True})

# self-ping для непрерывной работы сервиса
def ping_self():
    if not config.SELF_URL:
        return
    while True:
        try:
            requests.get(f"{config.SELF_URL}/webhook")
        except:
            pass
        time.sleep(300)

if __name__ == '__main__':
    threading.Thread(target=ping_self, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)))
