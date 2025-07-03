# main.py
import threading
import os
from flask import Flask, request, jsonify
from telegram import Update

from bot import bot, dispatcher
from utils import ping_self

app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    upd = Update.de_json(request.get_json(force=True), bot)
    dispatcher.process_update(upd)
    return jsonify({'ok': True})

if __name__ == '__main__':
    # Render прокручивает PORT и все ENV, никаких правок не нужно
    threading.Thread(target=ping_self, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)))
