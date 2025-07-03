# app.py
from flask import Flask, request, jsonify
import threading

from bot import bot, dispatcher
from utils import ping_self, get_update_json

app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    update = Update.de_json(get_update_json(), bot)
    dispatcher.process_update(update)
    return jsonify({'ok': True})

if __name__ == '__main__':
    threading.Thread(target=ping_self, daemon=True).start()
    # Render передаёт PORT в env, используем его
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)))
