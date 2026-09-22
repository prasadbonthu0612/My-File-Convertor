import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)


BOT_TOKEN = os.environ["BOT_TOKEN"]
PORT = int(os.environ.get("PORT", 10000))


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        return


def start_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    server.serve_forever()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎬 MKV → MP4 Converter\n\n"
        "Send me an MKV file and I'll convert it to MP4."
    )


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message

    if not message or not message.document:
        return

    filename = message.document.file_name or ""

    if not filename.lower().endswith(".mkv"):
        await message.reply_text("❌ Please send an MKV file.")
        return

    await message.reply_text(
        "📥 MKV received.\n"
        "Conversion system is being prepared..."
    )


def main():
    health_thread = threading.Thread(
        target=start_health_server,
        daemon=True
    )
    health_thread.start()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))

    app.add_handler(
        MessageHandler(
            filters.Document.ALL,
            handle_video
        )
    )

    print(f"🤖 Bot is running on port {PORT}...")

    app.run_polling()


if __name__ == "__main__":
    main()
