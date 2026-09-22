import os
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

BOT_TOKEN = os.environ["BOT_TOKEN"]


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

    await message.reply_text("📥 MKV received. Conversion system is being prepared...")


def main():
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))

    app.add_handler(
        MessageHandler(
            filters.Document.ALL,
            handle_video
        )
    )

    print("🤖 Bot is running...")
    app.run_polling()


if __name__ == "__main__":
    main()