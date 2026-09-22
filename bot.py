import os
import asyncio
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

from telethon import TelegramClient, events
from telethon.sessions import StringSession


# =========================
# CONFIG
# =========================

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]

PORT = int(os.environ.get("PORT", "10000"))

WORK_DIR = "/tmp/mkv_converter"
os.makedirs(WORK_DIR, exist_ok=True)


# =========================
# HEALTH SERVER
# =========================

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
        pass


def start_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    server.serve_forever()


# =========================
# TELEGRAM CLIENT
# =========================

client = TelegramClient(
    StringSession(),
    API_ID,
    API_HASH,
)


# =========================
# HELPERS
# =========================

def format_size(size):

    units = ["B", "KB", "MB", "GB", "TB"]

    size = float(size)

    for unit in units:
        if size < 1024:
            return f"{size:.2f} {unit}"

        size /= 1024

    return f"{size:.2f} PB"


def format_time(seconds):

    seconds = int(seconds)

    if seconds < 60:
        return f"{seconds}s"

    minutes = seconds // 60
    seconds = seconds % 60

    if minutes < 60:
        return f"{minutes}m {seconds}s"

    hours = minutes // 60
    minutes = minutes % 60

    return f"{hours}h {minutes}m"


def cleanup(*files):

    for file in files:

        try:

            if file and os.path.exists(file):
                os.remove(file)

        except Exception:
            pass


def convert_mkv_to_mp4(input_file, output_file):

    """
    First attempt:
    stream copy = extremely fast.

    If MP4 container cannot accept the streams,
    fall back to H.264/AAC encoding.
    """

    command_copy = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",

        "-i",
        input_file,

        "-map",
        "0:v:0",

        "-map",
        "0:a?",

        "-c",
        "copy",

        "-movflags",
        "+faststart",

        output_file,
    ]

    result = subprocess.run(
        command_copy,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if result.returncode == 0:
        return "stream-copy"

    # =========================
    # FALLBACK RE-ENCODE
    # =========================

    command_encode = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",

        "-i",
        input_file,

        "-map",
        "0:v:0",

        "-map",
        "0:a?",

        "-c:v",
        "libx264",

        "-preset",
        "ultrafast",

        "-crf",
        "23",

        "-c:a",
        "aac",

        "-b:a",
        "192k",

        "-movflags",
        "+faststart",

        output_file,
    ]

    result = subprocess.run(
        command_encode,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if result.returncode != 0:

        raise RuntimeError(
            result.stderr[-3000:]
        )

    return "re-encode"


# =========================
# PROGRESS
# =========================

async def update_progress(message, text):

    try:
        await message.edit(text)

    except Exception:
        pass


async def download_progress(
    received,
    total,
    state,
    message,
):

    now = time.time()

    if total <= 0:
        return

    percent = received * 100 / total

    elapsed = now - state["start"]

    if elapsed <= 0:
        return

    speed = received / elapsed

    if now - state["last_update"] < 3 and received < total:
        return

    state["last_update"] = now

    remaining = total - received

    eta = remaining / speed if speed > 0 else 0

    text = (
        "📥 Downloading MKV\n\n"
        f"Progress: {percent:.1f}%\n"
        f"Downloaded: {format_size(received)} / {format_size(total)}\n"
        f"Speed: {format_size(speed)}/s\n"
        f"ETA: {format_time(eta)}"
    )

    await update_progress(message, text)


async def upload_progress(
    sent,
    total,
    state,
    message,
):

    now = time.time()

    if total <= 0:
        return

    percent = sent * 100 / total

    elapsed = now - state["start"]

    if elapsed <= 0:
        return

    speed = sent / elapsed

    if now - state["last_update"] < 3 and sent < total:
        return

    state["last_update"] = now

    remaining = total - sent

    eta = remaining / speed if speed > 0 else 0

    text = (
        "📤 Uploading MP4\n\n"
        f"Progress: {percent:.1f}%\n"
        f"Uploaded: {format_size(sent)} / {format_size(total)}\n"
        f"Speed: {format_size(speed)}/s\n"
        f"ETA: {format_time(eta)}"
    )

    await update_progress(message, text)


# =========================
# MKV HANDLER
# =========================

@client.on(events.NewMessage)
async def handle_message(event):

    message = event.message

    if not message.file:
        return

    filename = message.file.name or ""

    if not filename.lower().endswith(".mkv"):
        return

    # Ignore messages that don't have document/video media
    if not message.media:
        return

    input_file = os.path.join(
        WORK_DIR,
        f"input_{message.id}.mkv"
    )

    output_file = os.path.join(
        WORK_DIR,
        f"output_{message.id}.mp4"
    )

    status = await event.reply(
        "📥 MKV detected.\n"
        "Preparing download..."
    )

    start_time = time.time()

    try:

        # =========================
        # DOWNLOAD
        # =========================

        download_state = {
            "start": time.time(),
            "last_update": 0,
        }

        await message.download_media(
            file=input_file,
            progress_callback=lambda received, total:
                asyncio.create_task(
                    download_progress(
                        received,
                        total,
                        download_state,
                        status,
                    )
                ),
        )

        await update_progress(
            status,
            "🔍 MKV downloaded.\n\n"
            "Checking the fastest conversion method..."
        )

        # =========================
        # CONVERT
        # =========================

        conversion_start = time.time()

        method = await asyncio.to_thread(
            convert_mkv_to_mp4,
            input_file,
            output_file,
        )

        conversion_time = time.time() - conversion_start

        output_size = os.path.getsize(output_file)

        if method == "stream-copy":

            conversion_text = (
                "⚡ Stream-copy conversion completed!\n\n"
                "No video re-encoding was required."
            )

        else:

            conversion_text = (
                "🔄 Re-encoding was required.\n\n"
                "The MP4 was created using H.264/AAC."
            )

        await update_progress(
            status,
            conversion_text
            + f"\n\n⏱ Conversion: {format_time(conversion_time)}"
            + f"\n📦 Output: {format_size(output_size)}"
            + "\n\n📤 Preparing upload..."
        )

        # =========================
        # UPLOAD
        # =========================

        upload_state = {
            "start": time.time(),
            "last_update": 0,
        }

        await client.send_file(
            event.chat_id,
            output_file,
            caption=(
                "✅ Conversion complete\n\n"
                f"Method: {method}\n"
                f"Output: {format_size(output_size)}\n"
                f"Conversion time: {format_time(conversion_time)}"
            ),
            force_document=True,
            progress_callback=lambda sent, total:
                asyncio.create_task(
                    upload_progress(
                        sent,
                        total,
                        upload_state,
                        status,
                    )
                ),
        )

        total_time = time.time() - start_time

        await status.edit(
            "✅ Finished!\n\n"
            f"⚡ Method: {method}\n"
            f"📦 Output: {format_size(output_size)}\n"
            f"⏱ Total time: {format_time(total_time)}"
        )

    except Exception as e:

        await update_progress(
            status,
            "❌ Conversion failed.\n\n"
            f"{str(e)[:3500]}"
        )

    finally:

        cleanup(
            input_file,
            output_file,
        )


# =========================
# START
# =========================

async def main():

    print("Starting Telegram converter...")

    await client.start(
        bot_token=BOT_TOKEN
    )

    me = await client.get_me()

    print(
        f"Bot connected: @{me.username}"
    )

    print(
        f"Health server running on port {PORT}"
    )

    await client.run_until_disconnected()


if __name__ == "__main__":

    health_thread = threading.Thread(
        target=start_health_server,
        daemon=True,
    )

    health_thread.start()

    asyncio.run(main())
