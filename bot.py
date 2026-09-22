print("🔥 MKV CONVERTER V3 - TELUGU AUDIO ONLY")

import os
import json
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

    server = HTTPServer(
        ("0.0.0.0", PORT),
        HealthHandler
    )

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
# START COMMAND
# =========================

@client.on(events.NewMessage(pattern=r"^/start$"))
async def start_handler(event):

    await event.respond(
        "🤖 MKV Converter Bot is online!\n\n"
        "Send me an MKV file and I will convert it to MP4.\n\n"
        "🎵 Telugu audio only\n"
        "⚡ Stream-copy conversion is used when possible.\n"
        "🔄 H.264/AAC re-encoding is used when required."
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


# =========================
# FIND TELUGU AUDIO
# =========================

def find_telugu_audio(input_file):

    """
    Inspect the MKV audio streams using ffprobe.

    Telugu can be identified through:
    - language metadata: tel
    - language metadata: te
    - language metadata: telugu
    - title containing Telugu
    - Telugu written in Telugu script
    """

    command = [

        "ffprobe",
        "-v",
        "error",

        "-select_streams",
        "a",

        "-show_entries",
        "stream=index:stream_tags=language,title",

        "-of",
        "json",

        input_file,
    ]

    result = subprocess.run(

        command,

        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,

        text=True,
    )

    if result.returncode != 0:

        raise RuntimeError(
            "Unable to inspect audio tracks.\n\n"
            + result.stderr[-2000:]
        )

    try:

        data = json.loads(result.stdout)

    except json.JSONDecodeError:

        raise RuntimeError(
            "Unable to read audio track information."
        )

    streams = data.get("streams", [])

    if not streams:
        return None, []


    audio_info = []


    for stream in streams:

        index = stream.get("index")

        tags = stream.get("tags") or {}

        language = (
            tags.get("language")
            or ""
        ).strip().lower()

        title = (
            tags.get("title")
            or ""
        ).strip().lower()


        audio_info.append({

            "index": index,

            "language": language,

            "title": title,
        })


    # =========================
    # SEARCH FOR TELUGU
    # =========================

    for audio in audio_info:

        language = audio["language"]
        title = audio["title"]


        # Exact/common language codes
        language_match = (

            language == "tel"
            or language == "te"
            or language == "telugu"
            or language.startswith("tel-")
            or language.startswith("te-")
        )


        # Track title
        title_match = (

            "telugu" in title
            or "తెలుగు" in title
        )


        if language_match or title_match:

            return audio["index"], audio_info


    return None, audio_info


# =========================
# FORMAT AUDIO INFORMATION
# =========================

def format_audio_tracks(audio_tracks):

    if not audio_tracks:
        return "No audio tracks found."

    lines = []

    for number, audio in enumerate(audio_tracks, start=1):

        language = audio["language"] or "unknown"
        title = audio["title"] or "untitled"

        lines.append(
            f"Audio {number}: "
            f"stream {audio['index']} | "
            f"language={language} | "
            f"title={title}"
        )

    return "\n".join(lines)


# =========================
# FFMPEG CONVERSION
# =========================

def convert_mkv_to_mp4(input_file, output_file):

    """
    Convert MKV to MP4 while keeping ONLY Telugu audio.

    First attempt:
        Video  -> stream copy
        Telugu -> stream copy

    This is the fastest method.

    If MP4 cannot accept the original Telugu audio codec:

        Video  -> H.264
        Telugu -> AAC

    Other audio tracks are never included.
    """


    # =========================
    # FIND TELUGU AUDIO
    # =========================

    telugu_audio_index, audio_tracks = find_telugu_audio(
        input_file
    )


    if telugu_audio_index is None:

        details = format_audio_tracks(
            audio_tracks
        )

        raise RuntimeError(

            "🇮🇳 Telugu audio track was not detected.\n\n"

            "Available audio tracks:\n"

            + details

            + "\n\n"
            "The bot did NOT select another language."
        )


    # =========================
    # STREAM COPY
    # =========================

    command_copy = [

        "ffmpeg",

        "-y",

        "-hide_banner",

        "-loglevel",
        "error",

        "-i",
        input_file,


        # Video
        "-map",
        "0:v:0",


        # ONLY Telugu audio
        "-map",
        f"0:{telugu_audio_index}",


        # No re-encoding
        "-c",
        "copy",


        # Make Telugu the default audio
        "-disposition:a:0",
        "default",


        # MP4 optimization
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

    # Delete potentially incomplete output
    cleanup(output_file)


    command_encode = [

        "ffmpeg",

        "-y",

        "-hide_banner",

        "-loglevel",
        "error",

        "-i",
        input_file,


        # Video
        "-map",
        "0:v:0",


        # ONLY Telugu audio
        "-map",
        f"0:{telugu_audio_index}",


        # Video encoding
        "-c:v",
        "libx264",

        "-preset",
        "ultrafast",

        "-crf",
        "23",


        # Telugu audio encoding
        "-c:a",
        "aac",

        "-b:a",
        "192k",


        # Make Telugu default
        "-disposition:a:0",
        "default",


        # MP4 optimization
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

    eta = (
        remaining / speed
        if speed > 0
        else 0
    )

    text = (

        "📥 Downloading MKV\n\n"

        f"Progress: {percent:.1f}%\n"

        f"Downloaded: "
        f"{format_size(received)} / "
        f"{format_size(total)}\n"

        f"Speed: "
        f"{format_size(speed)}/s\n"

        f"ETA: "
        f"{format_time(eta)}"
    )

    await update_progress(
        message,
        text
    )


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

    eta = (
        remaining / speed
        if speed > 0
        else 0
    )

    text = (

        "📤 Uploading MP4\n\n"

        f"Progress: {percent:.1f}%\n"

        f"Uploaded: "
        f"{format_size(sent)} / "
        f"{format_size(total)}\n"

        f"Speed: "
        f"{format_size(speed)}/s\n"

        f"ETA: "
        f"{format_time(eta)}"
    )

    await update_progress(
        message,
        text
    )


# =========================
# MKV HANDLER
# =========================

@client.on(events.NewMessage)
async def handle_message(event):

    message = event.message


    # =========================
    # IGNORE NON-FILES
    # =========================

    if not message.file:
        return


    filename = message.file.name or ""


    # =========================
    # ONLY MKV
    # =========================

    if not filename.lower().endswith(".mkv"):
        return


    # =========================
    # CHECK MEDIA
    # =========================

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

        "📥 MKV detected.\n\n"

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

            progress_callback=

                lambda received, total:

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

            "🎵 Detecting Telugu audio track..."
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


        conversion_time = (

            time.time()
            - conversion_start
        )


        output_size = os.path.getsize(

            output_file
        )


        # =========================
        # CONVERSION MESSAGE
        # =========================

        if method == "stream-copy":

            conversion_text = (

                "⚡ Stream-copy conversion completed!\n\n"

                "🎵 Telugu audio only\n"

                "🎬 Video was not re-encoded."
            )

        else:

            conversion_text = (

                "🔄 Re-encoding was required.\n\n"

                "🎵 Telugu audio only\n"

                "🎬 Video: H.264\n"

                "🎵 Audio: AAC"
            )


        await update_progress(

            status,

            conversion_text

            + f"\n\n⏱ Conversion: "
              f"{format_time(conversion_time)}"

            + f"\n📦 Output: "
              f"{format_size(output_size)}"

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

                f"🎵 Audio: Telugu only\n"

                f"⚡ Method: {method}\n"

                f"📦 Output: "
                f"{format_size(output_size)}\n"

                f"⏱ Conversion time: "
                f"{format_time(conversion_time)}"
            ),


            force_document=True,


            progress_callback=

                lambda sent, total:

                    asyncio.create_task(

                        upload_progress(

                            sent,

                            total,

                            upload_state,

                            status,
                        )
                    ),
        )


        # =========================
        # FINISHED
        # =========================

        total_time = (

            time.time()
            - start_time
        )


        await status.edit(

            "✅ Finished!\n\n"

            "🎵 Telugu audio only\n"

            f"⚡ Method: {method}\n"

            f"📦 Output: "
            f"{format_size(output_size)}\n"

            f"⏱ Total time: "
            f"{format_time(total_time)}"
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

            output_file
        )


# =========================
# START
# =========================

async def main():

    print(
        "Starting Telegram converter..."
    )


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


# =========================
# RUN
# =========================

if __name__ == "__main__":

    health_thread = threading.Thread(

        target=start_health_server,

        daemon=True
    )


    health_thread.start()


    asyncio.run(main())
