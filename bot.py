print("🔥 MKV CONVERTER V4 - TELUGU AUDIO + 9:16 + LIVE RESOURCE REPORT")

import os
import json
import asyncio
import subprocess
import threading
import time
import shutil
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

# Keep temporary media private inside the container.
os.umask(0o077)

os.makedirs(WORK_DIR, exist_ok=True)

# One conversion at a time prevents multiple large FFmpeg jobs from
# competing for Render Free's limited CPU/RAM.
CONVERSION_LOCK = asyncio.Semaphore(1)


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
        "📱 1080×1920 9:16 Reels format\n"
        "📦 Output is targeted not to exceed the original file size\n"
        "⚡ Fast H.264/AAC conversion with live resource monitoring."
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

    seconds = int(max(0, seconds))

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


def get_memory_usage():

    """
    Read Linux cgroup memory usage.

    On Render's Linux container this normally exposes the container's
    current memory and memory limit. No external monitoring service,
    credentials, or network request is used.

    Returns:
        current_bytes, limit_bytes, percent
    """

    current_paths = [
        "/sys/fs/cgroup/memory.current",
        "/sys/fs/cgroup/memory/memory.usage_in_bytes",
    ]

    limit_paths = [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ]

    current = None
    limit = None

    for path in current_paths:

        try:

            with open(path, "r") as f:
                value = f.read().strip()

            if value and value != "max":
                current = int(value)
                break

        except Exception:
            pass

    for path in limit_paths:

        try:

            with open(path, "r") as f:
                value = f.read().strip()

            if value and value != "max":
                limit = int(value)
                break

        except Exception:
            pass

    # Ignore an unrealistic cgroup limit.
    if limit is not None and limit > 0:

        percent = (
            current * 100 / limit
            if current is not None
            else 0
        )

        return current or 0, limit, percent

    return current or 0, None, 0


def get_disk_usage():

    try:

        usage = shutil.disk_usage(WORK_DIR)

        used = usage.total - usage.free

        percent = (
            used * 100 / usage.total
            if usage.total
            else 0
        )

        return usage.free, percent

    except Exception:

        return 0, 0


def get_resource_report():

    memory_used, memory_limit, memory_percent = (
        get_memory_usage()
    )

    disk_free, disk_percent = get_disk_usage()

    if memory_limit:

        memory_text = (
            f"{format_size(memory_used)} / "
            f"{format_size(memory_limit)} "
            f"({memory_percent:.1f}%)"
        )

    else:

        memory_text = (
            f"{format_size(memory_used)} "
            "(limit unavailable)"
        )

    return (
        f"🧠 RAM: {memory_text}\n"
        f"💾 Disk free: {format_size(disk_free)}\n"
        f"💿 Disk used: {disk_percent:.1f}%"
    )


def get_video_duration(input_file):

    command = [

        "ffprobe",
        "-v",
        "error",

        "-show_entries",
        "format=duration",

        "-of",
        "default=noprint_wrappers=1:nokey=1",

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
            "Unable to determine video duration.\n\n"
            + result.stderr[-2000:]
        )

    try:

        duration = float(result.stdout.strip())

    except (ValueError, TypeError):

        raise RuntimeError(
            "Unable to determine video duration."
        )

    if duration <= 0:

        raise RuntimeError(
            "Invalid video duration."
        )

    return duration


def calculate_target_video_bitrate(
    input_file,
    duration,
):
    """
    Calculate a conservative single-pass H.264 bitrate.

    The target is intentionally below the original file size so the
    resulting MP4 has room for AAC audio and MP4/container overhead.

    This is a single-pass bitrate-controlled encode. It is not a
    two-pass encode and therefore does not add another FFmpeg process.
    """

    original_size = os.path.getsize(input_file)

    # Target approximately 90% of the original file size.
    # This leaves a safety margin for mux/container overhead.
    target_bytes = int(original_size * 0.90)

    # Telugu AAC audio budget.
    audio_bitrate = 128_000

    # Conservative container/metadata allowance.
    overhead_bytes = 2 * 1024 * 1024

    available_video_bits = (
        (target_bytes - overhead_bytes) * 8
        - (audio_bitrate * duration)
    )

    if available_video_bits <= 0:

        # Very unusual case for extremely large-duration/very-small files.
        # Keep a usable minimum rather than generating an invalid bitrate.
        video_bitrate = 250_000

    else:

        video_bitrate = int(
            available_video_bits / duration
        )

    # Keep the bitrate within sane bounds.
    # The upper bound prevents a short source from creating a needlessly
    # huge 1080x1920 output.
    video_bitrate = max(
        250_000,
        min(video_bitrate, 8_000_000)
    )

    return video_bitrate, original_size


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

        language_match = (

            language == "tel"
            or language == "te"
            or language == "telugu"
            or language.startswith("tel-")
            or language.startswith("te-")
        )

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

async def convert_mkv_to_mp4(
    input_file,
    output_file,
    status,
    conversion_state,
):
    """
    ONE FFmpeg process:

    - Video -> 1080x1920 portrait canvas
    - Original aspect ratio preserved
    - No cropping
    - No stretching
    - Black padding where necessary
    - Centered
    - ONLY Telugu audio
    - H.264 video
    - AAC audio
    - Single-pass bitrate selected from original file size

    FFmpeg progress is read live from -progress pipe:1.
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
    # VIDEO DURATION + SIZE
    # =========================

    duration = get_video_duration(
        input_file
    )

    video_bitrate, original_size = (
        calculate_target_video_bitrate(
            input_file,
            duration,
        )
    )

    video_bitrate_k = max(
        1,
        int(video_bitrate / 1000)
    )

    disk_free, _ = get_disk_usage()
    # Keep a safety margin because the source MKV and growing MP4 coexist.
    required_free = original_size + (256 * 1024 * 1024)
    if disk_free < required_free:
        raise RuntimeError(
            "Not enough temporary disk space for safe conversion. "
            f"Need about {format_size(required_free)} free, "
            f"but only {format_size(disk_free)} is available."
        )

    conversion_state["duration"] = duration
    conversion_state["video_bitrate"] = video_bitrate
    conversion_state["original_size"] = original_size

    # =========================
    # PORTRAIT VIDEO FILTER
    # =========================

    video_filter = (
        "scale=1080:1920:flags=fast_bilinear:"
        "force_original_aspect_ratio=decrease,"
        "pad=1080:1920:"
        "(ow-iw)/2:"
        "(oh-ih)/2:"
        "color=black"
    )

    # =========================
    # ONE FFMPEG PROCESS
    # =========================

    command = [

        "ffmpeg",
        "-y",
        "-hide_banner",

        # Live machine-readable progress.
        "-progress",
        "pipe:1",
        "-nostats",

        "-i",
        input_file,

        # Video
        "-map",
        "0:v:0",

        "-vf",
        video_filter,

        "-c:v",
        "libx264",

        "-preset",
        "ultrafast",
        "-tune",
        "zerolatency",
        "-threads",
        "0",

        # Size-controlled single-pass encoding.
        "-b:v",
        f"{video_bitrate_k}k",

        "-maxrate",
        str(maxrate),

        "-bufsize",
        str(bufsize),

        # ONLY Telugu audio
        "-map",
        f"0:{telugu_audio_index}",

        "-c:a",
        "aac",

        "-b:a",
        "128k",

        "-disposition:a:0",
        "default",

        "-movflags",
        "+faststart",

        output_file,
    ]

    process = await asyncio.create_subprocess_exec(

        *command,

        stdout=asyncio.subprocess.PIPE,

        stderr=asyncio.subprocess.PIPE,
    )

    conversion_state["process"] = process

    stderr_task = asyncio.create_task(
        process.stderr.read()
    )

    last_report = 0

    try:

        while True:

            line = await process.stdout.readline()

            if not line:
                break

            line = line.decode(
                "utf-8",
                errors="ignore"
            ).strip()

            if "=" not in line:
                continue

            key, value = line.split(
                "=",
                1
            )

            if key == "out_time_ms":

                try:

                    current_time = (
                        float(value) / 1_000_000
                    )

                except ValueError:

                    continue

                conversion_state[
                    "current_time"
                ] = current_time

            elif key == "speed":

                conversion_state[
                    "speed"
                ] = value

            elif key == "fps":

                conversion_state[
                    "fps"
                ] = value

            elif key == "progress":

                conversion_state[
                    "progress_state"
                ] = value

            now = time.time()

            # Telegram status update every ~4 seconds.
            if now - last_report >= 4:

                last_report = now

                current_time = conversion_state.get(
                    "current_time",
                    0,
                )

                percent = min(
                    99.9,
                    max(
                        0,
                        current_time * 100 / duration
                    ),
                )

                elapsed = (
                    now
                    - conversion_state["start"]
                )

                speed = conversion_state.get(
                    "speed",
                    "N/A"
                )

                remaining_video = max(
                    0,
                    duration - current_time
                )

                if speed.endswith("x"):

                    try:

                        speed_value = float(
                            speed[:-1]
                        )

                        eta = (
                            remaining_video
                            / speed_value
                            if speed_value > 0
                            else 0
                        )

                    except ValueError:

                        eta = 0

                else:

                    eta = 0

                resource_report = (
                    get_resource_report()
                )

                text = (

                    "⚙️ Converting MKV → MP4\n\n"

                    f"Progress: {percent:.1f}%\n"

                    f"Video processed: "
                    f"{format_time(current_time)} / "
                    f"{format_time(duration)}\n"

                    f"Encoding speed: {speed}\n"

                    f"ETA: {format_time(eta)}\n"

                    f"Elapsed: {format_time(elapsed)}\n\n"

                    "📱 Output: 1080×1920 9:16\n"

                    "🎵 Audio: Telugu only\n"

                    f"🎞 Target video bitrate: "
                    f"{video_bitrate_k} kbps\n\n"

                    f"{resource_report}"
                )

                await update_progress(
                    status,
                    text
                )

        return_code = await process.wait()

        stderr_bytes = await stderr_task

        stderr_text = stderr_bytes.decode(
            "utf-8",
            errors="ignore"
        )

        if return_code != 0:

            raise RuntimeError(
                stderr_text[-3000:]
                if stderr_text
                else "FFmpeg conversion failed."
            )

        conversion_state[
            "current_time"
        ] = duration

        conversion_state[
            "progress_state"
        ] = "end"

        return (
            "portrait-1080x1920-size-controlled",
            original_size,
            video_bitrate,
            duration,
        )

    except Exception:

        if process.returncode is None:

            try:
                process.kill()
            except Exception:
                pass

        raise


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

        f"Downloaded: "
        f"{format_size(received)} / "
        f"{format_size(total)}\n"

        f"Speed: {format_size(speed)}/s\n"

        f"ETA: {format_time(eta)}"
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

    eta = remaining / speed if speed > 0 else 0

    text = (

        "📤 Uploading MP4\n\n"

        f"Progress: {percent:.1f}%\n"

        f"Uploaded: "
        f"{format_size(sent)} / "
        f"{format_size(total)}\n"

        f"Speed: {format_size(speed)}/s\n"

        f"ETA: {format_time(eta)}"
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

    # Ignore messages without files
    if not message.file:
        return

    filename = message.file.name or ""

    # Only process MKV
    if not filename.lower().endswith(".mkv"):
        return

    # Ignore messages without media
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
            "Preparing 1080×1920 Telugu Reels conversion..."
        )

        # =========================
        # CONVERT
        # =========================

        conversion_start = time.time()

        conversion_state = {

            "start": conversion_start,

            "last_update": 0,

            "duration": 0,

            "current_time": 0,

            "speed": "N/A",

            "fps": "N/A",

            "progress_state": "starting",

            "video_bitrate": 0,

            "original_size": 0,

            "process": None,
        }

        # Prevent multiple simultaneous large FFmpeg jobs on Render Free.
        async with CONVERSION_LOCK:

            await update_progress(

                status,

                "⏳ Waiting for the Render conversion slot...\n\n"
                "Only one large FFmpeg conversion runs at a time "
                "to protect the free-tier memory limit."
            )

            (
                method,
                original_size,
                video_bitrate,
                duration,
            ) = await convert_mkv_to_mp4(

                input_file,

                output_file,

                status,

                conversion_state,
            )

        conversion_time = (
            time.time() - conversion_start
        )

        output_size = os.path.getsize(
            output_file
        )

        size_change = (
            (output_size - original_size)
            * 100
            / original_size
            if original_size > 0
            else 0
        )

        if output_size <= original_size:

            size_result = (
                f"✅ Output is "
                f"{abs(size_change):.1f}% smaller than original."
                if output_size < original_size
                else
                "✅ Output is the same size as the original."
            )

        else:

            size_result = (
                f"⚠️ Output is "
                f"{size_change:.1f}% larger than original."
            )

        conversion_text = (

            "⚡ Conversion completed!\n\n"

            "📱 1080×1920 9:16\n"

            "🎵 Telugu audio only\n"

            "🎬 Original video preserved with "
            "black padding where needed.\n\n"

            f"📦 Original: {format_size(original_size)}\n"

            f"📦 Output: {format_size(output_size)}\n"

            f"{size_result}"
        )

        await update_progress(

            status,

            conversion_text

            + f"\n\n⏱ Conversion: "
              f"{format_time(conversion_time)}"

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

                "📱 Format: 1080×1920 9:16\n"

                "🎵 Audio: Telugu only\n"

                f"⚡ Method: {method}\n"

                f"📦 Original: "
                f"{format_size(original_size)}\n"

                f"📦 Output: "
                f"{format_size(output_size)}\n"

                f"🎞 Video bitrate: "
                f"{int(video_bitrate / 1000)} kbps\n"

                f"⏱ Conversion time: "
                f"{format_time(conversion_time)}"
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

        # =========================
        # FINISHED
        # =========================

        total_time = time.time() - start_time

        await status.edit(

            "✅ Finished!\n\n"

            "📱 1080×1920 9:16\n"

            "🎵 Telugu audio only\n"

            f"⚡ Method: {method}\n"

            f"📦 Original: "
            f"{format_size(original_size)}\n"

            f"📦 Output: "
            f"{format_size(output_size)}\n"

            f"⏱ Conversion: "
            f"{format_time(conversion_time)}\n"

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

        # Always delete temporary source/output files.
        # This prevents old media from accumulating on the Render
        # ephemeral filesystem.
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
