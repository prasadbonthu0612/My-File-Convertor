print("🔥 MKV CONVERTER V7 - QUEUE + 720p + TELUGU + LIVE RESOURCE REPORT + TIME INPUT")

import os
import json
import asyncio
import subprocess
import threading
import time
import shutil
from pathlib import Path
import urllib.request
import urllib.parse
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

# One worker processes one job at a time. The queue prevents multiple
# large MKV downloads/FFmpeg jobs from competing for Render Free CPU/RAM.
JOB_QUEUE = asyncio.Queue()
JOB_RECORDS = {}
COMPLETED_JOBS = {}
JOB_COUNTER = 0
CURRENT_JOB_ID = None
CURRENT_CONVERSION_STATE = None
QUEUE_WORKER_TASK = None

# User-input setup state. Each chat can have one active setup question at a time;
# additional videos wait in FIFO order without changing the conversion worker.
PENDING_SETUP_BY_CHAT = {}

# Branding assets/configuration. Put channel_logo.png at the repository root,
# or override CHANNEL_LOGO_PATH in the Render environment.
CHANNEL_LOGO_PATH = os.environ.get("CHANNEL_LOGO_PATH", "channel_logo.png")
CHANNEL_LOGO_WIDTH = max(80, int(os.environ.get("CHANNEL_LOGO_WIDTH", "260")))
CHANNEL_LOGO_TOP = max(0, int(os.environ.get("CHANNEL_LOGO_TOP", "25")))
CHANNEL_LOGO_OPACITY = max(0.0, min(1.0, float(os.environ.get("CHANNEL_LOGO_OPACITY", "1.0"))))

# The title is rendered below the logo. Noto Sans Telugu is preferred so Telugu
# titles work; override this path if the deployment image uses another font.
TITLE_FONT_PATH = os.environ.get(
    "TITLE_FONT_PATH",
    "/usr/share/fonts/truetype/noto/NotoSansTelugu-Regular.ttf",
)
TITLE_FONT_SIZE = max(20, int(os.environ.get("TITLE_FONT_SIZE", "34")))
TITLE_TEXT_MAX_WIDTH = max(200, int(os.environ.get("TITLE_TEXT_MAX_WIDTH", "640")))
TITLE_TOP_GAP = max(0, int(os.environ.get("TITLE_TOP_GAP", "12")))

# Graphical share/follow callout assets below the title.
# Put share_icon.png and follow_icon.png at the repository root, or override
# their paths with the corresponding environment variables.
SHOW_SHARE_FOLLOW = os.environ.get("SHOW_SHARE_FOLLOW", "1").strip().lower() not in {
    "0", "false", "no", "off"
}
SHARE_ICON_PATH = os.environ.get("SHARE_ICON_PATH", "share_icon.png")
FOLLOW_ICON_PATH = os.environ.get("FOLLOW_ICON_PATH", "follow_icon.png")
SHARE_FOLLOW_ICON_SIZE = max(24, int(os.environ.get("SHARE_FOLLOW_ICON_SIZE", "42")))
SHARE_FOLLOW_FONT_SIZE = max(14, int(os.environ.get("SHARE_FOLLOW_FONT_SIZE", "22")))
SHARE_FOLLOW_GAP = max(0, int(os.environ.get("SHARE_FOLLOW_GAP", "12")))
SHARE_FOLLOW_GROUP_GAP = max(20, int(os.environ.get("SHARE_FOLLOW_GROUP_GAP", "70")))
SHARE_LABEL = os.environ.get("SHARE_LABEL", "SHARE")
FOLLOW_LABEL = os.environ.get("FOLLOW_LABEL", "FOLLOW")

# Video containers accepted by the bot. Every accepted video is converted
# to an MP4 with a 720x1280 9:16 canvas.
VIDEO_EXTENSIONS = {
    ".mkv", ".mp4", ".mov", ".avi", ".webm", ".m4v",
    ".flv", ".wmv", ".mpeg", ".mpg", ".m2ts", ".mts",
    ".ts", ".3gp", ".ogv", ".vob", ".asf", ".f4v",
}


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
        "🤖 Video Converter Bot is online!\n\n"
        "Send me a video file and I will convert it to MP4.\n\n"
        "🎵 Telugu audio preferred when available\n"
        "📱 720×1280 9:16 Reels format\n"
        "🖼️ Channel logo + title + share/follow branding\n"
        "✂️ Interactive beginning/end trimming\n"
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
    # huge 720x1280 output.
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
# BRANDING + USER INPUT HELPERS
# =========================

def _resolve_asset_path(configured_path, asset_label):
    """Resolve a branding asset from its configured path or module directory."""
    configured = Path(configured_path)
    if configured.exists() and configured.is_file():
        return str(configured)

    module_relative = Path(__file__).resolve().parent / configured_path
    if module_relative.exists() and module_relative.is_file():
        return str(module_relative)

    raise FileNotFoundError(
        f"{asset_label} PNG not found. Expected '{configured_path}' "
        "at the repository root or configured environment variable."
    )


def resolve_channel_logo_path():
    """Resolve the configured channel logo."""
    return _resolve_asset_path(CHANNEL_LOGO_PATH, "Channel logo")


def resolve_share_icon_path():
    """Resolve the configured share icon."""
    return _resolve_asset_path(SHARE_ICON_PATH, "Share icon")


def resolve_follow_icon_path():
    """Resolve the configured follow icon."""
    return _resolve_asset_path(FOLLOW_ICON_PATH, "Follow icon")


def normalize_cut_value(value):
    """Parse a non-negative cut timestamp supplied by the user.

    Accepted formats:
      - seconds: 80, 80.5
      - minutes:seconds: 01:20, 8:20, 12:05.5
      - hours:minutes:seconds: 01:02:03.5

    Internally the bot always stores/uses seconds so the existing FFmpeg
    architecture remains unchanged.
    """
    raw = str(value).strip()

    if not raw:
        raise ValueError(
            "Please enter a time such as 01:20, 8:20, or 80 seconds."
        )

    if ":" not in raw:
        try:
            seconds = float(raw)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid time. Use MM:SS (for example 01:20 or 8:20), "
                "HH:MM:SS, or seconds such as 80."
            )
    else:
        parts = raw.split(":")
        if len(parts) not in (2, 3):
            raise ValueError(
                "Invalid time format. Use MM:SS (for example 01:20 or 8:20) "
                "or HH:MM:SS."
            )

        try:
            numeric_parts = [float(part.strip()) for part in parts]
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid time format. Use MM:SS (for example 01:20 or 8:20) "
                "or HH:MM:SS."
            )

        if any(part < 0 for part in numeric_parts):
            raise ValueError("The cut time cannot be negative.")

        if len(numeric_parts) == 2:
            minutes, seconds_part = numeric_parts
            if seconds_part >= 60:
                raise ValueError(
                    "In MM:SS format, the seconds part must be below 60. "
                    "Example: 08:20."
                )
            seconds = minutes * 60 + seconds_part
        else:
            hours, minutes, seconds_part = numeric_parts
            if minutes >= 60 or seconds_part >= 60:
                raise ValueError(
                    "In HH:MM:SS format, minutes and seconds must be below 60. "
                    "Example: 01:08:20."
                )
            seconds = hours * 3600 + minutes * 60 + seconds_part

    if seconds < 0:
        raise ValueError("The cut time cannot be negative.")

    return seconds


def format_cut_time(seconds):
    """Display an internal seconds value as H:MM:SS or M:SS for users."""
    total_seconds = max(0, float(seconds))
    whole_seconds = int(total_seconds)
    milliseconds = int(round((total_seconds - whole_seconds) * 1000))

    if milliseconds >= 1000:
        whole_seconds += 1
        milliseconds = 0

    hours, remainder = divmod(whole_seconds, 3600)
    minutes, secs = divmod(remainder, 60)

    if milliseconds:
        seconds_text = f"{secs:02d}.{milliseconds:03d}".rstrip("0")
    else:
        seconds_text = f"{secs:02d}"

    if hours:
        return f"{hours:02d}:{minutes:02d}:{seconds_text}"
    return f"{minutes:02d}:{seconds_text}"


def escape_ffmpeg_text(value):
    """Escape text for FFmpeg drawtext's filter syntax."""
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace(":", "\\:")
        .replace("'", "\\'")
        .replace("%", "\\%")
        .replace("[", "\\[")
        .replace("]", "\\]")
        .replace(";", "\\;")
        .replace("\n", "\\n")
    )


def make_title_text_file(job_id, title):
    """Write title to a UTF-8 text file so Telugu/special characters survive FFmpeg."""
    path = os.path.join(WORK_DIR, f"job_{job_id}_title.txt")
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(str(title).strip())
    return path


def make_share_follow_text_file(job_id):
    path = os.path.join(WORK_DIR, f"job_{job_id}_share_follow.txt")
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(SHARE_FOLLOW_TEXT)
    return path


def calculate_trimmed_duration(source_duration, intro_cut, end_cut):
    if intro_cut < 0 or end_cut < 0:
        raise ValueError("Cut times cannot be negative.")
    if intro_cut >= source_duration:
        raise ValueError(
            f"Beginning cut ({intro_cut:.2f}s) must be shorter than the video duration ({source_duration:.2f}s)."
        )
    if end_cut > source_duration:
        raise ValueError(
            f"End-card cut ({end_cut:.2f}s) is beyond the video duration ({source_duration:.2f}s)."
        )
    if end_cut <= intro_cut:
        raise ValueError(
            f"End-card cut ({end_cut:.2f}s) must be after the beginning cut ({intro_cut:.2f}s)."
        )
    return end_cut - intro_cut


def setup_prompt_for_job(job):
    stage = job.get("setup_stage")
    if stage == "title":
        return (
            f"🎬 Job #{job['id']}\n\n"
            "What is the title of this video?\n"
            "\n"
            "Send the title as your next message."
        )
    if stage == "intro_cut":
        return (
            f"✂️ Job #{job['id']} — Beginning cut\n\n"
            "Until what time should I remove from the beginning?\n"
            "Enter MM:SS, for example 01:20, or seconds such as 80.\n"
            "Reply 00:00 if you do not want to remove anything from the beginning."
        )
    if stage == "end_cut":
        return (
            f"✂️ Job #{job['id']} — End-card cut\n\n"
            "From what time should I remove the video through the end?\n"
            "Enter MM:SS, for example 08:20, or seconds such as 500.\n"
            "This means the final output keeps only the video from the beginning-cut time up to this time."
        )
    return None


async def start_next_setup_for_chat(chat_id):
    """Ask the next pending job's current setup question, if any."""
    pending = PENDING_SETUP_BY_CHAT.get(chat_id) or []
    while pending:
        job_id = pending[0]
        job = JOB_RECORDS.get(job_id)
        if not job or job.get("state") != "awaiting_input":
            pending.pop(0)
            continue

        prompt = setup_prompt_for_job(job)
        if prompt:
            await client.send_message(chat_id, prompt)
        return

    PENDING_SETUP_BY_CHAT.pop(chat_id, None)


# =========================
# FFMPEG CONVERSION
# =========================

async def convert_mkv_to_mp4(
    input_file,
    output_file,
    status,
    conversion_state,
    job_id,
    title,
    intro_cut,
    end_cut,
):
    """
    ONE FFmpeg process:

    - Video -> 720x1280 portrait canvas
    - Original aspect ratio preserved
    - No cropping
    - No stretching
    - Black padding where necessary
    - Centered
    - Telugu audio preferred; first available audio is used if Telugu is unavailable
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

    audio_label = "Telugu"

    if telugu_audio_index is None:
        if not audio_tracks:
            raise RuntimeError(
                "No audio track was found in this video."
            )

        # For non-Telugu/single-audio videos, keep the video usable instead
        # of rejecting it. Telugu is still preferred whenever metadata exists.
        telugu_audio_index = audio_tracks[0]["index"]
        audio_label = "Original/default audio"

    # =========================
    # VIDEO DURATION + SIZE
    # =========================

    duration = get_video_duration(
        input_file
    )

    trimmed_duration = calculate_trimmed_duration(
        duration,
        intro_cut,
        end_cut,
    )

    video_bitrate, original_size = (
        calculate_target_video_bitrate(
            input_file,
            trimmed_duration,
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

    conversion_state["duration"] = trimmed_duration
    conversion_state["source_duration"] = duration
    conversion_state["intro_cut"] = intro_cut
    conversion_state["end_cut"] = end_cut
    conversion_state["video_bitrate"] = video_bitrate
    conversion_state["original_size"] = original_size
    conversion_state["audio_label"] = audio_label

    # =========================
    # PORTRAIT VIDEO + BRANDING FILTER
    # =========================

    logo_path = resolve_channel_logo_path()
    title_file = make_title_text_file(job_id, title)
    share_icon_path = resolve_share_icon_path() if SHOW_SHARE_FOLLOW else None
    follow_icon_path = resolve_follow_icon_path() if SHOW_SHARE_FOLLOW else None
    share_follow_file = None
    conversion_state["title_file"] = title_file
    conversion_state["share_follow_file"] = None
    conversion_state["share_icon_path"] = share_icon_path
    conversion_state["follow_icon_path"] = follow_icon_path

    # Keep the existing 720x1280 architecture. The source video is trimmed
    # first, then scaled/padded, then the channel logo/title/callout are placed
    # in the top area. Everything remains inside one FFmpeg process.
    # Reserve a predictable top header so the logo, title, and callout never
    # overlap regardless of the source/logo aspect ratio.
    logo_max_height = max(60, int(os.environ.get("CHANNEL_LOGO_MAX_HEIGHT", "120")))
    title_y = CHANNEL_LOGO_TOP + logo_max_height + TITLE_TOP_GAP
    share_y = title_y + TITLE_FONT_SIZE + SHARE_FOLLOW_GAP
    icon_size = SHARE_FOLLOW_ICON_SIZE
    # Two fixed groups keep the branding centered and predictable at 720px.
    # The PNGs are scaled while preserving their transparency.
    share_icon_x = max(0, int(720 / 2 - SHARE_FOLLOW_GROUP_GAP / 2 - icon_size))
    share_text_x = share_icon_x + icon_size + 8
    follow_icon_x = min(720 - icon_size, int(720 / 2 + SHARE_FOLLOW_GROUP_GAP / 2))
    follow_text_x = follow_icon_x + icon_size + 8

    video_filter = (
        f"[0:v]trim=start={intro_cut}:end={end_cut},setpts=PTS-STARTPTS,"
        "scale=720:1280:flags=fast_bilinear:force_original_aspect_ratio=decrease,"
        "pad=720:1280:(ow-iw)/2:(oh-ih)/2:color=black[base];"
        f"[0:a]atrim=start={intro_cut}:end={end_cut},asetpts=PTS-STARTPTS[aout];"
        f"[1:v]scale={CHANNEL_LOGO_WIDTH}:{logo_max_height}:flags=fast_bilinear:"
        "force_original_aspect_ratio=decrease,pad="
        f"{CHANNEL_LOGO_WIDTH}:{logo_max_height}:(ow-iw)/2:(oh-ih)/2:color=black@0[logo];"
        f"[logo]format=rgba,colorchannelmixer=aa={CHANNEL_LOGO_OPACITY}[logo2];"
        "[base][logo2]overlay="
        f"x=(W-w)/2:y={CHANNEL_LOGO_TOP}:eof_action=repeat:format=auto[branded];"
        "[branded]drawtext="
        f"fontfile='{escape_ffmpeg_text(TITLE_FONT_PATH)}':"
        "textfile='" + escape_ffmpeg_text(title_file) + "':"
        f"fontsize={TITLE_FONT_SIZE}:fontcolor=white:bordercolor=black:borderw=3:"
        "x=(w-text_w)/2:"
        f"y={title_y}:line_spacing=2:"
        "enable='between(t,0,999999)'[titled]"
    )

    if SHOW_SHARE_FOLLOW:
        # Actual PNG icon assets are composited beside their labels. This replaces
        # the old Unicode-symbol callout without changing the FFmpeg architecture.
        video_filter += (
            f";[2:v]scale={icon_size}:{icon_size}:force_original_aspect_ratio=decrease,"
            f"format=rgba[shareicon];"
            f"[3:v]scale={icon_size}:{icon_size}:force_original_aspect_ratio=decrease,"
            f"format=rgba[followicon];"
            f"[titled][shareicon]overlay=x={share_icon_x}:y={share_y}:"
            "eof_action=repeat:format=auto[sharebranded];"
            "[sharebranded]drawtext="
            f"fontfile='{escape_ffmpeg_text(TITLE_FONT_PATH)}':"
            f"text='{escape_ffmpeg_text(SHARE_LABEL)}':"
            f"fontsize={SHARE_FOLLOW_FONT_SIZE}:fontcolor=white:bordercolor=black:borderw=2:"
            f"x={share_text_x}:y={share_y + max(0, (icon_size - SHARE_FOLLOW_FONT_SIZE) // 2)}[sharetext];"
            "[sharetext][followicon]overlay="
            f"x={follow_icon_x}:y={share_y}:eof_action=repeat:format=auto[followbranded];"
            "[followbranded]drawtext="
            f"fontfile='{escape_ffmpeg_text(TITLE_FONT_PATH)}':"
            f"text='{escape_ffmpeg_text(FOLLOW_LABEL)}':"
            f"fontsize={SHARE_FOLLOW_FONT_SIZE}:fontcolor=white:bordercolor=black:borderw=2:"
            f"x={follow_text_x}:y={share_y + max(0, (icon_size - SHARE_FOLLOW_FONT_SIZE) // 2)}[vout]"
        )
    else:
        video_filter += ";[titled]null[vout]"

    # Branding assets are looped for the full output duration and remain in the
    # same single FFmpeg process, preserving the existing one-worker architecture.
    command = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-progress",
        "pipe:1",
        "-nostats",
        "-i",
        input_file,
        "-loop",
        "1",
        "-i",
        logo_path,
    ]

    if SHOW_SHARE_FOLLOW:
        command.extend([
            "-loop",
            "1",
            "-i",
            share_icon_path,
            "-loop",
            "1",
            "-i",
            follow_icon_path,
        ])

    command.extend([
        "-filter_complex",
        video_filter,
        "-map",
        "[vout]",
        "-map",
        "[aout]",
        "-c:v",
        "libx264",
        "-preset",
        "ultrafast",
        "-tune",
        "zerolatency",
        "-threads",
        "0",
        "-b:v",
        f"{video_bitrate_k}k",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-disposition:a:0",
        "default",
        "-shortest",
        "-movflags",
        "+faststart",
        output_file,
    ])

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

                    "⚙️ Converting video → MP4\n\n"

                    f"Progress: {percent:.1f}%\n"

                    f"Video processed: "
                    f"{format_time(current_time)} / "
                    f"{format_time(duration)}\n"

                    f"Encoding speed: {speed}\n"

                    f"ETA: {format_time(eta)}\n"

                    f"Elapsed: {format_time(elapsed)}\n\n"

                    "📱 Output: 720×1280 9:16\n"

                    "🎵 Audio: Telugu preferred\n"

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
            "portrait-720x1280-size-controlled+trim+branding",
            original_size,
            video_bitrate,
            trimmed_duration,
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
# QUEUE + COMMANDS
# =========================

def queue_counts():
    waiting = sum(
        1 for job in JOB_RECORDS.values()
        if job["state"] in {"queued", "awaiting_input"}
    )

    # Only one queue worker exists, so there can only be one actual
    # processing job. CURRENT_JOB_ID is the source of truth.
    current_job = JOB_RECORDS.get(CURRENT_JOB_ID)
    processing = (
        1
        if CURRENT_JOB_ID is not None
        and current_job
        and current_job.get("state") == "processing"
        else 0
    )

    failed = sum(
        1 for job in JOB_RECORDS.values()
        if job["state"] == "failed"
    )
    completed = len(COMPLETED_JOBS)
    return waiting, processing, completed, failed


def queue_position(job_id):
    queued = [
        job["id"]
        for job in JOB_RECORDS.values()
        if job["state"] == "queued"
    ]
    try:
        return queued.index(job_id) + 1
    except ValueError:
        return None


def job_summary(job):
    text = (
        f"#{job['id']} — {job['filename']}\n"
        f"Status: {job['state']}"
    )
    if job.get("title"):
        text += f"\nTitle: {job['title']}"
    if job.get("intro_cut") is not None and job.get("end_cut") is not None:
        text += f"\nKeep: {float(job['intro_cut']):g}s → {float(job['end_cut']):g}s"
    return text


@client.on(events.NewMessage(pattern=r"^/(?:help|about)$"))
async def help_handler(event):
    await event.respond(
        "🤖 MKV Converter Bot\n\n"
        "Send one or more video files. They are placed into a FIFO queue "
        "and processed one at a time.\n\n"
        "🎬 Output: 720×1280 9:16\n"
        "🎵 Telugu audio preferred when available\n"
        "🖼️ Channel branding + title overlay\n"
        "✂️ Beginning/end cut setup per video\n"
        "⚡ H.264/AAC, optimized for Render Free\n"
        "🧹 Temporary files are deleted after each job\n\n"
        "COMMANDS\n"
        "/queue — show waiting/current jobs\n"
        "/status — show current queue status\n"
        "/cancel ID... — cancel one or more queued/current jobs\n"
        "/cancelall — cancel all queued/current jobs\n"
        "/retry ID — retry a failed job\n"
        "/done — list completed outputs\n"
        "/delete ID... — delete one or more completed outputs\n"
        "/deleteall — delete all completed output messages\n"
        "/clear — alias for /deleteall\n"
        "/help — show this help"
    )


@client.on(events.NewMessage(pattern=r"^/queue$"))
async def queue_handler(event):
    waiting, processing, completed, failed = queue_counts()

    lines = [
        "📋 CONVERSION QUEUE",
        "",
        f"⚙️ Processing: {CURRENT_JOB_ID or 'None'}",
        f"⏳ Waiting: {waiting}",
        f"✅ Completed: {completed}",
        f"❌ Failed: {failed}",
    ]

    if CURRENT_JOB_ID and CURRENT_JOB_ID in JOB_RECORDS:
        lines += ["", job_summary(JOB_RECORDS[CURRENT_JOB_ID])]

    waiting_jobs = [
        job for job in JOB_RECORDS.values()
        if job["state"] in {"queued", "awaiting_input"}
    ]

    if waiting_jobs:
        lines += ["", "⏳ WAITING"]
        for job in waiting_jobs[:20]:
            pos = queue_position(job["id"])
            lines.append(
                f"#{job['id']} — {job['filename'][:45]}\n"
                f"   Position: {pos}"
            )

        if len(waiting_jobs) > 20:
            lines.append(f"...and {len(waiting_jobs) - 20} more")

    await event.respond("\n".join(lines))


@client.on(events.NewMessage(pattern=r"^/status$"))
async def status_handler(event):
    waiting, processing, completed, failed = queue_counts()

    text = (
        "📊 BOT STATUS\n\n"
        f"⚙️ Current job: {CURRENT_JOB_ID or 'None'}\n"
        f"⏳ Waiting: {waiting}\n"
        f"✅ Completed: {completed}\n"
        f"❌ Failed: {failed}\n"
    )

    if CURRENT_JOB_ID and CURRENT_CONVERSION_STATE:
        state = CURRENT_CONVERSION_STATE
        duration = state.get("duration", 0)
        current = state.get("current_time", 0)
        percent = (
            min(99.9, current * 100 / duration)
            if duration > 0 else 0
        )
        text += (
            f"\n🎬 Conversion: {percent:.1f}%\n"
            f"⚡ Speed: {state.get('speed', 'N/A')}\n"
            f"{get_resource_report()}"
        )

    await event.respond(text)


@client.on(events.NewMessage(pattern=r"^/cancel(?:\s+(.+))?$"))
async def cancel_handler(event):
    raw = event.pattern_match.group(1)

    if not raw:
        await event.respond(
            "Usage:\n"
            "/cancel ID\n"
            "/cancel 1 2 3\n"
            "/cancelall — cancel all queued + current jobs"
        )
        return

    try:
        job_ids = [int(x) for x in raw.split()]
    except ValueError:
        await event.respond("❌ Invalid job ID. Example: /cancel 3 4 5")
        return

    cancelled = []
    skipped = []
    not_found = []

    for job_id in job_ids:
        job = JOB_RECORDS.get(job_id)

        if not job or job["chat_id"] != event.chat_id:
            not_found.append(job_id)
            continue

        state = job["state"]

        if state in {"queued", "awaiting_input"}:
            job["state"] = "cancelled"
            if state == "awaiting_input":
                pending = PENDING_SETUP_BY_CHAT.get(event.chat_id, [])
                if job_id in pending:
                    pending.remove(job_id)
            job["cancel_requested"] = True
            cancelled.append(job_id)
            continue

        if state == "processing":
            job["state"] = "cancelled"
            job["cancel_requested"] = True

            conversion_state = job.get("conversion_state")
            process = (
                conversion_state.get("process")
                if conversion_state else None
            )

            if process and process.returncode is None:
                try:
                    process.kill()
                except Exception:
                    pass

            task = job.get("task")
            if task and not task.done():
                task.cancel()

            cancelled.append(job_id)
            continue

        skipped.append((job_id, state))

    lines = ["🛑 CANCEL RESULT", ""]
    lines.append(
        f"Cancelled: {', '.join('#' + str(x) for x in cancelled) or 'None'}"
    )
    lines.append(
        f"Not found: {', '.join('#' + str(x) for x in not_found) or 'None'}"
    )

    if skipped:
        lines.append(
            "Already finished: "
            + ", ".join(f"#{x} ({state})" for x, state in skipped)
        )

    await event.respond("\n".join(lines))


@client.on(events.NewMessage(pattern=r"^/cancelall$"))
async def cancel_all_handler(event):
    cancelled = []

    for job_id, job in list(JOB_RECORDS.items()):
        if job["chat_id"] != event.chat_id:
            continue

        if job["state"] in {"queued", "awaiting_input"}:
            old_state = job["state"]
            job["state"] = "cancelled"
            if old_state == "awaiting_input":
                pending = PENDING_SETUP_BY_CHAT.get(event.chat_id, [])
                if job_id in pending:
                    pending.remove(job_id)
            job["cancel_requested"] = True
            cancelled.append(job_id)

        elif job["state"] == "processing":
            job["state"] = "cancelled"
            job["cancel_requested"] = True

            conversion_state = job.get("conversion_state")
            process = (
                conversion_state.get("process")
                if conversion_state else None
            )

            if process and process.returncode is None:
                try:
                    process.kill()
                except Exception:
                    pass

            task = job.get("task")
            if task and not task.done():
                task.cancel()

            cancelled.append(job_id)

    await event.respond(
        "🛑 CANCEL ALL\n\n"
        f"Cancelled jobs: {', '.join('#' + str(x) for x in cancelled) or 'None'}"
    )


@client.on(events.NewMessage(pattern=r"^/retry\s+(\d+)$"))
async def retry_handler(event):
    job_id = int(event.pattern_match.group(1))
    old_job = JOB_RECORDS.get(job_id)

    if not old_job:
        await event.respond(f"❌ Job #{job_id} was not found.")
        return

    if old_job["state"] != "failed":
        await event.respond(
            f"ℹ️ Job #{job_id} is {old_job['state']}. Only failed jobs can be retried."
        )
        return

    new_message = await client.get_messages(
        old_job["chat_id"],
        ids=old_job["message_id"]
    )

    if not new_message or not new_message.file:
        await event.respond("❌ Original Telegram file is no longer available.")
        return

    global JOB_COUNTER
    JOB_COUNTER += 1
    new_id = JOB_COUNTER

    status = await event.respond(
        f"🔁 Retrying #{job_id} as #{new_id}..."
    )

    job = {
        "id": new_id,
        "chat_id": old_job["chat_id"],
        "message_id": old_job["message_id"],
        "filename": old_job["filename"],
        "extension": old_job.get("extension") or Path(old_job["filename"]).suffix.lower() or ".mp4",
        "status_message_id": status.id,
        "state": "queued",
        "conversion_state": None,
        "output_message_id": None,
        "title": old_job.get("title", ""),
        "intro_cut": old_job.get("intro_cut", 0),
        "end_cut": old_job.get("end_cut"),
        "setup_stage": None,
    }

    JOB_RECORDS[new_id] = job
    await JOB_QUEUE.put(new_id)

    await status.edit(
        f"🔁 Job #{new_id} queued for retry.\n"
        f"📄 {job['filename']}\n"
        "📱 Output: 720×1280 9:16"
    )


@client.on(events.NewMessage(pattern=r"^/done$"))
async def done_handler(event):
    jobs = [
        job for job in COMPLETED_JOBS.values()
        if job["chat_id"] == event.chat_id
    ]

    if not jobs:
        await event.respond("ℹ️ No completed outputs are tracked in this chat.")
        return

    lines = ["✅ COMPLETED OUTPUTS", ""]
    for job in jobs[-30:]:
        lines.append(
            f"#{job['id']} — {job['filename'][:45]}\n"
            f"   Output: {format_size(job.get('output_size', 0))}"
        )

    lines += [
        "",
        "Delete one or more:",
        "/delete 1",
        "/delete 1 2 3",
        "",
        "Delete all completed outputs here:",
        "/deleteall",
    ]

    await event.respond("\n".join(lines))


@client.on(events.NewMessage(pattern=r"^/delete\s+(.+)$"))
async def delete_handler(event):
    raw_ids = event.pattern_match.group(1).split()

    try:
        job_ids = [int(value) for value in raw_ids]
    except ValueError:
        await event.respond(
            "❌ Invalid job ID.\n"
            "Examples: /delete 3  or  /delete 3 4 5"
        )
        return

    deleted = []
    failed = []
    not_found = []

    for job_id in job_ids:
        job = COMPLETED_JOBS.get(job_id)

        if not job or job["chat_id"] != event.chat_id:
            not_found.append(job_id)
            continue

        try:
            if job.get("output_message_id"):
                await client.delete_messages(
                    job["chat_id"],
                    [job["output_message_id"]]
                )

            COMPLETED_JOBS.pop(job_id, None)
            JOB_RECORDS.pop(job_id, None)
            deleted.append(job_id)

        except Exception:
            failed.append(job_id)

    await event.respond(
        "🗑️ DELETE RESULT\n\n"
        f"Deleted: {', '.join('#' + str(x) for x in deleted) or 'None'}\n"
        f"Not found: {', '.join('#' + str(x) for x in not_found) or 'None'}\n"
        f"Failed: {', '.join('#' + str(x) for x in failed) or 'None'}"
    )


@client.on(events.NewMessage(pattern=r"^/(?:deleteall|clear)$"))
async def delete_all_handler(event):
    jobs = [
        (job_id, job)
        for job_id, job in COMPLETED_JOBS.items()
        if job["chat_id"] == event.chat_id
    ]

    if not jobs:
        await event.respond("ℹ️ There are no completed outputs to delete in this chat.")
        return

    deleted = 0
    failed = 0

    for job_id, job in jobs:
        try:
            if job.get("output_message_id"):
                await client.delete_messages(
                    job["chat_id"],
                    [job["output_message_id"]]
                )

            COMPLETED_JOBS.pop(job_id, None)
            JOB_RECORDS.pop(job_id, None)
            deleted += 1

        except Exception:
            failed += 1

    await event.respond(
        "🗑️ COMPLETED OUTPUT CLEANUP\n\n"
        f"Deleted: {deleted}\n"
        f"Failed: {failed}"
    )


async def process_job(job_id):
    global CURRENT_JOB_ID, CURRENT_CONVERSION_STATE

    job = JOB_RECORDS[job_id]
    job["state"] = "processing"
    job["task"] = asyncio.current_task()
    job["cancel_requested"] = False
    CURRENT_JOB_ID = job_id

    input_extension = job.get("extension") or Path(job["filename"]).suffix.lower() or ".mp4"
    if not input_extension.startswith("."):
        input_extension = "." + input_extension

    input_file = os.path.join(
        WORK_DIR,
        f"job_{job_id}_input{input_extension}"
    )
    output_file = os.path.join(
        WORK_DIR,
        f"job_{job_id}_output.mp4"
    )

    status = await client.get_messages(
        job["chat_id"],
        ids=job["status_message_id"]
    )

    try:
        message = await client.get_messages(
            job["chat_id"],
            ids=job["message_id"]
        )

        if not message or not message.file:
            raise RuntimeError("Original Telegram video message is no longer available.")

        if not status:
            status = await client.send_message(
                job["chat_id"],
                f"⚙️ Processing job #{job_id}..."
            )

        job["status_message_id"] = status.id

        await update_progress(
            status,
            f"📥 Job #{job_id}\n\n"
            "Downloading source video..."
        )

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

        if job["state"] == "cancelled":
            raise asyncio.CancelledError()

        source_duration = get_video_duration(input_file)
        intro_cut = float(job.get("intro_cut", 0))
        end_cut = float(job.get("end_cut", source_duration))
        title = str(job.get("title", "")).strip() or Path(job["filename"]).stem.strip() or job["filename"]

        # Validate the user-provided cuts after the actual source is downloaded.
        trimmed_duration = calculate_trimmed_duration(
            source_duration,
            intro_cut,
            end_cut,
        )

        await update_progress(
            status,
            f"🔍 Job #{job_id}\n\n"
            "Source video downloaded.\n"
            f"✂️ Trim: {format_cut_time(intro_cut)} → {format_cut_time(end_cut)}\n"
            f"🎬 Output duration: {trimmed_duration:.2f}s\n"
            "🏷️ Adding channel branding and title...\n"
            "Preparing 720×1280 9:16 conversion..."
        )

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

        job["conversion_state"] = conversion_state
        CURRENT_CONVERSION_STATE = conversion_state

        method, original_size, video_bitrate, duration = (
            await convert_mkv_to_mp4(
                input_file,
                output_file,
                status,
                conversion_state,
                job_id,
                title,
                intro_cut,
                end_cut,
            )
        )

        if job["state"] == "cancelled":
            raise asyncio.CancelledError()

        conversion_time = time.time() - conversion_start
        output_size = os.path.getsize(output_file)

        original_title = Path(job["filename"]).stem.strip()
        if not original_title:
            original_title = job["filename"]
        job["original_title"] = original_title
        job["title"] = title

        size_change = (
            (output_size - original_size) * 100 / original_size
            if original_size > 0 else 0
        )

        if output_size <= original_size:
            size_result = (
                f"✅ Output is {abs(size_change):.1f}% smaller than original."
                if output_size < original_size
                else "✅ Output is the same size as the original."
            )
        else:
            size_result = (
                f"⚠️ Output is {size_change:.1f}% larger than original."
            )

        await update_progress(
            status,
            f"⚡ Job #{job_id} conversion completed!\n\n"
            "📱 720×1280 9:16\n"
            f"🎵 Audio: {conversion_state.get('audio_label', 'Audio')}\n"
            "🎬 Original video preserved with black padding where needed.\n\n"
            f"📦 Original: {format_size(original_size)}\n"
            f"📦 Output: {format_size(output_size)}\n"
            f"{size_result}\n\n"
            f"⏱ Conversion: {format_time(conversion_time)}\n\n"
            "📤 Preparing upload..."
        )

        upload_state = {
            "start": time.time(),
            "last_update": 0,
        }

        sent_message = await client.send_file(
            job["chat_id"],
            output_file,
            caption=(
                "✅ Conversion complete\n\n"
                f"🎬 Original title: {original_title}\n"
                "📱 Format: 720×1280 9:16\n"
                f"🎵 Audio: {conversion_state.get('audio_label', 'Audio')}\n"
                f"⚡ Method: {method}\n"
                f"🆔 Job: #{job_id}\n"
                f"📦 Original: {format_size(original_size)}\n"
                f"📦 Output: {format_size(output_size)}\n"
                f"🎞 Video bitrate: {int(video_bitrate / 1000)} kbps\n"
                f"⏱ Conversion time: {format_time(conversion_time)}"
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

        output_message_id = getattr(sent_message, "id", None)
        job["output_message_id"] = output_message_id
        job["output_size"] = output_size
        job["conversion_time"] = conversion_time
        job["state"] = "completed"

        COMPLETED_JOBS[job_id] = job.copy()

        total_time = time.time() - job["start_time"]

        await status.edit(
            "✅ Finished!\n\n"
            f"🆔 Job: #{job_id}\n"
            f"🎬 Original title: {original_title}\n"
            f"🏷️ Video title: {title}\n"
            f"✂️ Kept: {format_cut_time(intro_cut)} → {format_cut_time(end_cut)}\n"
            "🖼️ Channel branding added\n"
            "📱 720×1280 9:16\n"
            f"🎵 Audio: {conversion_state.get('audio_label', 'Audio')}\n"
            f"⚡ Method: {method}\n"
            f"📦 Original: {format_size(original_size)}\n"
            f"📦 Output: {format_size(output_size)}\n"
            f"⏱ Conversion: {format_time(conversion_time)}\n"
            f"⏱ Total time: {format_time(total_time)}\n\n"
            f"🗑️ Delete this output: /delete {job_id}\n"
            "🗑️ Delete all completed outputs: /deleteall"
        )

    except asyncio.CancelledError:
        job["state"] = "cancelled"
        conversion_state = job.get("conversion_state")
        process = conversion_state.get("process") if conversion_state else None
        if process and process.returncode is None:
            try:
                process.kill()
            except Exception:
                pass
        try:
            if status:
                await status.edit(
                    f"🛑 Job #{job_id} cancelled.\n\n"
                    "Temporary files will be deleted."
                )
        except Exception:
            pass

    except Exception as e:
        if job.get("cancel_requested") or job.get("state") == "cancelled":
            job["state"] = "cancelled"
            try:
                if status:
                    await status.edit(f"🛑 Job #{job_id} cancelled.")
            except Exception:
                pass
        else:
            job["state"] = "failed"
            job["error"] = str(e)

            try:
                if status:
                    await update_progress(
                        status,
                        f"❌ Job #{job_id} failed.\n\n"
                        f"{str(e)[:3500]}\n\n"
                        f"Retry: /retry {job_id}"
                    )
            except Exception:
                pass

    finally:
        pending = PENDING_SETUP_BY_CHAT.get(job["chat_id"], [])
        if job_id in pending and job.get("state") != "awaiting_input":
            pending.remove(job_id)
        if not pending:
            PENDING_SETUP_BY_CHAT.pop(job["chat_id"], None)

        cleanup(
            input_file,
            output_file,
            (job.get("conversion_state") or {}).get("title_file"),
            (job.get("conversion_state") or {}).get("share_follow_file"),
        )
        job["conversion_state"] = None
        CURRENT_CONVERSION_STATE = None
        CURRENT_JOB_ID = None


async def queue_worker():
    while True:
        job_id = await JOB_QUEUE.get()

        try:
            job = JOB_RECORDS.get(job_id)

            if not job:
                continue

            if job["state"] != "queued":
                continue

            task = asyncio.create_task(process_job(job_id))
            job["task"] = task

            try:
                await task
            except asyncio.CancelledError:
                # process_job handles cleanup/status; keep the worker alive.
                pass

        except Exception as e:
            job = JOB_RECORDS.get(job_id)
            if job and job["state"] != "cancelled":
                job["state"] = "failed"
                job["error"] = str(e)

        finally:
            job = JOB_RECORDS.get(job_id)
            if job:
                job["task"] = None
            JOB_QUEUE.task_done()


# =========================
# MKV HANDLER
# =========================

@client.on(events.NewMessage)
async def handle_text_input(event):
    """Collect title/cut settings for the next uploaded video in this chat."""
    message = event.message

    if not message or message.file or not message.message:
        return

    if str(message.message).startswith("/"):
        return

    pending = PENDING_SETUP_BY_CHAT.get(event.chat_id) or []
    if not pending:
        return

    job_id = pending[0]
    job = JOB_RECORDS.get(job_id)
    if not job or job.get("state") != "awaiting_input":
        pending.pop(0)
        await start_next_setup_for_chat(event.chat_id)
        return

    value = str(message.message).strip()
    stage = job.get("setup_stage")

    try:
        if stage == "title":
            if not value:
                raise ValueError("Title cannot be empty.")
            job["title"] = value
            job["setup_stage"] = "intro_cut"
            await event.reply(
                f"✅ Title saved for Job #{job_id}.\n\n"
                + setup_prompt_for_job(job)
            )
            return

        if stage == "intro_cut":
            intro_cut = normalize_cut_value(value)
            job["intro_cut"] = intro_cut
            job["setup_stage"] = "end_cut"
            await event.reply(
                f"✅ Beginning cut saved: {format_cut_time(intro_cut)} ({intro_cut:g}s)\n\n"
                + setup_prompt_for_job(job)
            )
            return

        if stage == "end_cut":
            end_cut = normalize_cut_value(value)
            intro_cut = float(job.get("intro_cut", 0))
            job["end_cut"] = end_cut

            # We cannot know the exact duration until download, so only enforce
            # the relationship that can be checked now. Full duration validation
            # happens again immediately after download.
            if end_cut <= intro_cut:
                raise ValueError(
                    f"End-card cut must be after the beginning cut ({format_cut_time(intro_cut)})."
                )

            job["state"] = "queued"
            job["setup_stage"] = None
            pending.pop(0)

            await JOB_QUEUE.put(job_id)
            position = queue_position(job_id)

            status = await client.get_messages(
                job["chat_id"],
                ids=job["status_message_id"],
            )
            if status:
                await status.edit(
                    f"📋 Queued successfully\n\n"
                    f"🆔 Job: #{job_id}\n"
                    f"📄 {job['filename']}\n"
                    f"🏷️ Title: {job['title']}\n"
                    f"✂️ Keep: {format_cut_time(intro_cut)} → {format_cut_time(end_cut)}\n"
                    f"📱 Output: 720×1280 9:16\n"
                    f"🎵 Audio: Telugu preferred\n"
                    f"🖼️ Branding: channel logo + share/follow\n"
                    f"⏳ Queue position: {position or 1}\n\n"
                    "Use /queue to see all jobs."
                )

            await event.reply(
                f"✅ Job #{job_id} setup complete and added to the conversion queue."
            )
            await start_next_setup_for_chat(event.chat_id)
            return

    except ValueError as exc:
        await event.reply(f"❌ {exc}\n\n{setup_prompt_for_job(job)}")


@client.on(events.NewMessage)
async def handle_message(event):

    message = event.message

    if not message.file:
        return

    filename = message.file.name or ""
    mime_type = getattr(message.file, "mime_type", "") or ""

    extension = Path(filename).suffix.lower()
    is_video = (
        extension in VIDEO_EXTENSIONS
        or mime_type.startswith("video/")
    )

    if not is_video:
        return

    if not message.media:
        return

    global JOB_COUNTER
    JOB_COUNTER += 1
    job_id = JOB_COUNTER

    status = await event.reply(
        f"📥 Video detected.\n"
        f"🆔 Job #{job_id}\n"
        f"📄 {filename}\n\n"
        "⏸️ Waiting for video setup..."
    )

    job = {
        "id": job_id,
        "chat_id": event.chat_id,
        "message_id": message.id,
        "filename": filename,
        "extension": extension or ".mp4",
        "status_message_id": status.id,
        "state": "awaiting_input",
        "setup_stage": "title",
        "title": "",
        "intro_cut": None,
        "end_cut": None,
        "conversion_state": None,
        "output_message_id": None,
        "start_time": time.time(),
    }

    JOB_RECORDS[job_id] = job
    PENDING_SETUP_BY_CHAT.setdefault(event.chat_id, []).append(job_id)

    if len(PENDING_SETUP_BY_CHAT[event.chat_id]) == 1:
        await status.edit(
            f"📋 Job #{job_id} received\n\n"
            f"🆔 Job: #{job_id}\n"
            f"📄 {filename}\n\n"
            "🏷️ I need a title, beginning cut, and end-card cut before processing."
        )
        await start_next_setup_for_chat(event.chat_id)
    else:
        position = len(PENDING_SETUP_BY_CHAT[event.chat_id])
        await status.edit(
            f"📋 Job #{job_id} waiting for setup\n\n"
            f"📄 {filename}\n"
            f"⏳ Setup position: #{position}\n\n"
            "The bot will ask for its title and cut times after the previous video is configured."
        )


# =========================
# TELEGRAM COMMAND MENU
# =========================

BOT_COMMANDS = [
    ("start", "Start the bot"),
    ("help", "Show all commands"),
    ("about", "About this converter"),
    ("queue", "Show conversion queue"),
    ("status", "Show live bot/resource status"),
    ("cancel", "Cancel queued/current job(s)"),
    ("cancelall", "Cancel all queued/current jobs"),
    ("retry", "Retry a failed job"),
    ("done", "List completed outputs"),
    ("delete", "Delete completed output(s)"),
    ("deleteall", "Delete all completed outputs"),
    ("clear", "Alias for deleteall"),
]


async def register_bot_commands():
    """Register Telegram's native command menu via the Bot API."""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands"

    payload = json.dumps([
        {
            "command": command,
            "description": description,
        }
        for command, description in BOT_COMMANDS
    ]).encode("utf-8")

    request = urllib.request.Request(
        url,
        data=urllib.parse.urlencode(
            {"commands": payload.decode("utf-8")}
        ).encode("utf-8"),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )

    try:
        await asyncio.to_thread(
            urllib.request.urlopen,
            request,
            timeout=15,
        )
        print("Telegram command menu registered.")
    except Exception as e:
        # A menu failure must never stop the converter.
        print(f"Warning: could not register Telegram command menu: {e}")


async def main():

    print(
        "Starting Telegram converter..."
    )

    await client.start(
        bot_token=BOT_TOKEN
    )

    await register_bot_commands()

    me = await client.get_me()

    print(
        f"Bot connected: @{me.username}"
    )

    print(
        f"Health server running on port {PORT}"
    )

    global QUEUE_WORKER_TASK
    QUEUE_WORKER_TASK = asyncio.create_task(queue_worker())

    print("Queue worker started.")

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
