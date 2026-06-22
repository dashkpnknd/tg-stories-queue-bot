from __future__ import annotations

import json
import mimetypes
import math
import shutil
import subprocess
from pathlib import Path

from telethon import types


MAX_STORY_BYTES = 30 * 1024 * 1024
MAX_BOT_DOWNLOAD_BYTES = 20 * 1024 * 1024
MAX_STORY_VIDEO_SECONDS = 60
STORY_VIDEO_SPLIT_SECONDS = 59
VIDEO_TARGET_BITRATE = "850k"
VIDEO_TARGET_BUFSIZE = "1700k"


async def build_story_media(client, media_path: str, media_kind: str):
    uploaded = await client.upload_file(media_path)
    if media_kind == "photo":
        return types.InputMediaUploadedPhoto(file=uploaded)

    duration, width, height = probe_video(media_path)
    mime_type = mimetypes.guess_type(media_path)[0] or "video/mp4"
    return types.InputMediaUploadedDocument(
        file=uploaded,
        mime_type=mime_type,
        attributes=[
            types.DocumentAttributeVideo(
                duration=duration,
                w=width,
                h=height,
                supports_streaming=True,
            )
        ],
    )


def probe_video(media_path: str) -> tuple[int, int, int]:
    metadata = probe_video_metadata(media_path)
    if metadata is not None:
        return metadata
    return 15, 1080, 1920


def probe_video_metadata(media_path: str) -> tuple[int, int, int] | None:
    try:
        output = subprocess.check_output(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=width,height,duration",
                "-of",
                "json",
                media_path,
            ],
            text=True,
            timeout=5,
        )
        streams = json.loads(output).get("streams") or []
        stream = streams[0] if streams else {}
        width = int(stream.get("width") or 1080)
        height = int(stream.get("height") or 1920)
        duration = max(1, math.ceil(float(stream.get("duration") or 15)))
        return duration, width, height
    except Exception:
        return None


def ensure_story_file(path: Path) -> None:
    if path.stat().st_size > MAX_STORY_BYTES:
        raise ValueError("Telegram Stories поддерживает медиа до 30 MB.")


def ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None and shutil.which("ffprobe") is not None


def split_story_video(path: Path, output_dir: Path, max_seconds: int = STORY_VIDEO_SPLIT_SECONDS) -> list[Path]:
    if not ffmpeg_available():
        raise ValueError(
            "Для подготовки видео на сервере нужен ffmpeg. Я не смогла обработать ролик автоматически."
        )

    metadata = probe_video_metadata(str(path))
    if metadata is None:
        raise ValueError("Не удалось прочитать длительность видео через ffprobe.")

    duration, _, _ = metadata
    output_dir.mkdir(parents=True, exist_ok=True)
    total = math.ceil(duration / max_seconds)
    parts: list[Path] = []
    for index in range(total):
        start = index * max_seconds
        part_duration = min(max_seconds, duration - start)
        part_path = output_dir / f"{path.stem}_part_{index + 1:02d}_of_{total:02d}.mp4"
        subprocess.run(
            normalize_video_command(path, part_path, start, part_duration),
            check=True,
            timeout=max(180, part_duration * 10),
        )
        ensure_story_file(part_path)
        parts.append(part_path)

    path.unlink(missing_ok=True)
    return parts


def normalize_video_command(source: Path, output: Path, start: int, duration: int) -> list[str]:
    return [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        str(start),
        "-i",
        str(source),
        "-t",
        str(duration),
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-vf",
        "scale=608:1080:force_original_aspect_ratio=decrease,pad=608:1080:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=30",
        "-c:v",
        "libx264",
        "-preset",
        "superfast",
        "-profile:v",
        "high",
        "-level",
        "3.1",
        "-crf",
        "30",
        "-maxrate",
        VIDEO_TARGET_BITRATE,
        "-bufsize",
        VIDEO_TARGET_BUFSIZE,
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-b:a",
        "96k",
        "-ar",
        "44100",
        "-movflags",
        "+faststart",
        str(output),
    ]
