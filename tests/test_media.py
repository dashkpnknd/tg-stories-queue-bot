from pathlib import Path
from unittest import TestCase

from bot_app.media import (
    MAX_STORY_BYTES,
    bot_download_limit_bytes,
    normalize_video_command,
)


def _option_value(command: list[str], option: str) -> str:
    return command[command.index(option) + 1]



class MediaTest(TestCase):
    def test_high_quality_story_video_command_keeps_story_resolution(self) -> None:
        command = normalize_video_command(
            Path("input.mp4"),
            Path("output.mp4"),
            start=0,
            duration=59,
        )

        self.assertIn("scale=1080:1920", _option_value(command, "-vf"))
        self.assertIn(_option_value(command, "-crf"), {"18", "19", "20", "21", "22"})
        self.assertEqual(_option_value(command, "-b:a"), "160k")
        self.assertGreaterEqual(int(_option_value(command, "-maxrate").rstrip("k")), 3500)

    def test_local_bot_api_removes_cloud_download_limit(self) -> None:
        self.assertEqual(bot_download_limit_bytes(None), 20 * 1024 * 1024)
        self.assertEqual(bot_download_limit_bytes(""), 20 * 1024 * 1024)
        self.assertIsNone(bot_download_limit_bytes("http://127.0.0.1:8081"))

    def test_story_file_limit_remains_telegram_story_limit(self) -> None:
        self.assertEqual(MAX_STORY_BYTES, 30 * 1024 * 1024)
