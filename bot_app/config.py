from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

import socks
from dotenv import load_dotenv


@dataclass(frozen=True)
class Config:
    bot_token: str
    api_id: int
    api_hash: str
    admin_ids: set[int]
    fernet_key: str
    database_path: Path
    media_dir: Path
    publish_check_seconds: int
    telethon_proxy_url: str | None

    @classmethod
    def from_env(cls) -> "Config":
        load_dotenv()

        bot_token = _required("BOT_TOKEN")
        api_id = int(_required("TELEGRAM_API_ID"))
        api_hash = _required("TELEGRAM_API_HASH")
        fernet_key = _required("FERNET_KEY")

        admin_ids_raw = os.getenv("ADMIN_IDS", "").strip()
        admin_ids = {
            int(item.strip())
            for item in admin_ids_raw.split(",")
            if item.strip()
        }

        database_path = Path(os.getenv("DATABASE_PATH", "./data/bot.sqlite3"))
        media_dir = Path(os.getenv("MEDIA_DIR", "./data/media"))
        publish_check_seconds = int(os.getenv("PUBLISH_CHECK_SECONDS", "60"))
        telethon_proxy_url = os.getenv("TELETHON_PROXY_URL", "").strip() or None

        return cls(
            bot_token=bot_token,
            api_id=api_id,
            api_hash=api_hash,
            admin_ids=admin_ids,
            fernet_key=fernet_key,
            database_path=database_path,
            media_dir=media_dir,
            publish_check_seconds=publish_check_seconds,
            telethon_proxy_url=telethon_proxy_url,
        )

    def is_admin(self, user_id: int | None) -> bool:
        if user_id is None:
            return False
        return not self.admin_ids or user_id in self.admin_ids

    def telethon_proxy(self):
        if not self.telethon_proxy_url:
            return None

        parsed = urlparse(self.telethon_proxy_url)
        schemes = {
            "http": socks.HTTP,
            "socks4": socks.SOCKS4,
            "socks5": socks.SOCKS5,
        }
        proxy_type = schemes.get(parsed.scheme.lower())
        if proxy_type is None or not parsed.hostname or not parsed.port:
            raise RuntimeError("Invalid TELETHON_PROXY_URL format")

        username = unquote(parsed.username) if parsed.username else None
        password = unquote(parsed.password) if parsed.password else None
        if username:
            return (proxy_type, parsed.hostname, parsed.port, True, username, password or "")
        return (proxy_type, parsed.hostname, parsed.port, True)


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value or value == "replace_me":
        raise RuntimeError(f"Set {name} in .env")
    return value
