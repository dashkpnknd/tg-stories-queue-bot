from __future__ import annotations

import re
from dataclasses import dataclass


_TELEGRAM_MESSAGE_LINK_RE = re.compile(
    r"(?:https?://)?t\.me/(?P<chat>c/\d+|[A-Za-z0-9_]{5,})/(?P<message_id>\d+)"
)


@dataclass(frozen=True)
class TelegramMessageLink:
    chat_ref: str | int
    message_id: int


def parse_telegram_message_link(text: str) -> TelegramMessageLink | None:
    match = _TELEGRAM_MESSAGE_LINK_RE.search(text.strip())
    if not match:
        return None

    chat = match.group("chat")
    message_id = int(match.group("message_id"))
    if chat.startswith("c/"):
        internal_id = chat.split("/", 1)[1]
        return TelegramMessageLink(chat_ref=int(f"-100{internal_id}"), message_id=message_id)

    return TelegramMessageLink(chat_ref=chat, message_id=message_id)
