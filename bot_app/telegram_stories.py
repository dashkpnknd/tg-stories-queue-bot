from __future__ import annotations

import random
import logging
from contextlib import asynccontextmanager

from telethon import TelegramClient, functions, types
from telethon.sessions import StringSession

from bot_app.media import build_story_media


ALLOWED_PERIOD_HOURS = {6, 12, 24, 48}
logger = logging.getLogger(__name__)


@asynccontextmanager
async def user_client(api_id: int, api_hash: str, session_string: str, proxy=None):
    client = TelegramClient(
        StringSession(session_string),
        api_id,
        api_hash,
        proxy=proxy,
    )
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise RuntimeError("Telegram session is not authorized anymore.")
        yield client
    finally:
        await client.disconnect()


async def send_story(
    client: TelegramClient,
    peer,
    media_path: str,
    media_kind: str,
    caption: str | None,
    period_hours: int,
) -> int | None:
    if period_hours not in ALLOWED_PERIOD_HOURS:
        raise ValueError("Story period must be one of 6, 12, 24, 48 hours.")

    input_peer = await resolve_story_peer(client, peer)
    await client(functions.stories.CanSendStoryRequest(peer=input_peer))

    logger.info("Uploading story media %s", media_path)
    media = await build_story_media(client, media_path, media_kind)
    logger.info("Sending story request for %s", media_path)
    updates = await client(
        functions.stories.SendStoryRequest(
            peer=input_peer,
            media=media,
            privacy_rules=[types.InputPrivacyValueAllowAll()],
            random_id=random.randint(-(1 << 63), (1 << 63) - 1),
            period=period_hours * 3600,
            caption=caption or "",
        )
    )
    return extract_story_id(updates)


async def resolve_story_peer(client: TelegramClient, peer):
    if peer == "self":
        return types.InputPeerSelf()
    if isinstance(peer, str) and peer.startswith("channel:"):
        _, channel_id, access_hash = peer.split(":", 2)
        return types.InputPeerChannel(
            channel_id=int(channel_id),
            access_hash=int(access_hash),
        )
    return await client.get_input_entity(peer)


def extract_story_id(updates) -> int | None:
    for update in getattr(updates, "updates", []) or []:
        story_id = getattr(update, "id", None)
        if story_id is not None:
            return int(story_id)
        story = getattr(update, "story", None)
        story_id = getattr(story, "id", None)
        if story_id is not None:
            return int(story_id)
    return None
