from __future__ import annotations

import random
import logging
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path

from telethon import TelegramClient, functions, types
from telethon.sessions import StringSession

from bot_app.media import build_story_media
from bot_app.media import ensure_story_file


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
    text = caption or ""
    # Telegram measures entity offsets in UTF-16 units, rather than Python
    # characters. This keeps a bold caption correct even when it includes
    # emoji, flags, or other non-BMP characters.
    entities = [types.MessageEntityBold(offset=0, length=_telegram_text_length(text))] if text else []
    updates = await client(
        functions.stories.SendStoryRequest(
            peer=input_peer,
            media=media,
            privacy_rules=[types.InputPrivacyValueAllowAll()],
            random_id=random.randint(-(1 << 63), (1 << 63) - 1),
            period=period_hours * 3600,
            pinned=False,
            caption=text,
            entities=entities,
        )
    )
    return extract_story_id(updates)


async def get_own_stories(client: TelegramClient, story_ids: list[int]):
    """Fetch full stories from the authenticated account, including archived ones."""
    result = await client(
        functions.stories.GetStoriesByIDRequest(peer=types.InputPeerSelf(), id=story_ids)
    )
    return {
        story.id: story
        for story in result.stories
        if isinstance(story, types.StoryItem)
    }


def archived_story_media_kind(story) -> str:
    media = story.media
    if isinstance(media, types.MessageMediaPhoto) and isinstance(media.photo, types.Photo):
        return "photo"
    if isinstance(media, types.MessageMediaDocument) and isinstance(media.document, types.Document):
        if not media.document.mime_type.startswith("video/"):
            raise ValueError(f"Story #{story.id} is not a photo or video")
        return "video"
    raise ValueError(f"Story #{story.id} has unsupported media")


def _existing_story_media(story):
    kind = archived_story_media_kind(story)
    source = story.media.photo if kind == "photo" else story.media.document
    if kind == "photo":
        return types.InputMediaPhoto(
            id=types.InputPhoto(source.id, source.access_hash, source.file_reference)
        )
    return types.InputMediaDocument(
        id=types.InputDocument(source.id, source.access_hash, source.file_reference)
    )


async def send_archived_story(
    client: TelegramClient,
    story_id: int,
    caption: str | None,
    media_dir: Path,
    peer="self",
) -> int | None:
    """Reuse Telegram media; download one temporary file only if Telegram rejects reuse."""
    stories = await get_own_stories(client, [story_id])
    story = stories.get(story_id)
    if story is None:
        raise ValueError(f"Исходная Story #{story_id} больше недоступна в этом аккаунте")
    kind = archived_story_media_kind(story)
    input_peer = await resolve_story_peer(client, peer)
    await client(functions.stories.CanSendStoryRequest(peer=input_peer))
    text = caption or ""
    entities = [types.MessageEntityBold(offset=0, length=_telegram_text_length(text))] if text else []
    music = getattr(story, "music", None)
    input_music = (
        types.InputDocument(music.id, music.access_hash, music.file_reference)
        if isinstance(music, types.Document) else None
    )
    kwargs = dict(
        peer=input_peer,
        privacy_rules=[types.InputPrivacyValueAllowAll()],
        random_id=random.randint(-(1 << 63), (1 << 63) - 1),
        period=48 * 3600,
        pinned=False,
        caption=text,
        entities=entities,
        media_areas=getattr(story, "media_areas", None),
        music=input_music,
    )
    try:
        updates = await client(
            functions.stories.SendStoryRequest(media=_existing_story_media(story), **kwargs)
        )
        return extract_story_id(updates)
    except Exception as exc:
        if not any(code in str(exc) for code in ("MEDIA_FILE_INVALID", "FILE_REFERENCE_EXPIRED", "FILE_REFERENCE_INVALID")):
            raise
        logger.warning("Direct media reuse failed for Story %s; trying one temporary download", story_id)

    media_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="archive-story-", dir=media_dir) as temp_dir:
        target = Path(temp_dir) / ("source.jpg" if kind == "photo" else "source.mp4")
        downloaded = await client.download_media(story.media, file=str(target))
        if not downloaded:
            raise RuntimeError(f"Не удалось скачать исходную Story #{story_id}")
        ensure_story_file(Path(downloaded))
        media = await build_story_media(client, str(downloaded), kind)
        kwargs["random_id"] = random.randint(-(1 << 63), (1 << 63) - 1)
        updates = await client(functions.stories.SendStoryRequest(media=media, **kwargs))
        return extract_story_id(updates)


def _telegram_text_length(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


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
