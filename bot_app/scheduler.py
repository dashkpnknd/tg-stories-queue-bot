from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from bot_app.config import Config
from bot_app.db import Database, now_ts
from bot_app.media import split_story_video
from bot_app.security import SessionCipher
from bot_app.telegram_stories import send_archived_story, send_story, user_client

logger = logging.getLogger(__name__)
DISPLAY_TZ = ZoneInfo("Europe/Moscow")


class StoryScheduler:
    def __init__(
        self,
        config: Config,
        db: Database,
        cipher: SessionCipher,
        bot: Any | None = None,
    ) -> None:
        self.config = config
        self.db = db
        self.cipher = cipher
        self.bot = bot
        self._lock = asyncio.Lock()

    async def run_forever(self) -> None:
        while True:
            try:
                await self.publish_due_items()
            except Exception:
                logger.exception("Publish cycle failed")
            await asyncio.sleep(self.config.publish_check_seconds)

    async def publish_due_items(self) -> None:
        if self._lock.locked():
            return
        async with self._lock:
            accounts = await self.db.accounts_ready_to_publish()
            if accounts:
                logger.info("Found %s account(s) ready to publish", len(accounts))
            for account in accounts:
                await self._publish_next_for_account(account)

    async def _publish_next_for_account(self, account) -> None:
        item = await self.db.next_queued_item(account["id"])
        if item is None:
            return

        batch_id = item["batch_id"] if "batch_id" in item.keys() else None
        batch_items = await self.db.queued_batch(account["id"], batch_id)
        if not batch_items:
            return

        session = self.cipher.decrypt(account["session_encrypted"])
        results = []

        async with user_client(
            self.config.api_id,
            self.config.api_hash,
            session,
            proxy=self.config.telethon_proxy(),
        ) as client:
            for batch_item in batch_items:
                results.append(await self._publish_one_item(client, account, batch_item))

        has_user_story = any(result["user_story_id"] is not None for result in results)
        next_at = None
        if has_user_story:
            if item["scheduled_at"] is not None:
                # Random campaigns carry their own durable timestamps. Do not
                # replace them with a fixed 48-hour cadence after a restart.
                next_at = await self.db.next_scheduled_at(account["id"])
                await self.db.set_next_publish_at(account["id"], None)
            else:
                next_at = now_ts() + int(item["period_hours"]) * 3600
                await self.db.set_next_publish_at(account["id"], next_at)

        await self._notify_results(account, batch_items, results, next_at)
        logger.info(
            "Published batch for account %s: %s",
            account["id"],
            ", ".join(f"#{result['item_id']}={result['status']}" for result in results),
        )

    async def _publish_one_item(self, client, account, item) -> dict[str, Any]:
        source_story_id = item["source_story_id"] if "source_story_id" in item.keys() else None
        if source_story_id is not None:
            user_story_id = None
            channel_story_id = None
            try:
                user_story_id = await asyncio.wait_for(
                    send_archived_story(
                        client, int(source_story_id), item["caption"], self.config.media_dir
                    ),
                    timeout=300,
                )
                if user_story_id is None:
                    raise RuntimeError("Telegram не вернул ID новой Story; нужна проверка вручную")
                if account["channel_ref"]:
                    # Save the account publication before trying the channel.
                    # A channel failure must never cause a second account post.
                    await self.db.finish_item(
                        item["id"], "partial", user_story_id, None,
                        "Публикация в канал не завершена",
                    )
                    channel_story_id = await asyncio.wait_for(
                        send_archived_story(
                            client, int(source_story_id), item["caption"],
                            self.config.media_dir, peer=account["channel_ref"],
                        ),
                        timeout=300,
                    )
                    if channel_story_id is None:
                        raise RuntimeError("Telegram не вернул ID Story в канале; нужна проверка вручную")
                await self.db.finish_item(
                    item["id"], "published", user_story_id, channel_story_id, None
                )
                return self._result(item, "published", user_story_id, channel_story_id, None)
            except Exception as exc:
                error = str(exc)
                status = "partial" if user_story_id is not None else "failed"
                await self.db.finish_item(
                    item["id"], status, user_story_id, channel_story_id, error
                )
                logger.exception("Failed to publish archived Story %s", source_story_id)
                return self._result(item, status, user_story_id, channel_story_id, error)

        media_path = Path(item["media_path"])
        user_story_id = None
        channel_story_id = None

        if not media_path.exists():
            error = f"Media file not found: {media_path}"
            await self.db.finish_item(item["id"], "failed", None, None, error)
            return self._result(item, "failed", None, None, error)

        try:
            logger.info(
                "Publishing queued item %s for account %s from %s",
                item["id"],
                account["id"],
                media_path,
            )
            user_story_id, media_path = await self._send_story_with_retry(
                client=client,
                peer="self",
                media_path=media_path,
                media_kind=item["media_kind"],
                caption=item["caption"],
                period_hours=item["period_hours"],
            )
            if user_story_id is None:
                raise RuntimeError("Telegram не вернул ID новой Story; нужна проверка вручную")
            logger.info("Published queued item %s to user story %s", item["id"], user_story_id)

            if account["channel_ref"]:
                await self.db.finish_item(
                    item["id"], "partial", user_story_id, None,
                    "Публикация в канал не завершена",
                )
                channel_story_id, media_path = await self._send_story_with_retry(
                    client=client,
                    peer=account["channel_ref"],
                    media_path=media_path,
                    media_kind=item["media_kind"],
                    caption=item["caption"],
                    period_hours=item["period_hours"],
                )
                if channel_story_id is None:
                    raise RuntimeError("Telegram не вернул ID Story в канале; нужна проверка вручную")

            await self.db.finish_item(item["id"], "published", user_story_id, channel_story_id, None)
            self._remove_media_files(Path(item["media_path"]), media_path)
            return self._result(item, "published", user_story_id, channel_story_id, None)

        except Exception as exc:
            status = "partial" if user_story_id is not None else "failed"
            error = str(exc)
            await self.db.finish_item(item["id"], status, user_story_id, channel_story_id, error)
            if user_story_id is not None:
                self._remove_media_files(Path(item["media_path"]), media_path)
            logger.exception("Failed to publish queued item %s", item["id"])
            return self._result(item, status, user_story_id, channel_story_id, error)

    @staticmethod
    def _remove_media_files(original_path: Path, prepared_path: Path) -> None:
        """A published item must not leave its upload or normalized copy behind."""
        for path in {original_path, prepared_path}:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                logger.warning("Could not remove published media file %s", path, exc_info=True)

    async def _send_story_with_retry(
        self,
        client,
        peer,
        media_path: Path,
        media_kind: str,
        caption: str | None,
        period_hours: int,
    ) -> tuple[int | None, Path]:
        try:
            story_id = await asyncio.wait_for(
                send_story(
                    client=client,
                    peer=peer,
                    media_path=str(media_path),
                    media_kind=media_kind,
                    caption=caption,
                    period_hours=period_hours,
                ),
                timeout=240,
            )
            return story_id, media_path
        except Exception as exc:
            if media_kind != "video" or "MEDIA_FILE_INVALID" not in str(exc):
                raise

            logger.warning("Telegram rejected %s as media file invalid, normalizing and retrying", media_path)
            normalized_paths = split_story_video(media_path, self.config.media_dir / "parts")
            if len(normalized_paths) != 1:
                raise RuntimeError(
                    "Video was normalized into multiple parts during retry; upload it again so it can enter the queue correctly."
                ) from exc

            normalized_path = normalized_paths[0]
            story_id = await asyncio.wait_for(
                send_story(
                    client=client,
                    peer=peer,
                    media_path=str(normalized_path),
                    media_kind=media_kind,
                    caption=caption,
                    period_hours=period_hours,
                ),
                timeout=240,
            )
            return story_id, normalized_path

    def _result(
        self,
        item,
        status: str,
        user_story_id: int | None,
        channel_story_id: int | None,
        error: str | None,
    ) -> dict[str, Any]:
        return {
            "item_id": item["id"],
            "status": status,
            "user_story_id": user_story_id,
            "channel_story_id": channel_story_id,
            "error": error,
        }

    async def _send_owner_message(self, owner_tg_id: int, text: str) -> None:
        if self.bot is None:
            return
        try:
            await self.bot.send_message(owner_tg_id, text)
        except Exception:
            logger.exception("Failed to send notification to %s", owner_tg_id)

    async def _notify_success(
        self,
        account,
        item,
        user_story_id: int | None,
        channel_story_id: int | None,
        next_at: int | None,
    ) -> None:
        await self._send_owner_message(
            int(item["owner_tg_id"]),
            "\n".join(
                [
                    "Story опубликована",
                    f"очередь: #{item['id']}",
                    f"аккаунт: {account['label']}",
                    f"аккаунт story_id: {user_story_id}",
                    f"канал story_id: {channel_story_id}" if channel_story_id else "канал: не подключён",
                    f"срок: {item['period_hours']} ч",
                    f"следующая публикация: {format_ts(next_at)}" if next_at else "очередь завершена",
                ]
            ),
        )

    async def _notify_partial(
        self,
        account,
        item,
        user_story_id: int | None,
        error: str,
    ) -> None:
        await self._send_owner_message(
            int(item["owner_tg_id"]),
            "\n".join(
                [
                    "Story частично опубликована",
                    f"очередь: #{item['id']}",
                    f"аккаунт: {account['label']}",
                    f"на аккаунт: да, story_id={user_story_id}",
                    "в канал: нет",
                    f"ошибка канала: {error}",
                ]
            ),
        )

    async def _notify_failure(self, account, item, error: str) -> None:
        await self._send_owner_message(
            int(item["owner_tg_id"]),
            "\n".join(
                [
                    "Story не опубликована",
                    f"очередь: #{item['id']}",
                    f"аккаунт: {account['label']}",
                    f"тип: {item['media_kind']}",
                    f"ошибка: {error}",
                ]
            ),
        )

    async def _notify_results(
        self,
        account,
        items,
        results: list[dict[str, Any]],
        next_at: int | None,
    ) -> None:
        if len(items) == 1:
            item = items[0]
            result = results[0]
            if result["status"] == "published":
                await self._notify_success(
                    account,
                    item,
                    result["user_story_id"],
                    result["channel_story_id"],
                    next_at,
                )
            elif result["status"] == "partial":
                await self._notify_partial(
                    account,
                    item,
                    result["user_story_id"],
                    result["error"] or "unknown error",
                )
            else:
                await self._notify_failure(account, item, result["error"] or "unknown error")
            if result["status"] == "published" and not await self.db.has_queued_items(account["id"]):
                await self._send_owner_message(
                    int(item["owner_tg_id"]),
                    f"✅ Очередь для аккаунта {account['label']} пуста. Добавь новую пачку, когда будет контент.",
                )
            return

        published = [result for result in results if result["user_story_id"] is not None]
        failed = [result for result in results if result["status"] == "failed"]
        partial = [result for result in results if result["status"] == "partial"]
        if len(published) == len(results) and not partial:
            title = "Story опубликована"
        elif published:
            title = "Story опубликована частично"
        else:
            title = "Story не опубликована"

        first_id = items[0]["id"]
        last_id = items[-1]["id"]
        lines = [
            title,
            f"очередь: #{first_id}-#{last_id}",
            f"аккаунт: {account['label']}",
            f"части: {len(published)}/{len(results)}",
            "аккаунт story_id: "
            + ", ".join(str(result["user_story_id"]) for result in published if result["user_story_id"]),
        ]
        channel_ids = [str(result["channel_story_id"]) for result in results if result["channel_story_id"]]
        lines.append("канал story_id: " + ", ".join(channel_ids) if channel_ids else "канал: не подключён")
        lines.append(f"срок: {items[0]['period_hours']} ч")
        if next_at:
            lines.append(f"следующая публикация после: {format_ts(next_at)}")
        if failed or partial:
            lines.append("ошибки:")
            for result in failed + partial:
                lines.append(f"#{result['item_id']}: {result['error']}")

        await self._send_owner_message(int(items[0]["owner_tg_id"]), "\n".join(lines))

        # A final positive confirmation is more useful than silently ending
        # the batch: it tells the operator that new content can be added.
        if all(result["status"] == "published" for result in results) and not await self.db.has_queued_items(account["id"]):
            await self._send_owner_message(
                int(items[0]["owner_tg_id"]),
                f"✅ Очередь для аккаунта {account['label']} пуста. Добавь новую пачку, когда будет контент.",
            )


def format_ts(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, DISPLAY_TZ).strftime("%Y-%m-%d %H:%M:%S МСК")
