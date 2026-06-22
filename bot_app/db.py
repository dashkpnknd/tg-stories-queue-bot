from __future__ import annotations

import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

import aiosqlite


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path

    async def init(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        async with self.connect() as db:
            await db.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_tg_id INTEGER NOT NULL,
                    label TEXT NOT NULL,
                    phone TEXT NOT NULL,
                    session_encrypted TEXT NOT NULL,
                    channel_ref TEXT,
                    default_period_hours INTEGER NOT NULL DEFAULT 48,
                    next_publish_at INTEGER,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS queued_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_tg_id INTEGER NOT NULL,
                    account_id INTEGER NOT NULL,
                    media_path TEXT NOT NULL,
                    media_kind TEXT NOT NULL,
                    caption TEXT,
                    period_hours INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    user_story_id INTEGER,
                    channel_story_id INTEGER,
                    error TEXT,
                    created_at INTEGER NOT NULL,
                    published_at INTEGER,
                    batch_id TEXT,
                    batch_order INTEGER NOT NULL DEFAULT 1,
                    batch_total INTEGER NOT NULL DEFAULT 1,
                    FOREIGN KEY(account_id) REFERENCES accounts(id)
                );

                CREATE INDEX IF NOT EXISTS idx_accounts_owner
                    ON accounts(owner_tg_id, is_active);
                CREATE INDEX IF NOT EXISTS idx_queue_account_status
                    ON queued_items(account_id, status, created_at);
                """
            )
            await self._ensure_queued_items_columns(db)
            await db.commit()

    async def _ensure_queued_items_columns(self, db: aiosqlite.Connection) -> None:
        cursor = await db.execute("PRAGMA table_info(queued_items)")
        columns = {row["name"] for row in await cursor.fetchall()}
        if "batch_id" not in columns:
            await db.execute("ALTER TABLE queued_items ADD COLUMN batch_id TEXT")
        if "batch_order" not in columns:
            await db.execute(
                "ALTER TABLE queued_items ADD COLUMN batch_order INTEGER NOT NULL DEFAULT 1"
            )
        if "batch_total" not in columns:
            await db.execute(
                "ALTER TABLE queued_items ADD COLUMN batch_total INTEGER NOT NULL DEFAULT 1"
            )

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[aiosqlite.Connection]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            yield db

    async def add_account(
        self,
        owner_tg_id: int,
        label: str,
        phone: str,
        session_encrypted: str,
    ) -> int:
        async with self.connect() as db:
            cursor = await db.execute(
                """
                INSERT INTO accounts
                    (owner_tg_id, label, phone, session_encrypted, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (owner_tg_id, label, phone, session_encrypted, now_ts()),
            )
            await db.commit()
            return int(cursor.lastrowid)

    async def list_accounts(self, owner_tg_id: int) -> list[aiosqlite.Row]:
        async with self.connect() as db:
            cursor = await db.execute(
                """
                SELECT *
                FROM accounts
                WHERE owner_tg_id = ? AND is_active = 1
                ORDER BY id DESC
                """,
                (owner_tg_id,),
            )
            return await cursor.fetchall()

    async def get_account(self, account_id: int) -> aiosqlite.Row | None:
        async with self.connect() as db:
            cursor = await db.execute(
                "SELECT * FROM accounts WHERE id = ? AND is_active = 1",
                (account_id,),
            )
            return await cursor.fetchone()

    async def update_account_channel(
        self,
        account_id: int,
        channel_ref: str | None,
    ) -> None:
        async with self.connect() as db:
            await db.execute(
                "UPDATE accounts SET channel_ref = ? WHERE id = ?",
                (channel_ref, account_id),
            )
            await db.commit()

    async def update_account_period(self, account_id: int, period_hours: int) -> None:
        async with self.connect() as db:
            await db.execute(
                "UPDATE accounts SET default_period_hours = ? WHERE id = ?",
                (period_hours, account_id),
            )
            await db.commit()

    async def delete_account(self, account_id: int) -> None:
        async with self.connect() as db:
            await db.execute(
                "UPDATE accounts SET is_active = 0 WHERE id = ?",
                (account_id,),
            )
            await db.commit()

    async def enqueue_item(
        self,
        owner_tg_id: int,
        account_id: int,
        media_path: str,
        media_kind: str,
        caption: str | None,
        period_hours: int,
        batch_id: str | None = None,
        batch_order: int = 1,
        batch_total: int = 1,
    ) -> int:
        batch_id = batch_id or uuid.uuid4().hex
        async with self.connect() as db:
            cursor = await db.execute(
                """
                INSERT INTO queued_items
                    (owner_tg_id, account_id, media_path, media_kind, caption,
                     period_hours, created_at, batch_id, batch_order, batch_total)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    owner_tg_id,
                    account_id,
                    media_path,
                    media_kind,
                    caption,
                    period_hours,
                    now_ts(),
                    batch_id,
                    batch_order,
                    batch_total,
                ),
            )
            await db.commit()
            return int(cursor.lastrowid)

    async def enqueue_batch(
        self,
        owner_tg_id: int,
        account_id: int,
        media_paths: list[str],
        media_kind: str,
        caption: str | None,
        period_hours: int,
    ) -> list[int]:
        batch_id = uuid.uuid4().hex
        total = len(media_paths)
        item_ids = []
        for index, media_path in enumerate(media_paths, start=1):
            item_ids.append(
                await self.enqueue_item(
                    owner_tg_id=owner_tg_id,
                    account_id=account_id,
                    media_path=media_path,
                    media_kind=media_kind,
                    caption=caption if index == 1 else None,
                    period_hours=period_hours,
                    batch_id=batch_id,
                    batch_order=index,
                    batch_total=total,
                )
            )
        return item_ids

    async def list_queue(self, owner_tg_id: int) -> list[aiosqlite.Row]:
        async with self.connect() as db:
            cursor = await db.execute(
                """
                SELECT qi.*, a.label AS account_label
                FROM queued_items qi
                JOIN accounts a ON a.id = qi.account_id
                WHERE qi.owner_tg_id = ?
                  AND qi.status = 'queued'
                ORDER BY qi.created_at DESC
                LIMIT 20
                """,
                (owner_tg_id,),
            )
            return await cursor.fetchall()

    async def next_queued_item(self, account_id: int) -> aiosqlite.Row | None:
        async with self.connect() as db:
            cursor = await db.execute(
                """
                SELECT *
                FROM queued_items
                WHERE account_id = ? AND status = 'queued'
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (account_id,),
            )
            return await cursor.fetchone()

    async def queued_batch(self, account_id: int, batch_id: str | None) -> list[aiosqlite.Row]:
        if not batch_id:
            item = await self.next_queued_item(account_id)
            return [item] if item else []

        async with self.connect() as db:
            cursor = await db.execute(
                """
                SELECT *
                FROM queued_items
                WHERE account_id = ? AND status = 'queued' AND batch_id = ?
                ORDER BY batch_order ASC, created_at ASC, id ASC
                """,
                (account_id, batch_id),
            )
            return await cursor.fetchall()

    async def queue_summary_by_account(self, owner_tg_id: int) -> dict[int, dict[str, int]]:
        async with self.connect() as db:
            cursor = await db.execute(
                """
                SELECT
                    account_id,
                    COUNT(*) AS parts_count,
                    COUNT(DISTINCT COALESCE(batch_id, 'single:' || id)) AS story_count
                FROM queued_items
                WHERE owner_tg_id = ? AND status = 'queued'
                GROUP BY account_id
                """,
                (owner_tg_id,),
            )
            rows = await cursor.fetchall()
            return {
                int(row["account_id"]): {
                    "stories": int(row["story_count"]),
                    "parts": int(row["parts_count"]),
                }
                for row in rows
            }

    async def account_queue_summary(self, account_id: int) -> dict[str, int]:
        async with self.connect() as db:
            cursor = await db.execute(
                """
                SELECT
                    COUNT(*) AS parts_count,
                    COUNT(DISTINCT COALESCE(batch_id, 'single:' || id)) AS story_count
                FROM queued_items
                WHERE account_id = ? AND status = 'queued'
                """,
                (account_id,),
            )
            row = await cursor.fetchone()
            return {
                "stories": int(row["story_count"] or 0),
                "parts": int(row["parts_count"] or 0),
            }

    async def queued_batches_for_account(self, account_id: int) -> list[aiosqlite.Row]:
        async with self.connect() as db:
            cursor = await db.execute(
                """
                SELECT
                    COALESCE(batch_id, 'single:' || id) AS batch_key,
                    MIN(id) AS first_id,
                    MAX(id) AS last_id,
                    COUNT(*) AS parts_count,
                    MIN(created_at) AS created_at,
                    MIN(media_kind) AS media_kind,
                    MAX(CASE WHEN batch_order = 1 THEN caption ELSE NULL END) AS caption,
                    MAX(period_hours) AS period_hours
                FROM queued_items
                WHERE account_id = ? AND status = 'queued'
                GROUP BY COALESCE(batch_id, 'single:' || id)
                ORDER BY MIN(created_at) ASC, MIN(id) ASC
                """,
                (account_id,),
            )
            return await cursor.fetchall()

    async def accounts_ready_to_publish(self) -> list[aiosqlite.Row]:
        current = now_ts()
        async with self.connect() as db:
            cursor = await db.execute(
                """
                SELECT DISTINCT a.*
                FROM accounts a
                JOIN queued_items qi
                    ON qi.account_id = a.id AND qi.status = 'queued'
                WHERE a.is_active = 1
                  AND (a.next_publish_at IS NULL OR a.next_publish_at <= ?)
                ORDER BY COALESCE(a.next_publish_at, 0), a.id
                """,
                (current,),
            )
            return await cursor.fetchall()

    async def finish_item(
        self,
        item_id: int,
        status: str,
        user_story_id: int | None,
        channel_story_id: int | None,
        error: str | None,
    ) -> None:
        async with self.connect() as db:
            await db.execute(
                """
                UPDATE queued_items
                SET status = ?,
                    user_story_id = ?,
                    channel_story_id = ?,
                    error = ?,
                    published_at = ?
                WHERE id = ?
                """,
                (status, user_story_id, channel_story_id, error, now_ts(), item_id),
            )
            await db.commit()

    async def set_next_publish_at(self, account_id: int, timestamp: int | None) -> None:
        async with self.connect() as db:
            await db.execute(
                "UPDATE accounts SET next_publish_at = ? WHERE id = ?",
                (timestamp, account_id),
            )
            await db.commit()


def now_ts() -> int:
    return int(time.time())


def row_to_dict(row: aiosqlite.Row) -> dict[str, Any]:
    return dict(row)
