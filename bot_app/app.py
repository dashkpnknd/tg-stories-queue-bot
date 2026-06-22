from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
import uuid
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import qrcode
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command, CommandStart
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BufferedInputFile,
    Message,
)
from telethon import TelegramClient, functions
from telethon.errors import (
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession
from telethon.tl.types import Channel, InputPeerChannel

from bot_app.config import Config
from bot_app.db import Database, now_ts
from bot_app.media import (
    MAX_BOT_DOWNLOAD_BYTES,
    bot_download_limit_bytes,
    ensure_story_file,
    split_story_video,
)
from bot_app.scheduler import StoryScheduler
from bot_app.security import SessionCipher
from bot_app.source_links import TelegramMessageLink, parse_telegram_message_link
from bot_app.telegram_stories import ALLOWED_PERIOD_HOURS, user_client

logger = logging.getLogger(__name__)
router = Router()
DISPLAY_TZ = ZoneInfo("Europe/Moscow")

LOGIN_FLOWS: dict[int, dict[str, Any]] = {}
QR_LOGIN_FLOWS: dict[int, dict[str, Any]] = {}


class AddAccount(StatesGroup):
    phone = State()
    code = State()
    password = State()


class BindChannel(StatesGroup):
    ref = State()


class UploadStory(StatesGroup):
    media = State()


class QrAccount(StatesGroup):
    password = State()


async def run() -> None:
    config = Config.from_env()
    db = Database(config.database_path)
    await db.init()
    config.media_dir.mkdir(parents=True, exist_ok=True)

    cipher = SessionCipher(config.fernet_key)
    bot_session_kwargs = {}
    if config.bot_api_base_url:
        bot_session_kwargs["api"] = TelegramAPIServer.from_base(config.bot_api_base_url)
    bot_session = AiohttpSession(**bot_session_kwargs)
    bot_session._connector_init["family"] = socket.AF_INET
    bot = Bot(config.bot_token, session=bot_session)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    scheduler = StoryScheduler(config=config, db=db, cipher=cipher, bot=bot)
    scheduler_task = asyncio.create_task(scheduler.run_forever())

    try:
        await dp.start_polling(
            bot,
            polling_timeout=3,
            config=config,
            db=db,
            cipher=cipher,
        )
    finally:
        scheduler_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await scheduler_task
        await bot.session.close()


@router.message(CommandStart())
async def start(message: Message, config: Config, state: FSMContext) -> None:
    await state.clear()
    if not await require_admin_message(message, config):
        return
    await message.answer(
        "Готов. Здесь можно подключать аккаунты, каналы и очередь Stories.",
        reply_markup=main_menu(),
    )


@router.message(Command("cancel"))
async def cancel(message: Message, config: Config, state: FSMContext) -> None:
    if not await require_admin_message(message, config):
        return
    await cleanup_login_flow(message.from_user.id if message.from_user else 0)
    await cleanup_qr_login_flow(message.from_user.id if message.from_user else 0)
    await state.clear()
    await message.answer("Ок, текущее действие отменено.", reply_markup=main_menu())


@router.message(Command("accounts"))
async def accounts_command(message: Message, config: Config, db: Database) -> None:
    if not await require_admin_message(message, config):
        return
    await message.answer("Аккаунты:", reply_markup=await accounts_keyboard(db, message.from_user.id))


@router.message(Command("queue"))
async def queue_command(message: Message, config: Config, db: Database) -> None:
    if not await require_admin_message(message, config):
        return
    await show_queue(message, db)


@router.callback_query(F.data == "menu")
async def menu_callback(callback: CallbackQuery, config: Config, state: FSMContext) -> None:
    if not await require_admin_callback(callback, config):
        return
    await cleanup_login_flow(callback.from_user.id)
    await cleanup_qr_login_flow(callback.from_user.id)
    await state.clear()
    await callback.message.edit_text("Главное меню:", reply_markup=main_menu())
    await callback.answer()


@router.callback_query(F.data == "accounts")
async def accounts_callback(callback: CallbackQuery, config: Config, db: Database) -> None:
    if not await require_admin_callback(callback, config):
        return
    await callback.message.edit_text(
        "Аккаунты:",
        reply_markup=await accounts_keyboard(db, callback.from_user.id),
    )
    await callback.answer()


@router.callback_query(F.data == "add_account")
async def add_account_callback(callback: CallbackQuery, config: Config, state: FSMContext) -> None:
    if not await require_admin_callback(callback, config):
        return
    await cleanup_login_flow(callback.from_user.id)
    await state.set_state(AddAccount.phone)
    await callback.message.edit_text(
        "Введи номер телефона аккаунта в международном формате, например `+79991234567`.",
        parse_mode="Markdown",
        reply_markup=back_keyboard(),
    )
    await callback.answer()


@router.callback_query(F.data == "add_account_qr")
async def add_account_qr_callback(
    callback: CallbackQuery,
    config: Config,
    db: Database,
    cipher: SessionCipher,
    state: FSMContext,
) -> None:
    if not await require_admin_callback(callback, config):
        return

    await cleanup_login_flow(callback.from_user.id)
    await cleanup_qr_login_flow(callback.from_user.id)

    client = TelegramClient(
        StringSession(),
        config.api_id,
        config.api_hash,
        proxy=config.telethon_proxy(),
    )

    try:
        await client.connect()
        qr_login = await client.qr_login()
    except Exception as exc:
        await client.disconnect()
        await callback.message.answer(
            "Не удалось подключиться к Telegram для QR-входа.\n"
            f"Ошибка: {exc}",
            reply_markup=main_menu(),
        )
        await callback.answer()
        return

    task = asyncio.create_task(
        wait_for_qr_login(
            message=callback.message,
            user_id=callback.from_user.id,
            client=client,
            qr_login=qr_login,
            db=db,
            cipher=cipher,
            state=state,
        )
    )
    QR_LOGIN_FLOWS[callback.from_user.id] = {"client": client, "task": task}

    png = make_qr_png(qr_login.url)
    await callback.message.answer_photo(
        BufferedInputFile(png, filename="telegram-login-qr.png"),
        caption=(
            "Отсканируй QR с нужного аккаунта:\n"
            "Telegram → Settings → Devices → Link Desktop Device.\n"
            "QR действует недолго, поэтому сканируй сразу."
        ),
        reply_markup=back_keyboard(),
    )
    await callback.answer()


@router.message(QrAccount.password)
async def add_account_qr_password(
    message: Message,
    config: Config,
    db: Database,
    cipher: SessionCipher,
    state: FSMContext,
) -> None:
    if not await require_admin_message(message, config):
        return

    flow = QR_LOGIN_FLOWS.get(message.from_user.id)
    if not flow or not flow.get("client"):
        await state.clear()
        await message.answer("QR-сессия потерялась. Нажми «Добавить по QR» еще раз.", reply_markup=main_menu())
        return

    client: TelegramClient = flow["client"]
    try:
        await client.sign_in(password=(message.text or "").strip())
        account_id = await save_authorized_client(message.from_user.id, db, cipher, client)
        me = await client.get_me()
        label = account_label(me)
    except Exception as exc:
        await message.answer(f"Пароль не подошел или Telegram отказал во входе: {exc}")
        return

    await cleanup_qr_login_flow(message.from_user.id)
    await state.clear()
    await message.answer(
        f"Аккаунт подключен: {label}\nID в панели: {account_id}",
        reply_markup=main_menu(),
    )


@router.message(AddAccount.phone)
async def add_account_phone(
    message: Message,
    config: Config,
    state: FSMContext,
) -> None:
    if not await require_admin_message(message, config):
        return

    phone = (message.text or "").strip()
    client = TelegramClient(
        StringSession(),
        config.api_id,
        config.api_hash,
        proxy=config.telethon_proxy(),
    )

    try:
        await client.connect()
        sent = await client.send_code_request(phone)
    except PhoneNumberInvalidError:
        await client.disconnect()
        await message.answer("Telegram не принял номер. Проверь формат и отправь еще раз.")
        return
    except Exception as exc:
        await client.disconnect()
        await message.answer(f"Не удалось отправить код: {exc}")
        return

    LOGIN_FLOWS[message.from_user.id] = {
        "client": client,
        "phone": phone,
        "phone_code_hash": sent.phone_code_hash,
    }
    await state.set_state(AddAccount.code)
    delivery = describe_code_delivery(sent)
    logger.info("Login code requested for %s: %s", phone, describe_sent_code(sent))
    await message.answer(
        f"Код запрошен. Способ доставки от Telegram: {delivery}.\n"
        f"{describe_sent_code(sent)}\n"
        "Обычно код приходит не по SMS, а в приложение Telegram от служебного аккаунта Telegram.\n"
        "Пришли код сюда одним сообщением.\n"
        "После входа сессия сохранится на сервере в зашифрованном виде.",
        reply_markup=code_keyboard(),
    )


@router.callback_query(F.data == "resend_login_code")
async def resend_login_code_callback(callback: CallbackQuery, config: Config) -> None:
    if not await require_admin_callback(callback, config):
        return

    flow = LOGIN_FLOWS.get(callback.from_user.id)
    if not flow:
        await callback.answer("Сначала начни добавление аккаунта заново.", show_alert=True)
        return

    try:
        sent = await flow["client"].send_code_request(flow["phone"])
    except Exception as exc:
        await callback.answer(f"Не удалось запросить код повторно: {exc}", show_alert=True)
        return

    flow["phone_code_hash"] = sent.phone_code_hash
    delivery = describe_code_delivery(sent)
    logger.info("Login code resent for %s: %s", flow["phone"], describe_sent_code(sent))
    await callback.message.answer(
        f"Код запрошен повторно. Способ доставки от Telegram: {delivery}.\n"
        f"{describe_sent_code(sent)}\n"
        "Проверь служебный чат Telegram на этом аккаунте. Если Telegram разрешит SMS, он сам покажет это как способ доставки.",
        reply_markup=code_keyboard(),
    )
    await callback.answer()


@router.message(AddAccount.code)
async def add_account_code(
    message: Message,
    config: Config,
    db: Database,
    cipher: SessionCipher,
    state: FSMContext,
) -> None:
    if not await require_admin_message(message, config):
        return

    flow = LOGIN_FLOWS.get(message.from_user.id)
    if not flow:
        await state.clear()
        await message.answer("Сессия входа потерялась. Начни добавление аккаунта заново.")
        return

    code = (message.text or "").strip().replace(" ", "")
    client: TelegramClient = flow["client"]

    try:
        await client.sign_in(
            phone=flow["phone"],
            code=code,
            phone_code_hash=flow["phone_code_hash"],
        )
    except SessionPasswordNeededError:
        await state.set_state(AddAccount.password)
        await message.answer("На аккаунте включена 2FA. Пришли пароль одним сообщением.")
        return
    except PhoneCodeInvalidError:
        await message.answer("Код не подошел. Пришли новый код или нажми /cancel.")
        return
    except Exception as exc:
        await cleanup_login_flow(message.from_user.id)
        await state.clear()
        await message.answer(f"Не удалось войти: {exc}", reply_markup=main_menu())
        return

    await save_authorized_account(message, db, cipher, state)


@router.message(AddAccount.password)
async def add_account_password(
    message: Message,
    config: Config,
    db: Database,
    cipher: SessionCipher,
    state: FSMContext,
) -> None:
    if not await require_admin_message(message, config):
        return

    flow = LOGIN_FLOWS.get(message.from_user.id)
    if not flow:
        await state.clear()
        await message.answer("Сессия входа потерялась. Начни добавление аккаунта заново.")
        return

    try:
        await flow["client"].sign_in(password=(message.text or "").strip())
    except Exception as exc:
        await message.answer(f"Пароль не подошел или Telegram отказал во входе: {exc}")
        return

    await save_authorized_account(message, db, cipher, state)


@router.callback_query(F.data.startswith("account:"))
async def account_detail_callback(callback: CallbackQuery, config: Config, db: Database) -> None:
    if not await require_admin_callback(callback, config):
        return
    account_id = int(callback.data.split(":")[1])
    account = await db.get_account(account_id)
    if not account or account["owner_tg_id"] != callback.from_user.id:
        await callback.answer("Аккаунт не найден.", show_alert=True)
        return
    await callback.message.edit_text(
        await format_account_text(db, account),
        reply_markup=account_keyboard(account),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bind_channel:"))
async def bind_channel_callback(callback: CallbackQuery, config: Config, db: Database, state: FSMContext) -> None:
    if not await require_admin_callback(callback, config):
        return
    account_id = int(callback.data.split(":")[1])
    account = await db.get_account(account_id)
    if not account or account["owner_tg_id"] != callback.from_user.id:
        await callback.answer("Аккаунт не найден.", show_alert=True)
        return
    await state.set_state(BindChannel.ref)
    await state.update_data(account_id=account_id)
    await callback.message.edit_text(
        "Пришли `@username` канала или ссылку `https://t.me/channel`.\n"
        "Аккаунт должен быть админом канала с правом Stories.",
        parse_mode="Markdown",
        reply_markup=back_to_account_keyboard(account_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("choose_channel:"))
async def choose_channel_callback(
    callback: CallbackQuery,
    config: Config,
    db: Database,
    cipher: SessionCipher,
) -> None:
    if not await require_admin_callback(callback, config):
        return
    account_id = int(callback.data.split(":")[1])
    account = await db.get_account(account_id)
    if not account or account["owner_tg_id"] != callback.from_user.id:
        await callback.answer("Аккаунт не найден.", show_alert=True)
        return

    await callback.answer("Ищу каналы...")
    try:
        channels = await list_admin_channels(config, cipher, account)
    except Exception as exc:
        await callback.message.edit_text(
            f"Не удалось получить список каналов: {exc}",
            reply_markup=back_to_account_keyboard(account_id),
        )
        return

    if not channels:
        await callback.message.edit_text(
            "Не нашла каналов, где этот аккаунт админ. Проверь, что аккаунт добавлен в канал администратором.",
            reply_markup=back_to_account_keyboard(account_id),
        )
        return

    await callback.message.edit_text(
        "Выбери канал для привязки:",
        reply_markup=channels_keyboard(account_id, channels),
    )


@router.callback_query(F.data.startswith("select_channel:"))
async def select_channel_callback(
    callback: CallbackQuery,
    config: Config,
    db: Database,
    cipher: SessionCipher,
) -> None:
    if not await require_admin_callback(callback, config):
        return

    _, account_id_raw, channel_id_raw, access_hash_raw = callback.data.split(":", 3)
    account_id = int(account_id_raw)
    account = await db.get_account(account_id)
    if not account or account["owner_tg_id"] != callback.from_user.id:
        await callback.answer("Аккаунт не найден.", show_alert=True)
        return

    channel_ref = f"channel:{channel_id_raw}:{access_hash_raw}"
    try:
        session = cipher.decrypt(account["session_encrypted"])
        async with user_client(
            config.api_id,
            config.api_hash,
            session,
            proxy=config.telethon_proxy(),
        ) as client:
            await client(
                functions.stories.CanSendStoryRequest(
                    peer=InputPeerChannel(
                        channel_id=int(channel_id_raw),
                        access_hash=int(access_hash_raw),
                    )
                )
            )
    except Exception as exc:
        await callback.message.edit_text(
            "Не удалось проверить право Stories для канала. Проверь права админа и бусты.\n"
            f"Ошибка: {exc}",
            reply_markup=back_to_account_keyboard(account_id),
        )
        return

    await db.update_account_channel(account_id, channel_ref)
    account = await db.get_account(account_id)
    await callback.message.edit_text(
        "Канал привязан.\n\n" + (await format_account_text(db, account)),
        reply_markup=account_keyboard(account),
    )
    await callback.answer()


@router.message(BindChannel.ref)
async def bind_channel_ref(
    message: Message,
    config: Config,
    db: Database,
    cipher: SessionCipher,
    state: FSMContext,
) -> None:
    if not await require_admin_message(message, config):
        return

    data = await state.get_data()
    account_id = int(data["account_id"])
    account = await db.get_account(account_id)
    if not account or account["owner_tg_id"] != message.from_user.id:
        await state.clear()
        await message.answer("Аккаунт не найден.", reply_markup=main_menu())
        return

    channel_ref = normalize_channel_ref((message.text or "").strip())
    try:
        session = cipher.decrypt(account["session_encrypted"])
        async with user_client(
            config.api_id,
            config.api_hash,
            session,
            proxy=config.telethon_proxy(),
        ) as client:
            input_peer = await client.get_input_entity(channel_ref)
            await client(functions.stories.CanSendStoryRequest(peer=input_peer))
    except Exception as exc:
        await message.answer(
            "Не удалось проверить канал. Проверь username, права админа, право Stories "
            f"и бусты канала.\nОшибка: {exc}"
        )
        return

    await db.update_account_channel(account_id, channel_ref)
    await state.clear()
    await message.answer(
        f"Канал привязан: {channel_ref}",
        reply_markup=account_keyboard(await db.get_account(account_id)),
    )


@router.callback_query(F.data.startswith("unbind_channel:"))
async def unbind_channel_callback(callback: CallbackQuery, config: Config, db: Database) -> None:
    if not await require_admin_callback(callback, config):
        return
    account_id = int(callback.data.split(":")[1])
    account = await db.get_account(account_id)
    if not account or account["owner_tg_id"] != callback.from_user.id:
        await callback.answer("Аккаунт не найден.", show_alert=True)
        return
    await db.update_account_channel(account_id, None)
    account = await db.get_account(account_id)
    await callback.message.edit_text(
        "Канал отвязан.\n\n" + (await format_account_text(db, account)),
        reply_markup=account_keyboard(account),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("default_period:"))
async def default_period_callback(callback: CallbackQuery, config: Config, db: Database) -> None:
    if not await require_admin_callback(callback, config):
        return
    _, account_id_raw, period_raw = callback.data.split(":")
    account_id = int(account_id_raw)
    period = int(period_raw)
    if period not in ALLOWED_PERIOD_HOURS:
        await callback.answer("Нельзя выбрать такой срок.", show_alert=True)
        return
    account = await db.get_account(account_id)
    if not account or account["owner_tg_id"] != callback.from_user.id:
        await callback.answer("Аккаунт не найден.", show_alert=True)
        return
    await db.update_account_period(account_id, period)
    account = await db.get_account(account_id)
    await callback.message.edit_text(
        await format_account_text(db, account),
        reply_markup=account_keyboard(account),
    )
    await callback.answer(f"По умолчанию: {period} ч.")


@router.callback_query(F.data.startswith("delete_account:"))
async def delete_account_callback(callback: CallbackQuery, config: Config, db: Database) -> None:
    if not await require_admin_callback(callback, config):
        return
    account_id = int(callback.data.split(":")[1])
    account = await db.get_account(account_id)
    if not account or account["owner_tg_id"] != callback.from_user.id:
        await callback.answer("Аккаунт не найден.", show_alert=True)
        return
    await db.delete_account(account_id)
    await callback.message.edit_text(
        "Аккаунт удален из панели. Telegram-сессия больше не используется.",
        reply_markup=await accounts_keyboard(db, callback.from_user.id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("publish_now:"))
async def publish_now_callback(callback: CallbackQuery, config: Config, db: Database) -> None:
    if not await require_admin_callback(callback, config):
        return
    account_id = int(callback.data.split(":")[1])
    account = await db.get_account(account_id)
    if not account or account["owner_tg_id"] != callback.from_user.id:
        await callback.answer("Аккаунт не найден.", show_alert=True)
        return
    item = await db.next_queued_item(account_id)
    if item is None:
        await callback.answer("В очереди для этого аккаунта ничего нет.", show_alert=True)
        return
    await db.set_next_publish_at(account_id, None)
    account = await db.get_account(account_id)
    await callback.message.edit_text(
        "Таймер сброшен. Следующая Story будет опубликована в ближайшую минуту.\n\n"
        + (await format_account_text(db, account)),
        reply_markup=account_keyboard(account),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("next_story:"))
async def next_story_callback(callback: CallbackQuery, config: Config, db: Database) -> None:
    if not await require_admin_callback(callback, config):
        return
    account_id = int(callback.data.split(":")[1])
    account = await db.get_account(account_id)
    if not account or account["owner_tg_id"] != callback.from_user.id:
        await callback.answer("Аккаунт не найден.", show_alert=True)
        return

    batches = await db.queued_batches_for_account(account_id)
    if not batches:
        await callback.message.edit_text(
            "Для этого аккаунта очередь пустая.",
            reply_markup=back_to_account_keyboard(account_id),
        )
        await callback.answer()
        return

    first_batch = batches[0]
    expected_at = estimate_batch_publish_at(account, batches, int(first_batch["first_id"]))
    await callback.message.edit_text(
        format_next_story(account, first_batch, len(batches), expected_at),
        reply_markup=back_to_account_keyboard(account_id),
    )
    await callback.answer()


@router.callback_query(F.data == "upload")
async def upload_callback(callback: CallbackQuery, config: Config, db: Database) -> None:
    if not await require_admin_callback(callback, config):
        return
    await callback.message.edit_text(
        "Выбери аккаунт для очереди:",
        reply_markup=await upload_accounts_keyboard(db, callback.from_user.id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("upload_account:"))
async def upload_account_callback(callback: CallbackQuery, config: Config, db: Database) -> None:
    if not await require_admin_callback(callback, config):
        return
    account_id = int(callback.data.split(":")[1])
    account = await db.get_account(account_id)
    if not account or account["owner_tg_id"] != callback.from_user.id:
        await callback.answer("Аккаунт не найден.", show_alert=True)
        return
    await callback.message.edit_text(
        "На сколько часов поставить Story?",
        reply_markup=period_keyboard(account_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("upload_period:"))
async def upload_period_callback(callback: CallbackQuery, config: Config, state: FSMContext) -> None:
    if not await require_admin_callback(callback, config):
        return
    _, account_id_raw, period_raw = callback.data.split(":")
    await state.set_state(UploadStory.media)
    await state.update_data(account_id=int(account_id_raw), period_hours=int(period_raw))
    await callback.message.edit_text(
        "Теперь отправь фото/видео для Stories или ссылку на сообщение из Telegram-канала с исходником. "
        "Подпись к сообщению станет подписью Story.",
        reply_markup=back_keyboard(),
    )
    await callback.answer()


@router.message(UploadStory.media)
async def upload_media(
    message: Message,
    bot: Bot,
    config: Config,
    db: Database,
    cipher: SessionCipher,
    state: FSMContext,
) -> None:
    if not await require_admin_message(message, config):
        return

    data = await state.get_data()
    account_id = int(data["account_id"])
    period_hours = int(data["period_hours"])
    account = await db.get_account(account_id)
    if not account or account["owner_tg_id"] != message.from_user.id:
        await state.clear()
        await message.answer("Аккаунт не найден.", reply_markup=main_menu())
        return

    source_link: TelegramMessageLink | None = None
    caption = message.caption

    if message.photo:
        media = message.photo[-1]
        media_kind = "photo"
        ext = ".jpg"
    elif message.video:
        media = message.video
        media_kind = "video"
        ext = guess_video_ext(message.video.file_name)
    elif message.text:
        source_link = parse_telegram_message_link(message.text)
        if source_link is None:
            await message.answer("Нужно отправить фото, видео или ссылку на сообщение Telegram с исходником.")
            return
        media = None
        media_kind = "video"
        ext = ".mp4"
    else:
        await message.answer("Нужно отправить фото, видео или ссылку на сообщение Telegram с исходником.")
        return

    config.media_dir.mkdir(parents=True, exist_ok=True)
    destination = config.media_dir / f"{uuid.uuid4().hex}{ext}"
    if source_link is None:
        file_size = getattr(media, "file_size", None)
        download_limit = bot_download_limit_bytes(config.bot_api_base_url)
        if file_size and download_limit and file_size > download_limit:
            await message.answer(
                "Файл слишком большой для загрузки через Bot API.\n"
                f"Размер: {format_bytes(file_size)}. Максимум: {format_bytes(MAX_BOT_DOWNLOAD_BYTES)}.\n"
                "Для больших исходников отправь ссылку на сообщение из Telegram-канала с этим видео."
            )
            return

        try:
            await bot.download(media, destination=destination)
        except TelegramBadRequest as exc:
            destination.unlink(missing_ok=True)
            await message.answer(f"Telegram не дал скачать файл: {exc.message}")
            return
    else:
        await message.answer("Скачиваю исходник по ссылке через подключенный аккаунт.")
        try:
            session = cipher.decrypt(account["session_encrypted"])
            async with user_client(
                config.api_id,
                config.api_hash,
                session,
                proxy=config.telethon_proxy(),
            ) as client:
                destination, media_kind, caption = await download_source_message_media(
                    client=client,
                    source_link=source_link,
                    media_dir=config.media_dir,
                )
        except Exception as exc:
            destination.unlink(missing_ok=True)
            await message.answer(
                "Не удалось скачать исходник по ссылке.\n"
                "Проверь, что выбранный аккаунт состоит в этом канале и видит сообщение.\n"
                f"Ошибка: {exc}"
            )
            return

    if media_kind == "video":
        await message.answer("Видео загружено. Готовлю файл для Stories, это может занять немного времени.")

    try:
        media_paths = [destination]
        if media_kind == "video":
            media_paths = split_story_video(destination, config.media_dir / "parts")
        else:
            ensure_story_file(destination)
    except ValueError as exc:
        destination.unlink(missing_ok=True)
        await message.answer(str(exc))
        return
    except Exception as exc:
        destination.unlink(missing_ok=True)
        await message.answer(f"Не удалось подготовить видео для Stories: {exc}")
        return

    item_ids = await db.enqueue_batch(
        owner_tg_id=message.from_user.id,
        account_id=account_id,
        media_paths=[str(path) for path in media_paths],
        media_kind=media_kind,
        caption=caption,
        period_hours=period_hours,
    )
    await state.clear()
    if len(item_ids) == 1:
        queue_text = f"Добавлено в очередь: #{item_ids[0]}."
    else:
        queue_text = (
            f"Добавлено в очередь: #{item_ids[0]}-#{item_ids[-1]} "
            f"({len(item_ids)} части)."
        )
    batches = await db.queued_batches_for_account(account_id)
    expected_at = estimate_batch_publish_at(account, batches, item_ids[0])
    publish_hint = format_upload_publish_hint(expected_at)
    await message.answer(
        f"{queue_text} {publish_hint}",
        reply_markup=main_menu(),
    )


@router.callback_query(F.data == "queue")
async def queue_callback(callback: CallbackQuery, config: Config, db: Database) -> None:
    if not await require_admin_callback(callback, config):
        return
    rows = await db.list_queue(callback.from_user.id)
    text = format_queue(rows)
    await callback.message.edit_text(text, reply_markup=main_menu())
    await callback.answer()


async def save_authorized_account(
    message: Message,
    db: Database,
    cipher: SessionCipher,
    state: FSMContext,
) -> None:
    flow = LOGIN_FLOWS.get(message.from_user.id)
    client: TelegramClient = flow["client"]
    me = await client.get_me()
    session_string = client.session.save()
    label = account_label(me)
    account_id = await db.add_account(
        owner_tg_id=message.from_user.id,
        label=label,
        phone=flow["phone"],
        session_encrypted=cipher.encrypt(session_string),
    )
    await cleanup_login_flow(message.from_user.id)
    await state.clear()
    await message.answer(
        f"Аккаунт подключен: {label}\nID в панели: {account_id}",
        reply_markup=main_menu(),
    )


async def save_authorized_client(
    owner_tg_id: int,
    db: Database,
    cipher: SessionCipher,
    client: TelegramClient,
) -> int:
    me = await client.get_me()
    session_string = client.session.save()
    label = account_label(me)
    phone = f"+{me.phone}" if getattr(me, "phone", None) else "qr-login"
    return await db.add_account(
        owner_tg_id=owner_tg_id,
        label=label,
        phone=phone,
        session_encrypted=cipher.encrypt(session_string),
    )


async def wait_for_qr_login(
    message: Message,
    user_id: int,
    client: TelegramClient,
    qr_login,
    db: Database,
    cipher: SessionCipher,
    state: FSMContext,
) -> None:
    keep_client_for_password = False
    try:
        await qr_login.wait()
        account_id = await save_authorized_client(user_id, db, cipher, client)
        me = await client.get_me()
        label = account_label(me)
        await message.answer(
            f"QR подтвержден. Аккаунт подключен: {label}\nID в панели: {account_id}",
            reply_markup=main_menu(),
        )
    except SessionPasswordNeededError:
        keep_client_for_password = True
        flow = QR_LOGIN_FLOWS.get(user_id)
        if flow:
            flow["task"] = None
        await state.set_state(QrAccount.password)
        await message.answer(
            "QR подтвержден, но на аккаунте включена 2FA. Пришли пароль двухэтапной проверки одним сообщением.",
            reply_markup=back_keyboard(),
        )
        return
    except asyncio.TimeoutError:
        await message.answer("QR истек. Нажми «Добавить по QR» еще раз.", reply_markup=main_menu())
    except Exception as exc:
        await message.answer(f"Не удалось подключить аккаунт по QR: {exc}", reply_markup=main_menu())
    finally:
        if not keep_client_for_password:
            QR_LOGIN_FLOWS.pop(user_id, None)
            with contextlib.suppress(Exception):
                await client.disconnect()


async def cleanup_login_flow(user_id: int) -> None:
    flow = LOGIN_FLOWS.pop(user_id, None)
    if flow and flow.get("client"):
        with contextlib.suppress(Exception):
            await flow["client"].disconnect()


async def cleanup_qr_login_flow(user_id: int) -> None:
    flow = QR_LOGIN_FLOWS.pop(user_id, None)
    if not flow:
        return
    task = flow.get("task")
    if task:
        task.cancel()
    client = flow.get("client")
    if client:
        with contextlib.suppress(Exception):
            await client.disconnect()


async def show_queue(message: Message, db: Database) -> None:
    rows = await db.list_queue(message.from_user.id)
    await message.answer(format_queue(rows), reply_markup=main_menu())


async def require_admin_message(message: Message, config: Config) -> bool:
    if config.is_admin(message.from_user.id if message.from_user else None):
        return True
    await message.answer("У тебя нет доступа к этой панели.")
    return False


async def require_admin_callback(callback: CallbackQuery, config: Config) -> bool:
    if config.is_admin(callback.from_user.id if callback.from_user else None):
        return True
    await callback.answer("Нет доступа.", show_alert=True)
    return False


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Аккаунты", callback_data="accounts")],
            [InlineKeyboardButton(text="Добавить контент", callback_data="upload")],
            [InlineKeyboardButton(text="Очередь", callback_data="queue")],
        ]
    )


def back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="menu")]]
    )


def code_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Запросить код повторно", callback_data="resend_login_code")],
            [InlineKeyboardButton(text="Отменить", callback_data="menu")],
        ]
    )


def back_to_account_keyboard(account_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data=f"account:{account_id}")],
        ]
    )


async def accounts_keyboard(db: Database, owner_tg_id: int) -> InlineKeyboardMarkup:
    accounts = await db.list_accounts(owner_tg_id)
    summaries = await db.queue_summary_by_account(owner_tg_id)
    rows = [
        [
            InlineKeyboardButton(
                text=f"{account['label']} | {format_queue_count(summaries.get(account['id']))}",
                callback_data=f"account:{account['id']}",
            )
        ]
        for account in accounts
    ]
    rows.append([InlineKeyboardButton(text="Добавить аккаунт", callback_data="add_account")])
    rows.append([InlineKeyboardButton(text="Добавить по QR", callback_data="add_account_qr")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def account_keyboard(account) -> InlineKeyboardMarkup:
    account_id = account["id"]
    rows = [
        [InlineKeyboardButton(text="Привязать канал", callback_data=f"bind_channel:{account_id}")],
        [InlineKeyboardButton(text="Выбрать из моих каналов", callback_data=f"choose_channel:{account_id}")],
    ]
    if account["channel_ref"]:
        rows.append(
            [InlineKeyboardButton(text="Отвязать канал", callback_data=f"unbind_channel:{account_id}")]
        )
    rows.append(
        [
            InlineKeyboardButton(text="6 ч", callback_data=f"default_period:{account_id}:6"),
            InlineKeyboardButton(text="12 ч", callback_data=f"default_period:{account_id}:12"),
            InlineKeyboardButton(text="24 ч", callback_data=f"default_period:{account_id}:24"),
            InlineKeyboardButton(text="48 ч", callback_data=f"default_period:{account_id}:48"),
        ]
    )
    rows.append([InlineKeyboardButton(text="Следующая Story", callback_data=f"next_story:{account_id}")])
    rows.append([InlineKeyboardButton(text="Опубликовать следующую сейчас", callback_data=f"publish_now:{account_id}")])
    rows.append([InlineKeyboardButton(text="Удалить аккаунт", callback_data=f"delete_account:{account_id}")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="accounts")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def upload_accounts_keyboard(db: Database, owner_tg_id: int) -> InlineKeyboardMarkup:
    accounts = await db.list_accounts(owner_tg_id)
    summaries = await db.queue_summary_by_account(owner_tg_id)
    rows = [
        [
            InlineKeyboardButton(
                text=f"{account['label']} | {format_queue_count(summaries.get(account['id']))}",
                callback_data=f"upload_account:{account['id']}",
            )
        ]
        for account in accounts
    ]
    rows.append([InlineKeyboardButton(text="Назад", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def period_keyboard(account_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="6 ч", callback_data=f"upload_period:{account_id}:6"),
                InlineKeyboardButton(text="12 ч", callback_data=f"upload_period:{account_id}:12"),
            ],
            [
                InlineKeyboardButton(text="24 ч", callback_data=f"upload_period:{account_id}:24"),
                InlineKeyboardButton(text="48 ч", callback_data=f"upload_period:{account_id}:48"),
            ],
            [InlineKeyboardButton(text="Назад", callback_data="upload")],
        ]
    )


def channels_keyboard(account_id: int, channels: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    rows = []
    for channel in channels[:45]:
        rows.append(
            [
                InlineKeyboardButton(
                    text=channel["title"][:60],
                    callback_data=(
                        f"select_channel:{account_id}:{channel['id']}:{channel['access_hash']}"
                    ),
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="Назад", callback_data=f"account:{account_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def format_account_text(db: Database, account) -> str:
    summary = await db.account_queue_summary(account["id"])
    return format_account(account, summary)


def format_account(account, summary: dict[str, int] | None = None) -> str:
    channel = format_channel_ref(account["channel_ref"]) if account["channel_ref"] else "не привязан"
    next_publish_at = account["next_publish_at"]
    next_text = (
        format_ts_msk(next_publish_at)
        if next_publish_at
        else "сразу, когда есть очередь"
    )
    return (
        f"Аккаунт: {account['label']}\n"
        f"Телефон: {account['phone']}\n"
        f"Канал: {channel}\n"
        f"Очередь: {format_queue_count(summary)}\n"
        f"Срок по умолчанию: {account['default_period_hours']} ч.\n"
        f"Следующая публикация: {next_text}"
    )


def format_upload_publish_hint(expected_at: int) -> str:
    return f"Ожидаемая публикация: {format_ts_msk(expected_at)}."


def estimate_batch_publish_at(account, batches, target_first_id: int) -> int:
    current = now_ts()
    next_publish_at = int(account["next_publish_at"] or 0)
    publish_at = max(next_publish_at, current + 60)
    for batch in batches:
        if int(batch["first_id"]) == target_first_id:
            return publish_at
        publish_at += int(batch["period_hours"]) * 3600
    return publish_at


def format_next_story(account, batch, queue_story_count: int, expected_at: int) -> str:
    first_id = int(batch["first_id"])
    last_id = int(batch["last_id"])
    parts_count = int(batch["parts_count"])
    if parts_count > 1:
        queue_id = f"#{first_id}-#{last_id} ({parts_count} части)"
    else:
        queue_id = f"#{first_id}"

    caption = (batch["caption"] or "").strip()
    if caption:
        caption = caption[:700] + ("..." if len(caption) > 700 else "")
    else:
        caption = "нет"

    return "\n".join(
        [
            "Следующая Story в очереди",
            f"аккаунт: {account['label']}",
            f"очередь: {queue_id}",
            f"тип: {batch['media_kind']}",
            f"подпись: {caption}",
            f"срок: {batch['period_hours']} ч",
            f"опубликуется: {format_ts_msk(expected_at)}",
            f"всего впереди в этом аккаунте: {queue_story_count} Story",
        ]
    )


def format_queue_count(summary: dict[str, int] | None) -> str:
    stories = int((summary or {}).get("stories") or 0)
    parts = int((summary or {}).get("parts") or 0)
    if stories == 0:
        return "0 Story"
    if parts > stories:
        return f"{stories} Story, {parts} части"
    return f"{stories} Story"


def format_ts_msk(timestamp: int) -> str:
    return datetime.fromtimestamp(int(timestamp), DISPLAY_TZ).strftime("%Y-%m-%d %H:%M:%S МСК")


def format_queue(rows) -> str:
    if not rows:
        return "Актуальная очередь пустая."

    lines = ["Актуальная очередь:"]
    for row in rows:
        created = datetime.fromtimestamp(row["created_at"]).strftime("%Y-%m-%d %H:%M")
        lines.append(
            f"#{row['id']}{format_batch_part(row)} | {row['account_label']} | {row['media_kind']} | "
            f"{row['period_hours']} ч | {created}"
        )
    return "\n".join(lines)


def format_batch_part(row) -> str:
    total = row["batch_total"] if "batch_total" in row.keys() else 1
    if not total or int(total) <= 1:
        return ""
    return f" ({row['batch_order']}/{total})"


def normalize_channel_ref(value: str) -> str:
    value = value.strip()
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if value.startswith(prefix):
            value = value[len(prefix):]
            break
    value = value.split("?")[0].strip("/")
    if value and not value.startswith("@") and "/" not in value:
        value = f"@{value}"
    return value


async def list_admin_channels(
    config: Config,
    cipher: SessionCipher,
    account,
) -> list[dict[str, Any]]:
    session = cipher.decrypt(account["session_encrypted"])
    channels: list[dict[str, Any]] = []
    async with user_client(
        config.api_id,
        config.api_hash,
        session,
        proxy=config.telethon_proxy(),
    ) as client:
        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            if not isinstance(entity, Channel):
                continue
            if not getattr(entity, "broadcast", False):
                continue
            if not getattr(entity, "access_hash", None):
                continue
            rights = getattr(entity, "admin_rights", None)
            if not getattr(entity, "creator", False) and rights is None:
                continue
            channels.append(
                {
                    "id": entity.id,
                    "access_hash": entity.access_hash,
                    "title": entity.title,
                }
            )
    channels.sort(key=lambda item: item["title"].casefold())
    return channels


def format_channel_ref(value: str) -> str:
    if value.startswith("channel:"):
        channel_id = value.split(":", 2)[1]
        return f"приватный канал #{channel_id}"
    return value


def guess_video_ext(file_name: str | None) -> str:
    if not file_name:
        return ".mp4"
    suffix = Path(file_name).suffix.lower()
    return suffix if suffix in {".mp4", ".mov", ".m4v"} else ".mp4"


async def download_source_message_media(
    client,
    source_link: TelegramMessageLink,
    media_dir: Path,
) -> tuple[Path, str, str | None]:
    source_message = await client.get_messages(source_link.chat_ref, ids=source_link.message_id)
    if not source_message or not getattr(source_message, "media", None):
        raise ValueError("в сообщении нет медиа")

    file_info = getattr(source_message, "file", None)
    mime_type = (getattr(file_info, "mime_type", None) or "").lower()
    file_name = getattr(file_info, "name", None)
    file_ext = (getattr(file_info, "ext", None) or "").lower()

    if getattr(source_message, "photo", None):
        media_kind = "photo"
        ext = ".jpg"
    elif getattr(source_message, "video", None) or mime_type.startswith("video/"):
        media_kind = "video"
        ext = guess_video_ext(file_name) if file_name else (file_ext if file_ext in {".mp4", ".mov", ".m4v"} else ".mp4")
    else:
        raise ValueError("поддерживаются только фото и видео")

    destination = media_dir / f"{uuid.uuid4().hex}{ext}"
    downloaded = await client.download_media(source_message, file=str(destination))
    if not downloaded:
        raise ValueError("Telegram не вернул файл после скачивания")

    caption = (getattr(source_message, "message", None) or "").strip() or None
    return Path(downloaded), media_kind, caption


def format_bytes(value: int) -> str:
    return f"{value / 1024 / 1024:.1f} MB"


def format_duration(seconds: int) -> str:
    minutes, rest = divmod(int(seconds), 60)
    if minutes:
        return f"{minutes}:{rest:02d}"
    return f"{rest} сек"


def account_label(me) -> str:
    label = f"{getattr(me, 'first_name', '') or 'Telegram'} {getattr(me, 'last_name', '') or ''}".strip()
    if getattr(me, "username", None):
        label = f"{label} (@{me.username})"
    return label


def make_qr_png(url: str) -> bytes:
    image = qrcode.make(url)
    output = BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()


def describe_code_delivery(sent) -> str:
    code_type = sent.type.__class__.__name__
    labels = {
        "SentCodeTypeApp": "приложение Telegram",
        "SentCodeTypeSms": "SMS",
        "SentCodeTypeCall": "звонок",
        "SentCodeTypeFlashCall": "flash-call",
        "SentCodeTypeEmailCode": "email",
        "SentCodeTypeFragmentSms": "Fragment SMS",
    }
    return labels.get(code_type, code_type)


def describe_sent_code(sent) -> str:
    sent_type = sent.type
    next_type = getattr(sent, "next_type", None)
    timeout = getattr(sent, "timeout", None)
    length = getattr(sent_type, "length", None)

    details = [f"детали: type={sent_type.__class__.__name__}"]
    if length:
        details.append(f"длина кода={length}")
    if timeout:
        details.append(f"повтор через={timeout} сек")
    if next_type:
        details.append(f"следующий способ={next_type.__class__.__name__}")
    return ", ".join(details)
