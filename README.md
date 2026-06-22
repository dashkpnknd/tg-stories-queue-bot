# Telegram Stories Queue Bot

Бот-панель для Telegram, в которую можно подключать пользовательские аккаунты, опционально привязывать канал к аккаунту и складывать фото/видео в очередь Stories.

## Что умеет

- Добавлять Telegram-аккаунты через интерфейс бота.
- Сохранять авторизованные сессии в зашифрованном виде.
- Привязывать к аккаунту канал по `@username` или ссылке `https://t.me/...`.
- При загрузке контента выбирать аккаунт и срок Stories: `6`, `12`, `24` или `48` часов.
- Публиковать Story на сам аккаунт и, если привязан канал, почти одновременно в канал.
- Автоматически брать следующий файл из очереди после окончания таймера.

## Важные условия Telegram

- Для публикации Stories нужен пользовательский аккаунт через Telegram API, одного BotFather-токена недостаточно.
- `48` часов доступны для Premium-аккаунтов.
- Для Stories в канале аккаунт должен быть администратором с правом публикации Stories, а канал должен соответствовать требованиям Telegram по бустам.
- Медиа для Stories должно быть вертикальным фото/видео, до 30 MB.

## Запуск на сервере

Рекомендую Python `3.10`, `3.11` или `3.12`.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Сгенерируй ключ для шифрования сессий:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Заполни `.env`:

- `BOT_TOKEN` — токен из BotFather.
- `TELEGRAM_API_ID` и `TELEGRAM_API_HASH` — из https://my.telegram.org.
- `ADMIN_IDS` — можно оставить пустым, тогда бот будет доступен всем.
- `FERNET_KEY` — сгенерированный ключ.

Запуск:

```bash
python main.py
```

## Как пользоваться

1. Открой бота и нажми `/start`.
2. `Аккаунты` → `Добавить по QR`, если аккаунт уже открыт в Telegram. Это самый надежный вариант.
3. Альтернатива: `Аккаунты` → `Добавить аккаунт`, затем введи номер телефона, код из Telegram и, если включена 2FA, пароль.
4. В карточке аккаунта можно привязать канал.
5. `Добавить контент` → выбери аккаунт → выбери срок → отправь фото или видео.

Контент попадет в очередь. Если аккаунт свободен, первая Story будет опубликована почти сразу. Следующая Story из очереди будет опубликована после выбранного срока предыдущей.

## Systemd пример

```ini
[Unit]
Description=Telegram Stories Queue Bot
After=network.target

[Service]
WorkingDirectory=/opt/stories-bot
ExecStart=/opt/stories-bot/.venv/bin/python /opt/stories-bot/main.py
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```
