import asyncio
import logging

from bot_app.app import run


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.info("Starting Telegram Stories Queue Bot...")
    asyncio.run(run())
