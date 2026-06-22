from unittest import TestCase

from bot_app.source_links import parse_telegram_message_link


class SourceLinksTest(TestCase):
    def test_public_message_link(self) -> None:
        link = parse_telegram_message_link("https://t.me/source_channel/123")

        self.assertIsNotNone(link)
        assert link is not None
        self.assertEqual(link.chat_ref, "source_channel")
        self.assertEqual(link.message_id, 123)

    def test_private_channel_message_link(self) -> None:
        link = parse_telegram_message_link("https://t.me/c/1234567890/45?single")

        self.assertIsNotNone(link)
        assert link is not None
        self.assertEqual(link.chat_ref, -1001234567890)
        self.assertEqual(link.message_id, 45)

    def test_ignores_plain_text(self) -> None:
        self.assertIsNone(parse_telegram_message_link("просто текст"))
