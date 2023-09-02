import unittest
from unittest.mock import AsyncMock, Mock, MagicMock

from src.politeness import PolitenessChecker


class TestPolitenessChecker(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def get_politeness_checker():
        mock_session = Mock()
        mock_redis = AsyncMock()
        politeness_checker = PolitenessChecker(mock_session, mock_redis)
        return politeness_checker

    async def test_get_robots_txt_url(self):
        test_url = "https://example.com/page"
        politeness_checker = self.get_politeness_checker()
        robots_txt_url = politeness_checker.get_robots_txt_url(test_url)
        self.assertEqual(robots_txt_url, "https://example.com/robots.txt")

    async def test_can_crawl_with_cached_robots_txt(self):
        test_url = "https://example.com/page"

        politeness_checker = self.get_politeness_checker()
        politeness_checker._redis.get.return_value = False

        # Test can_crawl
        result = await politeness_checker.can_crawl(test_url)
        self.assertFalse(result)

    async def test_can_crawl_with_fetched_robots_txt(self):
        test_url = "https://example.com/page"

        politeness_checker = self.get_politeness_checker()
        politeness_checker._redis.get.return_value = None

        # Mocking HTTP get call
        mock_response = MagicMock()
        mock_response.text = AsyncMock(return_value='User-agent: *\nAllow: /')

        # Mocking the async context manager
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = False  # No exceptions

        politeness_checker._session.get.return_value = mock_response

        # Test can_crawl
        result = await politeness_checker.can_crawl(test_url)
        self.assertTrue(result)

    async def test_should_crawl(self):
        test_url = "https://example.com/page"

        politeness_checker = self.get_politeness_checker()

        # Mocking HTTP head call for Content-Type check
        mock_response = MagicMock()
        mock_response.headers.get.return_value = 'text/html'
        politeness_checker._session.head.return_value = mock_response

        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = False  # No exceptions

        # Mocking can_crawl method to return True
        politeness_checker.can_crawl = AsyncMock(return_value=True)

        # Test should_crawl
        result = await politeness_checker.should_crawl(test_url)
        self.assertTrue(result)
