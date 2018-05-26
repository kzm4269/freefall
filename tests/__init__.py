import tempfile
import unittest

from freefall import BaseDownloader


class TestBaseDownloader(unittest.TestCase):
    def setUp(self):
        pass

    def test_init(self):
        with tempfile.TemporaryDirectory() as tempd:
            class Downloader(BaseDownloader):
                def _download_resource(self, resource_id, prefix, logger):
                    pass

            Downloader(prefix=tempd)

    def test_simple(self):
        with tempfile.TemporaryDirectory() as tempd:
            class Downloader(BaseDownloader):
                def _download_resource(self, resource_id, prefix, logger):
                    pass

            d = Downloader(prefix=tempd)
            d.download(['http://example.com'])
