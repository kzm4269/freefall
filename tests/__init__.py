import tempfile
import unittest

import multiprocessing as mp
import psutil

from freefall import BaseDownloader


def _open_files():
    return psutil.Process(mp.current_process().pid).open_files()


class TestBaseDownloader(unittest.TestCase):
    def setUp(self):
        self._open_files = _open_files()

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

            assert self._open_files == _open_files()
