import json
from abc import ABCMeta
from pathlib import Path

import filelock

from .base import BaseDownloader


class FileBasedDownloader(BaseDownloader, metaclass=ABCMeta):
    def __init__(self):
        self._filelock = {}

    def _exclusive_session(self, resource):
        path = str(self._filelock_path(resource))
        if path not in self._filelock:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            self._filelock[path] = filelock.FileLock(path)
        return self._filelock[path]

    def _load_status(self, session, resource):
        try:
            with open(str(self._status_path(resource))) as fp:
                return json.load(fp)
        except FileNotFoundError:
            return {}

    def _save_status(self, session, resource, status):
        with open(str(self._status_path(resource)), 'w') as fp:
            json.dump(status, fp)

    def _status_path(self, resource):
        return Path(self._archive_prefix(resource), '.status.json')

    def _filelock_path(self, resource):
        return Path(self._archive_prefix(resource), '.lock')
