import json
from abc import ABCMeta
from datetime import datetime
from pathlib import Path

import filelock

from .base import BaseDownloader

_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S%z'


def _object_hook(obj):
    if isinstance(obj, datetime):
        return obj.strftime(_DATETIME_FORMAT)
    raise TypeError(type(obj))


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
                status = json.load(fp)
                if 'waiting_until' in status:
                    status['waiting_until'] = datetime.strptime(
                        status['waiting_until'], _DATETIME_FORMAT)
                return status
        except FileNotFoundError:
            return {}

    def _save_status(self, session, resource, status):
        with open(str(self._status_path(resource)), 'w') as fp:
            json.dump(status, fp, default=_object_hook)

    def _status_path(self, resource):
        return Path(self.archive_prefix(resource), '.status.json')

    def _filelock_path(self, resource):
        return Path(self.archive_prefix(resource), '.lock')
