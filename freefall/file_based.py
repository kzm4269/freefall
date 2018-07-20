import json
from abc import ABCMeta, abstractmethod
from datetime import datetime
from pathlib import Path

import filelock

from .base import BaseDownloader
from .utils import localnow

_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f%z'


def _object_hook(obj):
    if isinstance(obj, datetime):
        return obj.strftime(_DATETIME_FORMAT)
    raise TypeError(type(obj))


class FileBasedDownloader(BaseDownloader, metaclass=ABCMeta):
    def __init__(self):
        self._filelock = {}

    def _exclusive_session(self, request):
        path = str(self._filelock_path(request))
        if path not in self._filelock:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            self._filelock[path] = filelock.FileLock(path)
        return self._filelock[path]

    def _load_status(self, session, request):
        try:
            with open(str(self._status_path(request))) as fp:
                status = json.load(fp)
                if status.get('scheduled_for'):
                    status['scheduled_for'] = datetime.strptime(
                        status['scheduled_for'],
                        _DATETIME_FORMAT)
                return status
        except FileNotFoundError:
            return {
                'processing': False,
                'failed': False,
                'scheduled_for': localnow(),
            }

    def _save_status(self, session, request, status):
        with open(str(self._status_path(request)), 'w') as fp:
            json.dump(status, fp, default=_object_hook)

    def _status_path(self, request):
        return Path(self.archive_prefix(request), '.status.json')

    def _filelock_path(self, request):
        return Path(self.archive_prefix(request), '.lock')

    @abstractmethod
    def archive_prefix(self, request):
        pass
