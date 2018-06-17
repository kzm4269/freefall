import sqlite3
from abc import ABCMeta
from contextlib import contextmanager

from .base import BaseDownloader


def _escape(text, quote='"'):
    return quote + text.replace(quote, quote * 2) + quote


class SqlBasedDownloader(BaseDownloader, metaclass=ABCMeta):
    def __init__(self, database):
        self._connection = sqlite3.connect(database)
        self._connection.row_factory = sqlite3.Row

        self._init_database()

    def _init_database(self):
        with self._connection:
            self._connection.execute(
                r"""
                CREATE TABLE IF NOT EXISTS resources (
                    id TEXT PRIMARY KEY,
                    archive_prefix TEXT,
                    downloading BOOLEAN NOT NULL DEFAULT 0,
                    completed BOOLEAN NOT NULL DEFAULT 0,
                    failed BOOLEAN NOT NULL DEFAULT 0)
                """)

    @contextmanager
    def _lock_status(self, resource):
        with self._connection:
            self._connection.execute(r"""BEGIN EXCLUSIVE""")
            yield

    def _load_status(self, resource):
        with self._connection:
            statuses = self._connection.execute(
                r"""SELECT * FROM resources WHERE id = :id""",
                {'id': self._resource_id(resource)})

            for status in statuses:
                return dict(status)
            else:
                return {
                    'id': self._resource_id(resource),
                    'archive_prefix': self._archive_prefix(resource),
                }

    def _save_status(self, resource, status):
        with self._connection:
            self._connection.execute(
                r"""
                INSERT OR REPLACE 
                INTO resources ({columns}) VALUES ({placeholders})
                """.format(
                    columns=','.join(map(_escape, status.keys())),
                    placeholders=','.join('?' * len(status))),
                list(status.values()))
