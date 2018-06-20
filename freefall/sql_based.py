from abc import ABCMeta
from contextlib import contextmanager

import sqlalchemy as sql
import sqlalchemy.ext.declarative

from .base import BaseDownloader

_Base = sql.ext.declarative.declarative_base()


def _repr_record(record):
    if not isinstance(record, _Base):
        raise TypeError(record)

    columns = ', '.join(
        '{}={!r}'.format(key, getattr(record, key))
        for key in record.__table__.columns.keys())
    return '{}({})'.format(type(record).__name__, columns)


_Base.__repr__ = _repr_record


class BaseSqlResource(_Base):
    __abstract__ = True

    id = sql.Column(sql.String, primary_key=True)
    downloading = sql.Column(sql.Boolean, nullable=False, default=False)
    completed = sql.Column(sql.Boolean, nullable=False, default=False)
    failed = sql.Column(sql.Boolean, nullable=False, default=False)


class SqlBasedDownloader(BaseDownloader, metaclass=ABCMeta):
    def __init__(self, sessionmaker):
        self._sessionmaker = sessionmaker

    @contextmanager
    def _exclusive_session(self, resource):
        session = self._sessionmaker(autocommit=True)
        with session.begin():
            session.execute('BEGIN EXCLUSIVE')
            yield session

    def _load_status(self, session, resource):
        resource_ = session.query(type(resource)).get(resource.id)

        if resource_ is None:
            session.add(resource)
            resource_ = resource

        return {
            key: getattr(resource_, key)
            for key in resource_.__table__.columns.keys()}

    def _save_status(self, session, resource, status):
        session.merge(type(resource)(**{
            c.name: status.get(c.name)
            for c in resource.__table__.columns
            if status.get(c.name) is not None or c.nullable}))

    def logger(self, resource=None):
        logger = super().logger(resource)
        if resource is not None:
            name = resource.__table__.name + '/' + resource.id
            logger = logger.getChild(name)
        return logger
