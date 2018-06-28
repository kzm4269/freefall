from abc import ABCMeta
from contextlib import contextmanager

import sqlalchemy as sa
import sqlalchemy.ext.declarative

from .base import BaseDownloader

_Base = sa.ext.declarative.declarative_base()


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

    id = sa.Column(sa.Integer, primary_key=True)
    downloading = sa.Column(sa.Boolean, nullable=False, default=False)
    completed = sa.Column(sa.Boolean, nullable=False, default=False)
    failed = sa.Column(sa.Boolean, nullable=False, default=False)


class SqlBasedDownloader(BaseDownloader, metaclass=ABCMeta):
    def __init__(self, sessionmaker):
        self._sessionmaker = sessionmaker

    @contextmanager
    def _exclusive_session(self, resource):
        session = self._sessionmaker(autocommit=True)

        with session.begin():
            if session.bind.name == 'sqlite':
                session.execute('BEGIN EXCLUSIVE')

            query = session.query(type(resource)).with_for_update()
            resource_ = query.get(resource.id)

            if resource_ is None:
                session.add(resource)
                resource_ = query.get(resource.id)
                assert resource_ is not None

            yield session, resource_

    def _load_status(self, session, resource):
        session, resource = session
        return {
            key: getattr(resource, key)
            for key in resource.__table__.columns.keys()}

    def _save_status(self, session, resource, status):
        session, resource = session
        session.merge(type(resource)(**{
            c.name: status.get(c.name)
            for c in resource.__table__.columns
            if status.get(c.name) is not None or c.nullable}))

    def logger(self, resource=None):
        logger = super().logger(resource)
        if resource is not None:
            name = resource.__table__.name + '/' + str(resource.id)
            logger = logger.getChild(name)
        return logger
