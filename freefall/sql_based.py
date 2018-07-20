import re
from abc import ABCMeta
from contextlib import contextmanager
from datetime import timezone, timedelta

import sqlalchemy as sa
import sqlalchemy.ext.declarative

from .base import BaseDownloader
from .utils import utcnow


class DateTime(sa.TypeDecorator):
    impl = sa.DateTime

    def process_bind_param(self, value, engine):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone(timedelta()))
        return value.astimezone(timezone(timedelta()))

    def process_result_value(self, value, engine):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone(timedelta()))
        return value.astimezone(timezone(timedelta()))


class SqlBasedRequest(sa.ext.declarative.declarative_base()):
    __abstract__ = True
    __tablename__ = 'requests'

    id = sa.Column(sa.Integer, nullable=False, primary_key=True)

    processing = sa.Column(sa.Boolean, nullable=False, default=False)
    finished = sa.Column(sa.Boolean, nullable=False, default=False)
    failed = sa.Column(sa.Boolean, nullable=False, default=False)
    deferred_until = sa.Column(DateTime, nullable=False, default=utcnow)

    def __repr__(self):
        columns = ', '.join(
            '{}={!r}'.format(key, getattr(self, key))
            for key in self.__table__.columns.keys())
        return '{}({})'.format(type(self).__name__, columns)


class SqlBasedDownloader(BaseDownloader, metaclass=ABCMeta):
    def __init__(self, sessionmaker):
        self._sessionmaker = sessionmaker

    @property
    def session(self):
        return self._sessionmaker

    @contextmanager
    def _exclusive_session(self, request):
        session = self._sessionmaker(autocommit=True)

        with session.begin():
            if session.bind.name == 'sqlite':
                session.execute('BEGIN EXCLUSIVE')

            query = session.query(type(request)).with_for_update()
            request_ = query.get(request.id)
            if request_ is None:
                session.add(request)
                request_ = query.get(request.id)
            yield session, request_

    def _load_status(self, session, request):
        session, request = session
        return {
            key: getattr(request, key)
            for key in request.__table__.columns.keys()}

    def _save_status(self, session, request, status):
        session, record = session
        session.merge(type(request)(**{
            c.name: status.get(c.name)
            for c in record.__table__.columns
            if status.get(c.name) is not None or c.nullable}))

    def _resource_type_name(self, request):
        return re.sub(r'(_|^)requests$', '', request.__table__.name)

    def logger(self, request=None):
        logger = super().logger(request)
        if request is not None:
            name = self._resource_type_name(request) + '/' + str(request.id)
            logger = logger.getChild(name)
        return logger
