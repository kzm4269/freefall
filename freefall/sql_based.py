import re
import sqlalchemy as sa
from abc import ABCMeta
from contextlib import contextmanager
from datetime import timezone, datetime

from .base import BaseDownloader
from .utils import localnow


class UtcDateTime(sa.TypeDecorator):
    impl = sa.DateTime

    def process_bind_param(self, value, engine):
        if isinstance(value, datetime):
            value = value.astimezone().astimezone(timezone.utc)
        return value

    def process_result_value(self, value, engine):
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            value = value.astimezone()
        return value


class SqlBasedRequest:
    id = sa.Column(sa.Integer, primary_key=True)

    processing = sa.Column(sa.Boolean, nullable=False, default=False)
    failed = sa.Column(sa.Boolean, nullable=False, default=False)
    scheduled_for = sa.Column(
        UtcDateTime(timezone=True), nullable=True, default=localnow)

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
        session = self._sessionmaker(autocommit=False)

        try:
            if session.bind.name == 'sqlite':
                session.execute('BEGIN EXCLUSIVE')

            query = session.query(type(request)).with_for_update()
            bounded_request = query.get(request.id)

            if bounded_request is None:
                session.add(request)
                bounded_request = query.get(request.id)
                session.expunge(request)

            yield session, bounded_request
        except BaseException:
            session.rollback()
            raise
        else:
            session.commit()

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

    @staticmethod
    def _content_type_name(request):
        return re.sub(
            r'(_|^)download$', '',
            request.__table__.name)

    def logger(self, request=None):
        logger = super().logger(request)
        if request is not None:
            name = self._content_type_name(request) + '/' + str(request.id)
            logger = logger.getChild(name)
        return logger

    def request(self, args):
        for request in self.as_requests(args):
            with self._exclusive_session(request):
                self.logger(request).info('Request')
