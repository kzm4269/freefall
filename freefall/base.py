import logging
from abc import ABCMeta, abstractmethod
from datetime import timedelta, datetime
from pathlib import Path

from .utils import localnow


class RequestClosed(Exception):
    def __init__(self, *args, **kwargs):
        self._failed = kwargs.pop('failed', None)
        self._retry_datetime = kwargs.pop('retry_datetime', None)
        if self._retry_datetime:
            assert isinstance(self._retry_datetime, datetime)
            self._retry_datetime.astimezone()

        super().__init__(*args, **kwargs)

    @property
    def failed(self):
        return self._failed

    @property
    def retry_datetime(self):
        return self._retry_datetime


class ContentError(Exception):
    def __init__(self, *args, **kwargs):
        retry_interval = kwargs.pop('retry_interval', None)
        if retry_interval is None:
            self._retry_datetime = None
        else:
            if isinstance(retry_interval, (int, float)):
                retry_interval = timedelta(seconds=retry_interval)
            self._retry_datetime = localnow() + retry_interval

        super().__init__(*args, **kwargs)

    @property
    def retry_datetime(self):
        return self._retry_datetime


class UnfinishedContent(Exception):
    def __init__(self, *args, **kwargs):
        retry_interval = kwargs.pop('retry_interval', None)
        if retry_interval is None:
            self._retry_datetime = None
        else:
            if isinstance(retry_interval, (int, float)):
                retry_interval = timedelta(seconds=retry_interval)
            self._retry_datetime = localnow() + retry_interval

        super().__init__(*args, **kwargs)

    @property
    def retry_datetime(self):
        return self._retry_datetime


class BaseDownloader(metaclass=ABCMeta):
    def download(self, args, ignore_exc=(RequestClosed, ContentError)):
        for request in self.as_requests(args):
            try:
                self.process_request(request)
            except ignore_exc or ():
                pass

    def process_request(self, request):
        logger = self.logger(request)
        log_handler = self._log_handler(request)
        logger.addHandler(log_handler)

        try:
            try:
                with self._exclusive_session(request) as session:
                    status = self._load_status(session, request)

                    if status['processing']:
                        raise RequestClosed('already processing')
                    elif (status['scheduled_for'] is None
                          or status['scheduled_for'] > localnow()):
                        raise RequestClosed(
                            failed=status['failed'],
                            retry_datetime=status['scheduled_for'])

                    status['processing'] = True
                    status['failed'] = False
                    status['scheduled_for'] = None
                    self._save_status(session, request, status)

                try:
                    try:
                        self._process_request(request)
                    except RequestClosed as e:
                        raise RuntimeError from e
                    finally:
                        with self._exclusive_session(request) as session:
                            status = self._load_status(session, request)
                        status['processing'] = False
                except UnfinishedContent as e:
                    status['failed'] = False
                    status['scheduled_for'] = e.retry_datetime
                except ContentError as e:
                    status['failed'] = True
                    status['scheduled_for'] = e.retry_datetime
                    raise
                except BaseException:
                    status['failed'] = True
                    status['scheduled_for'] = localnow()
                    raise
                else:
                    status['failed'] = False
                    status['scheduled_for'] = None
                finally:
                    with self._exclusive_session(request) as session:
                        self._save_status(session, request, status)
            except RequestClosed:
                raise
            except ContentError as e:
                logger.error('%s: %s', type(e).__name__, str(e))
                logger.debug('Detail', exc_info=True)
                raise
            except BaseException as e:
                logger.exception(str(e))
                raise
            else:
                logger.info('Completed successfully')
            finally:
                log_handler.close()
                logger.removeHandler(log_handler)
        except RequestClosed as e:
            if e.retry_datetime:
                logger.error('Temporarily closed until {}'.format(
                    e.retry_datetime.astimezone()))
            elif e.failed:
                logger.error('Closed')
            else:
                logger.info('Already completed')
            raise

    def logger(self, request=None):
        name = type(self).__module__ + '.' + type(self).__name__
        return logging.getLogger(name)

    def _log_handler(self, request):
        log_prefix = self.archive_prefix(request)
        if log_prefix is None:
            return logging.NullHandler()
        log_prefix = Path(log_prefix)

        log_prefix.mkdir(exist_ok=True, parents=True)
        file_handler = logging.FileHandler(
            str(log_prefix / 'log.txt'), 'a', encoding='utf-8')
        file_handler.setFormatter(logging.Formatter(
            '[%(asctime)s] %(name)s: %(levelname)s: %(message)s'))
        file_handler.setLevel('DEBUG')
        return file_handler

    def archive_prefix(self, request):
        return None

    @abstractmethod
    def as_requests(self, args):
        pass

    @abstractmethod
    def _process_request(self, request):
        pass

    @abstractmethod
    def _exclusive_session(self, request):
        pass

    @abstractmethod
    def _load_status(self, session, request):
        pass

    @abstractmethod
    def _save_status(self, session, request, status):
        pass
