import logging
from abc import ABCMeta, abstractmethod
from datetime import timedelta
from pathlib import Path

from .utils import utcnow


class AlreadyDownloadingError(Exception):
    pass


class AlreadyCompletedError(Exception):
    pass


class ResourceError(Exception):
    pass


class TemporaryResourceError(ResourceError):
    def __init__(self, *args, try_again_later=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._waiting_until = try_again_later or utcnow()
        if not self._waiting_until.tzinfo:
            raise ValueError('no timezone')

    @property
    def waiting_until(self):
        return self._waiting_until


class PartiallyCompleted(Exception):
    def __init__(self, *args, try_again_later=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._waiting_until = try_again_later or utcnow()
        if not self._waiting_until.tzinfo:
            raise ValueError('no timezone')

    @property
    def waiting_until(self):
        return self._waiting_until


class BaseDownloader(metaclass=ABCMeta):
    def download(self, args, ignore_exc=ResourceError):
        for resource in self.as_resources(args):
            logger = self.logger(resource)
            log_handler = self._log_handler(resource)
            logger.addHandler(log_handler)

            try:
                try:
                    self._download(resource)
                except (AlreadyCompletedError, AlreadyDownloadingError):
                    raise
                except TemporaryResourceError:
                    raise
                except ResourceError as e:
                    logger.warning('%s: %s', type(e).__name__, str(e))
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
            except AlreadyCompletedError:
                logger.info('Already completed')
            except AlreadyDownloadingError:
                logger.info('Already downloading')
            except TemporaryResourceError:
                logger.info('Resource temporary unavailable')
            except ignore_exc or ():
                pass

    def _download(self, resource):
        with self._exclusive_session(resource) as session:
            status = self._load_status(session, resource)

            if status.get('downloading'):
                raise AlreadyDownloadingError()
            if status.get('completed'):
                raise AlreadyCompletedError()

            now = utcnow().replace(microsecond=0) + timedelta(seconds=1)
            if status.get('waiting_until', now) > now:
                raise TemporaryResourceError(
                    'Please try again later {}'.format(status['waiting_until']),
                    try_again_later=status['waiting_until'])

            status['downloading'] = True
            status['completed'] = False
            status['failed'] = False
            self._save_status(session, resource, status)

        try:
            try:
                self._force_download(resource)
            finally:
                with self._exclusive_session(resource) as session:
                    status = self._load_status(session, resource)
        except PartiallyCompleted as e:
            status['waiting_until'] = e.waiting_until
        except TemporaryResourceError as e:
            status['waiting_until'] = e.waiting_until
            status['failed'] = True
            raise
        except ResourceError:
            status['completed'] = True
            status['failed'] = True
            raise
        except BaseException:
            status['failed'] = True
            raise
        else:
            status['completed'] = True
        finally:
            status['downloading'] = False

            with self._exclusive_session(resource) as session:
                self._save_status(session, resource, status)

    def logger(self, resource=None):
        name = type(self).__module__ + '.' + type(self).__name__
        return logging.getLogger(name)

    def _log_handler(self, resource):
        log_prefix = Path(self.archive_prefix(resource))
        log_prefix.mkdir(exist_ok=True, parents=True)
        file_handler = logging.FileHandler(
            str(log_prefix / 'log.txt'), 'a', encoding='utf-8')
        file_handler.setFormatter(logging.Formatter(
            '[%(asctime)s] %(name)s: %(levelname)s: %(message)s'))
        file_handler.setLevel('DEBUG')
        return file_handler

    @abstractmethod
    def as_resources(self, args):
        pass

    @abstractmethod
    def archive_prefix(self, resource):
        pass

    @abstractmethod
    def _force_download(self, resource):
        pass

    @abstractmethod
    def _exclusive_session(self, resource):
        pass

    @abstractmethod
    def _load_status(self, session, resource):
        pass

    @abstractmethod
    def _save_status(self, session, resource, status):
        pass
