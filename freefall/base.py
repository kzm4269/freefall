import logging
from abc import ABCMeta, abstractmethod
from datetime import timedelta
from pathlib import Path

from .utils import utcnow


class AlreadyProcessingError(Exception):
    pass


class AlreadyFinishedError(Exception):
    pass


class ContentError(Exception):
    pass


class TemporaryContentError(ContentError):
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
    def download(self, args, ignore_exc=(
            AlreadyProcessingError, AlreadyFinishedError, ContentError)):

        for request in self.as_requests(args):
            logger = self.logger(request)
            log_handler = self._log_handler(request)
            logger.addHandler(log_handler)

            try:
                try:
                    try:
                        self._process(request)
                    except (AlreadyFinishedError, AlreadyProcessingError):
                        raise
                    except TemporaryContentError:
                        raise
                    except ContentError as e:
                        logger.warning('%s: %s', type(e).__name__, str(e))
                        logger.debug('Detail', exc_info=True)
                        raise
                    except BaseException as e:
                        logger.exception(str(e))
                        raise
                    else:
                        logger.info('Completed')
                    finally:
                        log_handler.close()
                        logger.removeHandler(log_handler)
                except AlreadyFinishedError:
                    logger.info('Already finished')
                    raise
                except AlreadyProcessingError:
                    logger.info('Already processing')
                    raise
                except TemporaryContentError:
                    logger.info('Content temporary unavailable')
                    raise
            except ignore_exc or ():
                pass

    def _process(self, request):
        with self._exclusive_session(request) as session:
            status = self._load_status(session, request)

            if status.get('processing'):
                raise AlreadyProcessingError()
            if status.get('finished'):
                raise AlreadyFinishedError()

            now = utcnow().replace(microsecond=0) + timedelta(seconds=1)
            if status.get('waiting_until', now) > now:
                raise TemporaryContentError(
                    'Please try again later {}'.format(
                        status['waiting_until']),
                    try_again_later=status['waiting_until'])

            status['processing'] = True
            status['finished'] = False
            status['failed'] = False
            self._save_status(session, request, status)

        try:
            try:
                self._force_process(request)
            finally:
                with self._exclusive_session(request) as session:
                    status = self._load_status(session, request)
        except PartiallyCompleted as e:
            status['waiting_until'] = e.waiting_until
        except TemporaryContentError as e:
            status['waiting_until'] = e.waiting_until
            status['failed'] = True
            raise
        except ContentError:
            status['finished'] = True
            status['failed'] = True
            raise
        except BaseException:
            status['failed'] = True
            raise
        else:
            status['finished'] = True
        finally:
            status['processing'] = False

            with self._exclusive_session(request) as session:
                self._save_status(session, request, status)

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
    def _force_process(self, request):
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
