import logging
from abc import ABCMeta, abstractmethod
from pathlib import Path


class Downloading(Exception):
    pass


class Completed(Exception):
    pass


class RetryableError(Exception):
    pass


class NonRetryableError(Exception):
    pass


class BaseDownloader(metaclass=ABCMeta):
    def download(self, args, ignore=(RetryableError, NonRetryableError)):
        for resource in self.as_resources(args):
            logger = self.logger(resource)
            log_handler = self._log_handler(resource)
            logger.addHandler(log_handler)

            try:
                try:
                    self._download(resource)
                except (Completed, Downloading):
                    raise
                except (RetryableError, NonRetryableError) as e:
                    logger.warning('%s: %s', type(e).__name__, str(e))
                    logger.debug('Detail', exc_info=True)
                    raise
                except Exception as e:
                    logger.exception(str(e))
                    raise
                else:
                    logger.info('Completed successfully')
                finally:
                    log_handler.close()
                    logger.removeHandler(log_handler)
            except Completed:
                logger.info('Already completed')
            except Downloading:
                logger.info('Already downloading')
            except ignore or ():
                pass

    def _download(self, resource):
        with self._exclusive_session(resource) as session:
            status = self._load_status(session, resource)

            if status.get('downloading'):
                raise Downloading()
            if status.get('completed'):
                raise Completed()

            status['downloading'] = True
            status['completed'] = False
            status['failed'] = False
            self._save_status(session, resource, status)

        try:
            self._force_download(resource)
        except RetryableError:
            status['failed'] = True
            raise
        except NonRetryableError:
            status['completed'] = True
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
