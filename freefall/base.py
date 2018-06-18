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
    def download(self, args, exit_if_failed=False):
        for resource in self._as_resources(args):
            logger = self.logger(resource)
            log_handler = self._log_handler(resource)
            logger.addHandler(log_handler)

            try:
                self._download(resource)
            except Completed:
                logger.warning('Already completed')
            except Downloading:
                logger.warning('Already downloading')
            except (RetryableError, NonRetryableError) as e:
                logger.warning('%s: %s', type(e).__name__, str(e))
                logger.debug('Detail', exc_info=True)
                if exit_if_failed:
                    raise
            except Exception as e:
                logger.exception(str(e))
                raise
            else:
                logger.info('Completed')
            finally:
                log_handler.close()
                logger.removeHandler(log_handler)

    def _download(self, resource):
        with self._lock_status(resource):
            status = self._load_status(resource)

            if status.get('downloading'):
                raise Downloading()
            if status.get('completed'):
                raise Completed()

            status['downloading'] = True
            self._save_status(resource, status)

        try:
            self._force_download(resource)
        except RetryableError:
            status['completed'] = False
            status['failed'] = True
            raise
        except NonRetryableError:
            status['completed'] = True
            status['failed'] = True
            raise
        else:
            status['completed'] = True
            status['failed'] = False
        finally:
            status['downloading'] = False

            with self._lock_status(resource):
                self._save_status(resource, status)

    def logger(self, resource=None):
        logger = logging.getLogger(type(self).__name__)
        if resource is not None:
            return logger.getChild(str(self._resource_id(resource)))
        return logger

    def _log_handler(self, resource):
        log_prefix = Path(self._archive_prefix(resource))
        log_prefix.mkdir(exist_ok=True, parents=True)
        file_handler = logging.FileHandler(
            str(log_prefix / 'log.txt'), 'a', encoding='utf-8')
        file_handler.setFormatter(logging.Formatter(
            '[%(asctime)s] %(name)s: %(levelname)s: %(message)s'))
        file_handler.setLevel('DEBUG')
        return file_handler

    @abstractmethod
    def _as_resources(self, args):
        pass

    @abstractmethod
    def _force_download(self, resource):
        pass

    @abstractmethod
    def _lock_status(self, resource):
        pass

    @abstractmethod
    def _load_status(self, resource):
        pass

    @abstractmethod
    def _save_status(self, resource, status):
        pass

    def _resource_id(self, resource):
        return resource.id

    def _archive_prefix(self, resource):
        return resource.archive_prefix
