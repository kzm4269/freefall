import logging
from abc import ABCMeta, abstractmethod


class Downloading(Exception):
    pass


class Completed(Exception):
    pass


class RetryableError(Exception):
    pass


class NonRetryableError(Exception):
    pass


class BaseDownloader(metaclass=ABCMeta):
    def download(self, args):
        for resource in self.as_resources(args):
            logger = self.logger(resource)

            try:
                self.download_resource(resource)
            except Completed:
                logger.warning('Already completed')
            except Downloading:
                logger.warning('Already downloading')
            except (RetryableError, NonRetryableError) as e:
                logger.warning('%s: %s', type(e).__name__, str(e))
                logger.debug('Detail', exc_info=True)
            except Exception as e:
                logger.exception(str(e))
                raise

    def download_resource(self, resource):
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

    @abstractmethod
    def as_resources(self, args):
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

    @staticmethod
    def _resource_id(resource):
        return resource.id

    @staticmethod
    def _archive_prefix(resource):
        return resource.archive_prefix
