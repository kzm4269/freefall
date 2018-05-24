import abc
import logging
import os
from pathlib import Path


__all__ = [
    'Downloading', 'Completed', 'RetryableError', 'NonRetryableError', 
    'BaseDownloader',
]


class Downloading(Exception):
    pass


class Completed(Exception):
    pass


class RetryableError(Exception):
    pass


class NonRetryableError(Exception):
    pass


class BaseDownloader:
    def __init__(self, *, archive=None, log_formatter=None):
        if archive is None:
            self._archive = 'archive'
        else:
            self._archive = Path(archive)

        self._logger = logging.getLogger(type(self).__name__)

        if log_formatter is None:
            self._log_formatter = logging.Formatter(logging.BASIC_FORMAT)
        else:
            self._log_formatter = log_formatter

    @abc.abstractmethod
    def _resource_ids(self, urls):
        pass

    @abc.abstractmethod
    def _download_resource(self, resource_id, prefix, logger):
        pass

    @property
    def logger(self):
        return self._logger

    def _download(self, resource_id, force):
        logger = self._logger.getChild(resource_id)

        prefix = self._archive / resource_id
        completed_marker = prefix / '.completed'
        error_marker = prefix / '.error'
        downloading_marker = prefix / '.downloading'

        if completed_marker.exists() and not force:
            raise Completed(resource_id)

        os.makedirs(str(prefix), exist_ok=True)
        try:
            downloading_marker.touch(exist_ok=False)
        except FileExistsError:
            raise Downloading(resource_id)

        file_handler = logging.FileHandler(
            str(prefix / 'log.txt'), 'w', encoding='utf-8')
        file_handler.setFormatter(self._log_formatter)
        file_handler.setLevel('DEBUG')
        logger.addHandler(file_handler)

        logger.info('Start downloading')

        try:
            self._download_resource(resource_id, str(prefix), logger)
        except RetryableError as e:
            logger.warning('%s: %s', type(e).__name__, str(e))
            logger.debug('Detail', exc_info=True)
            error_marker.touch()
        except NonRetryableError as e:
            logger.error('%s: %s', type(e).__name__, str(e))
            logger.debug('Detail', exc_info=True)
            error_marker.touch()
            completed_marker.touch()
        else:
            logger.info('Completed')
            completed_marker.touch()
        finally:
            file_handler.close()
            os.remove(downloading_marker)

    def download(self, urls, force=False):
        if isinstance(urls, str):
            urls = [urls]

        for resource_id in self._resource_ids(urls):
            logger = self._logger.getChild(resource_id)
            try:
                self._download(resource_id, force=force)
            except Completed:
                logger.warning('Already completed')
            except Downloading:
                logger.warning('Already downloading')
            except Exception as e:
                logger.exception(str(e))
                raise
