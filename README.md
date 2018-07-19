# Freefall
Simple Archiver

## Install

```
pip install git+https://github.com/kzm4269/freefall.git
```

or

```
pip install -I git+https://github.com/kzm4269/freefall.git@develop
```

## Usage

```python
import freefall


class Downloader(freefall.FileBasedDownloader):
    def as_requests(self, args):
        """Convert given arguments (i.e. URLs) to request objects."""
        return args  # You can also return arguments as they are.

    def archive_prefix(self, request):
        """Return the path to the directory for saving downloaded files."""
        return 'archive/{}'.format(request)

    def _force_process(self, request):
        """Process given request."""
        print('download', request)


downloader = Downloader()
downloader.download(['hello'])
```

## License

MIT License
