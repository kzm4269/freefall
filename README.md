# Freefall
Simple Downloader

## Install

```
pip install -U git+https://github.com/kzm4269/freefall.git
```

Using Pipenv:

```
pipenv install --selective-upgrade "git+https://github.com/kzm4269/freefall.git#egg=freefall"
```


## Usage

```python
import freefall


class Downloader(freefall.FileBasedDownloader):
    def as_requests(self, args):
        """Convert arguments (i.e. URLs) to request objects."""
        return args  # You can also return arguments as they are.

    def archive_prefix(self, request):
        """Return the path to the downloads directory."""
        return 'archive/{}'.format(request)

    def _process_request(self, request):
        """Process given request."""
        print('download', request)


downloader = Downloader()
downloader.download(['hello'])
```

## License

MIT License
