from datetime import datetime, timezone


def utcnow():
    return datetime.now(timezone.utc)


def localnow():
    return utcnow().astimezone()


def local_timezone():
    return localnow().tzinfo
