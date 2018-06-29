from datetime import datetime, timezone, timedelta


def utcnow():
    return datetime.now(timezone(timedelta()))
