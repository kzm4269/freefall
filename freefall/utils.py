from datetime import datetime

from pytz import timezone


def utcnow():
    return datetime.now(timezone('UTC'))
