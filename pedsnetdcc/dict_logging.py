from datetime import datetime
import logging
from logging.handlers import QueueHandler
import time

nocolor = 0
red = 31
green = 32
yellow = 33
blue = 34
gray = 37
starttime = time.time()


def secs_since(starttime):
    """Return the (padded) number of whole seconds since `starttime`."""
    return '{0:0>4}'.format(int(time.time() - starttime))


def strtime():
    """Return the current time in a string conforming to RFC3339."""

    curr_time = time.time()
    fmtd_time = time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(curr_time))

    utc_offset = (datetime.fromtimestamp(curr_time) -
                  datetime.utcfromtimestamp(curr_time)).total_seconds()
    utc_hours = int(utc_offset // 3600)
    utc_mins = abs(int(utc_offset % 3600 // 60))

    return '{0}{1:0=+3}:{2:0>2}'.format(fmtd_time, utc_hours, utc_mins)


def levelcolor(level):
    """Return the terminal color number appropriate for the logging level."""

    if level == logging.DEBUG:
        return green
    elif level == logging.WARNING:
        return yellow
    elif level in (logging.ERROR, logging.CRITICAL):
        return red
    else:
        return blue


class DictLogFilter(object):
    """A logging 'filter' that adds arbitrary data to messages.

    Depending on the output format type in `self.output` which can be chosen at
    instantiation or assigned afterwards, the filter converts a dict-type log
    message to the appropriate format. The formats are intended to mimic the
    Sirupsen/logrus formats.
    """

    def __init__(self, output=None):
        self.output = output

    def filter(self, record):

        if not isinstance(record.msg, dict):
            return True

        if self.output == 'text':
            return self.text_filter(record)
        elif self.output == 'tty':
            return self.tty_filter(record)
        else:
            return self.json_filter(record)

    def json_filter(self, record):
        record.msg['time'] = strtime()
        record.msg['level'] = record.levelname.lower()

        return True

    def tty_filter(self, record):
        out = '\x1b[{0}m{1}\x1b[0m[{2}] {3}'.format(levelcolor(record.levelno),
                                                    record.levelname[:4],
                                                    secs_since(starttime),
                                                    record.msg.get('msg', ''))
        out = '{0:<80}'.format(out)
        for k, v in record.msg.items():
            if k != 'msg':
                out = out + ' \x1b[{0}m{1}\x1b[0m={2}'\
                    .format(levelcolor(record.levelno), k, v)

        record.msg = out

        return True

    def text_filter(self, record):
        out = 'time="{0}" level="{1}"'.format(strtime(),
                                              record.levelname.lower())
        for k, v in record.msg.items():
            v = str(v).replace('"', r'\"')
            out = out + ' {0}="{1}"'.format(k, v)

        record.msg = out

        return True


class DictQueueHandler(QueueHandler):
    """A Logging QueueHandler that does *not* format the record.

    The standard library's QueueHandler formats the record before enqueuing it,
    which turns the dict msg into a string message. This is problematic for
    the DictLogFilter on the receiving end. This handler does not format the
    record if the msg is a dict. However, it may run in to pickle-ability
    errors at some point, see https://hg.python.org/cpython/file/3.5/Lib/logging/handlers.py#l1289
    for details.
    """  # noqa

    def prepare(self, record):
        if not isinstance(record.msg, dict):
            return super(DictQueueHandler, self).prepare(record)
        return record
