import copy
import logging
from logging.handlers import QueueHandler
import time
from datetime import datetime

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
    ts = time.time()
    utc_offset = (datetime.fromtimestamp(ts) -
                  datetime.utcfromtimestamp(ts)).total_seconds()
    return '{0}{1:0=+3}:{2:0>2}'.format(time.strftime('%Y-%m-%dT%H:%M:%S',
                                                      time.localtime(ts)),
                                        int(utc_offset // 3600),
                                        abs(int(utc_offset % 3600 // 60)))


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

    Depending on the output format type chosen at instantiation, the filter
    converts a dict-type log message to the appropriate format and places it
    on the log record in an aptly named attribute. The formats are intended to
    mimic the Sirupsen/logrus formats.

    'text' output is placed at record.text_out, 'tty' output is placed at
    record.tty_out, and 'json' output is placed at record.json_out. This means
    that the filter should be paired with a log formatter that prints these
    attributes, like `logging.Formatter('%(text_out)s')` etc.
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
        record.json_out = copy.copy(record.msg)
        record.json_out['time'] = strtime()
        record.json_out['level'] = record.levelname.lower()

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

        record.tty_out = out

        return True

    def text_filter(self, record):
        out = "time='{0}' level='{1}'".format(strtime(),
                                              record.levelname.lower())
        for k, v in record.msg.items():
            out = out + " {0}='{1}'".format(k, v)

        record.text_out = out

        return True


class NoFmtQueueHandler(QueueHandler):
    def prepare(self, record):
        return record
