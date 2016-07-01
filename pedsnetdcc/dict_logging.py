import collections
import datetime
import logging
import logging.handlers
import time

nocolor = 0
red = 31
green = 32
yellow = 33
blue = 34
gray = 37
starttime = time.time()


def secs_since(starttime):
    """Return the (padded) number of whole seconds since `starttime`.

    :param starttime: time to calculate seconds since
    :type starttime:  int number of seconds since the epoch
    :returns:         number of seconds since starttime padded to 4 with 0s
    :rtype:           str
    """
    return '{0:0>4}'.format(int(time.time() - starttime))


def strtime():
    """Return the current time in a string conforming to RFC3339.

    :returns: current time in RFC3339 format
    :rtype:   str
    """

    curr_time = time.time()
    fmtd_time = time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(curr_time))

    utc_offset = ((datetime.datetime.fromtimestamp(curr_time) -
                   datetime.datetime.utcfromtimestamp(curr_time)).
                  total_seconds())
    utc_hours = int(utc_offset // 3600)
    utc_mins = abs(int(utc_offset % 3600 // 60))

    return '{0}{1:0=+3}:{2:0>2}'.format(fmtd_time, utc_hours, utc_mins)


def levelcolor(level):
    """Return the terminal color number appropriate for the logging level.

    :param int level: logging level in integer form
    :returns:         the SGR parameter number for foreground text color
    :rtype:           int
    """

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

    Depending on the output format type in self.output the filter converts a
    dict-type log msg to the appropriate format. The formats are intended to
    mimic the Sirupsen/logrus formats and are: 'json', 'text', and 'tty'. If
    self.output is None or a different string, 'json' formatting is used.

    The 'json' output format is a valid json object with current time and log
    level added to the log msg dict.

    The 'text' output is a 'key=val key=val' string with current time and log
    level added to the data from the log msg dict.

    The 'tty' output is a colorized output with the log level (truncated to
    four characters) followed by the number of seconds since program start
    followed by the 'msg' value from the log msg dict, followed by any other
    data from the dict in 'key=val key=val' format.
    """

    def __init__(self, output=None):
        """Create a DictLogFilter object, setting the output format if given.

        :param output: the output format
        :type output:  None or str from ['json', 'text', or 'tty']
        :returns:      a new DictLogFilter object
        :rtype:        DictLogFilter
        """
        self.output = output

    def filter(self, record):
        """Format the log record if record.msg is a dict.

        Dispatch the record to appropriate '*filter' method depending on the
        the value of self.output. json formatting is the default.

        :param record: a log record instance
        :type record:  logging.LogRecord
        :returns:      always True to indicate the record should be handled
        :rtype:        bool
        """

        if not isinstance(record.msg, dict):
            return True

        if self.output == 'text':
            return self.text_filter(record)
        elif self.output == 'tty':
            return self.tty_filter(record)
        else:
            return self.json_filter(record)

    def json_filter(self, record):
        """Format the log record in json style.

        :param record: a log record instance
        :type record:  logging.LogRecord
        :returns:      always True to indicate the record should be handled
        :rtype:        bool
        """
        record.msg['time'] = strtime()
        record.msg['level'] = record.levelname.lower()
        return True

    def tty_filter(self, record):
        """Format the log record in tty style.

        :param record: a log record instance
        :type record:  logging.LogRecord
        :returns:      always True to indicate the record should be handled
        :rtype:        bool
        """

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
        """Format the log record in text style.

        :param record: a log record instance
        :type record:  logging.LogRecord
        :returns:      always True to indicate the record should be handled
        :rtype:        bool
        """

        out = 'time="{0}" level="{1}"'.format(strtime(),
                                              record.levelname.lower())
        for k, v in record.msg.items():
            v = str(v).replace('"', r'\"')
            out = out + ' {0}="{1}"'.format(k, v)

        record.msg = out

        return True


class DictQueueHandler(logging.handlers.QueueHandler):
    """A logging QueueHandler that does *not* convert dict msgs to strings.

    In order to make the log record picklable, the logging QueueHandler calls
    self.prepare, which calls self.format, before enqueuing the log record.
    This is problematic for the DictLogFilter because it converts the dict into
    a string. See https://hg.python.org/cpython/file/3.5/Lib/logging/handlers.py#l1289
    for details.
    
    This handler attempts to make the log record picklable without converting
    dict msgs to strings. If the msg is a dict, it reconstructs the dict with
    the result of calling str on all its items. If args exist, it does the same
    there. If exc_info exists, it uses self.formatter.formatException to
    convert it to string and then stores it in the exc_text attribute and wipes
    exc_info.
    """  # noqa

    formatter = logging.Formatter()

    def prepare(self, record):
        """Prepare the log record for pickling.

        If record.msg is a dict, call str on all its items. If record.args is
        a sequence or dict, call str on all its items. Convert record.exc_info
        to a string at record.exc_text, using self.formatter.formatException,
        and wipe out record.exc_info.

        :param record: the log record to prepare
        :type record:  logging.LogRecord
        :returns:      the prepared log record
        :rtype:        logging.LogRecord
        """

        if isinstance(record.msg, collections.Mapping):
            new_msg = {str(k): str(v) for k, v in record.msg.items()}
            record.msg = new_msg

        if isinstance(record.args, collections.Sequence):
            new_args = [str(a) for a in record.args]
            record.args = tuple(new_args)
        elif isinstance(record.args, collections.Mapping):
            new_args = {str(k): str(v) for k, v in record.args.items()}
            record.args = new_args

        if record.exc_info:
            record.exc_text = self.formatter.formatException(record.exc_info)
            record.exc_info = None

        return record
