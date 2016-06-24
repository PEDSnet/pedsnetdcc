import re


def clean(s):
    """Strip leading & trailing space, remove newlines, compress space.
    Also expand '{NL}' to a literal newline.
    :param str s:
    :rtype: str
    """
    s = s.strip()
    s = re.sub(' +', ' ', s)
    s = s.replace('\n', '')
    s = s.replace('\r', '')
    s = s.replace('{NL}', '\n')
    return s
