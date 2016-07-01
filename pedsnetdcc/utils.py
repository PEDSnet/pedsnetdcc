import re
import urllib.parse


def make_conn_str(uri, search_path=None, password=None):
    """Return a libpq-compliant connection string usable with psycopg2.

    If `search_path` is supplied, it is incorporated into the connection string
    via the `options` parameter, overriding any existing search_path in the
    `uri`.

    If `password` is supplied, it is incorporated into the connection string,
    overriding any existing password in the `uri`.

    Keywords recognized: host, port, dbname, user, password, options.

    Other query parameters are passed as is.

    See https://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING

    :param str uri:     The base uri to build the conn string from.
    :param search_path: An optional search path to include.
    :type search_path:  str or None
    :param password:    An optional password to include.
    :type password:     str or None
    :returns:           The constructed conn string.
    :rtype:             str
    :raises ValueError: if more than one 'options' query parameter is given in `uri`.
    """  # noqa

    components = urllib.parse.urlparse(uri)
    params = urllib.parse.parse_qs(components.query)
    parts = []

    if components.hostname:
        parts.append('host=' + components.hostname)
    if components.port:
        parts.append('port=' + str(components.port))
    if components.path:
        parts.append('dbname=' + components.path.lstrip('/'))
    if components.username:
        parts.append('user=' + components.username)
    if password:
        parts.append('password=' + password)
    elif components.password:
        parts.append('password=' + components.password)

    o_dict = {}
    if 'options' in params:
        if len(params['options']) > 1:
            raise ValueError("More than one `options` query parameter in uri.")
        options_val = params['options'][0].strip("'")
        options = [o.strip() for o in options_val.split('-c ')]
        for option in options:
            if not option:
                continue
            k, v = option.split('=', 1)
            o_dict[k] = v

    if search_path:
        o_dict['search_path'] = search_path

    options_value = ' '.join(
        ["-c {}={}".format(k, o_dict[k]) for k in sorted(o_dict)])  # noqa
    if options_value:
        params['options'] = [options_value]

    for k in sorted(params):
        values = params[k]
        for value in values:
            if ' ' in value:
                value = "'{}'".format(value)
            parts.append('{0}={1}'.format(k, value))

    return ' '.join(parts)


def get_search_path(conn_str):
    """Return the `search_path` from a libpq-compliant connection string.

    The `search_path` is extracted using a regular expression. If the regular
    expression is not matched in the `conn_str`, None is returned.

    See https://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING

    :param str conn_str: A libpq-compliant connection string.
    :returns:            The extracted `search_path`, if one is found.
    :rtype:              str or None
    """  # noqa

    match = re.search(r"search_path=(.*?)[' ]", conn_str)

    if match:
        return match.group(1)

    return None
