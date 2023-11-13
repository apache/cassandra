# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import cmd
import codecs
import configparser
import csv
import getpass
import argparse
import os
import re
import subprocess
import sys
import traceback
import warnings
import webbrowser
from contextlib import contextmanager
from enum import Enum
from io import StringIO
from uuid import UUID

import cassandra
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.cqltypes import cql_typename
from cassandra.marshal import int64_unpack
from cassandra.metadata import (ColumnMetadata, KeyspaceMetadata, TableMetadata)
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement, ordered_dict_factory, TraceUnavailable
from cassandra.util import datetime_from_timestamp

from cqlshlib import cql3handling, pylexotron, sslhandling, cqlshhandling, authproviderhandling
from cqlshlib.copyutil import ExportTask, ImportTask
from cqlshlib.displaying import (ANSI_RESET, BLUE, COLUMN_NAME_COLORS, CYAN,
                                 RED, WHITE, FormattedValue, colorme)
from cqlshlib.formatting import (DEFAULT_DATE_FORMAT, DEFAULT_NANOTIME_FORMAT,
                                 DEFAULT_TIMESTAMP_FORMAT, CqlType, DateTimeFormat,
                                 format_by_type)
from cqlshlib.tracing import print_trace, print_trace_session
from cqlshlib.util import get_file_encoding_bomsize, is_file_secure
try:
    from cqlshlib.serverversion import version as build_version
except ImportError:
    build_version = 'UNKNOWN'


UTF8 = 'utf-8'

description = "CQL Shell for Apache Cassandra"
version = "6.2.0"

readline = None
try:
    # Check if tty first, as readline doesn't check and only cares about $TERM.
    # We don't want the funky escape code stuff to be output if not a tty.
    if sys.stdin.isatty():
        import readline
except ImportError:
    pass

# On Linux, the Python webbrowser module uses the 'xdg-open' executable
# to open a file/URL. But that only works, if the current session has been
# opened from _within_ a desktop environment. I.e. 'xdg-open' will fail,
# if the session's been opened via ssh to a remote box.
#
try:
    webbrowser.register_standard_browsers()  # registration is otherwise lazy in Python3
except AttributeError:
    pass
if webbrowser._tryorder and webbrowser._tryorder[0] == 'xdg-open' and os.environ.get('XDG_DATA_DIRS', '') == '':
    # only on Linux (some OS with xdg-open)
    webbrowser._tryorder.remove('xdg-open')
    webbrowser._tryorder.append('xdg-open')

warnings.filterwarnings("ignore", r".*blist.*")

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 9042
DEFAULT_SSL = False
DEFAULT_CONNECT_TIMEOUT_SECONDS = 5
DEFAULT_REQUEST_TIMEOUT_SECONDS = 10

DEFAULT_FLOAT_PRECISION = 5
DEFAULT_DOUBLE_PRECISION = 5
DEFAULT_MAX_TRACE_WAIT = 10

if readline is not None and readline.__doc__ is not None and 'libedit' in readline.__doc__:
    DEFAULT_COMPLETEKEY = '\t'
else:
    DEFAULT_COMPLETEKEY = 'tab'

cqldocs = None
cqlruleset = None
CASSANDRA_CQL_HTML = None

epilog = """Connects to %(DEFAULT_HOST)s:%(DEFAULT_PORT)d by default. These
defaults can be changed by setting $CQLSH_HOST and/or $CQLSH_PORT. When a
host (and optional port number) are given on the command line, they take
precedence over any defaults.""" % globals()


parser = argparse.ArgumentParser(description=description, epilog=epilog,
                                 usage="Usage: %(prog)s [options] [host [port]]",
                                 prog='cqlsh')
parser.add_argument('-v', '--version', action='version', version='cqlsh ' + version)
parser.add_argument("-C", "--color", action='store_true', dest='color',
                                            help='Always use color output')
parser.add_argument("--no-color", action='store_false', dest='color', help='Never use color output')
parser.add_argument("--browser", dest='browser', help="""The browser to use to display CQL help, where BROWSER can be:
                                                    - one of the supported browsers in https://docs.python.org/3/library/webbrowser.html.
                                                    - browser path followed by %%s, example: /usr/bin/google-chrome-stable %%s""")

parser.add_argument('--ssl', action='store_true', help='Use SSL', default=False)
parser.add_argument("-u", "--username", help="Authenticate as user.")
parser.add_argument("-p", "--password", help="Authenticate using password.")
parser.add_argument('-k', '--keyspace', help='Authenticate to the given keyspace.')
parser.add_argument("-f", "--file", help="Execute commands from FILE, then exit")
parser.add_argument('--debug', action='store_true',
                    help='Show additional debugging information')
parser.add_argument('--coverage', action='store_true',
                    help='Collect coverage data')
parser.add_argument("--encoding", help="Specify a non-default encoding for output."
                    + " (Default: %s)" % (UTF8,))
parser.add_argument("--cqlshrc", help="Specify an alternative cqlshrc file location.")
parser.add_argument("--credentials", help="Specify an alternative credentials file location.")
parser.add_argument('--cqlversion', default=None,
                    help='Specify a particular CQL version, '
                    'by default the highest version supported by the server will be used.'
                    ' Examples: "3.0.3", "3.1.0"')
parser.add_argument("--protocol-version", type=int, default=None,
                    help='Specify a specific protcol version otherwise the client will default and downgrade as necessary')

parser.add_argument("-e", "--execute", help='Execute the statement and quit.')
parser.add_argument("--connect-timeout", default=DEFAULT_CONNECT_TIMEOUT_SECONDS, dest='connect_timeout',
                    help='Specify the connection timeout in seconds (default: %(default)s seconds).')
parser.add_argument("--request-timeout", default=DEFAULT_REQUEST_TIMEOUT_SECONDS, dest='request_timeout',
                    help='Specify the default request timeout in seconds (default: %(default)s seconds).')
parser.add_argument("-t", "--tty", action='store_true', dest='tty',
                    help='Force tty mode (command prompt).')

# This is a hidden option to suppress the warning when the -p/--password command line option is used.
# Power users may use this option if they know no other people has access to the system where cqlsh is run or don't care about security.
# Use of this option in scripting is discouraged. Please use a (temporary) credentials file where possible.
# The Cassandra distributed tests (dtests) also use this option in some tests when a well-known password is supplied via the command line.
parser.add_argument("--insecure-password-without-warning", action='store_true', dest='insecure_password_without_warning',
                    help=argparse.SUPPRESS)

# use cfarguments for config file

cfarguments, args = parser.parse_known_args()

# BEGIN history config


def mkdirp(path):
    """Creates all parent directories up to path parameter or fails when path exists, but it is not a directory."""

    try:
        os.makedirs(path)
    except OSError:
        if not os.path.isdir(path):
            raise


def resolve_cql_history_file():
    default_cql_history = os.path.expanduser(os.path.join('~', '.cassandra', 'cqlsh_history'))
    if 'CQL_HISTORY' in os.environ:
        return os.environ['CQL_HISTORY']
    else:
        return default_cql_history


HISTORY = resolve_cql_history_file()
HISTORY_DIR = os.path.dirname(HISTORY)

try:
    mkdirp(HISTORY_DIR)
except OSError:
    print('\nWarning: Cannot create directory at `%s`. Command history will not be saved. Please check what was the environment property CQL_HISTORY set to.\n' % HISTORY_DIR)


# END history config

DEFAULT_CQLSHRC = os.path.expanduser(os.path.join('~', '.cassandra', 'cqlshrc'))

if cfarguments.cqlshrc is not None:
    CONFIG_FILE = os.path.expanduser(cfarguments.cqlshrc)
    if not os.path.exists(CONFIG_FILE):
        print('\nWarning: Specified cqlshrc location `%s` does not exist.  Using `%s` instead.\n' % (CONFIG_FILE, DEFAULT_CQLSHRC))
        CONFIG_FILE = DEFAULT_CQLSHRC
else:
    CONFIG_FILE = DEFAULT_CQLSHRC

CQL_DIR = os.path.dirname(CONFIG_FILE)

CQL_ERRORS = (
    cassandra.AlreadyExists, cassandra.AuthenticationFailed, cassandra.CoordinationFailure,
    cassandra.InvalidRequest, cassandra.Timeout, cassandra.Unauthorized, cassandra.OperationTimedOut,
    cassandra.cluster.NoHostAvailable,
    cassandra.connection.ConnectionBusy, cassandra.connection.ProtocolError, cassandra.connection.ConnectionException,
    cassandra.protocol.ErrorMessage, cassandra.protocol.InternalError, cassandra.query.TraceUnavailable
)

debug_completion = bool(os.environ.get('CQLSH_DEBUG_COMPLETION', '') == 'YES')


class NoKeyspaceError(Exception):
    pass


class KeyspaceNotFound(Exception):
    pass


class ColumnFamilyNotFound(Exception):
    pass


class IndexNotFound(Exception):
    pass


class MaterializedViewNotFound(Exception):
    pass


class ObjectNotFound(Exception):
    pass


class VersionNotSupported(Exception):
    pass


class UserTypeNotFound(Exception):
    pass


class FunctionNotFound(Exception):
    pass


class AggregateNotFound(Exception):
    pass


class DecodeError(Exception):
    verb = 'decode'

    def __init__(self, thebytes, err, colname=None):
        self.thebytes = thebytes
        self.err = err
        self.colname = colname

    def __str__(self):
        return str(self.thebytes)

    def message(self):
        what = 'value %r' % (self.thebytes,)
        if self.colname is not None:
            what = 'value %r (for column %r)' % (self.thebytes, self.colname)
        return 'Failed to %s %s : %s' \
               % (self.verb, what, self.err)

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.message())


def maybe_ensure_text(val):
    return str(val) if val else val


class FormatError(DecodeError):
    verb = 'format'


def full_cql_version(ver):
    while ver.count('.') < 2:
        ver += '.0'
    ver_parts = ver.split('-', 1) + ['']
    vertuple = tuple(list(map(int, ver_parts[0].split('.'))) + [ver_parts[1]])
    return ver, vertuple


class SwitchState(Enum):
    ON = True
    OFF = False


def format_value(val, cqltype, encoding, addcolor=False, date_time_format=None,
                 float_precision=None, colormap=None, nullval=None):
    if isinstance(val, DecodeError):
        if addcolor:
            return colorme(repr(val.thebytes), colormap, 'error')
        else:
            return FormattedValue(repr(val.thebytes))
    return format_by_type(val, cqltype=cqltype, encoding=encoding, colormap=colormap,
                          addcolor=addcolor, nullval=nullval, date_time_format=date_time_format,
                          float_precision=float_precision)


def show_warning_without_quoting_line(message, category, filename, lineno, file=None, line=None):
    if file is None:
        file = sys.stderr
    try:
        file.write(warnings.formatwarning(message, category, filename, lineno, line=''))
    except IOError:
        pass


warnings.showwarning = show_warning_without_quoting_line
warnings.filterwarnings('always', category=cql3handling.UnexpectedTableStructure)


class Shell(cmd.Cmd):
    custom_prompt = os.getenv('CQLSH_PROMPT', '')
    if custom_prompt != '':
        custom_prompt += "\n"
    default_prompt = custom_prompt + "cqlsh> "
    continue_prompt = "   ... "
    keyspace_prompt = custom_prompt + "cqlsh:{}> "
    keyspace_continue_prompt = "{}    ... "
    show_line_nums = False
    debug = False
    coverage = False
    coveragerc_path = None
    stop = False
    last_hist = None
    shunted_query_out = None
    use_paging = True

    default_page_size = 100

    def __init__(self, hostname, port, color=False,
                 username=None, encoding=None, stdin=None, tty=True,
                 completekey=DEFAULT_COMPLETEKEY, browser=None, use_conn=None,
                 cqlver=None, keyspace=None,
                 tracing_enabled=False, expand_enabled=False,
                 display_nanotime_format=DEFAULT_NANOTIME_FORMAT,
                 display_timestamp_format=DEFAULT_TIMESTAMP_FORMAT,
                 display_date_format=DEFAULT_DATE_FORMAT,
                 display_float_precision=DEFAULT_FLOAT_PRECISION,
                 display_double_precision=DEFAULT_DOUBLE_PRECISION,
                 display_timezone=None,
                 max_trace_wait=DEFAULT_MAX_TRACE_WAIT,
                 ssl=False,
                 single_statement=None,
                 request_timeout=DEFAULT_REQUEST_TIMEOUT_SECONDS,
                 protocol_version=None,
                 connect_timeout=DEFAULT_CONNECT_TIMEOUT_SECONDS,
                 is_subshell=False,
                 auth_provider=None):
        cmd.Cmd.__init__(self, completekey=completekey)
        self.hostname = hostname
        self.port = port
        self.auth_provider = auth_provider
        self.username = username

        if isinstance(auth_provider, PlainTextAuthProvider):
            self.username = auth_provider.username
            if not auth_provider.password:
                # if no password is provided, we need to query the user to get one.
                password = getpass.getpass()
                self.auth_provider = PlainTextAuthProvider(username=auth_provider.username, password=password)

        self.keyspace = keyspace
        self.ssl = ssl
        self.tracing_enabled = tracing_enabled
        self.page_size = self.default_page_size
        self.expand_enabled = expand_enabled
        if use_conn:
            self.conn = use_conn
        else:
            kwargs = {}
            if protocol_version is not None:
                kwargs['protocol_version'] = protocol_version
            self.conn = Cluster(contact_points=(self.hostname,), port=self.port, cql_version=cqlver,
                                auth_provider=self.auth_provider,
                                ssl_options=sslhandling.ssl_settings(hostname, CONFIG_FILE) if ssl else None,
                                load_balancing_policy=WhiteListRoundRobinPolicy([self.hostname]),
                                control_connection_timeout=connect_timeout,
                                connect_timeout=connect_timeout,
                                **kwargs)
        self.owns_connection = not use_conn

        if keyspace:
            self.session = self.conn.connect(keyspace)
        else:
            self.session = self.conn.connect()

        if browser == "":
            browser = None
        self.browser = browser
        self.color = color

        self.display_nanotime_format = display_nanotime_format
        self.display_timestamp_format = display_timestamp_format
        self.display_date_format = display_date_format

        self.display_float_precision = display_float_precision
        self.display_double_precision = display_double_precision

        self.display_timezone = display_timezone

        self.session.default_timeout = request_timeout
        self.session.row_factory = ordered_dict_factory
        self.session.default_consistency_level = cassandra.ConsistencyLevel.ONE
        self.get_connection_versions()
        self.set_expanded_cql_version(self.connection_versions['cql'])

        self.current_keyspace = keyspace

        self.max_trace_wait = max_trace_wait
        self.session.max_trace_wait = max_trace_wait

        self.tty = tty
        self.encoding = encoding

        self.output_codec = codecs.lookup(encoding)

        self.statement = StringIO()
        self.lineno = 1
        self.in_comment = False

        self.prompt = ''
        if stdin is None:
            stdin = sys.stdin

        self.check_build_versions()

        if tty:
            self.reset_prompt()
            self.report_connection()
            print('Use HELP for help.')
        else:
            self.show_line_nums = True
        self.stdin = stdin
        self.query_out = sys.stdout
        self.consistency_level = cassandra.ConsistencyLevel.ONE
        self.serial_consistency_level = cassandra.ConsistencyLevel.SERIAL

        self.empty_lines = 0
        self.statement_error = False
        self.single_statement = single_statement
        self.is_subshell = is_subshell

    def check_build_versions(self):
        baseversion = self.connection_versions['build']
        extra = baseversion.rfind('-')
        if extra != -1:
            baseversion = baseversion[0:extra]
        if baseversion != build_version:
            print("WARNING: cqlsh was built against {}, but this server is {}.  All features may not work!".format(build_version, baseversion))

    @property
    def batch_mode(self):
        return not self.tty

    def set_expanded_cql_version(self, ver):
        ver, vertuple = full_cql_version(ver)
        self.cql_version = ver
        self.cql_ver_tuple = vertuple

    def cqlver_atleast(self, major, minor=0, patch=0):
        return self.cql_ver_tuple[:3] >= (major, minor, patch)

    def myformat_value(self, val, cqltype=None, **kwargs):
        if isinstance(val, DecodeError):
            self.decoding_errors.append(val)
        try:
            dtformats = DateTimeFormat(timestamp_format=self.display_timestamp_format,
                                       date_format=self.display_date_format, nanotime_format=self.display_nanotime_format,
                                       timezone=self.display_timezone)
            precision = self.display_double_precision if cqltype is not None and cqltype.type_name == 'double' \
                else self.display_float_precision
            return format_value(val, cqltype=cqltype, encoding=self.output_codec.name,
                                addcolor=self.color, date_time_format=dtformats,
                                float_precision=precision, **kwargs)
        except Exception as e:
            err = FormatError(val, e)
            self.decoding_errors.append(err)
            return format_value(err, cqltype=cqltype, encoding=self.output_codec.name, addcolor=self.color)

    def myformat_colname(self, name, table_meta=None):
        column_colors = COLUMN_NAME_COLORS.copy()
        # check column role and color appropriately
        if table_meta:
            if name in [col.name for col in table_meta.partition_key]:
                column_colors.default_factory = lambda: RED
            elif name in [col.name for col in table_meta.clustering_key]:
                column_colors.default_factory = lambda: CYAN
            elif name in table_meta.columns and table_meta.columns[name].is_static:
                column_colors.default_factory = lambda: WHITE
        return self.myformat_value(name, colormap=column_colors)

    def report_connection(self):
        self.show_host()
        self.show_version()

    def show_host(self):
        print("Connected to {0} at {1}:{2}"
              .format(self.applycolor(self.get_cluster_name(), BLUE),
                      self.hostname,
                      self.port))

    def show_version(self):
        vers = self.connection_versions.copy()
        vers['shver'] = version
        # system.Versions['cql'] apparently does not reflect changes with
        # set_cql_version.
        vers['cql'] = self.cql_version
        print("[cqlsh %(shver)s | Cassandra %(build)s | CQL spec %(cql)s | Native protocol v%(protocol)s]" % vers)

    def show_session(self, sessionid, partial_session=False):
        print_trace_session(self, self.session, sessionid, partial_session)

    def show_replicas(self, token_value, keyspace=None):
        ks = self.current_keyspace if keyspace is None else keyspace
        token_map = self.conn.metadata.token_map
        nodes = token_map.get_replicas(ks, token_map.token_class(token_value))
        addresses = [x.address for x in nodes]
        print(f"{addresses}")

    def get_connection_versions(self):
        result, = self.session.execute("select * from system.local where key = 'local'")
        vers = {
            'build': result['release_version'],
            'protocol': self.conn.protocol_version,
            'cql': result['cql_version'],
        }
        self.connection_versions = vers

    def get_keyspace_names(self):
        return list(self.conn.metadata.keyspaces)

    def get_columnfamily_names(self, ksname=None):
        if ksname is None:
            ksname = self.current_keyspace

        return list(self.get_keyspace_meta(ksname).tables)

    def get_materialized_view_names(self, ksname=None):
        if ksname is None:
            ksname = self.current_keyspace

        return list(self.get_keyspace_meta(ksname).views)

    def get_index_names(self, ksname=None):
        if ksname is None:
            ksname = self.current_keyspace

        return list(self.get_keyspace_meta(ksname).indexes)

    def get_column_names(self, ksname, cfname):
        if ksname is None:
            ksname = self.current_keyspace
        layout = self.get_table_meta(ksname, cfname)
        return list(layout.columns)

    def get_usertype_names(self, ksname=None):
        if ksname is None:
            ksname = self.current_keyspace

        return list(self.get_keyspace_meta(ksname).user_types)

    def get_usertype_layout(self, ksname, typename):
        if ksname is None:
            ksname = self.current_keyspace

        ks_meta = self.get_keyspace_meta(ksname)

        try:
            user_type = ks_meta.user_types[typename]
        except KeyError:
            raise UserTypeNotFound("User type {!r} not found".format(typename))

        return list(zip(user_type.field_names, user_type.field_types))

    def get_userfunction_names(self, ksname=None):
        if ksname is None:
            ksname = self.current_keyspace

        return [f.name for f in list(self.get_keyspace_meta(ksname).functions.values())]

    def get_useraggregate_names(self, ksname=None):
        if ksname is None:
            ksname = self.current_keyspace

        return [f.name for f in list(self.get_keyspace_meta(ksname).aggregates.values())]

    def get_cluster_name(self):
        return self.conn.metadata.cluster_name

    def get_partitioner(self):
        return self.conn.metadata.partitioner

    def get_keyspace_meta(self, ksname):
        if ksname in self.conn.metadata.keyspaces:
            return self.conn.metadata.keyspaces[ksname]

        raise KeyspaceNotFound('Keyspace %r not found.' % ksname)

    def get_keyspaces(self):
        return list(self.conn.metadata.keyspaces.values())

    def get_ring(self, ks):
        self.conn.metadata.token_map.rebuild_keyspace(ks, build_if_absent=True)
        return self.conn.metadata.token_map.tokens_to_hosts_by_ks[ks]

    def get_table_meta(self, ksname, tablename):
        if ksname is None:
            ksname = self.current_keyspace
        ksmeta = self.get_keyspace_meta(ksname)
        if tablename not in ksmeta.tables:
            if ksname == 'system_auth' and tablename in ['roles', 'role_permissions']:
                self.get_fake_auth_table_meta(ksname, tablename)
            else:
                raise ColumnFamilyNotFound("Column family {} not found".format(tablename))
        else:
            return ksmeta.tables[tablename]

    def get_fake_auth_table_meta(self, ksname, tablename):
        # may be using external auth implementation so internal tables
        # aren't actually defined in schema. In this case, we'll fake
        # them up
        if tablename == 'roles':
            ks_meta = KeyspaceMetadata(ksname, True, None, None)
            table_meta = TableMetadata(ks_meta, 'roles')
            table_meta.columns['role'] = ColumnMetadata(table_meta, 'role', cassandra.cqltypes.UTF8Type)
            table_meta.columns['is_superuser'] = ColumnMetadata(table_meta, 'is_superuser', cassandra.cqltypes.BooleanType)
            table_meta.columns['can_login'] = ColumnMetadata(table_meta, 'can_login', cassandra.cqltypes.BooleanType)
        elif tablename == 'role_permissions':
            ks_meta = KeyspaceMetadata(ksname, True, None, None)
            table_meta = TableMetadata(ks_meta, 'role_permissions')
            table_meta.columns['role'] = ColumnMetadata(table_meta, 'role', cassandra.cqltypes.UTF8Type)
            table_meta.columns['resource'] = ColumnMetadata(table_meta, 'resource', cassandra.cqltypes.UTF8Type)
            table_meta.columns['permission'] = ColumnMetadata(table_meta, 'permission', cassandra.cqltypes.UTF8Type)
        else:
            raise ColumnFamilyNotFound("Column family {} not found".format(tablename))

    def get_index_meta(self, ksname, idxname):
        if ksname is None:
            ksname = self.current_keyspace
        ksmeta = self.get_keyspace_meta(ksname)

        if idxname not in ksmeta.indexes:
            raise IndexNotFound("Index {} not found".format(idxname))

        return ksmeta.indexes[idxname]

    def get_view_meta(self, ksname, viewname):
        if ksname is None:
            ksname = self.current_keyspace
        ksmeta = self.get_keyspace_meta(ksname)

        if viewname not in ksmeta.views:
            raise MaterializedViewNotFound("Materialized view '{}' not found".format(viewname))
        return ksmeta.views[viewname]

    def get_object_meta(self, ks, name):
        if name is None:
            if ks and ks in self.conn.metadata.keyspaces:
                return self.conn.metadata.keyspaces[ks]
            elif self.current_keyspace is None:
                raise ObjectNotFound("'{}' not found in keyspaces".format(ks))
            else:
                name = ks
                ks = self.current_keyspace

        if ks is None:
            ks = self.current_keyspace

        ksmeta = self.get_keyspace_meta(ks)

        if name in ksmeta.tables:
            return ksmeta.tables[name]
        elif name in ksmeta.indexes:
            return ksmeta.indexes[name]
        elif name in ksmeta.views:
            return ksmeta.views[name]

        raise ObjectNotFound("'{}' not found in keyspace '{}'".format(name, ks))

    def get_trigger_names(self, ksname=None):
        if ksname is None:
            ksname = self.current_keyspace

        return [trigger.name
                for table in list(self.get_keyspace_meta(ksname).tables.values())
                for trigger in list(table.triggers.values())]

    def reset_statement(self):
        self.reset_prompt()
        self.statement.truncate(0)
        self.statement.seek(0)
        self.empty_lines = 0

    def reset_prompt(self):
        if self.current_keyspace is None:
            self.set_prompt(self.default_prompt, True)
        else:
            self.set_prompt(self.keyspace_prompt.format(self.current_keyspace), True)

    def set_continue_prompt(self):
        if self.empty_lines >= 3:
            self.set_prompt("Statements are terminated with a ';'.  You can press CTRL-C to cancel an incomplete statement.")
            self.empty_lines = 0
            return
        if self.current_keyspace is None:
            self.set_prompt(self.continue_prompt)
        else:
            spaces = ' ' * len(str(self.current_keyspace))
            self.set_prompt(self.keyspace_continue_prompt.format(spaces))
        self.empty_lines = self.empty_lines + 1 if not self.lastcmd else 0

    @contextmanager
    def prepare_loop(self):
        readline = None
        if self.tty and self.completekey:
            try:
                import readline
            except ImportError:
                pass
            else:
                old_completer = readline.get_completer()
                readline.set_completer(self.complete)
                if readline.__doc__ is not None and 'libedit' in readline.__doc__:
                    readline.parse_and_bind("bind -e")
                    readline.parse_and_bind("bind '" + self.completekey + "' rl_complete")
                    readline.parse_and_bind("bind ^R em-inc-search-prev")
                else:
                    readline.parse_and_bind(self.completekey + ": complete")
        # start coverage collection if requested, unless in subshell
        if self.coverage and not self.is_subshell:
            # check for coveragerc file, write it if missing
            if os.path.exists(CQL_DIR):
                self.coveragerc_path = os.path.join(CQL_DIR, '.coveragerc')
                covdata_path = os.path.join(CQL_DIR, '.coverage')
                if not os.path.isfile(self.coveragerc_path):
                    with open(self.coveragerc_path, 'w') as f:
                        f.writelines(["[run]\n",
                                      "concurrency = multiprocessing\n",
                                      "data_file = {}\n".format(covdata_path),
                                      "parallel = true\n"]
                                     )
                # start coverage
                import coverage
                self.cov = coverage.Coverage(config_file=self.coveragerc_path)
                self.cov.start()
        try:
            yield
        finally:
            if readline is not None:
                readline.set_completer(old_completer)
            if self.coverage and not self.is_subshell:
                self.stop_coverage()

    def get_input_line(self, prompt=''):
        if self.tty:
            self.lastcmd = input(str(prompt))
            line = self.lastcmd + '\n'
        else:
            self.lastcmd = self.stdin.readline()
            line = self.lastcmd
            if not len(line):
                raise EOFError
        self.lineno += 1
        return line

    def use_stdin_reader(self, until='', prompt=''):
        until += '\n'
        while True:
            try:
                newline = self.get_input_line(prompt=prompt)
            except EOFError:
                return
            if newline == until:
                return
            yield newline

    def cmdloop(self, intro=None):
        """
        Adapted from cmd.Cmd's version, because there is literally no way with
        cmd.Cmd.cmdloop() to tell the difference between "EOF" showing up in
        input and an actual EOF.
        """
        with self.prepare_loop():
            while not self.stop:
                try:
                    if self.single_statement:
                        line = self.single_statement
                        self.stop = True
                    else:
                        line = self.get_input_line(self.prompt)
                    self.statement.write(line)
                    if self.onecmd(self.statement.getvalue()):
                        self.reset_statement()
                except EOFError:
                    self.handle_eof()
                except CQL_ERRORS as cqlerr:
                    self.printerr(cqlerr.message)
                except KeyboardInterrupt:
                    self.reset_statement()
                    print('')

    def strip_comment_blocks(self, statementtext):
        comment_block_in_literal_string = re.search('["].*[/][*].*[*][/].*["]', statementtext)
        if not comment_block_in_literal_string:
            result = re.sub('[/][*].*[*][/]', "", statementtext)
            if '*/' in result and '/*' not in result and not self.in_comment:
                raise SyntaxError("Encountered comment block terminator without being in comment block")
            if '/*' in result:
                result = re.sub('[/][*].*', "", result)
                self.in_comment = True
            if '*/' in result:
                result = re.sub('.*[*][/]', "", result)
                self.in_comment = False
            if self.in_comment and not re.findall('[/][*]|[*][/]', statementtext):
                result = ''
            return result
        return statementtext

    def onecmd(self, statementtext):
        """
        Returns true if the statement is complete and was handled (meaning it
        can be reset).
        """
        statementtext = self.strip_comment_blocks(statementtext)
        try:
            statements, endtoken_escaped = cqlruleset.cql_split_statements(statementtext)
        except pylexotron.LexingError as e:
            if self.show_line_nums:
                self.printerr('Invalid syntax at line {0}, char {1}'
                              .format(e.linenum, e.charnum))
            else:
                self.printerr('Invalid syntax at char {0}'.format(e.charnum))
            statementline = statementtext.split('\n')[e.linenum - 1]
            self.printerr('  {0}'.format(statementline))
            self.printerr(' {0}^'.format(' ' * e.charnum))
            return True

        while statements and not statements[-1]:
            statements = statements[:-1]
        if not statements:
            return True
        if endtoken_escaped or statements[-1][-1][0] != 'endtoken':
            self.set_continue_prompt()
            return
        for st in statements:
            try:
                self.handle_statement(st, statementtext)
            except Exception as e:
                if self.debug:
                    traceback.print_exc()
                else:
                    self.printerr(e)
        return True

    def handle_eof(self):
        if self.tty:
            print('')
        statement = self.statement.getvalue()
        if statement.strip():
            if not self.onecmd(statement):
                self.printerr('Incomplete statement at end of file')
        self.do_exit()

    def handle_statement(self, tokens, srcstr):
        # Concat multi-line statements and insert into history
        if readline is not None:
            nl_count = srcstr.count("\n")

            new_hist = srcstr.replace("\n", " ").rstrip()

            if nl_count > 1 and self.last_hist != new_hist:
                readline.add_history(new_hist)

            self.last_hist = new_hist
        cmdword = tokens[0][1]
        if cmdword == '?':
            cmdword = 'help'
        custom_handler = getattr(self, 'do_' + cmdword.lower(), None)
        if custom_handler:
            parsed = cqlruleset.cql_whole_parse_tokens(tokens, srcstr=srcstr,
                                                       startsymbol='cqlshCommand')
            if parsed and not parsed.remainder:
                # successful complete parse
                return custom_handler(parsed)
            else:
                return self.handle_parse_error(cmdword, tokens, parsed, srcstr)
        return self.perform_statement(cqlruleset.cql_extract_orig(tokens, srcstr))

    def handle_parse_error(self, cmdword, tokens, parsed, srcstr):
        if cmdword.lower() in ('select', 'insert', 'update', 'delete', 'truncate',
                               'create', 'drop', 'alter', 'grant', 'revoke',
                               'batch', 'list'):
            # hey, maybe they know about some new syntax we don't. type
            # assumptions won't work, but maybe the query will.
            return self.perform_statement(cqlruleset.cql_extract_orig(tokens, srcstr))
        if parsed:
            self.printerr('Improper %s command (problem at %r).' % (cmdword, parsed.remainder[0]))
        else:
            self.printerr(f'Improper {cmdword} command.')

    def do_use(self, parsed):
        ksname = parsed.get_binding('ksname')
        success, _ = self.perform_simple_statement(SimpleStatement(parsed.extract_orig()))
        if success:
            if ksname[0] == '"' and ksname[-1] == '"':
                self.current_keyspace = self.cql_unprotect_name(ksname)
            else:
                self.current_keyspace = ksname.lower()

    def do_select(self, parsed):
        tracing_was_enabled = self.tracing_enabled
        ksname = parsed.get_binding('ksname')
        stop_tracing = ksname == 'system_traces' or (ksname is None and self.current_keyspace == 'system_traces')
        self.tracing_enabled = self.tracing_enabled and not stop_tracing
        statement = parsed.extract_orig()
        self.perform_statement(statement)
        self.tracing_enabled = tracing_was_enabled

    def perform_statement(self, statement):

        stmt = SimpleStatement(statement, consistency_level=self.consistency_level, serial_consistency_level=self.serial_consistency_level, fetch_size=self.page_size if self.use_paging else None)
        success, future = self.perform_simple_statement(stmt)

        if future:
            if future.warnings:
                self.print_warnings(future.warnings)

            if self.tracing_enabled:
                try:
                    for trace in future.get_all_query_traces(max_wait_per=self.max_trace_wait, query_cl=self.consistency_level):
                        print_trace(self, trace)
                except TraceUnavailable:
                    msg = "Statement trace did not complete within %d seconds; trace data may be incomplete." % (self.session.max_trace_wait,)
                    self.writeresult(msg, color=RED)
                    for trace_id in future.get_query_trace_ids():
                        self.show_session(trace_id, partial_session=True)
                except Exception as err:
                    self.printerr("Unable to fetch query trace: %s" % (str(err),))

        return success

    def parse_for_select_meta(self, query_string):
        try:
            parsed = cqlruleset.cql_parse(query_string)[1]
        except IndexError:
            return None
        ks = self.cql_unprotect_name(parsed.get_binding('ksname', None))
        name = self.cql_unprotect_name(parsed.get_binding('cfname', None))
        try:
            return self.get_table_meta(ks, name)
        except ColumnFamilyNotFound:
            try:
                return self.get_view_meta(ks, name)
            except MaterializedViewNotFound:
                raise ObjectNotFound("'{}' not found in keyspace '{}'".format(name, ks))

    def parse_for_update_meta(self, query_string):
        try:
            parsed = cqlruleset.cql_parse(query_string)[1]
        except IndexError:
            return None
        ks = self.cql_unprotect_name(parsed.get_binding('ksname', None))
        cf = self.cql_unprotect_name(parsed.get_binding('cfname'))
        return self.get_table_meta(ks, cf)

    def perform_simple_statement(self, statement):
        if not statement:
            return False, None

        future = self.session.execute_async(statement, trace=self.tracing_enabled)
        result = None
        try:
            result = future.result()
        except CQL_ERRORS as err:
            err_msg = err.message if hasattr(err, 'message') else str(err)
            self.printerr(str(err.__class__.__name__) + ": " + err_msg)
        except Exception:
            import traceback
            self.printerr(traceback.format_exc())

        # Even if statement failed we try to refresh schema if not agreed (see CASSANDRA-9689)
        if not future.is_schema_agreed:
            try:
                self.conn.refresh_schema_metadata(5)  # will throw exception if there is a schema mismatch
            except Exception:
                self.printerr("Warning: schema version mismatch detected; check the schema versions of your "
                              "nodes in system.local and system.peers.")
                self.conn.refresh_schema_metadata(-1)

        if result is None:
            return False, None

        if statement.query_string[:6].lower() == 'select':
            self.print_result(result, self.parse_for_select_meta(statement.query_string))
        elif statement.query_string.lower().startswith("list users") or statement.query_string.lower().startswith("list roles"):
            self.print_result(result, self.get_table_meta('system_auth', 'roles'))
        elif statement.query_string.lower().startswith("list"):
            self.print_result(result, self.get_table_meta('system_auth', 'role_permissions'))
        elif result:
            # CAS INSERT/UPDATE
            self.writeresult("")
            self.print_static_result(result, self.parse_for_update_meta(statement.query_string), with_header=True, tty=self.tty)
        self.flush_output()
        return True, future

    def print_result(self, result, table_meta):
        self.decoding_errors = []

        self.writeresult("")

        def print_all(result, table_meta, tty):
            # Return the number of rows in total
            num_rows = 0
            is_first = True
            while True:
                # Always print for the first page even it is empty
                if result.current_rows or is_first:
                    with_header = is_first or tty
                    self.print_static_result(result, table_meta, with_header, tty, num_rows)
                    num_rows += len(result.current_rows)
                if result.has_more_pages:
                    if self.shunted_query_out is None and tty:
                        # Only pause when not capturing.
                        input("---MORE---")
                    result.fetch_next_page()
                else:
                    if not tty:
                        self.writeresult("")
                    break
                is_first = False
            return num_rows

        num_rows = print_all(result, table_meta, self.tty)
        self.writeresult("(%d rows)" % num_rows)

        if self.decoding_errors:
            for err in self.decoding_errors[:2]:
                self.writeresult(err.message(), color=RED)
            if len(self.decoding_errors) > 2:
                self.writeresult('%d more decoding errors suppressed.'
                                 % (len(self.decoding_errors) - 2), color=RED)

    def print_static_result(self, result, table_meta, with_header, tty, row_count_offset=0):
        if not result.column_names and not table_meta:
            return

        column_names = result.column_names or list(table_meta.columns.keys())
        formatted_names = [self.myformat_colname(name, table_meta) for name in column_names]
        if not result.current_rows:
            # print header only
            self.print_formatted_result(formatted_names, None, with_header=True, tty=tty)
            return

        cql_types = []
        if result.column_types:
            ks_name = table_meta.keyspace_name if table_meta else self.current_keyspace
            ks_meta = self.conn.metadata.keyspaces.get(ks_name, None)
            cql_types = [CqlType(cql_typename(t), ks_meta) for t in result.column_types]

        formatted_values = [list(map(self.myformat_value, [row[c] for c in column_names], cql_types)) for row in result.current_rows]

        if self.expand_enabled:
            self.print_formatted_result_vertically(formatted_names, formatted_values, row_count_offset)
        else:
            self.print_formatted_result(formatted_names, formatted_values, with_header, tty)

    def print_formatted_result(self, formatted_names, formatted_values, with_header, tty):
        # determine column widths
        widths = [n.displaywidth for n in formatted_names]
        if formatted_values is not None:
            for fmtrow in formatted_values:
                for num, col in enumerate(fmtrow):
                    widths[num] = max(widths[num], col.displaywidth)

        # print header
        if with_header:
            header = ' | '.join(hdr.ljust(w, color=self.color) for (hdr, w) in zip(formatted_names, widths))
            self.writeresult(' ' + header.rstrip())
            self.writeresult('-%s-' % '-+-'.join('-' * w for w in widths))

        # stop if there are no rows
        if formatted_values is None:
            self.writeresult("")
            return

        # print row data
        for row in formatted_values:
            line = ' | '.join(col.rjust(w, color=self.color) for (col, w) in zip(row, widths))
            self.writeresult(' ' + line)

        if tty:
            self.writeresult("")

    def print_formatted_result_vertically(self, formatted_names, formatted_values, row_count_offset):
        max_col_width = max([n.displaywidth for n in formatted_names])
        max_val_width = max([n.displaywidth for row in formatted_values for n in row])

        # for each row returned, list all the column-value pairs
        for i, row in enumerate(formatted_values):
            self.writeresult("@ Row %d" % (row_count_offset + i + 1))
            self.writeresult('-%s-' % '-+-'.join(['-' * max_col_width, '-' * max_val_width]))
            for field_id, field in enumerate(row):
                column = formatted_names[field_id].ljust(max_col_width, color=self.color)
                value = field.ljust(field.displaywidth, color=self.color)
                self.writeresult(' ' + " | ".join([column, value]))
            self.writeresult('')

    def print_warnings(self, warnings):
        if warnings is None or len(warnings) == 0:
            return

        self.writeresult('')
        self.writeresult('Warnings :')
        for warning in warnings:
            self.writeresult(warning)
            self.writeresult('')

    def emptyline(self):
        pass

    def parseline(self, line):
        # this shouldn't be needed
        raise NotImplementedError

    def complete(self, text, state):
        if readline is None:
            return
        if state == 0:
            try:
                self.completion_matches = self.find_completions(text)
            except Exception:
                if debug_completion:
                    import traceback
                    traceback.print_exc()
                else:
                    raise
        try:
            return self.completion_matches[state]
        except IndexError:
            return None

    def find_completions(self, text):
        curline = readline.get_line_buffer()
        prevlines = self.statement.getvalue()
        wholestmt = prevlines + curline
        begidx = readline.get_begidx() + len(prevlines)
        stuff_to_complete = wholestmt[:begidx]
        return cqlruleset.cql_complete(stuff_to_complete, text, cassandra_conn=self,
                                       debug=debug_completion, startsymbol='cqlshCommand')

    def set_prompt(self, prompt, prepend_user=False):
        if prepend_user and self.username:
            self.prompt = "{0}@{1}".format(self.username, prompt)
            return
        self.prompt = prompt

    def cql_unprotect_name(self, namestr):
        if namestr is None:
            return
        return cqlruleset.dequote_name(namestr)

    def cql_unprotect_value(self, valstr):
        if valstr is not None:
            return cqlruleset.dequote_value(valstr)

    def _columnize_unicode(self, name_list):
        """
        Used when columnizing identifiers that may contain unicode
        """
        names = [n for n in name_list]
        cmd.Cmd.columnize(self, names)
        print('')

    def do_describe(self, parsed):

        """
        DESCRIBE [cqlsh only]

        (DESC may be used as a shorthand.)

          Outputs information about the connected Cassandra cluster, or about
          the data objects stored in the cluster. Use in one of the following ways:

        DESCRIBE KEYSPACES

          Output the names of all keyspaces.

        DESCRIBE KEYSPACE [<keyspacename>]

          Output CQL commands that could be used to recreate the given keyspace,
          and the objects in it (such as tables, types, functions, etc.).
          In some cases, as the CQL interface matures, there will be some metadata
          about a keyspace that is not representable with CQL. That metadata will not be shown.
          The '<keyspacename>' argument may be omitted, in which case the current
          keyspace will be described.

        DESCRIBE TABLES

          Output the names of all tables in the current keyspace, or in all
          keyspaces if there is no current keyspace.

        DESCRIBE TABLE [<keyspace>.]<tablename>

          Output CQL commands that could be used to recreate the given table.
          In some cases, as above, there may be table metadata which is not
          representable and which will not be shown.

        DESCRIBE INDEX <indexname>

          Output the CQL command that could be used to recreate the given index.
          In some cases, there may be index metadata which is not representable
          and which will not be shown.

        DESCRIBE MATERIALIZED VIEW <viewname>

          Output the CQL command that could be used to recreate the given materialized view.
          In some cases, there may be materialized view metadata which is not representable
          and which will not be shown.

        DESCRIBE CLUSTER

          Output information about the connected Cassandra cluster, such as the
          cluster name, and the partitioner and snitch in use. When you are
          connected to a non-system keyspace, also shows endpoint-range
          ownership information for the Cassandra ring.

        DESCRIBE [FULL] SCHEMA

          Output CQL commands that could be used to recreate the entire (non-system) schema.
          Works as though "DESCRIBE KEYSPACE k" was invoked for each non-system keyspace
          k. Use DESCRIBE FULL SCHEMA to include the system keyspaces.

        DESCRIBE TYPES

          Output the names of all user-defined-types in the current keyspace, or in all
          keyspaces if there is no current keyspace.

        DESCRIBE TYPE [<keyspace>.]<type>

          Output the CQL command that could be used to recreate the given user-defined-type.

        DESCRIBE FUNCTIONS

          Output the names of all user-defined-functions in the current keyspace, or in all
          keyspaces if there is no current keyspace.

        DESCRIBE FUNCTION [<keyspace>.]<function>

          Output the CQL command that could be used to recreate the given user-defined-function.

        DESCRIBE AGGREGATES

          Output the names of all user-defined-aggregates in the current keyspace, or in all
          keyspaces if there is no current keyspace.

        DESCRIBE AGGREGATE [<keyspace>.]<aggregate>

          Output the CQL command that could be used to recreate the given user-defined-aggregate.

        DESCRIBE <objname>

          Output CQL commands that could be used to recreate the entire object schema,
          where object can be either a keyspace or a table or an index or a materialized
          view (in this order).
        """
        stmt = SimpleStatement(parsed.extract_orig(), consistency_level=cassandra.ConsistencyLevel.LOCAL_ONE, fetch_size=self.page_size if self.use_paging else None)
        future = self.session.execute_async(stmt)

        if self.connection_versions['build'][0] < '4':
            print('\nWARN: DESCRIBE|DESC was moved to server side in Cassandra 4.0. As a consequence DESRIBE|DESC '
                  'will not work in cqlsh %r connected to Cassandra %r, the version that you are connected to. '
                  'DESCRIBE does not exist server side prior Cassandra 4.0.'
                  % (version, self.connection_versions['build']))
        else:
            try:
                result = future.result()

                what = parsed.matched[1][1].lower()

                if what in ('columnfamilies', 'tables', 'types', 'functions', 'aggregates'):
                    self.describe_list(result)
                elif what == 'keyspaces':
                    self.describe_keyspaces(result)
                elif what == 'cluster':
                    self.describe_cluster(result)
                elif what:
                    self.describe_element(result)

            except CQL_ERRORS as err:
                err_msg = err.message if hasattr(err, 'message') else str(err)
                self.printerr(err_msg.partition("message=")[2].strip('"'))
            except Exception:
                import traceback
                self.printerr(traceback.format_exc())

            if future:
                if future.warnings:
                    self.print_warnings(future.warnings)

    do_desc = do_describe

    def describe_keyspaces(self, rows):
        """
        Print the output for a DESCRIBE KEYSPACES query
        """
        names = [r['name'] for r in rows]

        print('')
        cmd.Cmd.columnize(self, names)
        print('')

    def describe_list(self, rows):
        """
        Print the output for all the DESCRIBE queries for element names (e.g DESCRIBE TABLES, DESCRIBE FUNCTIONS ...)
        """
        keyspace = None
        names = list()
        for row in rows:
            if row['keyspace_name'] != keyspace:
                if keyspace is not None:
                    self.print_keyspace_element_names(keyspace, names)

                keyspace = row['keyspace_name']
                names = list()

            names.append(str(row['name']))

        if keyspace is not None:
            self.print_keyspace_element_names(keyspace, names)
            print('')

    def print_keyspace_element_names(self, keyspace, names):
        print('')
        if self.current_keyspace is None:
            print('Keyspace %s' % (keyspace))
            print('---------%s' % ('-' * len(keyspace)))
        cmd.Cmd.columnize(self, names)

    def describe_element(self, rows):
        """
        Print the output for all the DESCRIBE queries where an element name as been specified (e.g DESCRIBE TABLE, DESCRIBE INDEX ...)
        """
        for row in rows:
            print('')
            self.query_out.write(row['create_statement'])
            print('')

    def describe_cluster(self, rows):
        """
        Print the output for a DESCRIBE CLUSTER query.

        If a specified keyspace was in use the returned ResultSet will contains a 'range_ownership' column,
        otherwise not.
        """
        for row in rows:
            print('\nCluster: %s' % row['cluster'])
            print('Partitioner: %s' % row['partitioner'])
            print('Snitch: %s\n' % row['snitch'])
            if 'range_ownership' in row:
                print("Range ownership:")
                for entry in list(row['range_ownership'].items()):
                    print(' %39s  [%s]' % (entry[0], ', '.join([host for host in entry[1]])))
                print('')

    def do_copy(self, parsed):
        r"""
        COPY [cqlsh only]

          COPY x FROM: Imports CSV data into a Cassandra table
          COPY x TO: Exports data from a Cassandra table in CSV format.

        COPY <table_name> [ ( column [, ...] ) ]
             FROM ( '<file_pattern_1, file_pattern_2, ... file_pattern_n>' | STDIN )
             [ WITH <option>='value' [AND ...] ];

        File patterns are either file names or valid python glob expressions, e.g. *.csv or folder/*.csv.

        COPY <table_name> [ ( column [, ...] ) ]
             TO ( '<filename>' | STDOUT )
             [ WITH <option>='value' [AND ...] ];

        Available common COPY options and defaults:

          DELIMITER=','           - character that appears between records
          QUOTE='"'               - quoting character to be used to quote fields
          ESCAPE='\'              - character to appear before the QUOTE char when quoted
          HEADER=false            - whether to ignore the first line
          NULL=''                 - string that represents a null value
          DATETIMEFORMAT=         - timestamp strftime format
            '%Y-%m-%d %H:%M:%S%z'   defaults to time_format value in cqlshrc
          MAXATTEMPTS=5           - the maximum number of attempts per batch or range
          REPORTFREQUENCY=0.25    - the frequency with which we display status updates in seconds
          DECIMALSEP='.'          - the separator for decimal values
          THOUSANDSSEP=''         - the separator for thousands digit groups
          BOOLSTYLE='True,False'  - the representation for booleans, case insensitive, specify true followed by false,
                                    for example yes,no or 1,0
          NUMPROCESSES=n          - the number of worker processes, by default the number of cores minus one
                                    capped at 16
          CONFIGFILE=''           - a configuration file with the same format as .cqlshrc (see the Python ConfigParser
                                    documentation) where you can specify WITH options under the following optional
                                    sections: [copy], [copy-to], [copy-from], [copy:ks.table], [copy-to:ks.table],
                                    [copy-from:ks.table], where <ks> is your keyspace name and <table> is your table
                                    name. Options are read from these sections, in the order specified
                                    above, and command line options always override options in configuration files.
                                    Depending on the COPY direction, only the relevant copy-from or copy-to sections
                                    are used. If no configfile is specified then .cqlshrc is searched instead.
          RATEFILE=''             - an optional file where to print the output statistics

        Available COPY FROM options and defaults:

          CHUNKSIZE=5000          - the size of chunks passed to worker processes
          INGESTRATE=100000       - an approximate ingest rate in rows per second
          MINBATCHSIZE=10         - the minimum size of an import batch
          MAXBATCHSIZE=20         - the maximum size of an import batch
          MAXROWS=-1              - the maximum number of rows, -1 means no maximum
          SKIPROWS=0              - the number of rows to skip
          SKIPCOLS=''             - a comma separated list of column names to skip
          MAXPARSEERRORS=-1       - the maximum global number of parsing errors, -1 means no maximum
          MAXINSERTERRORS=1000    - the maximum global number of insert errors, -1 means no maximum
          ERRFILE=''              - a file where to store all rows that could not be imported, by default this is
                                    import_ks_table.err where <ks> is your keyspace and <table> is your table name.
          PREPAREDSTATEMENTS=True - whether to use prepared statements when importing, by default True. Set this to
                                    False if you don't mind shifting data parsing to the cluster. The cluster will also
                                    have to compile every batch statement. For large and oversized clusters
                                    this will result in a faster import but for smaller clusters it may generate
                                    timeouts.
          TTL=3600                - the time to live in seconds, by default data will not expire

        Available COPY TO options and defaults:

          ENCODING='utf8'          - encoding for CSV output
          PAGESIZE='1000'          - the page size for fetching results
          PAGETIMEOUT=10           - the page timeout in seconds for fetching results
          BEGINTOKEN=''            - the minimum token string to consider when exporting data
          ENDTOKEN=''              - the maximum token string to consider when exporting data
          MAXREQUESTS=6            - the maximum number of requests each worker process can work on in parallel
          MAXOUTPUTSIZE='-1'       - the maximum size of the output file measured in number of lines,
                                     beyond this maximum the output file will be split into segments,
                                     -1 means unlimited.
          FLOATPRECISION=5         - the number of digits displayed after the decimal point for cql float values
          DOUBLEPRECISION=12       - the number of digits displayed after the decimal point for cql double values

        When entering CSV data on STDIN, you can use the sequence "\."
        on a line by itself to end the data input.
        """

        ks = self.cql_unprotect_name(parsed.get_binding('ksname', None))
        if ks is None:
            ks = self.current_keyspace
            if ks is None:
                raise NoKeyspaceError("Not in any keyspace.")
        table = self.cql_unprotect_name(parsed.get_binding('cfname'))
        columns = parsed.get_binding('colnames', None)
        if columns is not None:
            columns = list(map(self.cql_unprotect_name, columns))
        else:
            # default to all known columns
            columns = self.get_column_names(ks, table)

        fname = parsed.get_binding('fname', None)
        if fname is not None:
            fname = self.cql_unprotect_value(fname)

        copyoptnames = list(map(str.lower, parsed.get_binding('optnames', ())))
        copyoptvals = list(map(self.cql_unprotect_value, parsed.get_binding('optvals', ())))
        opts = dict(list(zip(copyoptnames, copyoptvals)))

        direction = parsed.get_binding('dir').upper()
        if direction == 'FROM':
            task = ImportTask(self, ks, table, columns, fname, opts, self.conn.protocol_version, CONFIG_FILE)
        elif direction == 'TO':
            task = ExportTask(self, ks, table, columns, fname, opts, self.conn.protocol_version, CONFIG_FILE)
        else:
            raise SyntaxError("Unknown direction %s" % direction)

        task.run()

    def do_show(self, parsed):
        """
        SHOW [cqlsh only]

          Displays information about the current cqlsh session. Can be called in
          the following ways:

        SHOW VERSION

          Shows the version and build of the connected Cassandra instance, as
          well as the version of the CQL spec that the connected Cassandra
          instance understands.

        SHOW HOST

          Shows where cqlsh is currently connected.

        SHOW SESSION <sessionid>

          Pretty-prints the requested tracing session.

        SHOW REPLICAS <token> (<keyspace>)

          Lists the replica nodes by IP address for the given token. The current
          keyspace is used if one is not specified.
        """
        showwhat = parsed.get_binding('what').lower()
        if showwhat == 'version':
            self.get_connection_versions()
            self.show_version()
        elif showwhat == 'host':
            self.show_host()
        elif showwhat.startswith('session'):
            session_id = parsed.get_binding('sessionid').lower()
            self.show_session(UUID(session_id))
        elif showwhat.startswith('replicas'):
            token_id = parsed.get_binding('token')
            keyspace = parsed.get_binding('keyspace')
            self.show_replicas(token_id, keyspace)
        else:
            self.printerr('Wait, how do I show %r?' % (showwhat,))

    def do_source(self, parsed):
        """
        SOURCE [cqlsh only]

        Executes a file containing CQL statements. Gives the output for each
        statement in turn, if any, or any errors that occur along the way.

        Errors do NOT abort execution of the CQL source file.

        Usage:

          SOURCE '<file>';

        That is, the path to the file to be executed must be given inside a
        string literal. The path is interpreted relative to the current working
        directory. The tilde shorthand notation ('~/mydir') is supported for
        referring to $HOME.

        See also the --file option to cqlsh.
        """
        fname = parsed.get_binding('fname')
        fname = os.path.expanduser(self.cql_unprotect_value(fname))
        try:
            encoding, bom_size = get_file_encoding_bomsize(fname)
            f = codecs.open(fname, 'r', encoding)
            f.seek(bom_size)
        except IOError as e:
            self.printerr('Could not open %r: %s' % (fname, e))
            return
        subshell = Shell(self.hostname, self.port, color=self.color,
                         username=self.username,
                         encoding=self.encoding, stdin=f, tty=False, use_conn=self.conn,
                         cqlver=self.cql_version, keyspace=self.current_keyspace,
                         tracing_enabled=self.tracing_enabled,
                         display_nanotime_format=self.display_nanotime_format,
                         display_timestamp_format=self.display_timestamp_format,
                         display_date_format=self.display_date_format,
                         display_float_precision=self.display_float_precision,
                         display_double_precision=self.display_double_precision,
                         display_timezone=self.display_timezone,
                         max_trace_wait=self.max_trace_wait, ssl=self.ssl,
                         request_timeout=self.session.default_timeout,
                         connect_timeout=self.conn.connect_timeout,
                         is_subshell=True,
                         auth_provider=self.auth_provider)
        # duplicate coverage related settings in subshell
        if self.coverage:
            subshell.coverage = True
            subshell.coveragerc_path = self.coveragerc_path
        subshell.cmdloop()
        f.close()

    def do_capture(self, parsed):
        """
        CAPTURE [cqlsh only]

        Begins capturing command output and appending it to a specified file.
        Output will not be shown at the console while it is captured.

        Usage:

          CAPTURE '<file>';
          CAPTURE OFF;
          CAPTURE;

        That is, the path to the file to be appended to must be given inside a
        string literal. The path is interpreted relative to the current working
        directory. The tilde shorthand notation ('~/mydir') is supported for
        referring to $HOME.

        Only query result output is captured. Errors and output from cqlsh-only
        commands will still be shown in the cqlsh session.

        To stop capturing output and show it in the cqlsh session again, use
        CAPTURE OFF.

        To inspect the current capture configuration, use CAPTURE with no
        arguments.
        """
        fname = parsed.get_binding('fname')
        if fname is None:
            if self.shunted_query_out is not None:
                print("Currently capturing query output to %r." % (self.query_out.name,))
            else:
                print("Currently not capturing query output.")
            return

        if fname.upper() == 'OFF':
            if self.shunted_query_out is None:
                self.printerr('Not currently capturing output.')
                return
            self.query_out.close()
            self.query_out = self.shunted_query_out
            self.color = self.shunted_color
            self.shunted_query_out = None
            del self.shunted_color
            return

        if self.shunted_query_out is not None:
            self.printerr('Already capturing output to %s. Use CAPTURE OFF'
                          ' to disable.' % (self.query_out.name,))
            return

        fname = os.path.expanduser(self.cql_unprotect_value(fname))
        try:
            f = open(fname, 'a')
        except IOError as e:
            self.printerr('Could not open %r for append: %s' % (fname, e))
            return
        self.shunted_query_out = self.query_out
        self.shunted_color = self.color
        self.query_out = f
        self.color = False
        print('Now capturing query output to %r.' % (fname,))

    def do_tracing(self, parsed):
        """
        TRACING [cqlsh]

          Enables or disables request tracing.

        TRACING ON

          Enables tracing for all further requests.

        TRACING OFF

          Disables tracing.

        TRACING

          TRACING with no arguments shows the current tracing status.
        """
        self.tracing_enabled \
            = self.on_off_switch("TRACING", self.tracing_enabled, parsed.get_binding('switch'))

    def do_expand(self, parsed):
        """
        EXPAND [cqlsh]

          Enables or disables expanded (vertical) output.

        EXPAND ON

          Enables expanded (vertical) output.

        EXPAND OFF

          Disables expanded (vertical) output.

        EXPAND

          EXPAND with no arguments shows the current value of expand setting.
        """
        self.expand_enabled = self.on_off_switch("EXPAND", self.expand_enabled, parsed.get_binding('switch'))

    def do_consistency(self, parsed):
        """
        CONSISTENCY [cqlsh only]

           Overrides default consistency level (default level is ONE).

        CONSISTENCY <level>

           Sets consistency level for future requests.

           Valid consistency levels:

           ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_ONE, LOCAL_QUORUM, EACH_QUORUM, SERIAL and LOCAL_SERIAL.

           SERIAL and LOCAL_SERIAL may be used only for SELECTs; will be rejected with updates.

        CONSISTENCY

           CONSISTENCY with no arguments shows the current consistency level.
        """
        level = parsed.get_binding('level')
        if level is None:
            print('Current consistency level is %s.' % (cassandra.ConsistencyLevel.value_to_name[self.consistency_level]))
            return

        self.consistency_level = cassandra.ConsistencyLevel.name_to_value[level.upper()]
        print('Consistency level set to %s.' % (level.upper(),))

    def do_serial(self, parsed):
        """
        SERIAL CONSISTENCY [cqlsh only]

           Overrides serial consistency level (default level is SERIAL).

        SERIAL CONSISTENCY <level>

           Sets consistency level for future conditional updates.

           Valid consistency levels:

           SERIAL, LOCAL_SERIAL.

        SERIAL CONSISTENCY

           SERIAL CONSISTENCY with no arguments shows the current consistency level.
        """
        level = parsed.get_binding('level')
        if level is None:
            print('Current serial consistency level is %s.' % (cassandra.ConsistencyLevel.value_to_name[self.serial_consistency_level]))
            return

        self.serial_consistency_level = cassandra.ConsistencyLevel.name_to_value[level.upper()]
        print('Serial consistency level set to %s.' % (level.upper(),))

    def do_login(self, parsed):
        """
        LOGIN [cqlsh only]

           Changes login information without requiring restart.

        LOGIN <username> (<password>)

           Login using the specified username. If password is specified, it will be used
           otherwise, you will be prompted to enter.
        """
        username = parsed.get_binding('username')
        password = parsed.get_binding('password')
        if password is None:
            password = getpass.getpass()
        else:
            password = password[1:-1]

        auth_provider = PlainTextAuthProvider(username=username, password=password)

        conn = Cluster(contact_points=(self.hostname,), port=self.port, cql_version=self.conn.cql_version,
                       protocol_version=self.conn.protocol_version,
                       auth_provider=auth_provider,
                       ssl_options=self.conn.ssl_options,
                       load_balancing_policy=WhiteListRoundRobinPolicy([self.hostname]),
                       control_connection_timeout=self.conn.connect_timeout,
                       connect_timeout=self.conn.connect_timeout)

        if self.current_keyspace:
            session = conn.connect(self.current_keyspace)
        else:
            session = conn.connect()

        # Copy session properties
        session.default_timeout = self.session.default_timeout
        session.row_factory = self.session.row_factory
        session.default_consistency_level = self.session.default_consistency_level
        session.max_trace_wait = self.session.max_trace_wait

        # Update after we've connected in case we fail to authenticate
        self.conn = conn
        self.auth_provider = auth_provider
        self.username = username
        self.session = session

    def do_exit(self, parsed=None):
        """
        EXIT/QUIT [cqlsh only]

        Exits cqlsh.
        """
        self.stop = True
        if self.owns_connection:
            self.conn.shutdown()
    do_quit = do_exit

    def do_clear(self, parsed):
        """
        CLEAR/CLS [cqlsh only]

        Clears the console.
        """
        subprocess.call('clear', shell=True)
    do_cls = do_clear

    def do_debug(self, parsed):
        import pdb
        pdb.set_trace()

    def get_help_topics(self):
        topics = [t[3:] for t in dir(self) if t.startswith('do_') and getattr(self, t, None).__doc__]
        for hide_from_help in ('quit',):
            topics.remove(hide_from_help)
        return topics

    def columnize(self, slist, *a, **kw):
        return cmd.Cmd.columnize(self, sorted([u.upper() for u in slist]), *a, **kw)

    def do_help(self, parsed):
        """
        HELP [cqlsh only]

        Gives information about cqlsh commands. To see available topics,
        enter "HELP" without any arguments. To see help on a topic,
        use "HELP <topic>".
        """
        topics = parsed.get_binding('topic', ())
        if not topics:
            shell_topics = [t.upper() for t in self.get_help_topics()]
            self.print_topics("\nDocumented shell commands:", shell_topics, 15, 80)
            cql_topics = [t.upper() for t in cqldocs.get_help_topics()]
            self.print_topics("CQL help topics:", cql_topics, 15, 80)
            return
        for t in topics:
            if t.lower() in self.get_help_topics():
                doc = getattr(self, 'do_' + t.lower()).__doc__
                self.stdout.write(doc + "\n")
            elif t.lower() in cqldocs.get_help_topics():
                urlpart = cqldocs.get_help_topic(t)
                if urlpart is not None:
                    url = "%s#%s" % (CASSANDRA_CQL_HTML, urlpart)
                    if self.browser is not None:
                        opened = webbrowser.get(self.browser).open_new_tab(url)
                    else:
                        opened = webbrowser.open_new_tab(url)
                    if not opened:
                        self.printerr("*** No browser to display CQL help. URL for help topic %s : %s" % (t, url))
            else:
                self.printerr("*** No help on %s" % (t,))

    def do_history(self, parsed):
        """
        HISTORY [cqlsh only]

           Displays the most recent commands executed in cqlsh

        HISTORY (<n>)

           If n is specified, the history display length is set to n for this session
        """

        history_length = readline.get_current_history_length()

        n = parsed.get_binding('n')
        if (n is not None):
            self.max_history_length_shown = int(n)

        for index in range(max(1, history_length - self.max_history_length_shown), history_length):
            print(readline.get_history_item(index))

    def do_unicode(self, parsed):
        """
        Textual input/output

        When control characters, or other characters which can't be encoded
        in your current locale, are found in values of 'text' or 'ascii'
        types, it will be shown as a backslash escape. If color is enabled,
        any such backslash escapes will be shown in a different color from
        the surrounding text.

        Unicode code points in your data will be output intact, if the
        encoding for your locale is capable of decoding them. If you prefer
        that non-ascii characters be shown with Python-style "\\uABCD"
        escape sequences, invoke cqlsh with an ASCII locale (for example,
        by setting the $LANG environment variable to "C").
        """

    def do_paging(self, parsed):
        """
        PAGING [cqlsh]

          Enables or disables query paging.

        PAGING ON

          Enables query paging for all further queries.

        PAGING OFF

          Disables paging.

        PAGING

          PAGING with no arguments shows the current query paging status.
        """
        (self.use_paging, requested_page_size) = \
            self.on_off_switch_with_value("PAGING", self.use_paging, parsed.get_binding('switch'))
        if self.use_paging and requested_page_size is not None:
            self.page_size = requested_page_size
        if self.use_paging:
            print(("Page size: {}".format(self.page_size)))
        else:
            self.page_size = self.default_page_size

    def applycolor(self, text, color=None):
        if not color or not self.color:
            return text
        return color + text + ANSI_RESET

    def writeresult(self, text, color=None, newline=True, out=None):
        if out is None:
            out = self.query_out

        # convert Exceptions, etc to text
        if not isinstance(text, str):
            text = str(text)

        to_write = self.applycolor(text, color) + ('\n' if newline else '')
        out.write(to_write)

    def flush_output(self):
        self.query_out.flush()

    def printerr(self, text, color=RED, newline=True, shownum=None):
        self.statement_error = True
        if shownum is None:
            shownum = self.show_line_nums
        if shownum:
            text = '%s:%d:%s' % (self.stdin.name, self.lineno, text)
        self.writeresult(text, color, newline=newline, out=sys.stderr)

    def stop_coverage(self):
        if self.coverage and self.cov is not None:
            self.cov.stop()
            self.cov.save()
            self.cov = None

    def init_history(self):
        if readline is not None:
            try:
                readline.read_history_file(HISTORY)
            except IOError:
                pass
            delims = readline.get_completer_delims()
            delims.replace("'", "")
            delims += '.'
            readline.set_completer_delims(delims)

            # configure length of history shown
            self.max_history_length_shown = 50

    def save_history(self):
        if readline is not None:
            try:
                readline.write_history_file(HISTORY)
            except IOError:
                pass

    @staticmethod
    def on_off_switch(name, current, state_name=None):
        """
        switches between ON and OFF values

        :param name: a command name
        :param current: a boolean value
        :param state_name: "ON", "OFF" or None
        :return: a boolean value
        """

        if state_name is None:
            print(f"{name} is {SwitchState(current).name}")
            return current

        new_state = SwitchState[state_name.upper()]
        if current == new_state.value:
            print(f"{name} is already {SwitchState(current).name}")
            return current
        else:
            print(f"{name} set to {new_state.name}")
            return new_state.value

    @staticmethod
    def on_off_switch_with_value(name, current, value=None):
        """switches between ON and OFF values, and accepts an integer value in place of ON.

        This returns a tuple of the form: (SWITCH_VALUE, VALUE)
        eg: PAGING 50 returns (True, 50)
            PAGING OFF returns (False, None)
            PAGING ON returns (True, None)

        VALUE must be an Integer or None.
        """
        if value is None:
            print(f"{name} is {SwitchState(current).name}")
            return current, None
        if value.isdigit():
            return True, int(value)
        return Shell.on_off_switch(name, current, value), None


def option_with_default(cparser_getter, section, option, default=None):
    try:
        return cparser_getter(section, option)
    except configparser.Error:
        return default


def raw_option_with_default(configs, section, option, default=None):
    """
    Same (almost) as option_with_default() but won't do any string interpolation.
    Useful for config values that include '%' symbol, e.g. time format string.
    """
    try:
        return configs.get(section, option, raw=True)
    except configparser.Error:
        return default


def should_use_color():
    if not sys.stdout.isatty():
        return False
    if os.environ.get('TERM', '') in ('dumb', ''):
        return False
    try:
        p = subprocess.Popen(['tput', 'colors'], stdout=subprocess.PIPE)
        stdout, _ = p.communicate()
        if int(stdout.strip()) < 8:
            return False
    except (OSError, ImportError, ValueError):
        # oh well, we tried. at least we know there's a $TERM and it's
        # not "dumb".
        pass
    return True


def read_options(cmdlineargs, environment=os.environ):
    configs = configparser.ConfigParser()
    configs.read(CONFIG_FILE)

    rawconfigs = configparser.RawConfigParser()
    rawconfigs.read(CONFIG_FILE)

    username_from_cqlshrc = option_with_default(configs.get, 'authentication', 'username')
    password_from_cqlshrc = option_with_default(rawconfigs.get, 'authentication', 'password')
    if username_from_cqlshrc or password_from_cqlshrc:
        if password_from_cqlshrc and not is_file_secure(os.path.expanduser(CONFIG_FILE)):
            print("\nWarning: Password is found in an insecure cqlshrc file. The file is owned or readable by other users on the system.",
                  end='', file=sys.stderr)
        print("\nNotice: Credentials in the cqlshrc file is deprecated and will be ignored in the future."
              "\nPlease use a credentials file to specify the username and password.\n", file=sys.stderr)

    argvalues = argparse.Namespace()

    argvalues.username = None
    argvalues.password = None
    argvalues.credentials = os.path.expanduser(option_with_default(configs.get, 'authentication', 'credentials',
                                                                   os.path.join(CQL_DIR, 'credentials')))
    argvalues.keyspace = option_with_default(configs.get, 'authentication', 'keyspace')
    argvalues.browser = option_with_default(configs.get, 'ui', 'browser', None)
    argvalues.completekey = option_with_default(configs.get, 'ui', 'completekey',
                                                DEFAULT_COMPLETEKEY)
    argvalues.color = option_with_default(configs.getboolean, 'ui', 'color')
    argvalues.time_format = raw_option_with_default(configs, 'ui', 'time_format',
                                                    DEFAULT_TIMESTAMP_FORMAT)
    argvalues.nanotime_format = raw_option_with_default(configs, 'ui', 'nanotime_format',
                                                        DEFAULT_NANOTIME_FORMAT)
    argvalues.date_format = raw_option_with_default(configs, 'ui', 'date_format',
                                                    DEFAULT_DATE_FORMAT)
    argvalues.float_precision = option_with_default(configs.getint, 'ui', 'float_precision',
                                                    DEFAULT_FLOAT_PRECISION)
    argvalues.double_precision = option_with_default(configs.getint, 'ui', 'double_precision',
                                                     DEFAULT_DOUBLE_PRECISION)
    argvalues.field_size_limit = option_with_default(configs.getint, 'csv', 'field_size_limit', csv.field_size_limit())
    argvalues.max_trace_wait = option_with_default(configs.getfloat, 'tracing', 'max_trace_wait',
                                                   DEFAULT_MAX_TRACE_WAIT)
    argvalues.timezone = option_with_default(configs.get, 'ui', 'timezone', None)

    argvalues.debug = False

    argvalues.coverage = False
    if 'CQLSH_COVERAGE' in environment.keys():
        argvalues.coverage = True

    argvalues.file = None
    argvalues.ssl = option_with_default(configs.getboolean, 'connection', 'ssl', DEFAULT_SSL)
    argvalues.encoding = option_with_default(configs.get, 'ui', 'encoding', UTF8)

    argvalues.tty = option_with_default(configs.getboolean, 'ui', 'tty', sys.stdin.isatty())
    argvalues.protocol_version = option_with_default(configs.getint, 'protocol', 'version', None)
    argvalues.cqlversion = option_with_default(configs.get, 'cql', 'version', None)
    argvalues.connect_timeout = option_with_default(configs.getint, 'connection', 'timeout', DEFAULT_CONNECT_TIMEOUT_SECONDS)
    argvalues.request_timeout = option_with_default(configs.getint, 'connection', 'request_timeout', DEFAULT_REQUEST_TIMEOUT_SECONDS)
    argvalues.execute = None
    argvalues.insecure_password_without_warning = False

    options, arguments = parser.parse_known_args(cmdlineargs, argvalues)

    # Credentials from cqlshrc will be expanded,
    # credentials from the command line are also expanded if there is a space...
    # we need the following so that these two scenarios will work
    #   cqlsh --credentials=~/.cassandra/creds
    #   cqlsh --credentials ~/.cassandra/creds

    if options.credentials is not None:
        options.credentials = os.path.expanduser(options.credentials)

    if options.credentials is not None:
        if not is_file_secure(options.credentials):
            print("\nWarning: Credentials file '{0}' exists but is not used, because:"
                  "\n  a. the file owner is not the current user; or"
                  "\n  b. the file is readable by group or other."
                  "\nPlease ensure the file is owned by the current user and is not readable by group or other."
                  "\nOn a Linux or UNIX-like system, you often can do this by using the `chown` and `chmod` commands:"
                  "\n  chown YOUR_USERNAME credentials"
                  "\n  chmod 600 credentials\n".format(options.credentials),
                  file=sys.stderr)
            options.credentials = ''  # ConfigParser.read() will ignore unreadable files

    if not options.username:
        credentials = configparser.ConfigParser()
        if options.credentials is not None:
            credentials.read(options.credentials)

        # use the username from credentials file but fallback to cqlshrc if username is absent from the command line parameters
        options.username = username_from_cqlshrc

    if not options.password:
        rawcredentials = configparser.RawConfigParser()
        if options.credentials is not None:
            rawcredentials.read(options.credentials)

        # handling password in the same way as username, priority cli > credentials > cqlshrc
        options.password = option_with_default(rawcredentials.get, 'plain_text_auth', 'password', password_from_cqlshrc)
        options.password = password_from_cqlshrc
    elif not options.insecure_password_without_warning:
        print("\nWarning: Using a password on the command line interface can be insecure."
              "\nRecommendation: use the credentials file to securely provide the password.\n", file=sys.stderr)

    # Make sure some user values read from the command line are in unicode
    options.execute = maybe_ensure_text(options.execute)
    options.username = maybe_ensure_text(options.username)
    options.password = maybe_ensure_text(options.password)
    options.keyspace = maybe_ensure_text(options.keyspace)

    hostname = option_with_default(configs.get, 'connection', 'hostname', DEFAULT_HOST)
    port = option_with_default(configs.get, 'connection', 'port', DEFAULT_PORT)

    try:
        options.connect_timeout = int(options.connect_timeout)
    except ValueError:
        parser.error('"%s" is not a valid connect timeout.' % (options.connect_timeout,))
        options.connect_timeout = DEFAULT_CONNECT_TIMEOUT_SECONDS

    try:
        options.request_timeout = int(options.request_timeout)
    except ValueError:
        parser.error('"%s" is not a valid request timeout.' % (options.request_timeout,))
        options.request_timeout = DEFAULT_REQUEST_TIMEOUT_SECONDS

    hostname = environment.get('CQLSH_HOST', hostname)
    port = environment.get('CQLSH_PORT', port)

    if len(arguments) > 0:
        hostname = arguments[0]
    if len(arguments) > 1:
        port = arguments[1]

    if options.file or options.execute:
        options.tty = False

    if options.execute and not options.execute.endswith(';'):
        options.execute += ';'

    if argvalues.color in (True, False):
        options.color = argvalues.color
    else:
        if options.file is not None:
            options.color = False
        else:
            options.color = should_use_color()

    if options.cqlversion is not None:
        options.cqlversion, cqlvertup = full_cql_version(options.cqlversion)
        if cqlvertup[0] < 3:
            parser.error('%r is not a supported CQL version.' % options.cqlversion)
    options.cqlmodule = cql3handling

    try:
        port = int(port)
    except ValueError:
        parser.error('%r is not a valid port number.' % port)
    return options, hostname, port


def setup_cqlruleset(cqlmodule):
    global cqlruleset
    cqlruleset = cqlmodule.CqlRuleSet
    cqlruleset.append_rules(cqlshhandling.cqlsh_extra_syntax_rules)
    for rulename, termname, func in cqlshhandling.cqlsh_syntax_completers:
        cqlruleset.completer_for(rulename, termname)(func)
    cqlruleset.commands_end_with_newline.update(cqlshhandling.my_commands_ending_with_newline)


def setup_cqldocs(cqlmodule):
    global cqldocs
    cqldocs = cqlmodule.cqldocs


def setup_docspath(path):
    global CASSANDRA_CQL_HTML
    CASSANDRA_CQL_HTML_FALLBACK = 'https://cassandra.apache.org/doc/latest/cassandra/cql/cql_singlefile.html'
    #
    # default location of local CQL.html
    if os.path.exists(path + '/doc/cql3/CQL.html'):
        # default location of local CQL.html
        CASSANDRA_CQL_HTML = 'file://' + path + '/doc/cql3/CQL.html'
    elif os.path.exists('/usr/share/doc/cassandra/CQL.html'):
        # fallback to package file
        CASSANDRA_CQL_HTML = 'file:///usr/share/doc/cassandra/CQL.html'
    else:
        # fallback to online version
        CASSANDRA_CQL_HTML = CASSANDRA_CQL_HTML_FALLBACK


def insert_driver_hooks():

    class DateOverFlowWarning(RuntimeWarning):
        pass

    # Display milliseconds when datetime overflows (CASSANDRA-10625), E.g., the year 10000.
    # Native datetime types blow up outside of datetime.[MIN|MAX]_YEAR. We will fall back to an int timestamp
    def deserialize_date_fallback_int(byts, protocol_version):
        timestamp_ms = int64_unpack(byts)
        try:
            return datetime_from_timestamp(timestamp_ms / 1000.0)
        except OverflowError:
            warnings.warn(DateOverFlowWarning("Some timestamps are larger than Python datetime can represent. "
                                              "Timestamps are displayed in milliseconds from epoch."))
            return timestamp_ms

    cassandra.cqltypes.DateType.deserialize = staticmethod(deserialize_date_fallback_int)

    if hasattr(cassandra, 'deserializers'):
        del cassandra.deserializers.DesDateType

    # Return cassandra.cqltypes.EMPTY instead of None for empty values
    cassandra.cqltypes.CassandraType.support_empty_values = True


def main(cmdline, pkgpath):
    insert_driver_hooks()
    (options, hostname, port) = read_options(cmdline)

    setup_docspath(pkgpath)
    setup_cqlruleset(options.cqlmodule)
    setup_cqldocs(options.cqlmodule)
    csv.field_size_limit(options.field_size_limit)

    if options.file is None:
        stdin = None
    else:
        try:
            encoding, bom_size = get_file_encoding_bomsize(options.file)
            stdin = codecs.open(options.file, 'r', encoding)
            stdin.seek(bom_size)
        except IOError as e:
            sys.exit("Can't open %r: %s" % (options.file, e))

    if options.debug:
        sys.stderr.write("Using CQL driver: %s\n" % (cassandra,))
        sys.stderr.write("Using connect timeout: %s seconds\n" % (options.connect_timeout,))
        sys.stderr.write("Using '%s' encoding\n" % (options.encoding,))
        sys.stderr.write("Using ssl: %s\n" % (options.ssl,))

    # create timezone based on settings, environment or auto-detection
    timezone = None
    if options.timezone or 'TZ' in os.environ:
        if sys.version_info >= (3, 9):
            try:
                import zoneinfo
                if options.timezone:
                    try:
                        timezone = zoneinfo.ZoneInfo(options.timezone)
                    except Exception:
                        sys.stderr.write("Warning: could not recognize timezone '%s' specified in cqlshrc\n\n" % (options.timezone))
                if 'TZ' in os.environ:
                    try:
                        timezone = zoneinfo.ZoneInfo(os.environ['TZ'])
                    except Exception:
                        sys.stderr.write("Warning: could not recognize timezone '%s' from environment value TZ\n\n" % (os.environ['TZ']))
            except ImportError:
                sys.stderr.write("Warning: Timezone defined but unable to perform timezone conversion using 'zoneinfo' "
                                 "module. Timestamps will be displayed in UTC timezone.\n\n")
        else:
            try:
                import pytz
                if options.timezone:
                    try:
                        timezone = pytz.timezone(options.timezone)
                    except Exception:
                        sys.stderr.write("Warning: could not recognize timezone '%s' specified in cqlshrc\n\n" % (options.timezone))
                if 'TZ' in os.environ:
                    try:
                        timezone = pytz.timezone(os.environ['TZ'])
                    except Exception:
                        sys.stderr.write("Warning: could not recognize timezone '%s' from environment value TZ\n\n" % (os.environ['TZ']))
            except ImportError:
                sys.stderr.write("Warning: Timezone defined and 'pytz' module for timezone conversion not installed. Timestamps will be displayed in UTC timezone.\n\n")

    # try auto-detect timezone if tzlocal is installed
    if not timezone:
        try:
            from tzlocal import get_localzone
            timezone = get_localzone()
        except ImportError:
            # we silently ignore and fallback to UTC unless a custom timestamp format (which likely
            # does contain a TZ part) was specified
            if options.time_format != DEFAULT_TIMESTAMP_FORMAT:
                sys.stderr.write("Warning: custom timestamp format specified in cqlshrc, "
                                 + "but local timezone could not be detected.\n"
                                 + "Either install Python 'tzlocal' module for auto-detection "
                                 + "or specify client timezone in your cqlshrc.\n\n")

    try:
        shell = Shell(hostname,
                      port,
                      color=options.color,
                      username=options.username,
                      stdin=stdin,
                      tty=options.tty,
                      completekey=options.completekey,
                      browser=options.browser,
                      protocol_version=options.protocol_version,
                      cqlver=options.cqlversion,
                      keyspace=options.keyspace,
                      display_timestamp_format=options.time_format,
                      display_nanotime_format=options.nanotime_format,
                      display_date_format=options.date_format,
                      display_float_precision=options.float_precision,
                      display_double_precision=options.double_precision,
                      display_timezone=timezone,
                      max_trace_wait=options.max_trace_wait,
                      ssl=options.ssl,
                      single_statement=options.execute,
                      request_timeout=options.request_timeout,
                      connect_timeout=options.connect_timeout,
                      encoding=options.encoding,
                      auth_provider=authproviderhandling.load_auth_provider(
                          config_file=CONFIG_FILE,
                          cred_file=options.credentials,
                          username=options.username,
                          password=options.password))
    except KeyboardInterrupt:
        sys.exit('Connection aborted.')
    except CQL_ERRORS as e:
        sys.exit('Connection error: %s' % (e,))
    except VersionNotSupported as e:
        sys.exit('Unsupported CQL version: %s' % (e,))
    if options.debug:
        shell.debug = True
    if options.coverage:
        shell.coverage = True
        import signal

        def handle_sighup():
            shell.stop_coverage()
            shell.do_exit()

        signal.signal(signal.SIGHUP, handle_sighup)

    shell.init_history()
    shell.cmdloop()
    shell.save_history()

    if shell.batch_mode and shell.statement_error:
        sys.exit(2)


# vim: set ft=python et ts=4 sw=4 :
