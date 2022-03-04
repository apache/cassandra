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


import contextlib
import io
import os.path
import random
import string

from cassandra.cluster import Cluster
from cassandra.metadata import maybe_escape_name as quote_name
from cassandra.auth import PlainTextAuthProvider
from cqlshlib.cql3handling import CqlRuleSet

from .basecase import TEST_HOST, TEST_PORT, TEST_USER, TEST_PWD, cqlshlog, test_dir
from .run_cqlsh import run_cqlsh, call_cqlsh

test_keyspace_init = os.path.join(test_dir, 'test_keyspace_init.cql')


def get_cassandra_connection(cql_version=None):

    auth_provider = PlainTextAuthProvider(username=TEST_USER, password=TEST_PWD)
    conn = Cluster((TEST_HOST,), TEST_PORT, auth_provider=auth_provider, cql_version=cql_version)

    # until the cql lib does this for us
    conn.cql_version = cql_version
    return conn


def get_cassandra_cursor(cql_version=None):
    return get_cassandra_connection(cql_version=cql_version).cursor()


TEST_KEYSPACES_CREATED = []


def get_keyspace():
    return None if len(TEST_KEYSPACES_CREATED) == 0 else TEST_KEYSPACES_CREATED[-1]


_used_ks_names = set()


def make_ks_name():
    def random_ks():
        return 'cqlshtests_' + ''.join(random.choice(string.ascii_lowercase) for _ in range(10))

    s = random_ks()
    while s in _used_ks_names:
        s = random_ks()
    _used_ks_names.add(s)
    return s


def create_keyspace(cursor):
    ksname = make_ks_name().lower()
    qksname = quote_name(ksname)
    cursor.execute('''
        CREATE KEYSPACE %s WITH replication =
            {'class': 'SimpleStrategy', 'replication_factor': 1};
    ''' % qksname)
    cursor.execute('USE %s;' % qksname)
    TEST_KEYSPACES_CREATED.append(ksname)
    return ksname


def split_cql_commands(source):
    ruleset = cql_rule_set()
    statements, endtoken_escaped = ruleset.cql_split_statements(source)
    if endtoken_escaped:
        raise ValueError("CQL source ends unexpectedly")

    return [ruleset.cql_extract_orig(toks, source) for toks in statements if toks]


def execute_cql_commands(cursor, source, logprefix='INIT: '):
    for cql in split_cql_commands(source):
        cqlshlog.debug((logprefix + cql).encode("utf-8"))
        cursor.execute(cql)


def execute_cql_file(cursor, fname):
    with io.open(fname, "r", encoding="utf-8") as f:
        return execute_cql_commands(cursor, f.read())


def create_db():
    with cassandra_cursor(ks=None) as c:
        k = create_keyspace(c)
        execute_cql_file(c, test_keyspace_init)
    return k


def remove_db():
    with cassandra_cursor(ks=None) as c:
        c.execute('DROP KEYSPACE %s' % quote_name(TEST_KEYSPACES_CREATED.pop(-1)))


@contextlib.contextmanager
def cassandra_connection(cql_version=None):
    """
    Make a Cassandra CQL connection with the given CQL version and get a cursor
    for it, and optionally connect to a given keyspace.

    The connection is returned as the context manager's value, and it will be
    closed when the context exits.
    """

    conn = get_cassandra_connection(cql_version=cql_version)
    try:
        yield conn
    finally:
        conn.shutdown()


@contextlib.contextmanager
def cassandra_cursor(cql_version=None, ks=''):
    """
    Make a Cassandra CQL connection with the given CQL version and get a cursor
    for it, and optionally connect to a given keyspace. If ks is the empty
    string (default), connect to the last test keyspace created. If ks is None,
    do not connect to any keyspace. Otherwise, attempt to connect to the
    keyspace named.

    The cursor is returned as the context manager's value, and the connection
    will be closed when the context exits.
    """

    if ks == '':
        ks = get_keyspace()
    conn = get_cassandra_connection(cql_version=cql_version)
    try:
        c = conn.connect(ks)
        # increase default timeout to fix flacky tests, see CASSANDRA-12481
        c.default_timeout = 60.0
        yield c
    finally:
        conn.shutdown()


def cql_rule_set():
    return CqlRuleSet


class DEFAULTVAL:
    pass


__TEST__ = False
def testrun_cqlsh(keyspace=DEFAULTVAL, **kwargs):
    # use a positive default sentinel so that keyspace=None can be used
    # to override the default behavior
    if keyspace is DEFAULTVAL:
        keyspace = get_keyspace()
    return run_cqlsh(keyspace=keyspace, **kwargs)


__TEST__ = False
def testcall_cqlsh(keyspace=None, **kwargs):
    if keyspace is None:
        keyspace = get_keyspace()
    if 'input' in kwargs.keys() and isinstance(kwargs['input'], str):
        kwargs['input'] = kwargs['input'].encode('utf-8')
    return call_cqlsh(keyspace=keyspace, **kwargs)
