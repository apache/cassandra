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

from __future__ import with_statement

import contextlib
import tempfile
import os.path
from .basecase import cql, cqlsh, cqlshlog, TEST_HOST, TEST_PORT, rundir, policy, quote_name
from .run_cqlsh import run_cqlsh, call_cqlsh

test_keyspace_init = os.path.join(rundir, 'test_keyspace_init.cql')

def get_cassandra_connection(cql_version=cqlsh.DEFAULT_CQLVER):
    if cql_version is None:
        cql_version = cqlsh.DEFAULT_CQLVER
    conn = cql((TEST_HOST,), TEST_PORT, cql_version=cql_version, load_balancing_policy=policy)
    # until the cql lib does this for us
    conn.cql_version = cql_version
    return conn

def get_cassandra_cursor(cql_version=cqlsh.DEFAULT_CQLVER):
    return get_cassandra_connection(cql_version=cql_version).cursor()

TEST_KEYSPACES_CREATED = []

def get_test_keyspace():
    return TEST_KEYSPACES_CREATED[-1]

def make_test_ks_name():
    # abuse mktemp to get a quick random-ish name
    return os.path.basename(tempfile.mktemp(prefix='CqlshTests_'))

def create_test_keyspace(cursor):
    ksname = make_test_ks_name()
    qksname = quote_name(ksname)
    cursor.execute('''
        CREATE KEYSPACE %s WITH replication =
            {'class': 'SimpleStrategy', 'replication_factor': 1};
    ''' % quote_name(ksname))
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
        cqlshlog.debug(logprefix + cql)
        cursor.execute(cql)

def execute_cql_file(cursor, fname):
    with open(fname) as f:
        return execute_cql_commands(cursor, f.read())

def create_test_db():
    with cassandra_cursor(ks=None) as c:
        k = create_test_keyspace(c)
        execute_cql_file(c, test_keyspace_init)
    return k

def remove_test_db():
    with cassandra_cursor(ks=None) as c:
        c.execute('DROP KEYSPACE %s' % quote_name(TEST_KEYSPACES_CREATED.pop(-1)))

@contextlib.contextmanager
def cassandra_connection(cql_version=cqlsh.DEFAULT_CQLVER):
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
        conn.close()

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
        ks = get_test_keyspace()
    conn = get_cassandra_connection(cql_version=cql_version)
    try:
        c = conn.connect(ks)
        # if ks is not None:
        #     c.execute('USE %s;' % quote_name(c, ks))
        yield c
    finally:
        conn.shutdown()

def cql_rule_set():
    return cqlsh.cql3handling.CqlRuleSet

class DEFAULTVAL: pass

def testrun_cqlsh(keyspace=DEFAULTVAL, **kwargs):
    # use a positive default sentinel so that keyspace=None can be used
    # to override the default behavior
    if keyspace is DEFAULTVAL:
        keyspace = get_test_keyspace()
    return run_cqlsh(keyspace=keyspace, **kwargs)

def testcall_cqlsh(keyspace=None, **kwargs):
    if keyspace is None:
        keyspace = get_test_keyspace()
    return call_cqlsh(keyspace=keyspace, **kwargs)
