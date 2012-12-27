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
from .basecase import cql, cqlsh, cqlshlog, TEST_HOST, TEST_PORT, rundir
from .run_cqlsh import run_cqlsh, call_cqlsh

test_keyspace_init2 = os.path.join(rundir, 'test_keyspace_init2.cql')
test_keyspace_init3 = os.path.join(rundir, 'test_keyspace_init3.cql')

def get_cassandra_connection(cql_version=None):
    if cql_version is None:
        cql_version = '2.0.0'
    conn = cql.connect(TEST_HOST, TEST_PORT, cql_version=cql_version)
    # until the cql lib does this for us
    conn.cql_version = cql_version
    return conn

def get_cassandra_cursor(cql_version=None):
    return get_cassandra_connection(cql_version=cql_version).cursor()

TEST_KEYSPACES_CREATED = []

def get_test_keyspace():
    return TEST_KEYSPACES_CREATED[-1]

def make_test_ks_name():
    # abuse mktemp to get a quick random-ish name
    return os.path.basename(tempfile.mktemp(prefix='CqlshTests_'))

def create_test_keyspace(cursor):
    ksname = make_test_ks_name()
    qksname = quote_name(cursor, ksname)
    cursor.execute('''
        CREATE KEYSPACE %s WITH strategy_class = 'SimpleStrategy'
                           AND strategy_options:replication_factor = 1;
    ''' % quote_name(cursor, ksname))
    cursor.execute('USE %s;' % qksname)
    TEST_KEYSPACES_CREATED.append(ksname)
    return ksname

def split_cql_commands(source, cqlver='2.0.0'):
    ruleset = cql_rule_set(cqlver)
    statements, in_batch = ruleset.cql_split_statements(source)
    if in_batch:
        raise ValueError("CQL source ends unexpectedly")

    return [ruleset.cql_extract_orig(toks, source) for toks in statements if toks]

def execute_cql_commands(cursor, source, logprefix='INIT: '):
    for cql in split_cql_commands(source, cqlver=cursor._connection.cql_version):
        cqlshlog.debug(logprefix + cql)
        cursor.execute(cql)

def execute_cql_file(cursor, fname):
    with open(fname) as f:
        return execute_cql_commands(cursor, f.read())

def populate_test_db_cql3(cursor):
    execute_cql_file(cursor, test_keyspace_init3)

def populate_test_db_cql2(cursor):
    execute_cql_file(cursor, test_keyspace_init2)

def create_test_db():
    with cassandra_cursor(ks=None) as c:
        k = create_test_keyspace(c)
        populate_test_db_cql2(c)
    with cassandra_cursor(ks=k, cql_version='3.0.0') as c:
        populate_test_db_cql3(c)
    return k

def remove_test_db():
    with cassandra_cursor(ks=None) as c:
        c.execute('DROP KEYSPACE %s' % quote_name(c, TEST_KEYSPACES_CREATED.pop(-1)))

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
        c = conn.cursor()
        if ks is not None:
            c.execute('USE %s;' % quote_name(c, ks))
        yield c
    finally:
        conn.close()

def cql_rule_set(cqlver):
    if str(cqlver).startswith('2'):
        return cqlsh.cqlhandling.CqlRuleSet
    else:
        return cqlsh.cql3handling.CqlRuleSet

def quote_name(cqlver, name):
    if isinstance(cqlver, cql.cursor.Cursor):
        cqlver = cqlver._connection
    if isinstance(cqlver, cql.connection.Connection):
        cqlver = cqlver.cql_version
    return cql_rule_set(cqlver).maybe_escape_name(name)

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
