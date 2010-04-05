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

from . import AvroTester
from time import time
from random import randint
from avro.ipc import AvroRemoteException

COLUMNS = [
    dict(name="c0", value="v0", timestamp=1L),
    dict(name="c1", value="v1", timestamp=1L),
    dict(name="c2", value="v2", timestamp=1L),
    dict(name="c3", value="v3", timestamp=1L),
    dict(name="c4", value="v4", timestamp=1L),
    dict(name="c5", value="v5", timestamp=1L),
]

SUPERCOLUMNS = [
    dict(name="sc0", columns=COLUMNS[:3]),
    dict(name="sc1", columns=COLUMNS[3:]),
]

def _insert_column(client, name, value):
    _insert_columns(client, [(name, value)])

def _insert_columns(client, columns):
    params = dict()
    params['keyspace'] = 'Keyspace1'
    params['key'] = 'key1'
    params['column_path'] = dict(column_family='Standard1')
    params['consistency_level'] = 'ONE'

    for (name, value) in columns:
        params['column_path']['column'] = name
        params['value'] = value
        params['timestamp'] = long(time())
        client.request('insert', params)

def _insert_supercolumn(client, super_name, name, value):
    params = dict()
    params['keyspace'] = 'Keyspace1'
    params['key'] = 'key1'
    params['timestamp'] = long(time())
    params['consistency_level'] = 'ONE'

    params['column_path'] = dict()
    params['column_path']['column_family'] = 'Super4'
    params['column_path']['super_column'] = super_name
    params['column_path']['column'] = name
    params['value'] = value

    client.request('insert', params)

def _get_column(client, name):
    params = dict()
    params['keyspace'] = 'Keyspace1'
    params['key'] = 'key1'
    params['column_path'] = dict(column_family='Standard1', column=name)
    params['consistency_level'] = 'ONE'
    return client.request('get', params)

def _get_supercolumn(client, super_name, name):
    params = dict()
    params['keyspace'] = 'Keyspace1'
    params['key'] = 'key1'
    params['column_path'] = dict()
    params['column_path']['column_family'] = 'Super4'
    params['column_path']['super_column'] = super_name
    params['column_path']['column'] = name
    params['consistency_level'] = 'ONE'

    return client.request('get', params)

def assert_columns_match(colA, colB):
    assert colA['name'] == colB['name'], \
            "column name mismatch: %s != %s" % (colA['name'], colB['name'])
    assert colA['value'] == colB['value'], \
            "column value mismatch: %s != %s" % (colA['value'], colB['value'])

def assert_cosc(thing, with_supercolumn=False):
    containing = with_supercolumn and 'super_column' or 'column'
    assert isinstance(thing, dict), "Expected dict, got %s" % type(thing)
    assert thing.has_key(containing) and thing[containing].has_key('name'), \
            "Invalid or missing \"%s\"" % containing

def random_column(columns=COLUMNS):
    return columns[randint(0, len(columns)-1)]

def random_supercolumn(super_columns=SUPERCOLUMNS):
    return super_columns[randint(0, len(super_columns)-1)]

class TestRpcOperations(AvroTester):
    def test_insert_simple(self):       # Also tests get
        "setting and getting a simple column"
        column = random_column()

        _insert_column(self.client, column['name'], column['value'])
        result = _get_column(self.client, column['name'])

        assert_cosc(result)
        assert_columns_match(result['column'], column)

    def test_insert_super(self):
        "setting and getting a super column"
        sc = random_supercolumn()
        col = random_column(sc['columns'])

        _insert_supercolumn(self.client, sc['name'], col['name'], col['value'])
        result = _get_supercolumn(self.client, sc['name'], col['name'])

        assert_cosc(result)
        assert_columns_match(result['column'], col)

    def test_batch_insert(self):
        "performing a batch insert operation"
        params = dict()
        params['keyspace'] = 'Keyspace1'
        params['key'] = 'key1'
        params['consistency_level'] = 'ONE'

        # Map<string, list<ColumnOrSuperColumn>>
        params['cfmap'] = dict()
        params['cfmap']['Standard1'] = list()

        for i in range(0,3):
            params['cfmap']['Standard1'].append(dict(column=COLUMNS[i]))

        self.client.request('batch_insert', params)

        for i in range(0,3):
            assert_cosc(_get_column(self.client, COLUMNS[i]['name']))

    def test_batch_mutate(self):
        "performing batch mutation operations"
        params = dict()
        params['keyspace'] = 'Keyspace1'
        params['consistency_level'] = 'ONE'

        mutation_map = dict()
        mutation_map['key1'] = dict(Standard1=[
            dict(column_or_supercolumn=dict(column=COLUMNS[0])),
            dict(column_or_supercolumn=dict(column=COLUMNS[1])),
            dict(column_or_supercolumn=dict(column=COLUMNS[2]))
        ])

        params['mutation_map'] = mutation_map

        self.client.request('batch_mutate', params)

        for i in range(0,3):
            cosc = _get_column(self.client, COLUMNS[i]['name'])
            assert_cosc(cosc)
            assert_columns_match(cosc['column'], COLUMNS[i])

        # FIXME: still need to apply a mutation that deletes

        #try:
        #    assert not _get_column(self.client, COLUMNS[1]['name']), \
        #        "Mutation did not delete column %s" % COLUMNS[1]['name']
        #    assert not _get_column(self.client, COLUMNS[2]['name']), \
        #        "Mutation did not delete column %s" % COLUMNS[2]['name']
        #except AvroRemoteException:
        #    pass


    def test_get_api_version(self):
        "getting the remote api version string"
        vers = self.client.request('get_api_version', {})
        assert isinstance(vers, (str,unicode)), "api version is not a string"
        segs = vers.split('.')
        assert len(segs) == 3 and len([i for i in segs if i.isdigit()]) == 3, \
               "incorrect api version format: " + vers

# vi:ai sw=4 ts=4 tw=0 et
