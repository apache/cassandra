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

def _insert_column(client, column):
    _insert_columns(client, [column])

def _insert_columns(client, columns):
    params = dict()
    params['keyspace'] = 'Keyspace1'
    params['key'] = 'key1'
    params['column_path'] = dict(column_family='Standard1')
    params['consistency_level'] = 'ONE'

    for column in columns:
        params['column_path']['column'] = column['name']
        params['value'] = column['value']
        params['timestamp'] = long(time())
        client.request('insert', params)

def _get_column(client, name):
    params = dict()
    params['keyspace'] = 'Keyspace1'
    params['key'] = 'key1'
    params['column_path'] = dict(column_family='Standard1', column=name)
    params['consistency_level'] = 'ONE'
    return client.request('get', params)

def assert_columns_match(colA, colB):
    assert colA['name'] == colB['name'], \
            "column name mismatch: %s != %s" % (colA['name'], colB['name'])
    assert colA['value'] == colB['value'], \
            "column value mismatch: %s != %s" % (colA['value'], colB['value'])

def random_column():
    return COLUMNS[randint(0, len(COLUMNS)-1)]

def random_supercolumn():
    return SUPERCOLUMNS[randint(0, len(SUPERCOLUMNS)-1)]

class TestMutations(AvroTester):
    def test_insert_and_get(self):
        "setting and getting a column"
        column = random_column()
        _insert_column(self.client, column)
        result = _get_column(self.client, column['name'])

        assert isinstance(result, dict) and result.has_key('column') \
                and result['column'].has_key('name')
        assert_columns_match(result['column'], column)

    def test_batch_insert(self):
        "performing a batch insert operation"
        # TODO: do
        pass

    def test_get_api_version(self):
        "getting the remote api version string"
        vers = self.client.request('get_api_version', {})
        assert isinstance(vers, (str,unicode)) and len(vers.split('.')) == 3

# vi:ai sw=4 ts=4 tw=0 et
