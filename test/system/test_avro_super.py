
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
from avro.ipc import AvroRemoteException
import avro_utils
import time
    
def _make_write_params(key, cf, sc, c, v, clock=0, cl='ONE'):
    params = dict()
    params['key'] = key
    params['column_parent'] = dict()
    params['column_parent']['column_family'] = cf
    params['column_parent']['super_column'] = sc
    params['column'] = dict()
    params['column']['name'] = c
    params['column']['value'] = v
    params['column']['clock'] = { 'timestamp' : clock }
    params['consistency_level'] = cl
    return params
    
def _make_read_params(key, cf, sc, c, cl):
    params = dict()
    params['key'] = key
    column_path = dict()
    column_path['column_family'] = cf
    column_path['super_column'] = sc
    column_path['column'] = c
    params['column_path'] = column_path
    params['consistency_level'] = cl
    return params

def _col(name, value, clock, ttl=None):
    return {'name':name, 'value':value, 'clock': {'timestamp':clock}, 'ttl': ttl}

def _super_col(name, columns):
    return {'name': name, 'columns': columns}

_SUPER_COLUMNS = [_super_col('sc1', [_col(avro_utils.i64(4), 'value4', 0)]), 
                  _super_col('sc2', [_col(avro_utils.i64(5), 'value5', 0), 
                                     _col(avro_utils.i64(6), 'value6', 0)])]

class TestSuperOperations(AvroTester):

    def _set_keyspace(self, keyspace):
        self.client.request('set_keyspace', {'keyspace': keyspace})
        
    """
    Operations on Super column families
    """
    def test_super_insert(self):
        "simple super column insert"
        self._set_keyspace('Keyspace1')
        self._insert_super()
        self._verify_super()
        
    def test_slice_super(self):
        "tests simple insert and get_slice"
        self._set_keyspace('Keyspace1')
        self._insert_super()
        p = {'slice_range': {'start': '', 'finish': '', 'reversed': False, 'count': 10}}
        parent = {'column_family': 'Super1', 'super_column': 'sc1'}
        cosc = self.client.request('get_slice', {'key': 'key1', 'column_parent': parent, 'predicate': p, 'consistency_level': 'ONE'})
        avro_utils.assert_cosc(cosc[0])
    
    def test_missing_super(self):
        "verifies that inserting doesn't yield false positives."
        self._set_keyspace('Keyspace1')
        avro_utils.assert_raises(AvroRemoteException,
                self.client.request,
                'get',
                _make_read_params('key1', 'Super1', 'sc1', avro_utils.i64(1), 'ONE'))
        self._insert_super()
        avro_utils.assert_raises(AvroRemoteException,
                self.client.request,
                'get',
                _make_read_params('key1', 'Super1', 'sc1', avro_utils.i64(1), 'ONE'))

    def _insert_super(self, key='key1'):
        self.client.request('insert', _make_write_params(key, 'Super1', 'sc1', avro_utils.i64(4), 'value4', 0, 'ONE'))
        self.client.request('insert', _make_write_params(key, 'Super1', 'sc2', avro_utils.i64(5), 'value5', 0, 'ONE'))
        self.client.request('insert', _make_write_params(key, 'Super1', 'sc2', avro_utils.i64(6), 'value6', 0, 'ONE'))
    
    def _big_slice(self, key, column_parent):
        p = {'slice_range': {'start': '', 'finish': '', 'reversed': False, 'count': 1000}}
        return self.client.request('get_slice',  {'key': key, 'column_parent': column_parent, 'predicate': p, 'consistency_level': 'ONE'})
        
    def _verify_super(self, supercf='Super1', key='key1'):
        col = self.client.request('get', _make_read_params(key, supercf, 'sc1', avro_utils.i64(4), 'ONE'))['column']
        avro_utils.assert_columns_match(col, {'name': avro_utils.i64(4), 'value': 'value4', 'timestamp': 0})
        slice = [result['super_column'] for result in self._big_slice(key, {'column_family': supercf})]
        assert slice == _SUPER_COLUMNS, _SUPER_COLUMNS
