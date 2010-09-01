
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

class TestSuperOperations(AvroTester):

    def _set_keyspace(self, keyspace):
        self.client.request('set_keyspace', {'keyspace': keyspace})
        
    """
    Operations on Super column families
    """
    def test_insert_super(self):
        "setting and getting a super column"
        self._set_keyspace('Keyspace1')

        params = dict()
        params['key'] = 'key1'
        params['column_parent'] = dict()
        params['column_parent']['column_family'] = 'Super1'
        params['column_parent']['super_column'] = 'sc1'
        params['column'] = dict()
        params['column']['name'] = avro_utils.i64(1)
        params['column']['value'] = 'v1'
        params['column']['clock'] = { 'timestamp' : 0 }
        params['consistency_level'] = 'ONE'
        self.client.request('insert', params)

        read_params = dict()
        read_params['key'] = params['key']
        read_params['column_path'] = dict()
        read_params['column_path']['column_family'] = 'Super1'
        read_params['column_path']['super_column'] = params['column_parent']['super_column']
        read_params['column_path']['column'] = params['column']['name']
        read_params['consistency_level'] = 'ONE'

        cosc = self.client.request('get', read_params)

        avro_utils.assert_cosc(cosc)
        avro_utils.assert_columns_match(cosc['column'], params['column'])
    
    def test_missing_super(self):
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
        time.sleep(0.1)

