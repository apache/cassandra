
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

class TestSuperOperations(AvroTester):
    """
    Operations on Super column families
    """
    def test_insert_super(self):
        "setting and getting a super column"
        self.client.request('set_keyspace', {'keyspace': 'Keyspace1'})

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
