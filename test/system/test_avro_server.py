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

class TestMutations(AvroTester):
    def test_insert_and_get(self):
        params = dict()
        params['keyspace'] = 'Keyspace1'
        params['key'] = 'key1'
        params['column_path'] = dict(column_family='Standard1', column='c1')
        params['value'] = 'v1'
        params['timestamp'] = 1L
        params['consistency_level'] = 'ONE'

        self.client.request('insert', params)

        params = dict()
        params['keyspace'] = 'Keyspace1'
        params['key'] = 'key1'
        params['column_path'] = dict(column_family='Standard1', column='c1')
        params['consistency_level'] = 'ONE'

        response = self.client.request('get', params)

        assert isinstance(response, dict) and response.has_key('column') \
                and response['column'].has_key('name')
        assert response['column']['name'] == 'c1'
        assert response['column']['value'] == 'v1'
        
# vi:ai sw=4 ts=4 tw=0 et
