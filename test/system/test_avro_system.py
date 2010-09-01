
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

import avro_utils
from . import AvroTester
from avro.ipc import AvroRemoteException

class TestSystemOperations(AvroTester):
    """
    Cassandra system operations.
    """
    def test_system_keyspace_operations(self):
        "adding, renaming, and removing keyspaces"
        
        # create
        keyspace = dict()
        keyspace['name'] = 'CreateKeyspace'
        keyspace['strategy_class'] = 'org.apache.cassandra.locator.SimpleStrategy'
        keyspace['replication_factor'] = 1
        keyspace['strategy_options'] = {}
        cfdef = dict();
        cfdef['keyspace'] = 'CreateKeyspace'
        cfdef['name'] = 'CreateKsCf'
        keyspace['cf_defs'] = [cfdef]
        
        s = self.client.request('system_add_keyspace', {'ks_def' : keyspace})
        assert isinstance(s, unicode), 'returned type is %s, (not \'unicode\')' % type(s)
        
        # rename
        self.client.request('set_keyspace', {'keyspace' : 'CreateKeyspace'})
        s = self.client.request(
                'system_rename_keyspace', {'old_name' : 'CreateKeyspace', 'new_name' : 'RenameKeyspace'})
        assert isinstance(s, unicode), 'returned type is %s, (not \'unicode\')' % type(s)
        renameks = self.client.request('describe_keyspace',
                {'keyspace': 'RenameKeyspace'})
        assert renameks['name'] == 'RenameKeyspace'
        assert renameks['cf_defs'][0]['name'] == 'CreateKsCf'
        
        # drop
        s = self.client.request('system_drop_keyspace', {'keyspace' : 'RenameKeyspace'})
        assert isinstance(s, unicode), 'returned type is %s, (not \'unicode\')' % type(s)
        avro_utils.assert_raises(AvroRemoteException,
                      self.client.request,
                      'describe_keyspace',
                      {'keyspace' : 'RenameKeyspace'})
    def test_system_column_family_operations(self):
        "adding, renaming, and removing column families"
        self.client.request('set_keyspace', {'keyspace': 'Keyspace1'})
        
        # create
        columnDef = dict()
        columnDef['name'] = b'ValidationColumn'
        columnDef['validation_class'] = 'BytesType'
        
        cfDef = dict()
        cfDef['keyspace'] = 'Keyspace1'
        cfDef['name'] = 'NewColumnFamily'
        cfDef['column_metadata'] = [columnDef]
        s = self.client.request('system_add_column_family', {'cf_def' : cfDef})
        assert isinstance(s, unicode), \
            'returned type is %s, (not \'unicode\')' % type(s)
        
        ks1 = self.client.request(
            'describe_keyspace', {'keyspace' : 'Keyspace1'})
        assert 'NewColumnFamily' in [x['name'] for x in ks1['cf_defs']]

        # rename
        self.client.request('system_rename_column_family',
            {'old_name' : 'NewColumnFamily', 'new_name': 'RenameColumnFamily'})
        ks1 = self.client.request(
            'describe_keyspace', {'keyspace' : 'Keyspace1'})
        assert 'RenameColumnFamily' in [x['name'] for x in ks1['cf_defs']]

        # drop
        self.client.request('system_drop_column_family',
            {'column_family' : 'RenameColumnFamily'})
        ks1 = self.client.request(
                'describe_keyspace', {'keyspace' : 'Keyspace1'})
        assert 'RenameColumnFamily' not in [x['name'] for x in ks1['cf_defs']]
        assert 'NewColumnFamily' not in [x['name'] for x in ks1['cf_defs']]
        assert 'Standard1' in [x['name'] for x in ks1['cf_defs']]

