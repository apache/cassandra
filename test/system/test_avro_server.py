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

class TestRpcOperations(AvroTester):
    
    def test_describe_keyspaces(self):
        "retrieving a list of all keyspaces"
        keyspaces = self.client.request('describe_keyspaces', {})
        assert 'Keyspace1' in keyspaces, "Keyspace1 not in " + keyspaces

    def test_describe_keyspace(self):
        "retrieving a keyspace metadata"
        ks1 = self.client.request('describe_keyspace',
                {'keyspace': "Keyspace1"})
        assert ks1['replication_factor'] == 1
        cf0 = ks1['cf_defs'][0]
        assert cf0['comparator_type'] == "org.apache.cassandra.db.marshal.BytesType"

    def test_describe_cluster_name(self):
        "retrieving the cluster name"
        name = self.client.request('describe_cluster_name', {})
        assert 'Test' in name, "'Test' not in '" + name + "'"

    def test_describe_version(self):
        "getting the remote api version string"
        vers = self.client.request('describe_version', {})
        assert isinstance(vers, (str,unicode)), "api version is not a string"
        segs = vers.split('.')
        assert len(segs) == 3 and len([i for i in segs if i.isdigit()]) == 3, \
               "incorrect api version format: " + vers

    def test_describe_partitioner(self):
        "getting the partitioner"
        part = "org.apache.cassandra.dht.CollatingOrderPreservingPartitioner"
        result = self.client.request('describe_partitioner', {})
        assert result == part, "got %s, expected %s" % (result, part)

# vi:ai sw=4 ts=4 tw=0 et
