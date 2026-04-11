/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test.cms;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.CMSTestBase;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.transformations.Startup;

public class CMSAddressChangeTest extends CMSTestBase
{
    @Test
    public void testCMSAddressChange()
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        Keyspace.setInitialized();
        ClusterMetadataTestHelper.setInstanceForTest();
        for (int i = 1; i < 10; i++)
        {
            ClusterMetadataTestHelper.register(i);
            ClusterMetadataTestHelper.join(i, i);
        }

        ClusterMetadataTestHelper.reconfigureCms(ReplicationParams.ntsMeta(Collections.singletonMap("dc0", 3)));

        ClusterMetadata metadata = ClusterMetadata.current();
        InetAddressAndPort oldAddr = metadata.fullCMSMembers().iterator().next();
        InetAddressAndPort newAddr = ClusterMetadataTestHelper.addr(100);
        NodeId cmsMemberNodeId = metadata.directory.peerId(oldAddr);

        metadata = ClusterMetadataService.instance().commit(new Startup(cmsMemberNodeId,
                                                                        new NodeAddresses(newAddr),
                                                                        metadata.directory.version(cmsMemberNodeId)));

        Assert.assertFalse(metadata.fullCMSMembers().contains(oldAddr));
        Assert.assertTrue(metadata.fullCMSMembers().contains(newAddr));
    }
}
