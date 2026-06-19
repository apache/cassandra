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

package org.apache.cassandra.distributed.test.log;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;

import static org.junit.Assert.assertEquals;

public class CatchupViaSnapshotTest extends TestBaseImpl
{
    @Test
    public void catchupViaSnapshotTest() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withConfig(c -> c.set("metadata_snapshot_frequency", "10"))
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("alter keyspace %s with replication = {'class':'SimpleStrategy', 'replication_factor':3}"));
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));

            // isolate node2 and node3
            cluster.filters().inbound().from(1).to(2,3).drop();

            for (int i = 0; i < 30; i++)
                cluster.coordinator(1).execute(withKeyspace("alter table %s.tbl with comment='abc" + i + "'"), ConsistencyLevel.ONE);

            // Snapshot needs to be the last transformation in the log
            cluster.get(1).nodetoolResult("cms", "snapshot").asserts().success();

            // allow node2 to catch up:
            cluster.filters().reset();
            cluster.filters().inbound().from(1).to(3).drop();
            String node1Address = cluster.get(1).config().broadcastAddress().getHostString();

            fetchLogFromPeerAsync(cluster.get(2), node1Address);
            // allow node3 to catch up
            cluster.filters().reset();
            String node2Address = cluster.get(2).config().broadcastAddress().getHostString();
            // by now node2 has an incomplete log, it only caught up from the snapshot above
            // this means the log is continuous, but its current epoch is beyond the last entry in the log
            fetchLogFromPeerAsync(cluster.get(3), node2Address);

            long expectedEpoch = cluster.get(1).callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch());
            for (IInvokableInstance i : cluster)
                assertEquals(expectedEpoch, (long)i.callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch()));
        }
    }

    private static void fetchLogFromPeerAsync(IInvokableInstance i, String address)
    {
        i.runOnInstance(() -> {
            try
            {
                ClusterMetadataService.instance().fetchLogFromPeerAsync(InetAddressAndPort.getByNameUnchecked(address), Epoch.create(30)).get();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });
    }
}
