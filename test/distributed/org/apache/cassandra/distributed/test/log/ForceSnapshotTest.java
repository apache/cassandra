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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


public class ForceSnapshotTest extends TestBaseImpl
{
    @Test
    public void testForceSnapshot() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withConfig(c -> c.set("unsafe_tcm_mode", "true"))
                                             .start()))
        {
            cluster.get(2).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();

                Directory d = metadata.directory.with(new NodeAddresses(UUID.randomUUID(),
                                                                        InetAddressAndPort.getByNameUnchecked("127.0.0.5"),
                                                                        InetAddressAndPort.getByNameUnchecked("127.0.0.5"),
                                                                        InetAddressAndPort.getByNameUnchecked("127.0.0.5")),
                                                      new Location("AA", "BB"));
                metadata = metadata.transformer().with(d).build().metadata;

                ClusterMetadataService.instance().forceSnapshot(metadata);
            });

            cluster.forEach(() -> assertEquals(InetAddressAndPort.getByNameUnchecked("127.0.0.5"), ClusterMetadata.current().directory.endpoint(new NodeId(4))));
        }
    }

    @Test
    public void testRevertToEpoch() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withConfig(c -> c.set("metadata_snapshot_frequency", 7)
                                                               .set("unsafe_tcm_mode", "true"))
                                             .start()))
        {
            Map<Integer, Long> tblToEpoch = new HashMap<>();
            for (int i = 0; i < 20; i++)
            {
                // we create table "x"+i at this epoch - make sure the table is gone when revert to it
                tblToEpoch.put(i, cluster.get(1).callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch()));
                cluster.schemaChange(withKeyspace("create table %s.x" + i + " (id int primary key)"));
            }
            int startReverting = 15;
            for (int i = 0; i < 10; i++)
            {
                long revertTo = tblToEpoch.get(startReverting - i);
                cluster.get(2).runOnInstance(() -> ClusterMetadataService.instance().revertToEpoch(Epoch.create(revertTo)));
                for (int j = 0; j < startReverting - i; j++)
                    cluster.coordinator(1).execute(withKeyspace("insert into %s.x" + j + " (id) values (1)"), ConsistencyLevel.ALL);
                try
                {
                    int goneTable = startReverting - i;
                    cluster.coordinator(1).execute(withKeyspace("insert into %s.x" + goneTable + " (id) values (1)"), ConsistencyLevel.ALL);
                    fail("Table x" + goneTable + " should not exist");
                }
                catch (Exception e)
                {
                    //ignore
                }
            }
        }
    }

    @Test
    public void testDumpLoadMetadata() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withConfig(c -> c.set("metadata_snapshot_frequency", 7)
                                                               .set("unsafe_tcm_mode", "true"))
                                             .start()))
        {
            String filename = null;
            for (int i = 0; i < 20; i++)
            {
                if (i == 10)
                {
                    filename = cluster.get(2).callOnInstance(() -> {
                        try
                        {
                            return ClusterMetadataService.instance().dumpClusterMetadata(Epoch.EMPTY, ClusterMetadata.current().epoch, NodeVersion.CURRENT_METADATA_VERSION);
                        }
                        catch (IOException e)
                        {
                            throw new RuntimeException(e);
                        }
                    });
                }
                cluster.schemaChange(withKeyspace("create table %s.x" + i + " (id int primary key)"));
            }
            assertNotNull(filename);
            for (int i = 0; i < 20; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.x"+i+" (id) values (1)"), ConsistencyLevel.ALL);
            String loadFilename = filename;
            cluster.forEach(() -> assertEquals(20, Keyspace.open(KEYSPACE).getColumnFamilyStores().size()));
            cluster.get(1).runOnInstance(() -> {
                try
                {
                    ClusterMetadataService.instance().loadClusterMetadata(loadFilename);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            });
            cluster.forEach(() -> assertEquals(10, Keyspace.open(KEYSPACE).getColumnFamilyStores().size()));

            // make sure we execute more transformations;
            cluster.schemaChange(withKeyspace("create table %s.yyy (id int primary key)"));

        }
    }
}
