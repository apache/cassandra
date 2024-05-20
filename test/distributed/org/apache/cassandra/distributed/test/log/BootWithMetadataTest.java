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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.CMSOperations;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;

import static org.apache.cassandra.distributed.shared.ClusterUtils.start;
import static org.junit.Assert.assertEquals;
import static org.psjava.util.AssertStatus.assertTrue;

public class BootWithMetadataTest extends TestBaseImpl
{
    @Test
    public void resetTest() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .start()))
        {
            long epoch = 0;
            for (int i = 0; i < 10; i++)
            {
                cluster.schemaChange(withKeyspace("create table %s.x" + i + " (id int primary key)"));
                // later we reset to `epoch` - only tables x0 .. x5 should exist
                if (i == 5)
                    epoch = cluster.get(1).callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch());
            }

            long resetEpoch = epoch;
            String filename = cluster.get(1).callOnInstance(() -> {
                try
                {
                    return CMSOperations.instance.dumpClusterMetadata(resetEpoch, 1000, "V2");
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            });
            cluster.get(1).shutdown().get();

            start(cluster.get(1), (props) -> props.set(CassandraRelevantProperties.TCM_UNSAFE_BOOT_WITH_CLUSTERMETADATA, filename));

            cluster.schemaChange(withKeyspace("create table %s.yy (id int primary key)"));
            cluster.forEach(() -> {
                assertEquals(1, ClusterMetadata.current().fullCMSMembers().size());
                assertTrue(ClusterMetadata.current().fullCMSMembers().contains(InetAddressAndPort.getByNameUnchecked("127.0.0.1")));
                Keyspace ks = Keyspace.open(KEYSPACE);
                assertEquals(6, ks.getColumnFamilyStores().size());
                for (int i = 0; i < 6; i++)
                    assertTrue(ks.getColumnFamilyStore("x"+i) != null); // getColumnFamilyStore throws
            });
        }
    }

    @Test
    public void newCMSTest() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = init(builder().withNodes(4)
                                             .start()))
        {
            for (int i = 0; i < 10; i++)
                cluster.schemaChange(withKeyspace("create table %s.x" + i + " (id int primary key)"));

            String filename = cluster.get(1).callOnInstance(() -> {
                try
                {
                    ClusterMetadata metadata = ClusterMetadata.current();
                    Replica oldCMS = MetaStrategy.replica(InetAddressAndPort.getByNameUnchecked("127.0.0.1"));
                    Replica newCMS = MetaStrategy.replica(InetAddressAndPort.getByNameUnchecked("127.0.0.2"));
                    ClusterMetadata.Transformer transformer = metadata.transformer();
                    DataPlacement.Builder builder = metadata.placements.get(ReplicationParams.meta(metadata)).unbuild()
                                                                       .withoutReadReplica(metadata.nextEpoch(), oldCMS)
                                                                       .withoutWriteReplica(metadata.nextEpoch(), oldCMS)
                                                                       .withWriteReplica(metadata.nextEpoch(), newCMS)
                                                                       .withReadReplica(metadata.nextEpoch(), newCMS);
                    transformer = transformer.with(metadata.placements.unbuild().with(ReplicationParams.meta(metadata), builder.build()).build());
                    ClusterMetadata toDump = transformer.build().metadata.forceEpoch(Epoch.create(1000));
                    Path p = Files.createTempFile("clustermetadata", "dump");
                    try (FileOutputStreamPlus out = new FileOutputStreamPlus(p))
                    {
                        VerboseMetadataSerializer.serialize(ClusterMetadata.serializer, toDump, out, NodeVersion.CURRENT_METADATA_VERSION);
                    }
                    return p.toString();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            });
            cluster.get(1).shutdown().get();
            cluster.get(2).shutdown().get();
            start(cluster.get(2), (props) -> props.set(CassandraRelevantProperties.TCM_UNSAFE_BOOT_WITH_CLUSTERMETADATA, filename));
            for (int i = 2; i <= 4; i++)
                cluster.get(i).runOnInstance(() -> {
                    assertEquals(1, ClusterMetadata.current().fullCMSMembers().size());
                    assertTrue(ClusterMetadata.current().fullCMSMembers().contains(InetAddressAndPort.getByNameUnchecked("127.0.0.2")));
                });

            cluster.coordinator(3).execute(withKeyspace("create table %s.yy (id int primary key)"), ConsistencyLevel.ONE);
        }
    }
}
