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

package org.apache.cassandra.distributed.test.ring;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.log.FuzzTestBase;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;

public class RangeVersioningTest extends FuzzTestBase
{
    @Test
    public void existingPlacementsUnchangedAfterAlter() throws IOException
    {
        try (Cluster cluster = builder().withNodes(4)
                                        .createWithoutStarting())
        {
            cluster.get(1).startup();
            cluster.coordinator(1).execute("CREATE KEYSPACE IF NOT EXISTS test_ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};", ConsistencyLevel.QUORUM);
            cluster.get(2).startup();
            cluster.coordinator(1).execute("CREATE KEYSPACE IF NOT EXISTS test_ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};", ConsistencyLevel.QUORUM);
            cluster.get(3).startup();
            cluster.coordinator(1).execute("CREATE KEYSPACE IF NOT EXISTS test_ks3 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};", ConsistencyLevel.QUORUM);
            cluster.get(4).startup();
            cluster.coordinator(1).execute("CREATE KEYSPACE IF NOT EXISTS test_ks4 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 4};", ConsistencyLevel.QUORUM);
            cluster.get(1).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                Epoch previous = Epoch.EMPTY;

                for (int i = 1; i <= 4; i++)
                {
                    Epoch smallestSeen = null;
                    for (VersionedEndpoints.ForRange fr : metadata.placements.get(ReplicationParams.simple(i)).writes.endpoints)
                    {
                        if (smallestSeen == null || fr.lastModified().isBefore(smallestSeen))
                            smallestSeen = fr.lastModified();
                    }
                    System.out.printf("Smallest seen for rf %d is %s%n", i, smallestSeen);
                    Assert.assertTrue(String.format("%s should have been before %s for RF %d", previous, smallestSeen, i),
                                      previous.isBefore(smallestSeen));
                    previous = smallestSeen;
                }
            });
        }
    }
}
