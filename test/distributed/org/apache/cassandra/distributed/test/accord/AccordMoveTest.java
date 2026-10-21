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

package org.apache.cassandra.distributed.test.accord;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.execution.SaferCommandStore;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.service.accord.AccordService.getBlocking;

public class AccordMoveTest extends AccordBootstrapTestBase
{
    @Test
    public void moveTest() throws Throwable
    {
        try (Cluster cluster = Cluster.build().withNodes(3)
                                      .withoutVNodes()
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                      .withConfig(config -> config
                                                            .set("accord.shard_durability_target_splits", "1")
                                                            .set("accord.shard_durability_cycle", "20s")
                                                            .with(NETWORK, GOSSIP))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':2}");
            cluster.schemaChange("CREATE TABLE ks.tbl (k int, c int, v int, primary key(k, c)) WITH transactional_mode='full'");

            long[] tokens = new long[3];
            for (int i=0; i<3; i++)
            {
                tokens[i] = cluster.get(i+1).callOnInstance(() -> Long.valueOf(getOnlyElement(StorageService.instance.getTokens())));
            }

            awaitMaxEpochReadyToRead(cluster);

            for (int key = 0; key < 100; key++)
            {
                String query = "BEGIN TRANSACTION\n" +
                               "  LET row1 = (SELECT * FROM ks.tbl WHERE k = " + key + " AND c = 0);\n" +
                               "  SELECT row1.v;\n" +
                               "  IF row1 IS NULL THEN\n" +
                               "    INSERT INTO ks.tbl (k, c, v) VALUES (" + key + ", " + key + ", " + key + ");\n" +
                               "  END IF\n" +
                               "COMMIT TRANSACTION";
                AccordTestBase.executeWithRetry(cluster, query);
            }

            long token = ((tokens[1] - tokens[0]) / 2) + tokens[0];
            long preMove = maxEpoch(cluster);

            cluster.get(1).runOnInstance(() -> StorageService.instance.move(Long.toString(token)));

            long moveMax = awaitMaxEpochReadyToRead(cluster);

            for (IInvokableInstance node : cluster)
            {
                node.runOnInstance(() -> {
                    // validate streaming
                    List<Range<Token>> ranges = StorageService.instance.getLocalRanges("ks");
                    TableId tableId = Schema.instance.getTableMetadata("ks", "tbl").id;
                    for (int key = 0; key < 100; key++)
                    {
                        DecoratedKey dk = dk(key);
                        UntypedResultSet result = QueryProcessor.executeInternal("SELECT * FROM ks.tbl WHERE k=?", key);
                        if (ranges.stream().anyMatch(range -> range.contains(dk.getToken())))
                        {
                            UntypedResultSet.Row row = getOnlyElement(result);
                            Assert.assertEquals(key, row.getInt("c"));
                            Assert.assertEquals(key, row.getInt("v"));

                            PartitionKey partitionKey = new PartitionKey(tableId, dk);

                            getBlocking(service().node().commandStores().forEach("Test", RoutingKeys.of(partitionKey.toUnseekable()), moveMax, moveMax, safeStore -> {
                                if (!safeStore.ranges().allAt(preMove).contains(partitionKey))
                                {
                                    SaferCommandStore ss = (SaferCommandStore) safeStore;
                                    Assert.assertFalse(ss.bootstrapBeganAt().isEmpty());
                                    Assert.assertFalse(ss.safeToReadAt().isEmpty());

                                    Assert.assertEquals(1, ss.bootstrapBeganAt().entrySet().stream()
                                                                       .filter(entry -> entry.getValue().contains(partitionKey))
                                                                       .map(entry -> {
                                                                           Assert.assertTrue(entry.getKey().compareTo(Timestamp.NONE) > 0);
                                                                           return entry;
                                                                       }).count());
                                    Assert.assertEquals(1, ss.safeToReadAt().entrySet().stream()
                                                                       .filter(entry -> entry.getValue().contains(partitionKey))
                                                                       .map(entry -> {
                                                                           Assert.assertTrue(entry.getKey().compareTo(Timestamp.NONE) > 0);
                                                                           return entry;
                                                                       }).count());
                                }
                            }));
                        }
                    }
                });
            }
        }
    }
}
