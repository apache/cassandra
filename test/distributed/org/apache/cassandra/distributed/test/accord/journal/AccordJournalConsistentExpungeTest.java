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

package org.apache.cassandra.distributed.test.accord.journal;


import java.util.Iterator;

import org.junit.Test;

import accord.local.Command;
import accord.local.Node;
import accord.primitives.SaveStatus;
import accord.primitives.TxnId;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCacheEntry;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class AccordJournalConsistentExpungeTest extends TestBaseImpl
{
    private static DecoratedKey dk(int key)
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        return partitioner.decorateKey(ByteBufferUtil.bytes(key));
    }

    private static PartitionKey pk(int key, String keyspace, String table)
    {
        TableId tid = Schema.instance.getTableMetadata(keyspace, table).id;
        return new PartitionKey(tid, dk(key));
    }

    @Test
    public void loadCommandErasedTest() throws Throwable
    {
        try (Cluster cluster = Cluster.build().withNodes(3)
                                      .withoutVNodes()
                                      .withConfig(config -> config
                                                            .set("accord.shard_durability_cycle", "20s")
                                                            .set("accord.ephemeral_reads", false)
                                                            .with(NETWORK, GOSSIP))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':3}");
            cluster.schemaChange("CREATE TABLE ks.tbl (k int PRIMARY KEY, v int) WITH transactional_mode='full'");

            cluster.get(1).executeInternal("BEGIN TRANSACTION \n" +
                                           "SELECT * FROM ks.tbl WHERE k = 1; \n" +
                                           "COMMIT TRANSACTION");

            cluster.get(1).runOnInstance(() -> {
                AccordService service = (AccordService) AccordService.instance();
                PartitionKey key = pk(1, "ks", "tbl");

                Node node = service.node();
                AccordCommandStore commandStore = (AccordCommandStore) node.commandStores().unsafeForKey(key.toUnseekable());

                Iterator<AccordCacheEntry<TxnId, Command>> iterator = commandStore.cachesUnsafe().commands().iterator();

                TxnId txnId = TxnId.NONE;

                while (iterator.hasNext())
                {
                    txnId = iterator.next().key();
                    if (!txnId.isSystemTxn())
                        break;
                }

                assertFalse(txnId.isSystemTxn());

                TxnId finalTxnId = txnId;
                Util.spinUntilTrue(() -> commandStore.safeGetRedundantBefore().minGcBefore().compareTo(finalTxnId) >= 0, 25);

                service.journal().purge(service.node().commandStores(), node.topology()::minEpoch);

                assertEquals(SaveStatus.Erased, commandStore.loadCommand(txnId).saveStatus);
            });
        }
    }
}

