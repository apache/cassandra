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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import accord.api.RoutingKey;
import accord.local.CommandStore;
import accord.local.PreLoadContext;
import accord.primitives.AbstractRanges;
import accord.primitives.Ranges;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.api.TokenKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.service.accord.AccordService.getBlocking;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;

public class AccordNodetoolCleanupTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordNodetoolCleanupTest.class);

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupCluster(builder -> builder
                                               .withoutVNodes()
                                               .withConfig(config ->
                                                           config
                                                           .set("accord.shard_durability_target_splits", "1")
                                                           .set("accord.shard_durability_cycle", "20s")
                                                           .with(Feature.NETWORK, Feature.GOSSIP)), 6);
    }

    @Test
    public void accordNodetoolCleanupTest() throws Throwable
    {
        List<String> ddls = Arrays.asList("DROP KEYSPACE IF EXISTS " + KEYSPACE + ';',
                                          "CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}",
                                          "CREATE TABLE " + qualifiedAccordTableName + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");
        test(ddls, cluster -> {
            String tableName = accordTableName;

            cluster.coordinator(2).execute(wrapInTxn("INSERT INTO " + qualifiedAccordTableName + " (k, v) VALUES (?, ?)"), ConsistencyLevel.SERIAL, 1, 2);

            SimpleQueryResult result = cluster.coordinator(2).executeWithResult("SELECT token(k) FROM " + qualifiedAccordTableName + " WHERE k = 1 LIMIT 1", ConsistencyLevel.SERIAL);

            cluster.get(2).flush(withKeyspace("%s"));

            assertEquals(1, (int) cluster.get(2).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).getLiveSSTables().size()));

            String originalToken = cluster.get(2).callOnInstance(() -> getOnlyElement(StorageService.instance.getTokens()));

            long token = (Long) result.toObjectArrays()[0][0];

            assert(token < Long.parseLong(originalToken));

            cluster.get(2).runOnInstance(() -> {
                TableId tid = Schema.instance.getTableMetadata(KEYSPACE, tableName).id();
                RoutingKey key = TokenKey.parse(tid, String.valueOf(token), Murmur3Partitioner.instance);

                boolean tokenInCommandStore = false;
                for (CommandStore commandStore : AccordService.instance().node().commandStores().all())
                {
                    Ranges commandStoreRange = getBlocking(commandStore.submit((PreLoadContext.Empty) () -> "Get ranges", safeCommandStore -> {
                        return safeCommandStore.ranges().all();
                    }));

                    if (commandStoreRange.intersects(key))
                        tokenInCommandStore = true;
                }

                assertTrue(tokenInCommandStore);
            });

            cluster.get(2).runOnInstance(() -> {
                StorageService.instance.move(Long.toString(token - 1000));
            });

            cluster.get(2).nodetool("cleanup", KEYSPACE, accordTableName);

            assertEquals(1, (int) cluster.get(2).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).getLiveSSTables().size()));

        });
    }
}
