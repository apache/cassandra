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
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.primitives.Ranges;

import org.apache.cassandra.Util;
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

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.cassandra.service.accord.AccordService.getBlocking;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AccordNodetoolCleanupTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordNodetoolCleanupTest.class);

    protected String originalToken;

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
                                               .appendConfig(config -> config
                                                                       .set("accord.shard_durability_cycle", "20s")
                                                                       .with(Feature.GOSSIP, Feature.NETWORK)), 2);
        SHARED_CLUSTER.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE + ';');
        SHARED_CLUSTER.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 1}");
    }

    @Before
    public void getOriginalToken()
    {
         originalToken = SHARED_CLUSTER.get(1).callOnInstance(() -> getOnlyElement(StorageService.instance.getTokens()));
    }

    @After
    public void reset()
    {
        String token = originalToken;
        SHARED_CLUSTER.get(1).runOnInstance(() -> {
            StorageService.instance.move(token);
        });
    }

    @Test
    public void accordNodetoolCleanupTest() throws Throwable
    {
        String tableName = "tbl0";
        String qualifiedTableName = KEYSPACE + '.' + tableName;

        test("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'", cluster -> {
            cluster.coordinator(1).execute(wrapInTxn("INSERT INTO " + qualifiedTableName + " (k, v) VALUES (?, ?)"), ConsistencyLevel.SERIAL, 1, 2);
            SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT token(k) FROM " + qualifiedTableName + " WHERE k = 1 LIMIT 1", ConsistencyLevel.SERIAL);

            cluster.get(1).flush(withKeyspace("%s"));

            long token = (Long) result.toObjectArrays()[0][0];

            assertTrue(token < Long.parseLong(originalToken));

            assertEquals(1, (int) cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).getLiveSSTables().size()));

            // Cluster 1 no longer owns token
            cluster.get(1).runOnInstance(() -> {
                AccordService.instance().node().durability().shards().start();
                StorageService.instance.move(Long.toString(token - 1000));
            });

            // Wait until Accord retires range, so it no longer has ownership of token
            cluster.get(1).runOnInstance(() -> {
                TableId tid = Schema.instance.getTableMetadata(KEYSPACE, tableName).id();
                RoutingKey key = TokenKey.parse(tid, String.valueOf(token), Murmur3Partitioner.instance);

                Util.spinUntilTrue(() -> {
                    boolean doesNotContainsToken = true;
                    List<Ranges> inUseRanges = getBlocking(AccordService.instance().node().commandStores().getInUseRangesAndMarkRetiredRangesUnsafeToRead());
                    for (Ranges ranges : inUseRanges)
                    {
                        if (ranges.intersects(key))
                            doesNotContainsToken = false;
                    }
                    return doesNotContainsToken;
                }, 30);
            });

            NodeToolResult nodetoolResult = cluster.get(1).nodetoolResult("cleanup", KEYSPACE, tableName);

            assertEquals(0, nodetoolResult.getRc());
            assertEquals(0, (int) cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).getLiveSSTables().size()));

            // Ensure data is cleaned up
            assertEquals(0, cluster.get(1).executeInternal("SELECT k FROM " + qualifiedTableName + " WHERE k = 1 LIMIT 1").length);
        });
    }

    @Test
    public void accordNodetoolCleanupPartialSSTableTest() throws Throwable
    {
        String tableName = "tbl1";
        String qualifiedTableName = KEYSPACE + '.' + tableName;

        test("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'", cluster -> {
            cluster.coordinator(1).execute(wrapInTxn("INSERT INTO " + qualifiedTableName + " (k, v) VALUES (?, ?)"), ConsistencyLevel.SERIAL, 1, 2);
            cluster.coordinator(1).execute(wrapInTxn("INSERT INTO " + qualifiedTableName + " (k, v) VALUES (?, ?)"), ConsistencyLevel.SERIAL, 2, 2);

            SimpleQueryResult result1 = cluster.coordinator(1).executeWithResult("SELECT token(k) FROM " + qualifiedTableName + " WHERE k = 2 LIMIT 1", ConsistencyLevel.SERIAL);
            SimpleQueryResult result2 = cluster.coordinator(1).executeWithResult("SELECT token(k) FROM " + qualifiedTableName + " WHERE k = 1 LIMIT 1", ConsistencyLevel.SERIAL);

            cluster.get(1).flush(withKeyspace("%s"));

            long token1 = (Long) result1.toObjectArrays()[0][0];
            long token2 = (Long) result2.toObjectArrays()[0][0];

            assertTrue((token2 < (token1 - 1000)) && token1 < Long.parseLong(originalToken));

            assertEquals(1, (int) cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).getLiveSSTables().size()));

            // Cluster 1 now only owns token2, but Accord still requires token1
            cluster.get(1).runOnInstance(() -> {
                AccordService.instance().node().durability().shards().stop();
                StorageService.instance.move(Long.toString(token1 - 1000));
            });

            NodeToolResult nodetoolResult = cluster.get(1).nodetoolResult("cleanup", KEYSPACE, tableName);

            assertTrue(nodetoolResult.getStdout().contains("Some SSTables in keyspace " + KEYSPACE + " are still being used by Accord and were not cleaned up, check server logs for more information."));
            assertEquals(2, nodetoolResult.getRc());
            assertEquals(1, (int) cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).getLiveSSTables().size()));

            // Ensure data is still there
            assertEquals(1, cluster.get(1).executeInternal("SELECT k FROM " + qualifiedTableName + " WHERE k = 1 LIMIT 1").length);
            assertEquals(1, cluster.get(1).executeInternal("SELECT k FROM " + qualifiedTableName + " WHERE k = 2 LIMIT 1").length);
        });

    }

    @Test
    public void accordNodetoolCleanupRangeInUseTest() throws Throwable
    {
        String tableName = "tbl2";
        String qualifiedTableName = KEYSPACE + '.' + tableName;

        test("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'", cluster -> {
            cluster.coordinator(1).execute(wrapInTxn("INSERT INTO " + qualifiedTableName + " (k, v) VALUES (?, ?)"), ConsistencyLevel.SERIAL, 1, 2);

            SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT token(k) FROM " + qualifiedTableName + " WHERE k = 1 LIMIT 1", ConsistencyLevel.SERIAL);

            cluster.get(1).flush(withKeyspace("%s"));

            long token = (Long) result.toObjectArrays()[0][0];

            assertTrue(token < Long.parseLong(originalToken));

            assertEquals(1, (int) cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).getLiveSSTables().size()));

            cluster.get(1).runOnInstance(() -> {
                AccordService.instance().node().durability().shards().stop();
                StorageService.instance.move(Long.toString(token - 1000));
            });

            NodeToolResult nodetoolResult = cluster.get(1).nodetoolResult("cleanup", KEYSPACE, tableName);

            assertTrue(nodetoolResult.getStdout().contains("Some SSTables in keyspace " + KEYSPACE + " are still being used by Accord and were not cleaned up, check server logs for more information."));
            assertEquals(2, nodetoolResult.getRc());
            assertEquals(1, (int) cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).getLiveSSTables().size()));

            // Ensure data is still there
            assertEquals(1, cluster.get(1).executeInternal("SELECT k FROM " + qualifiedTableName + " WHERE k = 1 LIMIT 1").length);
        });
    }

    @Test
    public void nodetoolCleanupForNonAccordTableTest() throws Throwable
    {
        String tableName = "tbl3";
        String qualifiedTableName = KEYSPACE + '.' + tableName;

        test("CREATE TABLE " + qualifiedTableName + " (k int PRIMARY KEY, v int)", cluster -> {
            cluster.coordinator(1).execute("INSERT INTO " + qualifiedTableName + " (k, v) VALUES (?, ?)", ConsistencyLevel.ALL, 1, 2);

            SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT token(k) FROM " + qualifiedTableName + " WHERE k = 1 LIMIT 1", ConsistencyLevel.SERIAL);

            cluster.get(1).flush(withKeyspace("%s"));

            long token = (Long) result.toObjectArrays()[0][0];

            assertTrue(token < Long.parseLong(originalToken));

            assertEquals(1, (int) cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).getLiveSSTables().size()));

            cluster.get(1).runOnInstance(() -> {
                AccordService.instance().node().durability().shards().stop();
                StorageService.instance.move(Long.toString(token - 1000));
            });

            NodeToolResult nodetoolResult = cluster.get(1).nodetoolResult("cleanup", KEYSPACE, tableName);

            assertEquals(0, nodetoolResult.getRc());
            assertEquals(0, (int) cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).getLiveSSTables().size()));

            // Ensure data is cleaned up
            assertEquals(0, cluster.get(1).executeInternal("SELECT k FROM " + qualifiedTableName + " WHERE k = 1 LIMIT 1").length);
        });
    }
}

