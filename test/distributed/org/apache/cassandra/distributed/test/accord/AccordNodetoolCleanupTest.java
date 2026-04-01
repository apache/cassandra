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

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.service.StorageService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
                                                           .with(Feature.NETWORK, Feature.GOSSIP)), 2);
    }

    @Test
    public void accordNodetoolCleanupTest() throws Throwable
    {
        List<String> ddls = Arrays.asList("DROP KEYSPACE IF EXISTS " + KEYSPACE + ';',
                                          "CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 1}",
                                          "CREATE TABLE " + qualifiedAccordTableName + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");
        test(ddls, cluster -> {

            String tableName = accordTableName;

            cluster.coordinator(1).execute(wrapInTxn("INSERT INTO " + qualifiedAccordTableName + " (k, v) VALUES (?, ?)"), ConsistencyLevel.SERIAL, 1, 2);

            SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT token(k) FROM " + qualifiedAccordTableName + " WHERE k = 1 LIMIT 1", ConsistencyLevel.SERIAL);

            cluster.get(1).flush(withKeyspace("%s"));

            String originalToken = cluster.get(1).callOnInstance(() -> getOnlyElement(StorageService.instance.getTokens()));

            long token = (Long) result.toObjectArrays()[0][0];

            assertTrue(token < Long.parseLong(originalToken));

            assertEquals(1, (int) cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).getLiveSSTables().size()));

            cluster.get(1).runOnInstance(() -> {
                StorageService.instance.move(Long.toString(token - 1000));
            });

            // Wait until Accord retires range
            try
            {
                Thread.sleep(20000);
            }
            catch (InterruptedException e)
            {
                fail();
            }

            cluster.get(1).nodetool("cleanup", KEYSPACE, accordTableName);

            assertEquals(0, (int) cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).getLiveSSTables().size()));
        });
    }

    @Test
    public void accordNodetoolCleanupRangeInUseTest() throws Throwable
    {
        List<String> ddls = Arrays.asList("DROP KEYSPACE IF EXISTS " + KEYSPACE + ';',
                                          "CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 1}",
                                          "CREATE TABLE " + qualifiedAccordTableName + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");
        test(ddls, cluster -> {

            String tableName = accordTableName;

            cluster.coordinator(1).execute(wrapInTxn("INSERT INTO " + qualifiedAccordTableName + " (k, v) VALUES (?, ?)"), ConsistencyLevel.SERIAL, 1, 2);

            SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT token(k) FROM " + qualifiedAccordTableName + " WHERE k = 1 LIMIT 1", ConsistencyLevel.SERIAL);

            cluster.get(1).flush(withKeyspace("%s"));

            String originalToken = cluster.get(1).callOnInstance(() -> getOnlyElement(StorageService.instance.getTokens()));

            long token = (Long) result.toObjectArrays()[0][0];

            assertTrue(token < Long.parseLong(originalToken));

            assertEquals(1, (int) cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).getLiveSSTables().size()));

            cluster.get(1).runOnInstance(() -> StorageService.instance.move(Long.toString(token - 1000)));

            String accordTableName = qualifiedAccordTableName;

            cluster.get(1).nodetool("cleanup", KEYSPACE, accordTableName);

            assertEquals(1, (int) cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).getLiveSSTables().size()));
        });
    }

    @Test
    public void nodetoolCleanupForNonAccordTableTest() throws Throwable
    {
        List<String> ddls = Arrays.asList("DROP KEYSPACE IF EXISTS " + KEYSPACE + ';',
                                          "CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 1}",
                                          "CREATE TABLE " + qualifiedRegularTableName + " (k int PRIMARY KEY, v int)");
        test(ddls, cluster -> {
            String tableName = regularTableName;

            cluster.coordinator(1).execute("INSERT INTO " + qualifiedRegularTableName + " (k, v) VALUES (?, ?)", ConsistencyLevel.ALL, 1, 2);

            SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT token(k) FROM " + qualifiedRegularTableName + " WHERE k = 1 LIMIT 1", ConsistencyLevel.SERIAL);

            cluster.get(1).flush(withKeyspace("%s"));

            String originalToken = cluster.get(1).callOnInstance(() -> getOnlyElement(StorageService.instance.getTokens()));

            long token = (Long) result.toObjectArrays()[0][0];

            assertTrue(token < Long.parseLong(originalToken));

            assertEquals(1, (int) cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).getLiveSSTables().size()));

            cluster.get(1).runOnInstance(() -> {
                StorageService.instance.move(Long.toString(token - 1000));
            });

            cluster.get(1).nodetool("cleanup", KEYSPACE, regularTableName);

            assertEquals(0, (int) cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).getLiveSSTables().size()));
        });
    }
}

