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

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;

public class AccordSingleTokenBatchTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordSingleTokenBatchTest.class);

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
    public void accordSinglePartitionKeyBatchTest() throws Throwable
    {
        List<String> ddls = Arrays.asList("DROP KEYSPACE IF EXISTS " + KEYSPACE + ';',
                                          "CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}",
                                          "CREATE TABLE " + qualifiedAccordTableName + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'",
                                          "CREATE TABLE " + qualifiedRegularTableName + " (k int PRIMARY KEY, v int)");
        test(ddls, cluster -> {
            cluster.coordinator(1).execute("BEGIN BATCH\n" +
                                           "INSERT INTO " + qualifiedAccordTableName + " (k, v) VALUES (1, 2);\n" +
                                           "INSERT INTO " + qualifiedRegularTableName + " (k, v) VALUES (1, 3);\n" +
                                           "APPLY BATCH;", ConsistencyLevel.ONE);

            String tableName = accordTableName;

            SimpleQueryResult r1 = cluster.coordinator(1).executeWithResult("SELECT * FROM " + qualifiedAccordTableName + " WHERE k = 1", ConsistencyLevel.ONE);
            SimpleQueryResult r2 = cluster.coordinator(1).executeWithResult("SELECT * FROM " + qualifiedRegularTableName + " WHERE k = 1", ConsistencyLevel.ONE);

            assert(r1.toObjectArrays().length == 1);
            assert(r2.toObjectArrays().length == 1);
            // Assert the key has the value

            cluster.get(1).runOnInstance(() -> {
                TableId tid = Schema.instance.getTableMetadata(KEYSPACE, tableName).id();
            });

            // Chore: Add an assert somewhere to ensure that writes
        });
    }

    /*@Test
    public void accordSinglePartitionKeyBatchWithConditionalTest() throws Throwable
    {
        List<String> ddls = Arrays.asList("DROP KEYSPACE IF EXISTS " + KEYSPACE + ';',
                                          "CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}",
                                          "CREATE TABLE " + qualifiedAccordTableName + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'",
                                          "CREATE TABLE " + qualifiedRegularTableName + " (k int PRIMARY KEY, v int)");
        test(ddls, cluster -> {

            cluster.coordinator(2).execute("INSERT INTO " + qualifiedRegularTableName + "(k, v) VALUES", ConsistencyLevel.TWO, 1, 2);
            cluster.coordinator(1).execute("BEGIN BATCH\n" +
                                           "INSERT INTO " + qualifiedAccordTableName + " (k, v) VALUES (1, 2);\n" +
                                           "INSERT INTO " + qualifiedRegularTableName + " (k, v) VALUES (1, 3) IF NOT EXISTS;\n" +
                                           "APPLY BATCH;", ConsistencyLevel.ONE, 1, 2);

            String tableName = regularTableName;

            cluster.get(1).runOnInstance(() -> {
                TableId tid = Schema.instance.getTableMetadata(KEYSPACE, tableName).id();
                System.out.println("here");
            });

            // Chore: Add an assert somewhere to ensure that writes
        });*/

        // Add a test for a migration, and ensure that we hit the same consistency level?
    // Add test that shows this path only goes through for single partition batches?
    // }
}
