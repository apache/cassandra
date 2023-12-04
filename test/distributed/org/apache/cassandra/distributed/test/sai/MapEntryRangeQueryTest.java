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

package org.apache.cassandra.distributed.test.sai;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.cql3.CQLTester.row;
import static org.apache.cassandra.cql3.CQLTester.getRandom;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

public class MapEntryRangeQueryTest extends TestBaseImpl
{
    @Rule
    public SAITester.FailureWatcher failureRule = new SAITester.FailureWatcher();

    private static final String CREATE_KEYSPACE = "CREATE KEYSPACE %%s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d}";
    private static final String CREATE_TABLE = "CREATE TABLE %%s (pk int PRIMARY KEY, inventory map<text,int>)";
    private static final String CREATE_INDEX = "CREATE CUSTOM INDEX ON %%s(entries(%s)) USING 'StorageAttachedIndex'";
    private static final int NUM_REPLICAS = 3;
    private static final int RF = 2;

    private static final AtomicInteger seq = new AtomicInteger();
    private static String table;

    private static Cluster cluster;

    private static int dimensionCount;

    @BeforeClass
    public static void setupCluster() throws Exception
    {
        cluster = Cluster.build(NUM_REPLICAS)
                         .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                         .start();

        cluster.schemaChange(withKeyspace(String.format(CREATE_KEYSPACE, RF)));
    }

    @AfterClass
    public static void closeCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Before
    public void before()
    {
        table = "table_" + seq.getAndIncrement();
        dimensionCount = getRandom().nextIntBetween(100, 2048);
    }

    @After
    public void after()
    {
        cluster.schemaChange(formatQuery("DROP TABLE IF EXISTS %s"));
    }

    @Test
    public void testRangeQueryOnMapEntries()
    {
        cluster.schemaChange(formatQuery(String.format(CREATE_TABLE, dimensionCount)));
        cluster.schemaChange(formatQuery(String.format(CREATE_INDEX, "inventory")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);

        int entryCount = getRandom().nextIntBetween(500, 1000);
        List<Pair<Integer, Integer>> values = IntStream.range(0, entryCount)
                                                       .mapToObj(i -> Pair.create(i, getRandom().nextIntBetween(0, 100)))
                                                       .collect(Collectors.toList());

        for (var value : values)
            execute(String.format("INSERT INTO %s (pk, inventory) VALUES (%s, {'apple':%s})",
                KEYSPACE + '.' + table, value.left, value.right));

        // Test each kind or range query
        assertThat(execute("SELECT pk FROM %s WHERE inventory['apple'] >= 50"))
        .hasSameElementsAs(getExpectedFilteredResults(values, i -> i >= 50));

        assertThat(execute("SELECT pk FROM %s WHERE inventory['apple'] > 25"))
        .hasSameElementsAs(getExpectedFilteredResults(values, i -> i > 25));

        assertThat(execute("SELECT pk FROM %s WHERE inventory['apple'] <= 30"))
        .hasSameElementsAs(getExpectedFilteredResults(values, i -> i <= 30));

        assertThat(execute("SELECT pk FROM %s WHERE inventory['apple'] < 56"))
        .hasSameElementsAs(getExpectedFilteredResults(values, i -> i < 56));
    }

    private List<Object[]> getExpectedFilteredResults(List<Pair<Integer, Integer>> values, Predicate<Integer> filter)
    {
        return values.stream()
                     .filter(p -> filter.test(p.right))
                     .map(p -> row(p.left))
                     .collect(Collectors.toList());
    }

    private static Object[][] execute(String query)
    {
        return execute(query, ConsistencyLevel.QUORUM);
    }

    private static Object[][] execute(String query, ConsistencyLevel consistencyLevel)
    {
        return cluster.coordinator(1).execute(formatQuery(query), consistencyLevel);
    }

    private static String formatQuery(String query)
    {
        return String.format(query, KEYSPACE + '.' + table);
    }
}
