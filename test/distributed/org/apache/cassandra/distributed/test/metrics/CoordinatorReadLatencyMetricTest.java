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

package org.apache.cassandra.distributed.test.metrics;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.Txn;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.metrics.ClientRequestsMetricsHolder;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.consensus.migration.TransactionalMigrationFromMode;
import org.apache.cassandra.service.paxos.Paxos;

import static org.apache.cassandra.cql3.ast.Conditional.Where.Inequality.LESS_THAN;
import static org.junit.Assert.assertTrue;

public class CoordinatorReadLatencyMetricTest extends TestBaseImpl
{
    @Test
    public void singleRowTest() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));
            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (pk, ck ,v) values (0, ?, 1)"), ConsistencyLevel.ALL, i);

            var select = Select.builder()
                               //TODO (now, correctness, coverage): count(v) breaks accord as we get mutliple rows rather than the count of rows...
//                               .withSelection(FunctionCall.count("v"))
                               .table(KEYSPACE, "tbl")
                               .value("pk", 0)
                               .where("ck", LESS_THAN, 42)
                               .limit(1)
                               .build();

            verifyTableLatency(cluster, 1, () -> verifyLatencyMetrics(cluster, select.toCQL(), ConsistencyLevel.QUORUM, select.binds()));
            cluster.get(1).runOnInstance(() -> Paxos.setPaxosVariant(Config.PaxosVariant.v1));
            verifyTableLatency(cluster, 1, () -> verifyLatencyMetrics(cluster, select.toCQL(), ConsistencyLevel.SERIAL, select.binds()));
            cluster.get(1).runOnInstance(() -> Paxos.setPaxosVariant(Config.PaxosVariant.v2));
            verifyTableLatency(cluster, 1, () -> verifyLatencyMetrics(cluster, select.toCQL(), ConsistencyLevel.SERIAL, select.binds()));

            cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl WITH " + TransactionalMode.full.asCqlParam() + " AND " + TransactionalMigrationFromMode.none.asCqlParam()));
            var txn = Txn.wrap(select);
            verifyTableLatency(cluster, 1, () -> verifyLatencyMetrics(cluster, txn.toCQL(), ConsistencyLevel.QUORUM, txn.binds()));

            var let = Txn.builder()
                         .addLet("a", select)
                         .addReturnReferences("a.v")
                         .build();
            verifyTableLatency(cluster, 1, () -> verifyLatencyMetrics(cluster, let.toCQL(), ConsistencyLevel.QUORUM, let.binds()));
        }
    }

    @Test
    public void internalPagingWithAggregateTest() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));
            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (pk, ck ,v) values (0, ?, 1)"), ConsistencyLevel.ALL, i);

            // Serial and non-serial reads have separates code paths, so exercise them both
            testAggregationQuery(cluster, ConsistencyLevel.ALL);
            cluster.get(1).runOnInstance(() -> Paxos.setPaxosVariant(Config.PaxosVariant.v1));
            testAggregationQuery(cluster, ConsistencyLevel.SERIAL);
            cluster.get(1).runOnInstance(() -> Paxos.setPaxosVariant(Config.PaxosVariant.v2));
            testAggregationQuery(cluster, ConsistencyLevel.SERIAL);
        }
    }

    private void testAggregationQuery(Cluster cluster, ConsistencyLevel cl)
    {
        for (int sliceSize : new int[]{1, 100})
        {
            // This statement utilises an AggregationQueryPager, which breaks the slice being read into a
            // number of subslices and performs a single read for each of them. The number of subpages is
            // dictated by the pagesize, so for testing purposes we keep it to 1 which ensures that the number
            // of subpages is equal to overall slice size.
            String query = withKeyspace("SELECT count(v) from %s.tbl WHERE pk=0 and ck < " + sliceSize);
            verifyLatencyMetricsWhenPaging(cluster, 1, sliceSize, query, cl);
        }
    }

    @Test
    public void multiplePartitionKeyInClauseTest() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, v int, PRIMARY KEY (pk))"));
            for (int i = 0; i < 100; i++)
                    cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (pk, v) values (?, 1)"), ConsistencyLevel.ALL, i);

            for (int partitionKeys : new int[] {1, 100})
            {
                // This statement translates to a single partition read for each value in the IN clause
                // Latency metrics should be uniquely and independently recorded for each of these reads
                // i.e. the timing of the read n does not include that of (n-1, n-2, n-3...)
                String pkList = IntStream.range(0, partitionKeys)
                                         .mapToObj(Integer::toString)
                                         .collect(Collectors.joining(",", "(", ")"));
                String query = withKeyspace("SELECT pk, v FROM %s.tbl WHERE pk IN " + pkList);
                // We only keep executing the single partition reads until we have enough results to fill a page, so
                // keep pagesize >= the number of partition keys in the IN clause to ensure that we read them all
                verifyLatencyMetricsWhenPaging(cluster, 100, partitionKeys, query, ConsistencyLevel.ALL);
            }
        }
    }

    private static void verifyLatencyMetricsWhenPaging(Cluster cluster,
                                                       int pagesize,
                                                       int expectedQueries,
                                                       String query,
                                                       ConsistencyLevel consistencyLevel)
    {
        verifyLatencyMetrics(cluster, expectedQueries, () -> cluster.coordinator(1).executeWithPaging(query, consistencyLevel, pagesize));
    }

    private static void verifyLatencyMetrics(Cluster cluster, String query, ConsistencyLevel consistencyLevel, Object[] bindings)
    {
        verifyLatencyMetrics(cluster, 1, () -> cluster.coordinator(1).execute(query, consistencyLevel, bindings));
    }

    private static void verifyLatencyMetrics(Cluster cluster, int expectedQueries, Runnable query)
    {
        long countBefore = cluster.get(1).callOnInstance(() -> ClientRequestsMetricsHolder.readMetrics.latency.getCount());
        long totalLatencyBefore = cluster.get(1).callOnInstance(() -> ClientRequestsMetricsHolder.readMetrics.totalLatency.getCount());
        long startTime = System.nanoTime();
        query.run();
        long elapsedTime = System.nanoTime() - startTime;
        long countAfter = cluster.get(1).callOnInstance(() -> ClientRequestsMetricsHolder.readMetrics.latency.getCount());
        long totalLatencyAfter = cluster.get(1).callOnInstance(() -> ClientRequestsMetricsHolder.readMetrics.totalLatency.getCount());

        long latenciesRecorded = countAfter - countBefore;
        assertTrue("Expected to have recorded at least 1 latency measurement per-individual read", latenciesRecorded >= expectedQueries);

        long totalLatencyRecorded = TimeUnit.MICROSECONDS.toNanos(totalLatencyAfter - totalLatencyBefore);
        assertTrue(String.format("Total latency delta %s should not exceed wall clock time elapsed %s", totalLatencyRecorded, elapsedTime),
                   totalLatencyRecorded <= elapsedTime);
    }

    private static void verifyTableLatency(Cluster cluster, int expectedQueries, Runnable query)
    {
        IInvokableInstance inst = cluster.get(1);
        LongSupplier tableMetric = () -> inst.callOnInstance(() -> Keyspace.open("distributed_test_keyspace").getColumnFamilyStore("tbl").getMetrics().readLatency.latency.getCount());

        long tableBefore = tableMetric.getAsLong();
        query.run();
        long tableAfter = tableMetric.getAsLong();

        Assertions.assertThat(tableAfter - tableBefore).isEqualTo(expectedQueries);
    }

}
