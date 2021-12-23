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

package org.apache.cassandra.db.virtual;

import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.metrics.BatchMetrics;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class BatchMetricsTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private BatchMetricsTable table;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
    }

    @Before
    public void config()
    {
        table = new BatchMetricsTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
    }

    @Test
    void testSelectAll() throws Throwable
    {
        String ks = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        String tbl = createTable(ks, "CREATE TABLE %s (id int primary key, val int)");

        String batch = format("BEGIN BATCH " +
                              "INSERT INTO %s.%s (id, val) VALUES (0, 0) USING TTL %d; " +
                              "INSERT INTO %s.%s (id, val) VALUES (1, 1) USING TTL %d; " +
                              "APPLY BATCH;",
                              ks, tbl, 1,
                              ks, tbl, 1);
        execute(batch);

        BatchMetrics metrics = BatchStatement.metrics;
        ResultSet result = executeNet("SELECT * FROM vts.batch_metrics");
        assertEquals(5, result.getColumnDefinitions().size());
        AtomicInteger rowCount = new AtomicInteger(0);
        result.forEach(r -> {
            Snapshot snapshot;
            if(r.getString("name").equals("partitions_per_logged_batch")) {
                snapshot = metrics.partitionsPerLoggedBatch.getSnapshot();
            } else if(r.getString("name").equals("partitions_per_unlogged_batch")) {
                snapshot = metrics.partitionsPerUnloggedBatch.getSnapshot();
            } else {
                snapshot = metrics.partitionsPerCounterBatch.getSnapshot();
            }
            assertEquals(r.getDouble("p50"), snapshot.getMedian(), 1.0);
            assertEquals(r.getDouble("p99"), snapshot.get99thPercentile(), 1.0);
            rowCount.addAndGet(1);
        });

        assertEquals(3, rowCount.get());
    }
}
