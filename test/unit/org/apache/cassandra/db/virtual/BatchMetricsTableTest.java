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

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
    }

    @Before
    public void config()
    {
        BatchMetricsTable table = new BatchMetricsTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
    }

    @Test
    public void testSelectAll() throws Throwable
    {
        BatchMetrics metrics = BatchStatement.metrics;

        for (int i = 0; i < 10; i++)
        {
            metrics.partitionsPerLoggedBatch.update(i);
            metrics.partitionsPerUnloggedBatch.update(i + 10);
            metrics.partitionsPerCounterBatch.update(i * 10);
        }

        ResultSet result = executeNet(format("SELECT * FROM %s.batch_metrics", KS_NAME));
        assertEquals(5, result.getColumnDefinitions().size());
        AtomicInteger rowCount = new AtomicInteger(0);
        result.forEach(r -> {
            Snapshot snapshot = getExpectedHistogram(metrics, r.getString("name")).getSnapshot();
            assertEquals(snapshot.getMedian(), r.getDouble("p50th"), 0.0);
            assertEquals(snapshot.get99thPercentile(), r.getDouble("p99th"), 0.0);
            rowCount.addAndGet(1);
        });

        assertEquals(3, rowCount.get());
    }

    private Histogram getExpectedHistogram(BatchMetrics metrics, String name)
    {
        if ("partitions_per_logged_batch".equals(name))
            return metrics.partitionsPerLoggedBatch;

        if ("partitions_per_unlogged_batch".equals(name))
            return metrics.partitionsPerUnloggedBatch;

        return metrics.partitionsPerCounterBatch;
    }
}
