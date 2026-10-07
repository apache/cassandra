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

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.metrics.TableMetrics;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_VIEWS;
import static org.junit.Assert.assertTrue;

public class TableMetricTablesTest extends CQLTester
{
    private static final List<String> LATENCY_TABLES = List.of("local_read_latency",
                                                               "local_scan_latency",
                                                               "local_write_latency",
                                                               "coordinator_read_latency",
                                                               "coordinator_scan_latency",
                                                               "coordinator_write_latency");

    private static final List<String> LATENCY_COLUMNS = List.of("p50th_ms", "p99th_ms", "max_ms");

    @BeforeClass
    public static void setup()
    {
        addVirtualKeyspace();
    }

    @Test
    public void testLatencyIsReportedInMilliseconds()
    {
        assertLatencyTablesReport(MILLISECONDS.toMicros(5));
    }

    @Test
    public void testSubMillisecondLatencyIsNotRoundedToZero()
    {
        assertLatencyTablesReport(250);
    }

    private void assertLatencyTablesReport(long latencyMicros)
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        TableMetrics metrics = getCurrentColumnFamilyStore().metric;

        // Record straight into the timers: reads or writes on the table would record their own latencies.
        long latencyNanos = MICROSECONDS.toNanos(latencyMicros);
        metrics.readLatency.addNano(latencyNanos);
        metrics.rangeLatency.addNano(latencyNanos);
        metrics.writeLatency.addNano(latencyNanos);
        metrics.coordinatorReadLatency.update(latencyNanos, NANOSECONDS);
        metrics.coordinatorScanLatency.update(latencyNanos, NANOSECONDS);
        metrics.coordinatorWriteLatency.update(latencyNanos, NANOSECONDS);

        double expectedMs = latencyMicros / 1000.0;
        for (String table : LATENCY_TABLES)
        {
            UntypedResultSet.Row row = execute("SELECT * FROM " + VIRTUAL_VIEWS + '.' + table + " WHERE keyspace_name = ? AND table_name = ?",
                                               keyspace(), currentTable()).one();
            for (String column : LATENCY_COLUMNS)
            {
                // The timer histogram reports the upper bound of a bucket, at most 20% above the recorded value.
                double actualMs = row.getDouble(column);
                assertTrue(String.format("%s.%s: expected %s ms (up to 20%% higher), got %s ms", table, column, expectedMs, actualMs),
                           actualMs >= expectedMs && actualMs <= expectedMs * 1.2);
            }
        }
    }
}
