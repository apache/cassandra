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
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.codahale.metrics.UniformSnapshot;
import com.google.common.collect.ImmutableList;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.SnapshottingTimer;
import org.apache.cassandra.metrics.TableMetrics;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.DEFAULT_TIMER_UNIT;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_VIEWS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TableMetricTablesTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private static final String LATENCY_TABLE = "test_latency";

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

    @Test
    public void testColumnsWithoutMsSuffixAreNotConverted()
    {
        // A real timer reports a five minute rate of 0 until its meter ticks, so use one that reports fixed values
        Timer timer = new Timer()
        {
            @Override
            public long getCount()
            {
                return 3;
            }

            @Override
            public double getFiveMinuteRate()
            {
                return 1234.5;
            }

            @Override
            public Snapshot getSnapshot()
            {
                return new UniformSnapshot(new long[]{ DEFAULT_TIMER_UNIT.convert(5, MILLISECONDS) });
            }
        };

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        UntypedResultSet.Row row = queryLatencyTable(TableMetricTables.latencyTable(KS_NAME, LATENCY_TABLE, t -> timer, DEFAULT_TIMER_UNIT));

        for (String column : LATENCY_COLUMNS)
            assertEquals(column, 5.0, row.getDouble(column), 0.0);
        assertEquals("count", 3, row.getLong("count"));
        assertEquals("per_second", 1234.5, row.getDouble("per_second"), 0.0);
    }

    @Test
    public void testLatencyIsReportedInMillisecondsForAnyTimerUnit()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        for (TimeUnit timerUnit : List.of(MILLISECONDS, MICROSECONDS, NANOSECONDS))
        {
            // Created the way Metrics.timer() would create it if timerUnit were the DEFAULT_TIMER_UNIT
            SnapshottingTimer timer = new SnapshottingTimer(CassandraMetricsRegistry.createReservoir(timerUnit));
            timer.update(5, MILLISECONDS);
            UntypedResultSet.Row row = queryLatencyTable(TableMetricTables.latencyTable(KS_NAME, LATENCY_TABLE, t -> timer, timerUnit));
            assertLatencyColumns(LATENCY_TABLE + '[' + timerUnit + ']', row, 5.0);
        }
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
            assertLatencyColumns(table, row, expectedMs);
        }
    }

    private static void assertLatencyColumns(String table, UntypedResultSet.Row row, double expectedMs)
    {
        for (String column : LATENCY_COLUMNS)
        {
            // The timer histogram reports the upper bound of a bucket, at most 20% above the recorded value.
            double actualMs = row.getDouble(column);
            assertTrue(String.format("%s.%s: expected %s ms (up to 20%% higher), got %s ms", table, column, expectedMs, actualMs),
                       actualMs >= expectedMs && actualMs <= expectedMs * 1.2);
        }
    }

    private UntypedResultSet.Row queryLatencyTable(VirtualTable table)
    {
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
        // Every table gets a row, all reporting the given metric
        return execute("SELECT * FROM " + KS_NAME + '.' + LATENCY_TABLE + " WHERE keyspace_name = ? AND table_name = ?",
                       keyspace(), currentTable()).one();
    }
}
