/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.monitoring.MonitoringTask;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.utils.Throwables;
import org.assertj.core.api.AbstractIterableAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;

import static java.util.regex.Pattern.quote;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.utils.MonotonicClock.approxTime;

public class SlowQueryLoggerTest extends TestBaseImpl
{
    private static final int SLOW_QUERY_LOG_TIMEOUT_MS = 100;
    private static final AtomicInteger SEQ = new AtomicInteger();

    private static Cluster cluster;
    private static String table;
    private static ICoordinator coordinator;
    private static IInvokableInstance node;

    @BeforeClass
    public static void setupCluster() throws Exception
    {
        // effectively disable the scheduled monitoring task so we control it manually for better test stability
        CassandraRelevantProperties.MONITORING_REPORT_INTERVAL_MS.setInt((int) TimeUnit.HOURS.toMillis(1));

        cluster = init(Cluster.build(2)
                              .withInstanceInitializer(SlowQueryLoggerTest.BBHelper::install)
                              .withConfig(config -> {
                                  config.set("slow_query_log_timeout_in_ms", SLOW_QUERY_LOG_TIMEOUT_MS);
                                  config.set("read_request_timeout_in_ms", 60000L);
                                  config.set("range_request_timeout_in_ms", 60000L);
                              })
                              .start());
        coordinator = cluster.coordinator(1);
        node = cluster.get(2);
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
        CassandraRelevantProperties.MONITORING_EXECUTION_INFO_ENABLED.setBoolean(true);
        table = "t_" + SEQ.getAndIncrement();

        // trigger the monitoring task to flush any pending slow operations before the test starts
        node.runOnInstance(() -> MonitoringTask.instance.logOperations(approxTime.now()));
    }

    @After
    public void after()
    {
        cluster.schemaChange(format("DROP TABLE IF EXISTS %s.%s"));
    }

    /**
     * Test that the slow query logger does not log sensitive data.
     */
    @Test
    public void testDoesNotLogSensitiveData()
    {
        cluster.schemaChange(format("CREATE TABLE %s.%s (k text, c text, v text, b blob, PRIMARY KEY (k, c))"));
        coordinator.execute(format("INSERT INTO %s.%s (k, c, v) VALUES ('secret_k', 'secret_c', 'secret_v')"), ALL);

        // verify that slow queries are logged with redacted values
        long mark = node.logs().mark();
        String query = format("SELECT * FROM %s.%s WHERE k = 'secret_k' AND c = 'secret_c' AND v = 'secret_v' ALLOW FILTERING");
        Object[][] rows = coordinator.execute(query, ALL);
        Assertions.assertThat(rows).hasNumberOfRows(1);
        assertLogsContain(mark, node, "operations were slow", format("<SELECT \\* FROM %s\\.%s WHERE k = \\? AND c = \\? AND v = \\? ALLOW FILTERING>"));
        assertLogsDoNotContain(mark, node, "secret_k", "secret_c", "secret_v");

        // verify again with paging
        mark = node.logs().mark();
        Iterator<Object[]> pagedRows = coordinator.executeWithPaging(query, ALL, 1);
        while (pagedRows.hasNext())
            pagedRows.next();
        assertLogsContain(mark, node, "operations were slow", quote(format("<SELECT * FROM %s.%s WHERE k = ? AND c = ? AND v = ? LIMIT 1 ALLOW FILTERING>")));
        assertLogsContain(mark, node, "operations were slow", quote(format("<SELECT * FROM %s.%s WHERE k = ? AND v = ? LIMIT 1 ALLOW FILTERING [paging continuation]>")));
        assertLogsDoNotContain(mark, node, "secret_k", "secret_c", "secret_v");

        // verify that large values include size hints
        mark = node.logs().mark();
        query = format("SELECT * FROM %s.%s WHERE b = ? ALLOW FILTERING");
        coordinator.execute(query, ALL, ByteBuffer.allocate(100 + 1));
        coordinator.execute(query, ALL, ByteBuffer.allocate(1024 + 1));
        coordinator.execute(query, ALL, ByteBuffer.allocate(10 * 1024 + 1));
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s WHERE b = \\?\\[>100B\\] ALLOW FILTERING>"),
                          format("<SELECT \\* FROM %s\\.%s WHERE b = \\?\\[>1KiB\\] ALLOW FILTERING>"),
                          format("<SELECT \\* FROM %s\\.%s WHERE b = \\?\\[>10KiB\\] ALLOW FILTERING>"));

        coordinator.execute(format("SELECT * FROM %s.%s"), ALL);
    }

    /**
     * Test that the slow query logger outputs the correct metrics for number of returned partitions, rows, cells, etc.
     */
    @Test
    public void testLogsReadMetrics()
    {
        cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, l int, s int, PRIMARY KEY (k, c))"));
        cluster.schemaChange(format("CREATE INDEX legacy_idx ON %s.%s (l)"));
        cluster.schemaChange(format("CREATE CUSTOM INDEX sai_idx ON %s.%s (s) USING 'StorageAttachedIndex'"));
        int numPartitions = 10;
        int numClusterings = 10;
        int numRows = 0;
        for (int k = 0; k < numPartitions; k++)
            for (int c = 0; c < numClusterings; c++)
                coordinator.execute(format("INSERT INTO %s.%s (k, c, v, l, s) VALUES (?, ?, ?, ?, ?)"),
                                    ALL, k, c, numRows++, numRows, numRows);

        // unrestricted query
        long mark = node.logs().mark();
        Object[][] rows = coordinator.execute(format("SELECT * FROM %s.%s"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numRows);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 10/10/0",
                          "    rows: 100/100/0",
                          "    cells: 300/300/-");

        // partition query
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE k = 2"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numClusterings);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s WHERE k = \\? ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 10/10/0",
                          "    cells: 30/30/-");

        // clustering query
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE k = 2 AND c = 2"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(1);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s WHERE k = \\? AND c = \\? ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 1/1/0",
                          "    cells: 3/3/-");

        // filtered range query
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE v < 25 ALLOW FILTERING"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(25);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s WHERE v < \\? ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 10/3/0",
                          "    rows: 100/25/0",
                          "    cells: 300/75/-");

        // paged query
        mark = node.logs().mark();
        Iterator<Object[]> pagedRows = coordinator.executeWithPaging(format("SELECT * FROM %s.%s"), ALL, 5);
        int readRows = 0;
        while (pagedRows.hasNext())
        {
            pagedRows.next();
            readRows++;
        }
        Assertions.assertThat(readRows).isEqualTo(numRows);
        assertLogsContain(mark, node, quote(format("<SELECT * FROM %s.%s LIMIT 5 ALLOW FILTERING>")));
        assertLogsContain(mark, node, quote(format("<SELECT * FROM %s.%s WHERE token(k) >= token(?) LIMIT 5 ALLOW FILTERING [paging continuation]>")));

        // test multiple slow runs of different queries with the same redacted form, it should log the slowest one
        mark = node.logs().mark();
        for (int i = 0; i < numPartitions; i++)
        {
            rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE k = " + i), ALL);
            Assertions.assertThat(rows).hasNumberOfRows(numClusterings);
        }
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s WHERE k = \\? ALLOW FILTERING>"),
                          "  Slowest fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 10/10/0",
                          "    cells: 30/30/-");

        // delete a partition and query again, to see partition tombstone metrics
        coordinator.execute(format("DELETE FROM %s.%s WHERE k = 0"), ALL);
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numRows - numClusterings);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 9/9/1",
                          "    rows: 90/90/0",
                          "    cells: 270/270/-");

        // delete a row and query again, to see row tombstone metrics
        coordinator.execute(format("DELETE FROM %s.%s WHERE k = 1 AND c = 1"), ALL);
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numRows - numClusterings - 1);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 9/9/1",
                          "    rows: 89/89/1",
                          "    cells: 267/267/-");

        // delete a range of rows and query again, to see more row tombstone metrics
        coordinator.execute(format("DELETE FROM %s.%s WHERE k = 2 AND c < 5"), ALL);
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(84);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 9/9/1",
                          "    rows: 84/84/3", // one from before, plus opening and closing bounds
                          "    cells: 252/252/-");

        // query with a legacy index, which doesn't provide its own execution info, so generic execution info should be used
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE l = 99"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(1);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s WHERE l = \\? ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 1/1/0",
                          "    cells: 3/3/-");

        // query with a SAI index, which provides its own execution info, so generic execution info shouldn't be used
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE s = 99"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(1);
        assertLogsContain(mark, node, format("<SELECT \\* FROM %s\\.%s WHERE s = \\? ALLOW FILTERING>"));
        assertLogsDoNotContain(mark, node, "  Fetched/returned/tombstones:");

        // disable execution info logging and verify that info is not logged anymore
        CassandraRelevantProperties.MONITORING_EXECUTION_INFO_ENABLED.setBoolean(false);
        mark = node.logs().mark();
        coordinator.execute(format("SELECT * FROM %s.%s"), ALL);
        assertLogsContain(mark, node, format("<SELECT \\* FROM %s\\.%s ALLOW FILTERING>"));
        assertLogsDoNotContain(mark, node, "  Fetched/returned/tombstones:");
    }

    /**
     * Test that the slow query logger outputs the correct metrics for number of returned cells in collection columns.
     */
    @Test
    public void testCollections()
    {
        cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, l list<int>, s set<int>, m map<int, int>,  PRIMARY KEY(k, c))"));
        int numPartitions = 10;
        int numClusterings = 10;
        int numRows = 0;
        String insert = format("INSERT INTO %s.%s (k, c, v, l, s, m) VALUES (?, ?, ?, [1, 2], {1, 2, 3}, {1:10, 2:20, 3:30, 4:40})");
        for (int k = 0; k < numPartitions; k++)
            for (int c = 0; c < numClusterings; c++)
                coordinator.execute(insert, ALL, k, c, numRows++);

        // unrestricted query
        long mark = node.logs().mark();
        Object[][] rows = coordinator.execute(format("SELECT * FROM %s.%s"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numRows);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 10/10/0",
                          "    rows: 100/100/0",
                          "    cells: 1000/1000/-");

        // filtered range query
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE v >= 50 ALLOW FILTERING"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(50);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s WHERE v >= \\? ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 10/5/0",
                          "    rows: 100/50/0",
                          "    cells: 1000/500/-");

        // partition query
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE k = 1"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numClusterings);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s WHERE k = \\? ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 10/10/0",
                          "    cells: 100/100/-");

        // filtered partition query
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE k = 1 AND v >= 15 ALLOW FILTERING"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(5);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s WHERE k = \\? AND v >= \\? ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 10/5/0",
                          "    cells: 100/50/-");

        // row query
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE k = 1 AND c = 1"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(1);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s WHERE k = \\? AND c = \\? ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 1/1/0",
                          "    cells: 10/10/-");

        // column query (single-cell)
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT v FROM %s.%s"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numRows);
        assertLogsContain(mark, node,
                          format("<SELECT v FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 10/10/0",
                          "    rows: 100/100/0",
                          "    cells: 1000/1000/-");

        // column query (list)
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT l FROM %s.%s"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numRows);
        assertLogsContain(mark, node,
                          format("<SELECT l FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 10/10/0",
                          "    rows: 100/100/0",
                          "    cells: 1000/1000/-");

        // column query (set)
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT s FROM %s.%s"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numRows);
        assertLogsContain(mark, node,
                          format("<SELECT s FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 10/10/0",
                          "    rows: 100/100/0",
                          "    cells: 1000/1000/-");

        // column query (map)
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT m FROM %s.%s"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numRows);
        assertLogsContain(mark, node,
                          format("<SELECT m FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 10/10/0",
                          "    rows: 100/100/0",
                          "    cells: 1000/1000/-");

        // remove some cells
        coordinator.execute(format("UPDATE %s.%s SET l = l - [1] WHERE k = 1 AND c = 1"), ALL);
        coordinator.execute(format("UPDATE %s.%s SET s = s - {1} WHERE k = 1 AND c = 1"), ALL);
        coordinator.execute(format("UPDATE %s.%s SET m = m - {1} WHERE k = 1 AND c = 1"), ALL);

        // unrestricted query, with tombstones
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numRows);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 10/10/0",
                          "    rows: 100/100/0",
                          "    cells: 1000/1000/-");

        // partition query, with tombstones
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE k = 1"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numClusterings);
        assertLogsContain(mark, node,
                format("<SELECT \\* FROM %s\\.%s WHERE k = \\? ALLOW FILTERING>"),
                "  Fetched/returned/tombstones:",
                "    partitions: 1/1/0",
                "    rows: 10/10/0",
                "    cells: 100/100/-");

        // row query, with tombstones
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE k = 1 AND c = 1"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(1);
        assertLogsContain(mark, node,
                format("<SELECT \\* FROM %s\\.%s WHERE k = \\? AND c = \\? ALLOW FILTERING>"),
                "  Fetched/returned/tombstones:",
                "    partitions: 1/1/0",
                "    rows: 1/1/0",
                "    cells: 10/10/-");
    }

    /**
     * Test that the slow query logger outputs the correct metrics for number of returned cells in user-defined types.
     */
    @Test
    public void testUDTs()
    {
        cluster.schemaChange(withKeyspace("CREATE TYPE %s.udt (x int, y int, z int)"));
        cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, u udt, fu frozen<udt>, PRIMARY KEY(k, c))"));
        int numPartitions = 10;
        int numClusterings = 10;
        int numRows = 0;
        String insert = format("INSERT INTO %s.%s (k, c, v, u, fu) VALUES (?, ?, ?, {x:1, y:2, z:3}, {x:1, y:2, z:3})");
        for (int k = 0; k < numPartitions; k++)
            for (int c = 0; c < numClusterings; c++)
                coordinator.execute(insert, ALL, k, c, numRows++);

        // unrestricted query
        long mark = node.logs().mark();
        Object[][] rows = coordinator.execute(format("SELECT * FROM %s.%s"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numRows);
        assertLogsContain(mark, node,
                          quote(format("<SELECT * FROM %s.%s ALLOW FILTERING>")),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 10/10/0",
                          "    rows: 100/100/0",
                          "    cells: 500/500/-");

        // partition query
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE k = 1 ALLOW FILTERING"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(10);
        assertLogsContain(mark, node,
                          quote(format("<SELECT * FROM %s.%s WHERE k = ? ALLOW FILTERING>")),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 10/10/0",
                          "    cells: 50/50/-");

        // filtered partition query
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE k = 1 AND v >= 15 ALLOW FILTERING"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(5);
        assertLogsContain(mark, node,
                          quote(format("<SELECT * FROM %s.%s WHERE k = ? AND v >= ? ALLOW FILTERING>")),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 10/5/0",
                          "    cells: 50/25/-");

        // row query
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE k = 1 AND c = 1"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(1);
        assertLogsContain(mark, node,
                          quote(format("<SELECT * FROM %s.%s WHERE k = ? AND c = ? ALLOW FILTERING>")),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 1/1/0",
                          "    cells: 5/5/-");

        // remove a field from the non-frozen UDT
        coordinator.execute(format("DELETE u.z FROM %s.%s WHERE k = 1 AND c = 1"), ALL);
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE k = 1 AND c = 1"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(1);
        assertLogsContain(mark, node,
                          quote(format("<SELECT * FROM %s.%s WHERE k = ? AND c = ? ALLOW FILTERING>")),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 1/1/0",
                          "    cells: 5/5/-");
    }

    /**
     * Test that the slow query logger outputs the correct metrics for static rows.
     */
    @Test
    public void testStaticRows()
    {
        cluster.schemaChange(format("CREATE TABLE %s.%s (" +
                                    "k int, c int, r int, " +
                                    "v int static, l list<int> static, s set<int> static, m map<int, int> static, " +
                                    "PRIMARY KEY(k, c))"));

        // Insert a static row without any regular rows for it.
        coordinator.execute(format("INSERT INTO %s.%s (k, v, l, s, m) VALUES (0, 0, [1, 2], {1, 2, 3}, {1:10, 2:20, 3:30, 4:40})"), ALL);
        long mark = node.logs().mark();
        Object[][] rows = coordinator.execute(format("SELECT * FROM %s.%s"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(1);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 0/0/0",
                          "    cells: 10/10/-");
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT v FROM %s.%s"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(1);
        assertLogsContain(mark, node,
                          format("<SELECT v FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 0/0/0",
                          "    cells: 10/10/-");

        // Add a regular row for the partition with the previous static row.
        coordinator.execute(format("INSERT INTO %s.%s (k, c, r) VALUES (0, 0, 0)"), ALL);
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(1);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 1/1/0",
                          "    cells: 11/11/-");
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT r FROM %s.%s"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(1);
        assertLogsContain(mark, node,
                          format("<SELECT r FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 1/1/0",
                          "    cells: 11/11/-");

        // Add a second regular row for the previous partition.
        coordinator.execute(format("INSERT INTO %s.%s (k, c, r) VALUES (0, 1, 0)"), ALL);
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(2);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 2/2/0",
                          "    cells: 12/12/-");

        // Remove a cell from a regular row.
        coordinator.execute(format("INSERT INTO %s.%s (k, c, r) VALUES (0, 1, null)"), ALL);
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(2);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 2/2/0",
                          "    cells: 12/12/-");

        // Remove some cells from the static row.
        coordinator.execute(format("UPDATE %s.%s SET l = l - [1] WHERE k = 0"), ALL);
        coordinator.execute(format("UPDATE %s.%s SET s = s - {1} WHERE k = 0"), ALL);
        coordinator.execute(format("UPDATE %s.%s SET m = m - {1} WHERE k = 0"), ALL);
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(2);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 1/1/0",
                          "    rows: 2/2/0",
                          "    cells: 12/12/");

        // Add another partition, without statics
        coordinator.execute(format("INSERT INTO %s.%s (k, c, r) VALUES (1, 1, 0)"), ALL);
        coordinator.execute(format("INSERT INTO %s.%s (k, c, r) VALUES (1, 2, 0)"), ALL);
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(4);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 2/2/0",
                          "    rows: 4/4/0",
                          "    cells: 14/14/-");

        // Add some static cell to the new partition
        coordinator.execute(format("INSERT INTO %s.%s (k, l) VALUES (1, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])"), ALL);
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(4);
        assertLogsContain(mark, node,
                          format("<SELECT \\* FROM %s\\.%s ALLOW FILTERING>"),
                          "  Fetched/returned/tombstones:",
                          "    partitions: 2/2/0",
                          "    rows: 4/4/0",
                          "    cells: 24/24/-");
    }

    private static String format(String query)
    {
        return String.format(query, KEYSPACE, table);
    }

    public static void assertLogsContain(long mark, IInvokableInstance node, String... lines)
    {
        assertLogs(mark, node, AbstractIterableAssert::isNotEmpty, lines);
    }

    public static void assertLogsDoNotContain(long mark, IInvokableInstance node, String... lines)
    {
        assertLogs(mark, node, AbstractIterableAssert::isEmpty, lines);
    }

    public static void assertLogs(long mark, IInvokableInstance node, Consumer<ListAssert<String>> listAssert, String... lines)
    {
        // manually trigger the monitoring task to log the slow operations
        node.runOnInstance(() -> MonitoringTask.instance.logOperations(approxTime.now()));

        for (String line : lines)
        {
            List<String> matchingLines = node.logs().grep(mark, line).getResult();
            listAssert.accept(Assertions.assertThat(matchingLines));
        }
    }

    /**
     * Bytebuddy interceptor to slow down reads on node 2.
     */
    public static class BBHelper
    {
        @SuppressWarnings("resource")
        public static void install(ClassLoader classLoader, Integer node)
        {
            if (node == 2)
            {
                new ByteBuddy().rebase(ReadCommand.class)
                               .method(named("executeLocally"))
                               .intercept(MethodDelegation.to(SlowQueryLoggerTest.BBHelper.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController,
                                                                 @SuperCall Callable<UnfilteredPartitionIterator> zuperCall)
        {
            try
            {
                if (executionController.metadata().keyspace.equals(KEYSPACE))
                    Thread.sleep(SLOW_QUERY_LOG_TIMEOUT_MS * 2);

                return zuperCall.call();
            }
            catch (Exception e)
            {
                throw Throwables.unchecked(e);
            }
        }
    }
}
