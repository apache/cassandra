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
package org.apache.cassandra.distributed.test.sai;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;
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
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.monitoring.MonitoringTask;
import org.apache.cassandra.db.monitoring.MonitoringTaskTest;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.index.sai.plan.QueryMonitorableExecutionInfo;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;

import static java.util.regex.Pattern.quote;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.test.SlowQueryLoggerTest.assertLogsContain;
import static org.apache.cassandra.distributed.test.SlowQueryLoggerTest.assertLogsDoNotContain;
import static org.apache.cassandra.utils.MonotonicClock.approxTime;

/**
 * Tests {@link QueryMonitorableExecutionInfo} combined with the {@link MonitoringTask} mechanism,
 * the core functionality testing of that feature should is in {@link MonitoringTaskTest}.
 */
public class SlowSAIQueryLoggerTest extends TestBaseImpl
{
    private static final int SLOW_QUERY_LOG_TIMEOUT_IN_MS = 100;

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

        // enable term statistics, same as in SAITester
        CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_USE_TERM_STATISTICS.setBoolean(true);

        cluster = init(Cluster.build(1)
                              .withConfig(c -> c.set("slow_query_log_timeout_in_ms", SLOW_QUERY_LOG_TIMEOUT_IN_MS))
                              .withInstanceInitializer(BB::install)
                              .start());
        coordinator = cluster.coordinator(1);
        node = cluster.get(1);
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

    private static String format(String query)
    {
        return String.format(query, KEYSPACE, table);
    }

    @Test
    public void testSlowSAIQueryLogger()
    {
        // effectively disable the scheduled monitoring task so we control it manually for better test stability
        CassandraRelevantProperties.MONITORING_REPORT_INTERVAL_MS.setInt((int) TimeUnit.HOURS.toMillis(1));

        // enable term statistics, same as in SAITester
        CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_USE_TERM_STATISTICS.setBoolean(true);

        // create a table with numeric, text and vector indexes
        cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, n int, s text, v vector<float, 2>, l int, PRIMARY KEY(k, c))"));
        cluster.schemaChange(format("CREATE CUSTOM INDEX ON %s.%s (n) USING 'StorageAttachedIndex'"));
        cluster.schemaChange(format("CREATE CUSTOM INDEX ON %s.%s (s) USING 'StorageAttachedIndex'"));
        cluster.schemaChange(format("CREATE CUSTOM INDEX ON %s.%s (v) USING 'StorageAttachedIndex'"));

        // insert some data
        coordinator.execute(format("INSERT INTO %s.%s (k, c, n, s, v, l) VALUES (1, 1, 1, 's_1', [1, 1], 1)"), ConsistencyLevel.ONE);
        coordinator.execute(format("INSERT INTO %s.%s (k, c, n, s, v, l) VALUES (1, 2, 2, 's_2', [1, 2], 2)"), ConsistencyLevel.ONE);
        coordinator.execute(format("INSERT INTO %s.%s (k, c, n, s, v, l) VALUES (2, 1, 3, 's_3', [1, 3], 3)"), ConsistencyLevel.ONE);
        coordinator.execute(format("INSERT INTO %s.%s (k, c, n, s, v, l) VALUES (2, 2, 4, 's_4', [1, 4], 4)"), ConsistencyLevel.ONE);
        node.flush(KEYSPACE);

        // test single numeric query
        long mark = node.logs().mark();
        String numericQuery = format("SELECT * FROM %s.%s WHERE n > 1");
        coordinator.execute(numericQuery, ConsistencyLevel.ONE);
        assertLogsContain(mark, node,
                          "SAI slow query metrics:",
                          "sstablesHit: 1",
                          "segmentsHit: 1",
                          "keysFetched: 3",
                          "partitionsFetched: 2",
                          "partitionsReturned: 2",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 3",
                          "rowsReturned: 3",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 12",
                          "cellsReturned: 12",
                          "trieSegmentsHit: 0",
                          "triePostingsSkips: 0",
                          "triePostingsDecodes: 0",
                          "bkdSegmentsHit: 1",
                          "bkdPostingListsHit: 1",
                          "bkdPostingsSkips: 0",
                          "bkdPostingsDecodes: 4",
                          "annGraphSearchLatencyNanos: 0",
                          "SAI slow query plan:",
                          "NumericIndexScan",
                          quote("predicate: Expression{name: n, op: RANGE, lower: (?, false), upper: (null, false), exclusions: []}"));

        // test aggregated numeric query
        mark = node.logs().mark();
        for (int i = 0; i < 2; i++)
            coordinator.execute(numericQuery, ConsistencyLevel.ONE);
        assertLogsContain(mark, node,
                          "SAI slowest query metrics:",
                          "sstablesHit: 1",
                          "segmentsHit: 1",
                          "keysFetched: 3",
                          "partitionsFetched: 2",
                          "partitionsReturned: 2",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 3",
                          "rowsReturned: 3",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 12",
                          "cellsReturned: 12",
                          "trieSegmentsHit: 0",
                          "triePostingsSkips: 0",
                          "triePostingsDecodes: 0",
                          "bkdSegmentsHit: 1",
                          "bkdPostingListsHit: 1",
                          "bkdPostingsSkips: 0",
                          "bkdPostingsDecodes: 4",
                          "annGraphSearchLatencyNanos: 0",
                          "SAI slowest query plan:",
                          "NumericIndexScan",
                          quote("predicate: Expression{name: n, op: RANGE, lower: (?, false), upper: (null, false), exclusions: []}"));

        // test single text query
        mark = node.logs().mark();
        String textQuery = format("SELECT * FROM %s.%s WHERE s = 's_2' OR s = 's_3'");
        coordinator.execute(textQuery, ConsistencyLevel.ONE);
        assertLogsContain(mark, node,
                          "SAI slow query metrics:",
                          "sstablesHit: 2",
                          "segmentsHit: 2",
                          "keysFetched: 2",
                          "partitionsFetched: 2",
                          "partitionsReturned: 2",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 2",
                          "rowsReturned: 2",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 8",
                          "cellsReturned: 8",
                          "trieSegmentsHit: 2",
                          "triePostingsSkips: 0",
                          "triePostingsDecodes: 2",
                          "bkdSegmentsHit: 0",
                          "bkdPostingListsHit: 0",
                          "bkdPostingsSkips: 0",
                          "bkdPostingsDecodes: 0",
                          "annGraphSearchLatencyNanos: 0",
                          "SAI slow query plan:",
                          "LiteralIndexScan",
                          quote("predicate: Expression{name: s, op: EQ, lower: (?, true), upper: (?, true), exclusions: []}"));

        // test aggregated text query
        mark = node.logs().mark();
        for (int i = 0; i < 2; i++)
            coordinator.execute(textQuery, ConsistencyLevel.ONE);
        assertLogsContain(mark, node,
                          "SAI slowest query metrics:",
                          "sstablesHit: 2",
                          "segmentsHit: 2",
                          "keysFetched: 2",
                          "partitionsFetched: 2",
                          "partitionsReturned: 2",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 2",
                          "rowsReturned: 2",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 8",
                          "cellsReturned: 8",
                          "trieSegmentsHit: 2",
                          "triePostingsSkips: 0",
                          "triePostingsDecodes: 2",
                          "bkdSegmentsHit: 0",
                          "bkdPostingListsHit: 0",
                          "bkdPostingsSkips: 0",
                          "bkdPostingsDecodes: 0",
                          "annGraphSearchLatencyNanos: 0",
                          "SAI slowest query plan:",
                          "LiteralIndexScan",
                          quote("predicate: Expression{name: s, op: EQ, lower: (?, true), upper: (?, true), exclusions: []}"));

        // test single ANN query
        mark = node.logs().mark();
        String annQuery = format("SELECT * FROM %s.%s ORDER BY v ANN OF [1, 1] LIMIT 10");
        coordinator.execute(annQuery, ConsistencyLevel.ONE);
        assertLogsContain(mark, node,
                          "SAI slow query metrics:",
                          "sstablesHit: 1",
                          "segmentsHit: 1",
                          "keysFetched: 4",
                          "partitionsFetched: 4",
                          "partitionsReturned: 2",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 4",
                          "rowsReturned: 4",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 16",
                          "cellsReturned: 16",
                          "trieSegmentsHit: 0",
                          "triePostingsSkips: 0",
                          "triePostingsDecodes: 0",
                          "bkdPostingListsHit: 0",
                          "bkdSegmentsHit: 0",
                          "bkdPostingsSkips: 0",
                          "bkdPostingsDecodes: 0",
                          "annGraphSearchLatencyNanos: [1-9][0-9]*", // unknown, but greater than zero
                          "SAI slow query plan:",
                          "AnnIndexScan",
                          quote("v ANN OF ? DESC"));

        // test aggregated ANN query
        mark = node.logs().mark();
        for (int i = 0; i < 2; i++)
            coordinator.execute(annQuery, ConsistencyLevel.ONE);
        assertLogsContain(mark, node,
                          "SAI slowest query metrics:",
                          "sstablesHit: 1",
                          "segmentsHit: 1",
                          "keysFetched: 4",
                          "partitionsFetched: 4",
                          "partitionsReturned: 2",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 4",
                          "rowsReturned: 4",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 16",
                          "cellsReturned: 16",
                          "trieSegmentsHit: 0",
                          "triePostingsSkips: 0",
                          "triePostingsDecodes: 0",
                          "bkdSegmentsHit: 0",
                          "bkdPostingListsHit: 0",
                          "bkdPostingsSkips: 0",
                          "bkdPostingsDecodes: 0",
                          "annGraphSearchLatencyNanos: [1-9][0-9]*", // unknown, but greater than zero
                          "SAI slowest query plan:",
                          "AnnIndexScan",
                          quote("v ANN OF ? DESC"));

        // test single hybrid query
        mark = node.logs().mark();
        String hybridQuery = format("SELECT * FROM %s.%s WHERE n > 1 ORDER BY s LIMIT 10");
        coordinator.execute(hybridQuery, ConsistencyLevel.ONE);
        assertLogsContain(mark, node,
                          "SAI slow query metrics:",
                          "sstablesHit: 1",
                          "segmentsHit: 1",
                          "keysFetched: 4",
                          "partitionsFetched: 4",
                          "partitionsReturned: 2",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 4",
                          "rowsReturned: 3",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 16",
                          "cellsReturned: 12",
                          "trieSegmentsHit: 0",
                          "triePostingsSkips: 0",
                          "triePostingsDecodes: 0",
                          "bkdSegmentsHit: 0",
                          "bkdPostingListsHit: 0",
                          "bkdPostingsSkips: 0",
                          "bkdPostingsDecodes: 0",
                          "annGraphSearchLatencyNanos: 0",
                          "SAI slow query plan:",
                          "LiteralIndexScan",
                          quote("ordering: s ASC"));

        // test aggregated hybrid query
        mark = node.logs().mark();
        for (int i = 0; i < 2; i++)
            coordinator.execute(hybridQuery, ConsistencyLevel.ONE);
        assertLogsContain(mark, node,
                          "SAI slowest query metrics:",
                          "sstablesHit: 1",
                          "segmentsHit: 1",
                          "keysFetched: 4",
                          "partitionsFetched: 4",
                          "partitionsReturned: 2",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 4",
                          "rowsReturned: 3",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 16",
                          "cellsReturned: 12",
                          "trieSegmentsHit: 0",
                          "triePostingsSkips: 0",
                          "triePostingsDecodes: 0",
                          "bkdSegmentsHit: 0",
                          "bkdPostingListsHit: 0",
                          "bkdPostingsSkips: 0",
                          "bkdPostingsDecodes: 0",
                          "annGraphSearchLatencyNanos: 0",
                          "SAI slowest query plan:",
                          "LiteralIndexScan",
                          quote("ordering: s ASC"));

        // Disable query optimizer to prevent skipping hybrid query logic and hit orderByResults to verify metrics update
        node.runOnInstance(() -> QueryController.QUERY_OPT_LEVEL = 0);
        mark = node.logs().mark();
        coordinator.execute(hybridQuery, ConsistencyLevel.ONE);
        assertLogsContain(mark, node,
                          "SAI slow query metrics:",
                          "sstablesHit: 2",
                          "segmentsHit: 2",
                          "keysFetched: 3",
                          "partitionsFetched: 3",
                          "partitionsReturned: 2",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 3",
                          "rowsReturned: 3",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 12",
                          "cellsReturned: 12",
                          "trieSegmentsHit: 0",
                          "triePostingsSkips: 0",
                          "triePostingsDecodes: 0",
                          "bkdSegmentsHit: 1",
                          "bkdPostingListsHit: 1",
                          "bkdPostingsSkips: 0",
                          "bkdPostingsDecodes: 4",
                          "annGraphSearchLatencyNanos: 0",
                          "SAI slow query plan:",
                          "KeysSort",
                          "NumericIndexScan",
                          quote("predicate: Expression{name: n, op: RANGE, lower: (?, false), upper: (null, false), exclusions: []}"));

        node.runOnInstance(() -> QueryController.QUERY_OPT_LEVEL = CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_LEVEL.getInt());

        // test changing data between identical queries, making one of them slower than the other,
        // so we can check that only the execution info of the slowest query are reported
        mark = node.logs().mark();
        coordinator.execute(numericQuery, ConsistencyLevel.ONE);
        node.runOnInstance(() -> BB.queryDelay.updateAndGet(x -> x * 4)); // make queries 4x slower
        coordinator.execute(format("INSERT INTO %s.%s (k, c, n) VALUES (1, 3, 5)"), ConsistencyLevel.ONE);
        coordinator.execute(format("INSERT INTO %s.%s (k, c, n) VALUES (2, 3, 6)"), ConsistencyLevel.ONE);
        coordinator.execute(format("INSERT INTO %s.%s (k, c, n) VALUES (3, 1, 7)"), ConsistencyLevel.ONE);
        node.flush(KEYSPACE);
        coordinator.execute(numericQuery, ConsistencyLevel.ONE);
        assertLogsContain(mark, node,
                          "SAI slowest query metrics:",
                          "sstablesHit: 3",
                          "segmentsHit: 3",
                          "partitionsFetched: 3",
                          "rowsFetched: 6",
                          "cellsFetched: 15");
        node.runOnInstance(() -> BB.queryDelay.updateAndGet(x -> x / 4)); // restore the query delay

        // disable execution info logging and verify they are not logged
        CassandraRelevantProperties.SAI_MONITORING_EXECUTION_INFO_ENABLED.setBoolean(false);
        mark = node.logs().mark();
        coordinator.execute(numericQuery, ConsistencyLevel.ONE);
        coordinator.execute(textQuery, ConsistencyLevel.ONE);
        coordinator.execute(annQuery, ConsistencyLevel.ONE);
        coordinator.execute(hybridQuery, ConsistencyLevel.ONE);
        assertLogsContain(mark, node, "4 operations were slow");
        assertLogsDoNotContainSAIExecutionInfo(mark, node);
        CassandraRelevantProperties.SAI_MONITORING_EXECUTION_INFO_ENABLED.setBoolean(true);

        // test with a legacy index, there should be no SAI execution info
        cluster.schemaChange(format("CREATE INDEX legacy_idx ON %s.%s (l)"));
        final String t = table;
        Awaitility.waitAtMost(1, TimeUnit.MINUTES).until(() -> cluster.get(1).callOnInstance(() -> {
            SecondaryIndexManager sim = Keyspace.open(KEYSPACE).getColumnFamilyStore(t).indexManager;
            return sim.isIndexQueryable("legacy_idx");
        }));
        mark = node.logs().mark();
        String legacyIndexQuery = format("SELECT * FROM %s.%s WHERE l = 1");
        coordinator.execute(legacyIndexQuery, ConsistencyLevel.ONE);
        assertLogsContain(mark, node, "1 operations were slow", "WHERE l = ?");
        assertLogsDoNotContainSAIExecutionInfo(mark, node);

        // test with a regular, non-indexed query, there should be no SAI execution info
        mark = node.logs().mark();
        String regularQuery = format("SELECT * FROM %s.%s WHERE k = 1");
        coordinator.execute(regularQuery, ConsistencyLevel.ONE);
        assertLogsContain(mark, node, "1 operations were slow", "WHERE k = ?");
        assertLogsDoNotContainSAIExecutionInfo(mark, node);

        // test that queries with the same relations and different values get grouped due to redaction
        mark = node.logs().mark();
        coordinator.execute(format("SELECT * FROM %s.%s WHERE n = 1"), ConsistencyLevel.ONE);
        coordinator.execute(format("SELECT * FROM %s.%s WHERE n = 2"), ConsistencyLevel.ONE);
        coordinator.execute(format("SELECT * FROM %s.%s WHERE n > 1"), ConsistencyLevel.ONE);
        coordinator.execute(format("SELECT * FROM %s.%s WHERE n > 2"), ConsistencyLevel.ONE);
        coordinator.execute(format("SELECT * FROM %s.%s WHERE n > 3"), ConsistencyLevel.ONE);
        assertLogsContain(mark, node, "was slow 2 times", "WHERE n = ?", "SAI slowest query metrics:");
        assertLogsContain(mark, node, "was slow 3 times", "WHERE n > ?", "SAI slowest query metrics:");
        assertLogsDoNotContain(mark, node, "WHERE n = 1", "WHERE n = 2", "WHERE n > 1", "WHERE n > 2", "WHERE n > 3");

        // test some partition and row deletions
        coordinator.execute(format("DELETE FROM %s.%s WHERE k = 1 AND c = 1"), ConsistencyLevel.ONE);
        coordinator.execute(format("DELETE FROM %s.%s WHERE k = 1 AND c = 2"), ConsistencyLevel.ONE);
        coordinator.execute(format("DELETE FROM %s.%s WHERE k = 2"), ConsistencyLevel.ONE);
        node.flush(KEYSPACE);
        String selectAllQuery = format("SELECT * FROM %s.%s WHERE n >= 0");
        coordinator.execute(selectAllQuery, ConsistencyLevel.ONE);
        assertLogsContain(mark, node,
                          "partitionTombstonesFetched: 1",
                          "rowTombstonesFetched: 2");

        // test with paged queries
        mark = node.logs().mark();
        String query =  format("SELECT * FROM %s.%s WHERE n >= 0");
        Iterator<Object[]> pagedRows = coordinator.executeWithPaging(query, ConsistencyLevel.ONE, 1);
        while (pagedRows.hasNext())
            pagedRows.next();
        assertLogsContain(mark, node,
                          quote(format("<SELECT * FROM %s.%s WHERE n >= ? LIMIT 1 ALLOW FILTERING>")),
                          "NumericIndexScan");
        assertLogsContain(mark, node,
                          quote(format("<SELECT * FROM %s.%s WHERE token(k) >= token(?) AND n >= ? LIMIT 1 ALLOW FILTERING [paging continuation]>")),
                          "NumericIndexScan");
    }

    /**
     * Test that the slow query logger outputs the correct metrics for number of returned cells in collection columns.
     */
    @Test
    public void testCollections()
    {
        cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, l list<int>, s set<int>, m map<int, int>,  PRIMARY KEY(k, c))"));
        cluster.schemaChange(format("CREATE CUSTOM INDEX ON %s.%s (l) USING 'StorageAttachedIndex'"));
        cluster.schemaChange(format("CREATE CUSTOM INDEX ON %s.%s (s) USING 'StorageAttachedIndex'"));
        cluster.schemaChange(format("CREATE CUSTOM INDEX ON %s.%s (KEYS(m)) USING 'StorageAttachedIndex'"));
        cluster.schemaChange(format("CREATE CUSTOM INDEX ON %s.%s (VALUES(m)) USING 'StorageAttachedIndex'"));
        cluster.schemaChange(format("CREATE CUSTOM INDEX ON %s.%s (ENTRIES(m)) USING 'StorageAttachedIndex'"));

        int numPartitions = 10;
        int numClusterings = 10;
        int numRows = 0;
        String insert = format("INSERT INTO %s.%s (k, c, v, l, s, m) VALUES (?, ?, ?, [1, 2], {1, 2, 3}, {1:10, 2:20, 3:30, 4:40})");
        for (int k = 0; k < numPartitions; k++)
            for (int c = 0; c < numClusterings; c++)
                coordinator.execute(insert, ALL, k, c, numRows++);

        node.flush(KEYSPACE);

        // list query
        long mark = node.logs().mark();
        Object[][] rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE l CONTAINS 1"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numRows);
        assertLogsContain(mark, node,
                          quote(format("<SELECT * FROM %s.%s WHERE l CONTAINS ? ALLOW FILTERING>")),
                          "SAI slow query metrics:",
                          "sstablesHit: 3",
                          "segmentsHit: 3",
                          "keysFetched: 100",
                          "partitionsFetched: 10",
                          "partitionsReturned: 10",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 100",
                          "rowsReturned: 100",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 1000",
                          "cellsReturned: 1000",
                          "trieSegmentsHit: 0",
                          "triePostingsSkips: 0",
                          "triePostingsDecodes: 0",
                          "bkdSegmentsHit: 3",
                          "bkdPostingListsHit: 3",
                          "bkdPostingsSkips: 0",
                          "bkdPostingsDecodes: 200",
                          "annGraphSearchLatencyNanos: 0",
                          "SAI slow query plan:",
                          "NumericIndexScan",
                          quote("predicate: Expression{name: l, op: CONTAINS_VALUE, lower: (?, true), upper: (?, true), exclusions: []}"));

        // set query
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE s CONTAINS 1"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numRows);
        assertLogsContain(mark, node,
                          quote(format("<SELECT * FROM %s.%s WHERE s CONTAINS ? ALLOW FILTERING>")),
                          "SAI slow query metrics:",
                          "sstablesHit: 3",
                          "segmentsHit: 3",
                          "keysFetched: 100",
                          "partitionsFetched: 10",
                          "partitionsReturned: 10",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 100",
                          "rowsReturned: 100",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 1000",
                          "cellsReturned: 1000",
                          "trieSegmentsHit: 0",
                          "triePostingsSkips: 0",
                          "triePostingsDecodes: 0",
                          "bkdSegmentsHit: 3",
                          "bkdPostingListsHit: 3",
                          "bkdPostingsSkips: 0",
                          "bkdPostingsDecodes: 300",
                          "annGraphSearchLatencyNanos: 0",
                          "SAI slow query plan:",
                          "NumericIndexScan",
                          quote("predicate: Expression{name: s, op: CONTAINS_VALUE, lower: (?, true), upper: (?, true), exclusions: []}"));

        // map key query
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE m CONTAINS KEY 1"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numRows);
        assertLogsContain(mark, node,
                          quote(format("<SELECT * FROM %s.%s WHERE m CONTAINS KEY ? ALLOW FILTERING>")),
                          "SAI slow query metrics:",
                          "sstablesHit: 3",
                          "segmentsHit: 3",
                          "keysFetched: 100",
                          "partitionsFetched: 10",
                          "partitionsReturned: 10",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 100",
                          "rowsReturned: 100",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 1000",
                          "cellsReturned: 1000",
                          "trieSegmentsHit: 0",
                          "triePostingsSkips: 0",
                          "triePostingsDecodes: 0",
                          "bkdSegmentsHit: 3",
                          "bkdPostingListsHit: 3",
                          "bkdPostingsSkips: 0",
                          "bkdPostingsDecodes: 400",
                          "annGraphSearchLatencyNanos: 0",
                          "SAI slow query plan:",
                          "NumericIndexScan",
                          quote("predicate: Expression{name: m, op: CONTAINS_KEY, lower: (?, true), upper: (?, true), exclusions: []}"));

        // map value query
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE m CONTAINS 10"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numRows);
        assertLogsContain(mark, node,
                          quote(format("<SELECT * FROM %s.%s WHERE m CONTAINS ? ALLOW FILTERING>")),
                          "SAI slow query metrics:",
                          "sstablesHit: 3",
                          "segmentsHit: 3",
                          "keysFetched: 100",
                          "partitionsFetched: 10",
                          "partitionsReturned: 10",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 100",
                          "rowsReturned: 100",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 1000",
                          "cellsReturned: 1000",
                          "trieSegmentsHit: 0",
                          "triePostingsSkips: 0",
                          "triePostingsDecodes: 0",
                          "bkdSegmentsHit: 3",
                          "bkdPostingListsHit: 3",
                          "bkdPostingsSkips: 0",
                          "bkdPostingsDecodes: 400",
                          "annGraphSearchLatencyNanos: 0",
                          "SAI slow query plan:",
                          "NumericIndexScan",
                          quote("predicate: Expression{name: m, op: CONTAINS_VALUE, lower: (?, true), upper: (?, true), exclusions: []}"));

        // map entry query
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE m[1] = 10"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numRows);
        assertLogsContain(mark, node,
                          quote(format("<SELECT * FROM %s.%s WHERE m[?] = ? ALLOW FILTERING>")),
                          "SAI slow query metrics:",
                          "sstablesHit: 3",
                          "segmentsHit: 3",
                          "keysFetched: 100",
                          "partitionsFetched: 10",
                          "partitionsReturned: 10",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 100",
                          "rowsReturned: 100",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 1000",
                          "cellsReturned: 1000",
                          "trieSegmentsHit: 3",
                          "triePostingsSkips: 0",
                          "triePostingsDecodes: 100",
                          "bkdSegmentsHit: 0",
                          "bkdPostingListsHit: 0",
                          "bkdPostingsSkips: 0",
                          "bkdPostingsDecodes: 0",
                          "annGraphSearchLatencyNanos: 0",
                          "SAI slow query plan:",
                          "LiteralIndexScan",
                          quote("predicate: Expression{name: m, op: EQ, lower: (?, true), upper: (?, true), exclusions: []}"));

        // remove some cells
        coordinator.execute(format("UPDATE %s.%s SET l = l - [1] WHERE k = 1 AND c = 1"), ALL);
        coordinator.execute(format("UPDATE %s.%s SET s = s - {1} WHERE k = 1 AND c = 1"), ALL);
        coordinator.execute(format("UPDATE %s.%s SET m = m - {1} WHERE k = 1 AND c = 1"), ALL);

        // list query
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE l CONTAINS 1"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numRows - 1);
        assertLogsContain(mark, node,
                          quote(format("<SELECT * FROM %s.%s WHERE l CONTAINS ? ALLOW FILTERING>")),
                          "SAI slow query metrics:",
                          "sstablesHit: 3",
                          "segmentsHit: 3",
                          "keysFetched: 100",
                          "partitionsFetched: 10",
                          "partitionsReturned: 10",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 100",
                          "rowsReturned: 99",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 1000",
                          "cellsReturned: 990",
                          "trieSegmentsHit: 0",
                          "triePostingsSkips: 0",
                          "triePostingsDecodes: 0",
                          "bkdSegmentsHit: 3",
                          "bkdPostingListsHit: 3",
                          "bkdPostingsSkips: 0",
                          "bkdPostingsDecodes: 200",
                          "annGraphSearchLatencyNanos: 0",
                          "SAI slow query plan:",
                          "NumericIndexScan",
                          quote("predicate: Expression{name: l, op: CONTAINS_VALUE, lower: (?, true), upper: (?, true), exclusions: []}"));

        // set query
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE s CONTAINS 1"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numRows - 1);
        assertLogsContain(mark, node,
                          quote(format("<SELECT * FROM %s.%s WHERE s CONTAINS ? ALLOW FILTERING>")),
                          "SAI slow query metrics:",
                          "sstablesHit: 3",
                          "segmentsHit: 3",
                          "keysFetched: 100",
                          "partitionsFetched: 10",
                          "partitionsReturned: 10",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 100",
                          "rowsReturned: 99",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 1000",
                          "cellsReturned: 990",
                          "trieSegmentsHit: 0",
                          "triePostingsSkips: 0",
                          "triePostingsDecodes: 0",
                          "bkdSegmentsHit: 3",
                          "bkdPostingListsHit: 3",
                          "bkdPostingsSkips: 0",
                          "bkdPostingsDecodes: 300",
                          "annGraphSearchLatencyNanos: 0",
                          "SAI slow query plan:",
                          "NumericIndexScan",
                          quote("predicate: Expression{name: s, op: CONTAINS_VALUE, lower: (?, true), upper: (?, true), exclusions: []}"));

        // map key query
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE m CONTAINS KEY 1"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numRows - 1);
        assertLogsContain(mark, node,
                          quote(format("<SELECT * FROM %s.%s WHERE m CONTAINS KEY ? ALLOW FILTERING>")),
                          "SAI slow query metrics:",
                          "sstablesHit: 3",
                          "segmentsHit: 3",
                          "keysFetched: 100",
                          "partitionsFetched: 10",
                          "partitionsReturned: 10",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 100",
                          "rowsReturned: 99",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 1000",
                          "cellsReturned: 990",
                          "trieSegmentsHit: 0",
                          "triePostingsSkips: 0",
                          "triePostingsDecodes: 0",
                          "bkdSegmentsHit: 3",
                          "bkdPostingListsHit: 3",
                          "bkdPostingsSkips: 0",
                          "bkdPostingsDecodes: 400",
                          "annGraphSearchLatencyNanos: 0",
                          "SAI slow query plan:",
                          "NumericIndexScan",
                          quote("predicate: Expression{name: m, op: CONTAINS_KEY, lower: (?, true), upper: (?, true), exclusions: []}"));

        // map value query
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE m CONTAINS 10"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numRows - 1);
        assertLogsContain(mark, node,
                          quote(format("<SELECT * FROM %s.%s WHERE m CONTAINS ? ALLOW FILTERING>")),
                          "SAI slow query metrics:",
                          "sstablesHit: 3",
                          "segmentsHit: 3",
                          "keysFetched: 100",
                          "partitionsFetched: 10",
                          "partitionsReturned: 10",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 100",
                          "rowsReturned: 99",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 1000",
                          "cellsReturned: 990",
                          "trieSegmentsHit: 0",
                          "triePostingsSkips: 0",
                          "triePostingsDecodes: 0",
                          "bkdSegmentsHit: 3",
                          "bkdPostingListsHit: 3",
                          "bkdPostingsSkips: 0",
                          "bkdPostingsDecodes: 400",
                          "annGraphSearchLatencyNanos: 0",
                          "SAI slow query plan:",
                          "NumericIndexScan",
                          quote("predicate: Expression{name: m, op: CONTAINS_VALUE, lower: (?, true), upper: (?, true), exclusions: []}"));

        // map entry query
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE m[1] = 10"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(numRows - 1);
        assertLogsContain(mark, node,
                          quote(format("<SELECT * FROM %s.%s WHERE m[?] = ? ALLOW FILTERING>")),
                          "SAI slow query metrics:",
                          "sstablesHit: 3",
                          "segmentsHit: 3",
                          "keysFetched: 100",
                          "partitionsFetched: 10",
                          "partitionsReturned: 10",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 100",
                          "rowsReturned: 99",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 1000",
                          "cellsReturned: 990",
                          "trieSegmentsHit: 3",
                          "triePostingsSkips: 0",
                          "triePostingsDecodes: 100",
                          "bkdSegmentsHit: 0",
                          "bkdPostingListsHit: 0",
                          "bkdPostingsSkips: 0",
                          "bkdPostingsDecodes: 0",
                          "annGraphSearchLatencyNanos: 0",
                          "SAI slow query plan:",
                          "LiteralIndexScan",
                          quote("predicate: Expression{name: m, op: EQ, lower: (?, true), upper: (?, true), exclusions: []}"));
    }

    /**
     * Test that the slow query logger outputs the correct metrics for number of returned cells in user-defined types.
     */
    @Test
    public void testUDTs()
    {
        cluster.schemaChange(withKeyspace("CREATE TYPE %s.udt (x int, y int, z int)"));
        cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, u udt, fu frozen<udt>, PRIMARY KEY(k, c))"));
        cluster.schemaChange(format("CREATE CUSTOM INDEX ON %s.%s (v) USING 'StorageAttachedIndex'"));
        int numPartitions = 10;
        int numClusterings = 10;
        int numRows = 0;
        String insert = format("INSERT INTO %s.%s (k, c, v, u, fu) VALUES (?, ?, ?, {x:1, y:2, z:3}, {x:1, y:2, z:3})");
        for (int k = 0; k < numPartitions; k++)
            for (int c = 0; c < numClusterings; c++)
                coordinator.execute(insert, ALL, k, c, numRows++);

        node.flush(KEYSPACE);

        // filtered range query
        long mark = node.logs().mark();
        Object[][] rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE v >= 20"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(80);
        assertLogsContain(mark, node,
                          quote(format("<SELECT * FROM %s.%s WHERE v >= ? ALLOW FILTERING>")),
                          "SAI slow query metrics:",
                          "sstablesHit: 3",
                          "segmentsHit: 3",
                          "keysFetched: 80",
                          "partitionsFetched: 8",
                          "partitionsReturned: 8",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 80",
                          "rowsReturned: 80",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 400",
                          "cellsReturned: 400");

        // filtered partition query
        mark = node.logs().mark();
        rows = coordinator.execute(format("SELECT * FROM %s.%s WHERE k = 1 AND v >= 15"), ALL);
        Assertions.assertThat(rows).hasNumberOfRows(5);
        assertLogsContain(mark, node,
                          quote(format("<SELECT * FROM %s.%s WHERE k = ? AND v >= ? ALLOW FILTERING>")),
                          "SAI slow query metrics:",
                          "sstablesHit: 1",
                          "segmentsHit: 1",
                          "keysFetched: 5",
                          "partitionsFetched: 1",
                          "partitionsReturned: 1",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 5",
                          "rowsReturned: 5",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 25",
                          "cellsReturned: 25");
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
        cluster.schemaChange(format("CREATE CUSTOM INDEX ON %s.%s (v) USING 'StorageAttachedIndex'"));

        String query = format("SELECT * FROM %s.%s WHERE v = 0");
        String loggedQuery = quote(format("SELECT * FROM %s.%s WHERE v = ? ALLOW FILTERING"));

        // Insert a static row without any regular rows for it.
        coordinator.execute(format("INSERT INTO %s.%s (k, v, l, s, m) VALUES (0, 0, [1, 2], {1, 2, 3}, {1:10, 2:20, 3:30, 4:40})"), ALL);
        long mark = node.logs().mark();
        Object[][] rows = coordinator.execute(query, ALL);
        Assertions.assertThat(rows).hasNumberOfRows(1);
        assertLogsContain(mark, node, loggedQuery,
                          "SAI slow query metrics:",
                          "keysFetched: 1",
                          "partitionsFetched: 1",
                          "partitionsReturned: 1",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 0",
                          "rowsReturned: 0",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 10",
                          "cellsReturned: 10");

        // Add a regular row for the partition with the previous static row.
        coordinator.execute(format("INSERT INTO %s.%s (k, c, r) VALUES (0, 0, 0)"), ALL);
        mark = node.logs().mark();
        rows = coordinator.execute(query, ALL);
        Assertions.assertThat(rows).hasNumberOfRows(1);
        assertLogsContain(mark, node, loggedQuery,
                          "SAI slow query metrics:",
                          "keysFetched: 1",
                          "partitionsFetched: 1",
                          "partitionsReturned: 1",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 1",
                          "rowsReturned: 1",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 11",
                          "cellsReturned: 11");

        // Add a second regular row for the previous partition.
        coordinator.execute(format("INSERT INTO %s.%s (k, c, r) VALUES (0, 1, 0)"), ALL);
        mark = node.logs().mark();
        rows = coordinator.execute(query, ALL);
        Assertions.assertThat(rows).hasNumberOfRows(2);
        assertLogsContain(mark, node, loggedQuery,
                          "SAI slow query metrics:",
                          "keysFetched: 1",
                          "partitionsFetched: 1",
                          "partitionsReturned: 1",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 2",
                          "rowsReturned: 2",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 12",
                          "cellsReturned: 12");

        // Remove a cell from a regular row.
        coordinator.execute(format("INSERT INTO %s.%s (k, c, r) VALUES (0, 1, null)"), ALL);
        mark = node.logs().mark();
        rows = coordinator.execute(query, ALL);
        Assertions.assertThat(rows).hasNumberOfRows(2);
        assertLogsContain(mark, node, loggedQuery,
                          "SAI slow query metrics:",
                          "keysFetched: 1",
                          "partitionsFetched: 1",
                          "partitionsReturned: 1",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 2",
                          "rowsReturned: 2",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 12",
                          "cellsReturned: 12");

        // Remove some cells from the static row.
        coordinator.execute(format("UPDATE %s.%s SET l = l - [1] WHERE k = 0"), ALL);
        coordinator.execute(format("UPDATE %s.%s SET s = s - {1} WHERE k = 0"), ALL);
        coordinator.execute(format("UPDATE %s.%s SET m = m - {1} WHERE k = 0"), ALL);
        mark = node.logs().mark();
        rows = coordinator.execute(query, ALL);
        Assertions.assertThat(rows).hasNumberOfRows(2);
        assertLogsContain(mark, node, loggedQuery,
                          "SAI slow query metrics:",
                          "keysFetched: 1",
                          "partitionsFetched: 1",
                          "partitionsReturned: 1",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 2",
                          "rowsReturned: 2",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 12",
                          "cellsReturned: 12");

        // Add another partition, without statics
        coordinator.execute(format("INSERT INTO %s.%s (k, c, r) VALUES (1, 1, 0)"), ALL);
        coordinator.execute(format("INSERT INTO %s.%s (k, c, r) VALUES (1, 2, 0)"), ALL);
        mark = node.logs().mark();
        rows = coordinator.execute(query, ALL);
        Assertions.assertThat(rows).hasNumberOfRows(2);
        assertLogsContain(mark, node, loggedQuery,
                          "SAI slow query metrics:",
                          "keysFetched: 1",
                          "partitionsFetched: 1",
                          "partitionsReturned: 1",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 2",
                          "rowsReturned: 2",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 12",
                          "cellsReturned: 12");

        // Add some static cell to the new partition, so it get reachable by the index
        coordinator.execute(format("INSERT INTO %s.%s (k, v, l) VALUES (1, 0, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])"), ALL);
        mark = node.logs().mark();
        rows = coordinator.execute(query, ALL);
        Assertions.assertThat(rows).hasNumberOfRows(4);
        assertLogsContain(mark, node, loggedQuery,
                          "SAI slow query metrics:",
                          "keysFetched: 2",
                          "partitionsFetched: 2",
                          "partitionsReturned: 2",
                          "partitionTombstonesFetched: 0",
                          "rowsFetched: 4",
                          "rowsReturned: 4",
                          "rowTombstonesFetched: 0",
                          "cellsFetched: 25",
                          "cellsReturned: 25");
    }

    private static void assertLogsDoNotContainSAIExecutionInfo(long mark, IInvokableInstance node)
    {
        assertLogsDoNotContain(mark, node,
                               "SAI slow query metrics:",
                               "SAI slow query plan:",
                               "SAI slowest query metrics:",
                               "SAI slowest query plan:");
    }

    /**
     * ByteBuddy interceptor to slow down SAI queries so they are logged as slow.
     */
    public static class BB
    {
        static AtomicInteger queryDelay = new AtomicInteger(SLOW_QUERY_LOG_TIMEOUT_IN_MS * 2);

        @SuppressWarnings("resource")
        public static void install(ClassLoader classLoader, int node)
        {
            new ByteBuddy().rebase(ReadCommand.class)
                           .method(named("executeLocally"))
                           .intercept(MethodDelegation.to(SlowSAIQueryLoggerTest.BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController,
                                                                 @SuperCall Callable<UnfilteredPartitionIterator> zuper)
        {
            if (executionController.metadata().keyspace.equals(KEYSPACE))
                Uninterruptibles.sleepUninterruptibly(queryDelay.get(), TimeUnit.MILLISECONDS);
            try
            {
                return zuper.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
