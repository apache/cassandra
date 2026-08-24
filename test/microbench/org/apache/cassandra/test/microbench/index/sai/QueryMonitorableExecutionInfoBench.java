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

package org.apache.cassandra.test.microbench.index.sai;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.monitoring.Monitorable;
import org.apache.cassandra.db.monitoring.MonitoringTask;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.sai.SAITester;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmarks SAI's extended {@link org.apache.cassandra.index.sai.plan.QueryMonitorableExecutionInfo},
 * to verify whether it has a noticeable performance impact compared to the default {@link Monitorable.ExecutionInfo}.
 * </p>
 * This should be evaluated on the context of a slow query, which by default should have taken at least 500ms,
 * possibly more, as defined by {@link org.apache.cassandra.config.Config#slow_query_log_timeout_in_ms}.
 * </p>
 * Note that this is a worst case scenario, as repeated slow queries within the same reporting window will be aggregated
 * and not logged individually.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = {
"-Xmx512M",
"-Dcassandra.disable_user_defined_functions=true",
"--add-exports", "java.base/jdk.internal.ref=ALL-UNNAMED",
"--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED",
"--add-opens", "java.base/jdk.internal.ref=ALL-UNNAMED"
})
@Threads(1)
@State(Scope.Benchmark)
public class QueryMonitorableExecutionInfoBench extends SAITester
{
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryMonitorableExecutionInfoBench.class);
    private static final Random RANDOM = new Random(1234);

    /**
     * Whether to enable the extended SAI execution info logging.
     */
    @Param({ "false", "true" })
    public boolean enabled;

    /**
     * Size of the written collections, used to control the number of cells per row.
     */
    @Param({ "1", "100" })
    public int collectionSize;

    private ReadCommand command;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        CassandraRelevantProperties.SAI_MONITORING_EXECUTION_INFO_ENABLED.setBoolean(enabled);
        CQLTester.setUpClass();
        CQLTester.prepareServer();
        beforeTest();

        // create the schema
        createTable("CREATE TABLE %s (k int, c int, n int, b bigint, s text, v vector<float, 2>, l list<int>, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(l) USING 'StorageAttachedIndex'");

        // insert some data
        for (int k = 0; k < 100; k++)
        {
            for (int c = 0; c < 100; c++)
            {
                int n = RANDOM.nextInt(100);
                List<Integer> l = new ArrayList<>(collectionSize);
                for (int i = 0; i < collectionSize; i++)
                    l.add(RANDOM.nextInt());
                execute("INSERT INTO %s (k, c, n, b, s, v, l) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        k, c, n, (long) n, "value_" + n, vector(1, n), l);
            }
        }
        flush();
    }

    @Setup(Level.Invocation)
    public void setupTest()
    {
        command = generateRunAndConsumeCommand();
    }

    private ReadCommand generateRunAndConsumeCommand()
    {
        int n = RANDOM.nextInt(100);
        String query = "SELECT * FROM %%s WHERE n = %d AND (b >= %<d OR s = 'value_%<d') ORDER BY v ANN OF [1, %<d] LIMIT 10";
        ReadCommand command = parseReadCommand(String.format(query, n));
        try (UnfilteredPartitionIterator partitions = command.executeLocally(command.executionController(false)))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    while (partition.hasNext())
                        partition.next();
                }
            }
        }
        return command;
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        CommitLog.instance.shutdownBlocking();
        CQLTester.cleanup();
    }

    /**
     * Test the cost of creating the execution info object.
     */
    @Benchmark
    public Object executionInfo()
    {
        Monitorable.ExecutionInfo info = command.executionInfo();
        return info.toLogString(true);
    }

    /**
     * Test the cost of creating a slow operation and logging it.
     */
    @Benchmark
    public Object logSlowOperation()
    {
        MonitoringTask.SlowOperation operation = new MonitoringTask.SlowOperation(command, 0);
        String log = operation.getLogMessage();
        LOGGER.info(log);
        return log;
    }

    /**
     * Test the combined cost of running a slow query that takes {@code slow_query_log_timeout_in_ms} and reporting it
     * as slow in logs.
     */
    @Benchmark
    @SuppressWarnings("StatementWithEmptyBody")
    public Object slowQuery()
    {
        // Simulate the minimum query time that would trigger the slow query logger.
        // This is a worst case scenario, since queries can and generally will take longer than the default 500ms,
        // which would dilute the cost of logging compared to the query time.
        long minQueryTime = DatabaseDescriptor.getRawConfig().slow_query_log_timeout_in_ms;
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < minQueryTime)
        {
            // busy wait
        }

        return logSlowOperation();
    }

    /**
     * Test the cost of running a query without logging it as slow, just to see the effect of counting things in case
     * it is slow.
     */
    @Benchmark
    public Object runFastOperation()
    {
        return generateRunAndConsumeCommand();
    }
}
