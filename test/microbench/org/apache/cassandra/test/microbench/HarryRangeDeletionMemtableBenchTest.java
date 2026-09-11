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

package org.apache.cassandra.test.microbench;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collections;

import com.sun.management.ThreadMXBean;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.execution.CQLTesterVisitExecutor;
import org.apache.cassandra.harry.execution.CQLVisitExecutor;
import org.apache.cassandra.harry.execution.DataTracker;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.harry.op.Visit;

import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

/**
 * Many range deletions into one unflushed partition, generated and validated by Harry and executed
 * through the regular write path (memtable merge). Reports wall time and bytes allocated on the
 * executing thread for the deletion phase; the trailing full-partition select is validated by
 * {@link QuiescentChecker} against the model, so a wrong merge fails the test rather than the timing.
 */
public class HarryRangeDeletionMemtableBenchTest extends CQLTester
{
    private static final int[] DELETIONS = { 1000, 4000, 16000 };
    private static final int ROWS = 200_000;

    @Test
    public void measureRangeDeletionsIntoOnePartition()
    {
        ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        StringBuilder report = new StringBuilder("\nRANGE_DELETION_BENCH deletions,disjoint,ms,allocated_mb\n");
        for (boolean disjoint : new boolean[]{ true, false })
        {
            for (int deletions : DELETIONS)
            {
                long[] result = run(threads, deletions, disjoint);
                report.append(String.format("RANGE_DELETION_BENCH %d,%s,%d,%.1f%n", deletions, disjoint, result[0], result[1] / 1048576.0));
            }
        }
        System.out.print(report);
        logger.info(report.toString());
    }

    private long[] run(ThreadMXBean threads, int deletions, boolean disjoint)
    {
        long[] result = new long[2];
        withRandom(205413964293041L, rng -> {
            String keyspace = "rd_bench_ks";
            String table = "rd_bench_" + deletions + (disjoint ? "d" : "o");
            SchemaSpec schema = new SchemaSpec(rng.next(), ROWS, keyspace, table,
                                               Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.int64Type)),
                                               Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.int64Type, false)),
                                               Arrays.asList(ColumnSpec.regularColumn("r1", ColumnSpec.int64Type)),
                                               Collections.emptyList());
            schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", keyspace));
            createTable(schema.compile());

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);
            // A handful of rows so the partition is not deletion-only.
            for (int i = 0; i < 16; i++)
                history.insert(0, rng.nextInt(0, ROWS));
            int width = ROWS / deletions;
            for (int i = 0; i < deletions; i++)
            {
                int lower = disjoint ? i * width : rng.nextInt(0, ROWS - 2);
                int upper = disjoint ? lower + width - 2 : rng.nextInt(lower + 1, ROWS - 1);
                history.deleteRowRange(0, lower, upper, 0, true, true);
            }
            history.selectPartition(0);

            DataTracker tracker = new DataTracker.SequentialDataTracker();
            CQLVisitExecutor executor = new CQLTesterVisitExecutor(schema, tracker,
                                                                   new QuiescentChecker(schema.valueGenerators, tracker, history),
                                                                   statement -> execute(statement.cql(), statement.bindings()));
            long tid = Thread.currentThread().getId();
            long allocatedBefore = threads.getThreadAllocatedBytes(tid);
            long start = System.nanoTime();
            Visit last = null;
            for (Visit visit : history)
            {
                if (last != null)
                    executor.execute(last);
                last = visit;
            }
            result[0] = (System.nanoTime() - start) / 1_000_000;
            result[1] = threads.getThreadAllocatedBytes(tid) - allocatedBefore;
            // The final visit is the full-partition select validated against the model; excluded from the timing.
            executor.execute(last);
        });
        return result;
    }
}
