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

package org.apache.cassandra.tools.nodetool;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.tools.ToolRunner;

import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class CompactionStatsTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testCompactionStats()
    {
        createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        long bytesCompacted = 123;
        long bytesTotal = 123456;
        UUID compactionId = randomUUID();
        List<SSTableReader> sstables = IntStream.range(0, 10)
                                                .mapToObj(i -> MockSchema.sstable(i, i * 10L, i * 10L + 9, cfs))
                                                .collect(Collectors.toList());
        CompactionInfo.Holder compactionHolder = new CompactionInfo.Holder()
        {
            public CompactionInfo getCompactionInfo()
            {
                return new CompactionInfo(cfs.metadata(), OperationType.COMPACTION, bytesCompacted, bytesTotal, compactionId, sstables);
            }

            public boolean isGlobal()
            {
                return false;
            }
        };

        CompactionManager.instance.active.beginCompaction(compactionHolder);
        String stdout = waitForNumberOfPendingTasks(1, "compactionstats");
        assertThat(stdout).containsPattern("id\\s+compaction type\\s+keyspace\\s+table\\s+completed\\s+total\\s+unit\\s+progress");
        String expectedStatsPattern = format("%s\\s+%s\\s+%s\\s+%s\\s+%s\\s+%s\\s+%s\\s+%.2f%%",
                                             compactionId, OperationType.COMPACTION, CQLTester.KEYSPACE, currentTable(), bytesCompacted, bytesTotal,
                                             CompactionInfo.Unit.BYTES, (double) bytesCompacted / bytesTotal * 100);

        assertThat(stdout).containsPattern(expectedStatsPattern);
        assertThat(stdout).containsPattern("concurrent compactors\\s+[0-9]*");
        assertThat(stdout).containsPattern("pending tasks\\s+[0-9]*");
        assertThat(stdout).containsPattern("compactions completed\\s+[0-9]*");
        assertThat(stdout).containsPattern("minute rate\\s+[0-9]*.[0-9]*[0-9]*/second");
        assertThat(stdout).containsPattern("5 minute rate\\s+[0-9]*.[0-9]*[0-9]*/second");
        assertThat(stdout).containsPattern("15 minute rate\\s+[0-9]*.[0-9]*[0-9]*/second");
        assertThat(stdout).containsPattern("mean rate\\s+[0-9]*.[0-9]*[0-9]*/second");
        assertThat(stdout).containsPattern("compaction throughput \\(MBps\\)\\s+throttling disabled \\(0\\)");
        assertThat(stdout).containsPattern("compactions completed\\s+[0-9]*");
        assertThat(stdout).containsPattern("data compacted\\s+[0-9]*");
        assertThat(stdout).containsPattern("compactions aborted\\s+[0-9]*");
        assertThat(stdout).containsPattern("compactions reduced\\s+[0-9]*");
        assertThat(stdout).containsPattern("sstables dropped from compaction\\s+[0-9]*");

        CompactionManager.instance.active.finishCompaction(compactionHolder);
        waitForNumberOfPendingTasks(0, "compactionstats");
    }

    private String waitForNumberOfPendingTasks(int pendingTasksToWaitFor, String... args)
    {
        AtomicReference<String> stdout = new AtomicReference<>();
        await().until(() -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool(args);
            tool.assertOnCleanExit();
            String output = tool.getStdout();
            stdout.set(output);

            try
            {
                assertThat(output).containsPattern("pending tasks\\s+" + pendingTasksToWaitFor);
                return true;
            }
            catch (AssertionError e)
            {
                return false;
            }
        });

        return stdout.get();
    }
}
