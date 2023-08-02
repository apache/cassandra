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
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
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
    @SuppressWarnings("SingleCharacterStringConcatenation")
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "compactionstats");
        tool.assertOnCleanExit();

        String help = "NAME\n" +
                      "        nodetool compactionstats - Print statistics on compactions\n" +
                      "\n" +
                      "SYNOPSIS\n" +
                      "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                      "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                      "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                      "                [(-u <username> | --username <username>)] compactionstats\n" +
                      "                [(-H | --human-readable)] [(-V | --vtable)]\n" +
                      "\n" +
                      "OPTIONS\n" +
                      "        -h <host>, --host <host>\n" +
                      "            Node hostname or ip address\n" +
                      "\n" +
                      "        -H, --human-readable\n" +
                      "            Display bytes in human readable form, i.e. KiB, MiB, GiB, TiB\n" +
                      "\n" +
                      "        -p <port>, --port <port>\n" +
                      "            Remote jmx agent port number\n" +
                      "\n" +
                      "        -pp, --print-port\n" +
                      "            Operate in 4.0 mode with hosts disambiguated by port number\n" +
                      "\n" +
                      "        -pw <password>, --password <password>\n" +
                      "            Remote jmx agent password\n" +
                      "\n" +
                      "        -pwf <passwordFilePath>, --password-file <passwordFilePath>\n" +
                      "            Path to the JMX password file\n" +
                      "\n" +
                      "        -u <username>, --username <username>\n" +
                      "            Remote jmx agent username\n" +
                      "\n" +
                      "        -V, --vtable\n" +
                      "            Display fields matching vtable output\n" +
                      "\n" +
                      "\n";
        assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testCompactionStats()
    {
        createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        long bytesCompacted = 123;
        long bytesTotal = 123456;
        TimeUUID compactionId = nextTimeUUID();
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
        String expectedStatsPattern = String.format("%s\\s+%s\\s+%s\\s+%s\\s+%s\\s+%s\\s+%s\\s+%.2f%%",
                                                    compactionId, OperationType.COMPACTION, CQLTester.KEYSPACE, currentTable(), bytesCompacted, bytesTotal,
                                                    CompactionInfo.Unit.BYTES, (double) bytesCompacted / bytesTotal * 100);
        assertThat(stdout).containsPattern(expectedStatsPattern);

        assertThat(stdout).containsPattern(expectedStatsPattern);
        assertThat(stdout).containsPattern("concurrent compactors\\s+[0-9]*");
        assertThat(stdout).containsPattern("pending tasks\\s+[0-9]*");
        assertThat(stdout).containsPattern("compactions completed\\s+[0-9]*");
        assertThat(stdout).containsPattern("data compacted\\s+[0-9]*");
        assertThat(stdout).containsPattern("compactions aborted\\s+[0-9]*");
        assertThat(stdout).containsPattern("compactions reduced\\s+[0-9]*");
        assertThat(stdout).containsPattern("sstables dropped from compaction\\s+[0-9]*");
        assertThat(stdout).containsPattern("15 minute rate\\s+[0-9]*.[0-9]*[0-9]*/minute");
        assertThat(stdout).containsPattern("mean rate\\s+[0-9]*.[0-9]*[0-9]*/hour");
        assertThat(stdout).containsPattern("compaction throughput \\(MiB/s\\)\\s+throttling disabled \\(0\\)");

        CompactionManager.instance.active.finishCompaction(compactionHolder);
        waitForNumberOfPendingTasks(0, "compactionstats");
    }

    @Test
    public void testCompactionStatsVtable()
    {
        createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        long bytesCompacted = 123;
        long bytesTotal = 123456;
        TimeUUID compactionId = nextTimeUUID();
        List<SSTableReader> sstables = IntStream.range(0, 10)
                                                .mapToObj(i -> MockSchema.sstable(i, i * 10L, i * 10L + 9, cfs))
                                                .collect(Collectors.toList());
        String targetDirectory = "/some/dir/" + cfs.metadata.keyspace + '/' + cfs.metadata.name + '-' + cfs.metadata.id.asUUID();
        CompactionInfo.Holder compactionHolder = new CompactionInfo.Holder()
        {
            public CompactionInfo getCompactionInfo()
            {
                return new CompactionInfo(cfs.metadata(), OperationType.COMPACTION, bytesCompacted, bytesTotal, compactionId, sstables, targetDirectory);
            }

            public boolean isGlobal()
            {
                return false;
            }
        };

        CompactionInfo.Holder nonCompactionHolder = new CompactionInfo.Holder()
        {
            public CompactionInfo getCompactionInfo()
            {
                return new CompactionInfo(cfs.metadata(), OperationType.CLEANUP, bytesCompacted, bytesTotal, compactionId, sstables);
            }

            public boolean isGlobal()
            {
                return false;
            }
        };

        CompactionManager.instance.active.beginCompaction(compactionHolder);
        CompactionManager.instance.active.beginCompaction(nonCompactionHolder);
        String stdout = waitForNumberOfPendingTasks(2, "compactionstats", "-V");
        assertThat(stdout).containsPattern("keyspace\\s+table\\s+task id\\s+completion ratio\\s+kind\\s+progress\\s+sstables\\s+total\\s+unit\\s+target directory");
        String expectedStatsPattern = String.format("%s\\s+%s\\s+%s\\s+%.2f%%\\s+%s\\s+%s\\s+%s\\s+%s\\s+%s\\s+%s",
                                                    CQLTester.KEYSPACE, currentTable(), compactionId, (double) bytesCompacted / bytesTotal * 100,
                                                    OperationType.COMPACTION, bytesCompacted, sstables.size(), bytesTotal, CompactionInfo.Unit.BYTES,
                                                    targetDirectory);
        assertThat(stdout).containsPattern(expectedStatsPattern);

        String expectedStatsPatternForNonCompaction = String.format("%s\\s+%s\\s+%s\\s+%.2f%%\\s+%s\\s+%s\\s+%s\\s+%s\\s+%s",
                                                                    CQLTester.KEYSPACE, currentTable(), compactionId, (double) bytesCompacted / bytesTotal * 100,
                                                                    OperationType.COMPACTION, bytesCompacted, sstables.size(), bytesTotal, CompactionInfo.Unit.BYTES);
        assertThat(stdout).containsPattern(expectedStatsPatternForNonCompaction);

        CompactionManager.instance.active.finishCompaction(compactionHolder);
        CompactionManager.instance.active.finishCompaction(nonCompactionHolder);
        waitForNumberOfPendingTasks(0, "compactionstats", "-V");
    }

    @Test
    public void testCompactionStatsHumanReadable()
    {
        createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        long bytesCompacted = 123;
        long bytesTotal = 123456;
        TimeUUID compactionId = nextTimeUUID();
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
        String stdout = waitForNumberOfPendingTasks(1, "compactionstats", "--human-readable");
        assertThat(stdout).containsPattern("id\\s+compaction type\\s+keyspace\\s+table\\s+completed\\s+total\\s+unit\\s+progress");
        String expectedStatsPattern = String.format("%s\\s+%s\\s+%s\\s+%s\\s+%s\\s+%s\\s+%s\\s+%.2f%%",
                                                    compactionId, OperationType.COMPACTION, CQLTester.KEYSPACE, currentTable(), "123 bytes", "120.56 KiB",
                                                    CompactionInfo.Unit.BYTES, (double) bytesCompacted / bytesTotal * 100);
        assertThat(stdout).containsPattern(expectedStatsPattern);

        CompactionManager.instance.active.finishCompaction(compactionHolder);
        waitForNumberOfPendingTasks(0, "compactionstats", "--human-readable");
    }

    @Test
    public void testCompactionStatsVtableHumanReadable()
    {
        createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        long bytesCompacted = 123;
        long bytesTotal = 123456;
        TimeUUID compactionId = nextTimeUUID();
        List<SSTableReader> sstables = IntStream.range(0, 10)
                                                .mapToObj(i -> MockSchema.sstable(i, i * 10L, i * 10L + 9, cfs))
                                                .collect(Collectors.toList());
        String targetDirectory = "/some/dir/" + cfs.metadata.keyspace + '/' + cfs.metadata.name + '-' + cfs.metadata.id.asUUID();
        CompactionInfo.Holder compactionHolder = new CompactionInfo.Holder()
        {
            public CompactionInfo getCompactionInfo()
            {
                return new CompactionInfo(cfs.metadata(), OperationType.COMPACTION, bytesCompacted, bytesTotal, compactionId, sstables, targetDirectory);
            }

            public boolean isGlobal()
            {
                return false;
            }
        };

        CompactionInfo.Holder nonCompactionHolder = new CompactionInfo.Holder()
        {
            public CompactionInfo getCompactionInfo()
            {
                return new CompactionInfo(cfs.metadata(), OperationType.CLEANUP, bytesCompacted, bytesTotal, compactionId, sstables);
            }

            public boolean isGlobal()
            {
                return false;
            }
        };

        CompactionManager.instance.active.beginCompaction(compactionHolder);
        CompactionManager.instance.active.beginCompaction(nonCompactionHolder);
        String stdout = waitForNumberOfPendingTasks(2, "compactionstats", "--vtable", "--human-readable");
        assertThat(stdout).containsPattern("keyspace\\s+table\\s+task id\\s+completion ratio\\s+kind\\s+progress\\s+sstables\\s+total\\s+unit\\s+target directory");
        String expectedStatsPattern = String.format("%s\\s+%s\\s+%s\\s+%.2f%%\\s+%s\\s+%s\\s+%s\\s+%s\\s+%s\\s+%s",
                                                    CQLTester.KEYSPACE, currentTable(), compactionId, (double) bytesCompacted / bytesTotal * 100,
                                                    OperationType.COMPACTION, "123 bytes", sstables.size(), "120.56 KiB", CompactionInfo.Unit.BYTES,
                                                    targetDirectory);
        assertThat(stdout).containsPattern(expectedStatsPattern);
        String expectedStatsPatternForNonCompaction = String.format("%s\\s+%s\\s+%s\\s+%.2f%%\\s+%s\\s+%s\\s+%s\\s+%s\\s+%s",
                                                                    CQLTester.KEYSPACE, currentTable(), compactionId, (double) bytesCompacted / bytesTotal * 100,
                                                                    OperationType.CLEANUP, "123 bytes", sstables.size(), "120.56 KiB", CompactionInfo.Unit.BYTES);
        assertThat(stdout).containsPattern(expectedStatsPatternForNonCompaction);

        CompactionManager.instance.active.finishCompaction(compactionHolder);
        CompactionManager.instance.active.finishCompaction(nonCompactionHolder);
        waitForNumberOfPendingTasks(0, "compactionstats", "--vtable", "--human-readable");
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