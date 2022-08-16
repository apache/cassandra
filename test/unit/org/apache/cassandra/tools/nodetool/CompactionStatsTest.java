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

import java.util.ArrayList;
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
import org.awaitility.Awaitility;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.assertj.core.api.Assertions.assertThat;

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

        String help =   "NAME\n" +
                "        nodetool compactionstats - Print statistics on compactions\n" +
                "\n" +
                "SYNOPSIS\n" +
                "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                "                [(-u <username> | --username <username>)] compactionstats\n" +
                "                [(-H | --human-readable)] [(-v | --verbose)] [(-V | --vtable)]\n" +
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
                "        -v, --verbose\n" +
                "            Sort and display sstable paths for the compactions\n" +
                "\n" +
                "        -V, --vtable\n" +
                "            Display fields matching vtable output\n" +
                "\n" +
                "\n";
        assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testVerboseCompactionStats()
    {
        List<CompactionInfo.Holder> compactionHolders = new ArrayList<>();
        List<String> expectedStatsPatterns = new ArrayList<>();

        int entry = 0;
        for (int i = 0; i < 5; i++)
        {
            String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");

            for (int j = 0; j < 5; j++)
            {
                entry++;
                createTable(keyspace, "CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))");

                ColumnFamilyStore cfs = getCurrentColumnFamilyStore(keyspace);

                long bytesCompacted = 123;
                long bytesTotal = 123456;
                TimeUUID compactionId = nextTimeUUID();
                List<SSTableReader> sstables = IntStream.range(0, 10)
                                                        .mapToObj(k -> MockSchema.sstable(k, k * 10L, k * 10L + 9, cfs))
                                                        .collect(Collectors.toList());
                final int dataDir = i;
                CompactionInfo.Holder compactionHolder = new CompactionInfo.Holder()
                {
                    public CompactionInfo getCompactionInfo()
                    {
                        return new CompactionInfo(cfs.metadata(), OperationType.COMPACTION, bytesCompacted, bytesTotal, compactionId, sstables,
                                                  String.format("/my/data/dir%s/%s/%s-%s", dataDir, cfs.metadata.keyspace, cfs.metadata.name, cfs.metadata.id.toString()));
                    }

                    public boolean isGlobal()
                    {
                        return false;
                    }
                };

                compactionHolders.add(compactionHolder);

                String expectedStatsPattern = String.format("%s\\s+%s\\s+%s\\s+%s\\s+%s\\s+%s\\s+%s\\s+%.2f%%",
                                                            compactionHolder.getCompactionInfo().getTaskId(), OperationType.COMPACTION, keyspace,
                                                            currentTable(), bytesCompacted, bytesTotal,
                                                            CompactionInfo.Unit.BYTES, (double) bytesCompacted / bytesTotal * 100);

                expectedStatsPatterns.add(expectedStatsPattern);

                // add some other, non-compaction, operation too

                if (entry % 2 == 0)
                {
                    OperationType operationType = OperationType.values()[entry % (OperationType.values().length - 1)];
                    CompactionInfo.Holder nonCompactionHolder = new CompactionInfo.Holder()
                    {
                        public CompactionInfo getCompactionInfo()
                        {
                            return CompactionInfo.withoutSSTables(cfs.metadata(), operationType, bytesCompacted, bytesTotal, CompactionInfo.Unit.BYTES, compactionId);
                        }

                        public boolean isGlobal()
                        {
                            return false;
                        }
                    };

                    compactionHolders.add(nonCompactionHolder);

                    String expectedStatsPattern2 = String.format("%s\\s+%s\\s+%s\\s+%s\\s+%s\\s+%s\\s+%s\\s+%.2f%%",
                                                                 compactionHolder.getCompactionInfo().getTaskId(), operationType, keyspace,
                                                                 currentTable(), bytesCompacted, bytesTotal,
                                                                 CompactionInfo.Unit.BYTES, (double) bytesCompacted / bytesTotal * 100);

                    expectedStatsPatterns.add(expectedStatsPattern2);
                }
            }
        }

        compactionHolders.forEach(CompactionManager.instance.active::beginCompaction);

        String stdout = waitForNumberOfPendingTasks(compactionHolders.size(), "compactionstats", "--verbose");
        String stdout2 = waitForNumberOfPendingTasks(compactionHolders.size(), "compactionstats", "--verbose");
        assertThat(stdout).isEqualTo(stdout2);
        assertThat(stdout).containsPattern("id\\s+compaction type\\s+keyspace\\s+table\\s+completed\\s+total\\s+unit\\s+progress");

        for (String expectedStatsPattern : expectedStatsPatterns)
        {
            assertThat(stdout).containsPattern(expectedStatsPattern);
        }

        compactionHolders.forEach(CompactionManager.instance.active::finishCompaction);

        waitForNumberOfPendingTasks(0, "compactionstats");
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
                return new CompactionInfo(cfs.metadata(), OperationType.COMPACTION, bytesCompacted, bytesTotal, compactionId, sstables,
                                          targetDirectory);
            }

            public boolean isGlobal()
            {
                return false;
            }
        };

        TimeUUID nonCompactionId = nextTimeUUID();
        CompactionInfo.Holder nonCompactionHolder = new CompactionInfo.Holder()
        {
            public CompactionInfo getCompactionInfo()
            {
                return CompactionInfo.withoutSSTables(cfs.metadata(), OperationType.CLEANUP, bytesCompacted, bytesTotal, CompactionInfo.Unit.BYTES, nonCompactionId);
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
            OperationType.COMPACTION, bytesCompacted, sstables.size(), bytesTotal, CompactionInfo.Unit.BYTES, targetDirectory);
        assertThat(stdout).containsPattern(expectedStatsPattern);

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
                return new CompactionInfo(cfs.metadata(), OperationType.COMPACTION, bytesCompacted, bytesTotal, compactionId, sstables,
                                          targetDirectory);
            }

            public boolean isGlobal()
            {
                return false;
            }
        };

        CompactionManager.instance.active.beginCompaction(compactionHolder);

        String stdout = waitForNumberOfPendingTasks(1,"compactionstats", "--vtable", "--human-readable");

        assertThat(stdout).containsPattern("keyspace\\s+table\\s+task id\\s+completion ratio\\s+kind\\s+progress\\s+sstables\\s+total\\s+unit\\s+target directory");
        String expectedStatsPattern = String.format("%s\\s+%s\\s+%s\\s+%.2f%%\\s+%s\\s+%s\\s+%s\\s+%s\\s+%s\\s+%s",
            CQLTester.KEYSPACE, currentTable(), compactionId, (double) bytesCompacted / bytesTotal * 100,
            OperationType.COMPACTION, "123 bytes", sstables.size(), "120.56 KiB", CompactionInfo.Unit.BYTES, targetDirectory);
        assertThat(stdout).containsPattern(expectedStatsPattern);

        CompactionManager.instance.active.finishCompaction(compactionHolder);

        waitForNumberOfPendingTasks(0,"compactionstats", "--vtable", "--human-readable");
    }

    private String waitForNumberOfPendingTasks(int pendingTasksToWaitFor, String... args)
    {
        AtomicReference<String> stdout = new AtomicReference<>();
        Awaitility.await().until(() -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool(args);
            tool.assertOnCleanExit();
            String output = tool.getStdout();
            stdout.set(output);
            return output.contains("pending tasks: " + pendingTasksToWaitFor);
        });

        return stdout.get();
    }
}
