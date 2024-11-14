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

package org.apache.cassandra.db.compaction.unified;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.TimeUUID;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(BMUnitRunner.class)
@BMUnitConfig(debug=true)
@BMRule(
name = "Get stats before task completion",
targetClass = "org.apache.cassandra.db.compaction.ActiveCompactions",
targetMethod = "finishCompaction",
targetLocation = "AT ENTRY",
action = "org.apache.cassandra.db.compaction.unified.BackgroundCompactionTrackingTest.getStats()"
)
public class BackgroundCompactionTrackingTest extends CQLTester
{
    // Get rid of commitlog noise
    @Before
    public void disableCommitlog()
    {
        schemaChange("ALTER KEYSPACE " + KEYSPACE + " WITH durable_writes = false");
    }
    @After
    public void enableCommitlog()
    {
        schemaChange("ALTER KEYSPACE " + KEYSPACE + " WITH durable_writes = true");
    }

    @Test
    public void testBackgroundCompactionTracking()
    {
        testBackgroundCompactionTracking(false, 3);
    }

    @Test
    public void testBackgroundCompactionTrackingParallelized()
    {
        testBackgroundCompactionTracking(true, 3);
    }

    public void testBackgroundCompactionTracking(boolean parallelize, int shards)
    {
        int bytes_per_row = 5000;
        int partitions = 5000;
        int rows_per_partition = 10;
        long targetSize = 1L * partitions * rows_per_partition * bytes_per_row / shards;
        CompactionManager.instance.setMaximumCompactorThreads(50);
        CompactionManager.instance.setCoreCompactorThreads(50);
        String table = createTable(String.format("CREATE TABLE %%s (k int, t int, v blob, PRIMARY KEY (k, t))" +
                                                 " with compression = {'enabled': false} " +
                                                 " and compaction = {" +
                                                 "'class': 'UnifiedCompactionStrategy', " +
                                                 "'parallelize_output_shards': '%s', " +
                                                 "'base_shard_count': %d, " +
                                                 "'min_sstable_size': '%dB', " + // to disable sharding on flush
                                                 "'scaling_parameters': 'T4, T7'" +
                                   "}",
                                   parallelize, shards, targetSize));
        ColumnFamilyStore cfs = getColumnFamilyStore(KEYSPACE, table);
        cfs.disableAutoCompaction();

        for (int iter = 1; iter <= 5; ++iter)
        {
            byte [] payload = new byte[bytes_per_row];
            new Random(42).nextBytes(payload);
            ByteBuffer b = ByteBuffer.wrap(payload);
            Set<SSTableReader> before = new HashSet<>(cfs.getLiveSSTables());

            for (int i = 0; i < partitions; i++)
            {
                for (int j = 0; j < rows_per_partition; j++)
                    execute(String.format("INSERT INTO %s.%s(k, t, v) VALUES (?, ?, ?)", KEYSPACE, table), i, j, b);

                if ((i + 1) % ((partitions  + 3) / 4) == 0)
                    cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            }
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            operations = new ArrayList<>();
            Set<SSTableReader> newSSTables = new HashSet<>(cfs.getLiveSSTables());
            newSSTables.removeAll(before);
            long uncompressedSize = newSSTables.stream().mapToLong(SSTableReader::uncompressedLength).sum();

            cfs.enableAutoCompaction(true); // since the trigger is hit, this initiates an L0 compaction
            cfs.disableAutoCompaction();

            // Check that the background compactions state is correct during the compaction
            Assert.assertTrue("Byteman rule did not fire", !operations.isEmpty());
            printStats();
            int tasks = parallelize ? shards : 1;
            assertEquals(tasks, operations.size());
            TimeUUID mainOpId = null;
            for (int i = 0; i < operations.size(); ++i)
            {
                BitSet seqs = new BitSet(shards);
                int expectedSize = tasks - i;
                final List<CompactionInfo> ops = getCompactionOps(i, cfs);
                final int size = ops.size();
                int finished = tasks - size;
                assertTrue(size >= expectedSize); // some task may have not managed to close
                for (var op : ops)
                {
                    final TimeUUID opIdSeq0 = op.getTaskId().withSequence(0);
                    if (mainOpId == null)
                        mainOpId = opIdSeq0;
                    else
                        assertEquals(mainOpId, opIdSeq0);
                    seqs.set(op.getTaskId().sequence());

                    if (i == 0)
                        Assert.assertEquals(uncompressedSize * 1.0 / tasks, op.getTotal(), uncompressedSize * 0.03);
                    assertTrue(op.getCompleted() <= op.getTotal() * 1.03);
                    if (op.getCompleted() >= op.getTotal() * 0.97)
                        ++finished;
                }
                assertTrue(finished > i);
                assertEquals(size, seqs.cardinality());
            }
            assertEquals(iter * shards, cfs.getLiveSSTables().size());

            // Check that the background compactions state is correct after the compaction
            operations.clear();
            getStats();
            printStats();
            final List<CompactionInfo> ops = getCompactionOps(0, cfs);
            assertEquals(0, ops.size());
        }
    }

    private static List<CompactionInfo> getCompactionOps(int i, ColumnFamilyStore cfs)
    {
        return operations.get(i)
                         .stream()
                         .filter(op -> op.getTableMetadata() == cfs.metadata())
                         .collect(Collectors.toList());
    }

    private void printStats()
    {
        for (int i = 0; i < operations.size(); ++i)
        {
            System.out.println(operations.get(i).stream().map(Object::toString).collect(Collectors.joining("\n")));
        }
    }

    public static synchronized void getStats()
    {
        operations.add(CompactionManager.instance.getSSTableTasks());
    }

    static List<List<CompactionInfo>> operations;
}
