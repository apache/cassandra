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

package org.apache.cassandra.db.compaction.differential;

import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertTrue;

/**
 * A size-capped CompactionAwareWriter switches output sstables at partition boundaries, so one
 * compaction writes several. CursorCompactionPipeline and IteratorCompactionPipeline both call
 * task.getCompactionAwareWriter, so both receive the same writer type. A divergent switch
 * decision then shows up as a different output count. A divergent per-output finalization
 * (first and last keys, promoted index, stats) shows up as a component diff.
 */
public class MultiOutputDifferentialCompactionTest extends DifferentialCompactionTester
{

    private static TaskFactory sizeCapped(long maxBytes)
    {
        return (cfs, txn, gcBefore) -> new CompactionTask(cfs, txn, gcBefore, true /* keepOriginals */)
        {
            @Override
            public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                                  Directories directories,
                                                                  ILifecycleTransaction transaction,
                                                                  Set<SSTableReader> nonExpiredSSTables)
            {
                return new MaxSSTableSizeWriter(cfs, directories, transaction, nonExpiredSSTables,
                                                maxBytes, 0, true /* keepOriginals */);
            }
        };
    }

    @Test
    public void manyPartitionsSplitAcrossOutputs() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String padding = "x".repeat(100);
        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < 40; pk++)
                for (long ck = 0; ck < 10; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, padding + round + "-" + ck);
            flush();
        }

        // The 40 partitions hold about 1KB each, so an 8KB cap writes several outputs and puts
        // the switch points mid-stream.
        CapturedOutput out = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), sizeCapped(8 * 1024));
        assertTrue("scenario must produce multiple outputs to test anything, got " + out.sstables.size(),
                   out.sstables.size() >= 2);
    }

    @Test
    public void tombstonesAndStaticsAcrossOutputs() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, s text static, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000 AND compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String padding = "y".repeat(100);
        for (long pk = 0; pk < 30; pk++)
        {
            if (pk % 3 == 0)
                execute("INSERT INTO %s (pk, s, ck, v) VALUES (?, ?, ?, ?)", pk, "static" + pk, 0L, padding);
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, padding + ck);
        }
        flush();

        for (long pk = 0; pk < 30; pk += 2)
            execute("DELETE FROM %s WHERE pk = ? AND ck >= 3 AND ck < 7", pk);
        execute("DELETE FROM %s WHERE pk = 5");
        execute("DELETE FROM %s WHERE pk = 15");
        flush();

        CapturedOutput out = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), sizeCapped(8 * 1024));
        assertTrue("scenario must produce multiple outputs to test anything, got " + out.sstables.size(),
                   out.sstables.size() >= 2);
    }

    @Test
    public void widePartitionsForceFrequentSwitches() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String padding = "z".repeat(300);
        // Each partition holds about 6KB, which is more than half the 8KB cap, so nearly every
        // partition triggers a switch decision.
        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < 12; pk++)
                for (long ck = 0; ck < 20; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, padding + round);
            execute("DELETE FROM %s WHERE pk = ? AND ck >= 5 AND ck < 15", (long) round);
            flush();
        }

        CapturedOutput out = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), sizeCapped(8 * 1024));
        assertTrue("scenario must produce multiple outputs to test anything, got " + out.sstables.size(),
                   out.sstables.size() >= 2);
    }
}
