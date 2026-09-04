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


import org.junit.Test;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Purge-boundary scenarios, plus variants for compression algorithms and for the disk access
 * mode of compaction reads. Purge requires localDeletionTime < gcBefore. Each scenario reads
 * the deletion time from the flushed sstable's stats, then passes gcBefore at, and one past,
 * that boundary.
 */
public class PurgeBoundaryDifferentialCompactionTest extends DifferentialCompactionTester
{

    /** gcBefore at the tombstone's deletion time retains it; gcBefore one past purges it. */
    @Test
    public void boundaryExactlyAtDeletionTime() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 0");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 4; pk++)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 100", pk, ck, "v" + ck);
        flush();
        execute("DELETE FROM %s USING TIMESTAMP 200 WHERE pk = 1");
        for (long ck = 0; ck < 5; ck++)
            execute("DELETE FROM %s USING TIMESTAMP 200 WHERE pk = 2 AND ck = ?", ck);
        flush();

        // an sstable holding any live cell reports the NO_DELETION sentinel as its max local
        // deletion time
        long maxLdt = maxTombstoneLocalDeletionTime(cfs.getLiveSSTables());

        CapturedOutput at = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), DEFAULT_TASK, maxLdt);
        assertTrue("tombstones at exactly gcBefore must be retained",
                   allJson(at).contains("\"marked_deleted\":\"200\""));

        CapturedOutput past = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), DEFAULT_TASK, maxLdt + 1);
        assertFalse("tombstones strictly before gcBefore must purge",
                    allJson(past).contains("\"marked_deleted\":\"200\""));
    }

    /**
     * A purgeable complex deletion must still delete the cells below it.
     *
     * The iterator applies the deletion during the merge, in Row.Merger.ColumnDataReducer, and
     * purges it afterwards, in ComplexColumnData.purge. Code that purges the merged deletion
     * before it shadows the cells resurrects them.
     *
     * The cells and the deletion sit in separate sstables, so the merge applies the deletion, and
     * not the memtable.
     */
    @Test
    public void purgeableComplexDeletionShadowsCells() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 0");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // Data that always survives, so the output is never empty.
        for (long ck = 0; ck < 4; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 100", 0L, ck, "keep" + ck);
        // UPDATE, not INSERT, so these rows hold complex cells and no row liveness.
        for (long ck = 0; ck < 4; ck++)
            execute("UPDATE %s USING TIMESTAMP 100 SET m['k1'] = ?, m['k2'] = ? WHERE pk = ? AND ck = ?",
                    "doomedA" + ck, "doomedB" + ck, 1L, ck);
        flush();

        for (long ck = 0; ck < 4; ck++)
            execute("DELETE m FROM %s USING TIMESTAMP 200 WHERE pk = ? AND ck = ?", 1L, ck);
        flush();

        long maxLdt = Long.MIN_VALUE;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            long ldt = sstable.getSSTableMetadata().maxLocalDeletionTime;
            if (ldt != Long.MAX_VALUE)
                maxLdt = Math.max(maxLdt, ldt);
        }
        assertTrue("scenario produced no deletion times", maxLdt > 0 && maxLdt < Long.MAX_VALUE);

        // gcBefore equals the deletion's ldt, so the deletion is not purgeable.
        CapturedOutput at = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), DEFAULT_TASK, maxLdt);
        assertTrue("retained complex deletion must be in the output",
                   allJson(at).contains("\"marked_deleted\":\"200\""));
        assertFalse("shadowed cells must not survive a retained complex deletion",
                    allJson(at).contains("doomed"));

        // gcBefore is one past the deletion's ldt, so the deletion is purgeable.
        CapturedOutput past = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), DEFAULT_TASK, maxLdt + 1);
        assertFalse("purged complex deletion must not be in the output",
                    allJson(past).contains("\"marked_deleted\":\"200\""));
        assertFalse("cells shadowed by a purged complex deletion must not be resurrected",
                    allJson(past).contains("doomed"));
    }

    /** Same differential flow under direct I/O for the compaction read path. */
    @Test
    public void directDiskAccessMode() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 10; pk++)
                for (long ck = 0; ck < 15; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "r" + round + "v" + ck);
            execute("DELETE FROM %s WHERE pk = ? AND ck >= 3 AND ck < 9", (long) round);
            flush();
        }

        DiskAccessMode original = DatabaseDescriptor.getCompactionReadDiskAccessMode();
        DatabaseDescriptor.setCompactionReadDiskAccessMode(DiskAccessMode.direct);
        try
        {
            assertCursorMatchesIterator(cfs);
        }
        finally
        {
            DatabaseDescriptor.setCompactionReadDiskAccessMode(original);
        }
    }

    /** Output equivalence must hold for every compressor, not just the default LZ4. */
    @Test
    public void compressionVariants() throws Exception
    {
        for (String compression : new String[]{ "ZstdCompressor", "DeflateCompressor", "SnappyCompressor" })
        {
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                        "WITH gc_grace_seconds = 864000 AND compression = {'class': '" + compression + "'}");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();

            for (int round = 0; round < 2; round++)
            {
                for (long pk = 0; pk < 8; pk++)
                    for (long ck = 0; ck < 10; ck++)
                        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, compression + round + ck);
                execute("DELETE FROM %s WHERE pk = ? AND ck >= 2 AND ck < 6", (long) round);
                flush();
            }

            assertCursorMatchesIterator(cfs);
        }
    }
}
