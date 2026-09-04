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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Partial-set compaction: only a subset of the live sstables participates. The purge evaluator
 * must consult the overlapping NON-participants before it drops a tombstone
 * (CompactionController.getPurgeEvaluator, CursorCompactor.Purger.shouldPurge). The differential
 * assertion proves only that the two paths agree, so every scenario also asserts the purge
 * outcome it sets up.
 */
public class PartialSetDifferentialCompactionTest extends DifferentialCompactionTester
{

    /** Returns the one sstable this flush created, and appends it to {@code flushed} in flush order. */
    private SSTableReader flushAndTrack(ColumnFamilyStore cfs, List<SSTableReader> flushed) throws Throwable
    {
        Set<SSTableReader> before = new HashSet<>(cfs.getLiveSSTables());
        flush();
        List<SSTableReader> fresh = new ArrayList<>(cfs.getLiveSSTables());
        fresh.removeAll(before);
        if (fresh.size() != 1)
            throw new AssertionError("expected exactly one new sstable from flush, got " + fresh.size());
        flushed.add(fresh.get(0));
        return fresh.get(0);
    }

    /**
     * Purge BLOCKED by an overlapping non-participant. The partition also lives in a
     * non-compacting sstable with OLDER timestamps, so the tombstones must survive.
     */
    @Test
    public void purgeBlockedByOverlappingNonParticipant() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 0");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        List<SSTableReader> flushed = new ArrayList<>();
        // sstable A: data at ts=100
        for (long pk = 0; pk < 6; pk++)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 100", pk, ck, "a" + ck);
        flushAndTrack(cfs, flushed);

        // sstable B: row + partition tombstones at ts=200
        for (long ck = 0; ck < 5; ck++)
            execute("DELETE FROM %s USING TIMESTAMP 200 WHERE pk = 1 AND ck = ?", ck);
        execute("DELETE FROM %s USING TIMESTAMP 200 WHERE pk = 2");
        flushAndTrack(cfs, flushed);

        // sstable C (non-participant): the same partitions with OLDER data at ts=50
        for (long pk = 1; pk <= 2; pk++)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 50", pk, ck, "old" + ck);
        flushAndTrack(cfs, flushed);

        // gcBefore makes every tombstone in sstable B purgeable, so only the overlap can block it
        long gcBefore = maxTombstoneLocalDeletionTime(List.of(flushed.get(1))) + 1;

        CapturedOutput out = assertCursorMatchesIterator(cfs, new HashSet<>(flushed.subList(0, 2)), DEFAULT_TASK, gcBefore);
        assertTrue("expected ts=200 tombstones retained because of the overlapping non-participant",
                   allJson(out).contains("\"marked_deleted\":\"200\""));
    }

    /**
     * Purge ALLOWED despite a non-participant. The non-compacting sstable holds different
     * partitions only, so nothing blocks the drop.
     */
    @Test
    public void purgeAllowedWithDisjointNonParticipant() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 0");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        List<SSTableReader> flushed = new ArrayList<>();
        // sstable A: data at ts=100 for pk 0-5
        for (long pk = 0; pk < 6; pk++)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 100", pk, ck, "a" + ck);
        flushAndTrack(cfs, flushed);

        // sstable B: tombstones at ts=200 for pk 1-2
        for (long ck = 0; ck < 10; ck++)
            execute("DELETE FROM %s USING TIMESTAMP 200 WHERE pk = 1 AND ck = ?", ck);
        execute("DELETE FROM %s USING TIMESTAMP 200 WHERE pk = 2");
        flushAndTrack(cfs, flushed);

        // sstable C (non-participant): DIFFERENT partitions only (pk 100-105)
        for (long pk = 100; pk < 106; pk++)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 50", pk, ck, "other" + ck);
        flushAndTrack(cfs, flushed);

        // gcBefore makes every tombstone in sstable B purgeable
        long gcBefore = maxTombstoneLocalDeletionTime(List.of(flushed.get(1))) + 1;

        CapturedOutput out = assertCursorMatchesIterator(cfs, new HashSet<>(flushed.subList(0, 2)), DEFAULT_TASK, gcBefore);
        assertFalse("expected ts=200 tombstones purged (disjoint non-participant cannot block)",
                    allJson(out).contains("\"marked_deleted\":\"200\""));
    }

    /**
     * The OLDEST sstable stays out of the compaction. The tombstones in the newer sstables
     * still shadow its data, so those tombstones must be retained.
     */
    @Test
    public void tombstonesRetainedOverShadowedNonParticipant() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 0");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        List<SSTableReader> flushed = new ArrayList<>();
        // sstable A (will be the non-participant): the data being shadowed, ts=100
        for (long pk = 0; pk < 6; pk++)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 100", pk, ck, "base" + ck);
        flushAndTrack(cfs, flushed);

        // sstable B: tombstones at ts=200 over A's data
        for (long ck = 0; ck < 10; ck++)
            execute("DELETE FROM %s USING TIMESTAMP 200 WHERE pk = 3 AND ck = ?", ck);
        execute("DELETE FROM %s USING TIMESTAMP 200 WHERE pk = 4");
        flushAndTrack(cfs, flushed);

        // sstable C: unrelated newer writes, merges with B
        for (long pk = 0; pk < 6; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 300", pk, 20L, "new");
        flushAndTrack(cfs, flushed);

        // gcBefore makes every tombstone in sstable B purgeable, so only the overlap can block it
        long gcBefore = maxTombstoneLocalDeletionTime(List.of(flushed.get(1))) + 1;

        // compact B+C, leaving A (the shadowed data) out
        CapturedOutput out = assertCursorMatchesIterator(cfs, new HashSet<>(flushed.subList(1, 3)), DEFAULT_TASK, gcBefore);
        assertTrue("expected ts=200 tombstones retained over the shadowed non-participant",
                   allJson(out).contains("\"marked_deleted\":\"200\""));
    }
}
