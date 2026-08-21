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
 * Partial-set compactions: only a subset of the live sstables participates, and the purge
 * evaluator must consult overlapping NON-participating sstables before dropping tombstones
 * (CompactionController.getPurgeEvaluator / CursorCompactor's shouldPurge). The differential
 * assertion is agnostic to which outcome is correct — both paths must simply agree, byte for
 * byte. All writes use explicit timestamps so purge decisions are deterministic.
 */
public class PartialSetDifferentialCompactionTest extends DifferentialCompactionTester
{

    /** Flushes and returns the sstable that flush produced; tracks flush order explicitly. */
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
     * Tombstones whose purge is BLOCKED by an overlapping non-participant: the partition also
     * exists in a non-compacting sstable with OLDER timestamps, so the tombstone must survive
     * the partial compaction in both paths.
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

        // sstable C (non-participant): same partitions with OLDER data (ts=50) — purging the
        // ts=200 tombstones would resurrect this data, so the evaluator must block it
        for (long pk = 1; pk <= 2; pk++)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 50", pk, ck, "old" + ck);
        flushAndTrack(cfs, flushed);

        // purge boundary: gcBefore strictly past sstable B's tombstones, read from its own stats
        long gcBefore = maxTombstoneLocalDeletionTime(List.of(flushed.get(1))) + 1;

        CapturedOutput out = assertCursorMatchesIterator(cfs, new HashSet<>(flushed.subList(0, 2)), DEFAULT_TASK, gcBefore);
        // non-vacuousness: the overlap must have actually BLOCKED the purge
        assertTrue("expected ts=200 tombstones retained because of the overlapping non-participant",
                   allJson(out).contains("\"marked_deleted\":\"200\""));
    }

    /**
     * Tombstones whose purge is ALLOWED despite a non-participant existing: the non-compacting
     * sstable holds entirely different partitions, so nothing blocks the drop. Both paths must
     * purge identically (and drop the emptied partitions).
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

        // purge boundary: gcBefore strictly past sstable B's tombstones, read from its own stats
        long gcBefore = maxTombstoneLocalDeletionTime(List.of(flushed.get(1))) + 1;

        CapturedOutput out = assertCursorMatchesIterator(cfs, new HashSet<>(flushed.subList(0, 2)), DEFAULT_TASK, gcBefore);
        // non-vacuousness: with no overlap on those keys, the tombstones must actually purge
        assertFalse("expected ts=200 tombstones purged (disjoint non-participant cannot block)",
                    allJson(out).contains("\"marked_deleted\":\"200\""));
    }

    /**
     * Compacting newer sstables while the OLDEST (holding the shadowed data) stays out:
     * tombstones must be retained because the overlapping non-participant holds data they
     * still shadow.
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

        // purge boundary: gcBefore strictly past sstable B's tombstones, read from its own stats
        long gcBefore = maxTombstoneLocalDeletionTime(List.of(flushed.get(1))) + 1;

        // compact B+C, leaving A (the shadowed data) out
        CapturedOutput out = assertCursorMatchesIterator(cfs, new HashSet<>(flushed.subList(1, 3)), DEFAULT_TASK, gcBefore);
        // non-vacuousness: tombstones still shadow A's data and must survive
        assertTrue("expected ts=200 tombstones retained over the shadowed non-participant",
                   allJson(out).contains("\"marked_deleted\":\"200\""));
    }
}
