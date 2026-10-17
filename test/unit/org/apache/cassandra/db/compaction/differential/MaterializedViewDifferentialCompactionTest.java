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


import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.btree.BTree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Differential coverage for MATERIALIZED VIEW tables. The cursor gate admits view tables, and they
 * compact through the same pipelines. Their row shapes are view-specific: strict liveness
 * (CursorCompactor.enforceStrictLiveness mirrors PurgeFunction), view-generated row tombstones when
 * a base row moves between view partitions, and expired-liveness markers.
 * <p>
 * Shadowable row deletions are deprecated since 4.0 (CASSANDRA-11500) and nothing produces new
 * ones, but old view data still carries them. {@link #shadowableRowDeletion} constructs them
 * directly.
 */
public class MaterializedViewDifferentialCompactionTest extends DifferentialCompactionTester
{

    @BeforeClass
    public static void startup()
    {
        requireNetwork();
    }

    @Test
    public void materializedViewTable() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT pk, ck, v1 FROM %s " +
                                 "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v1 IS NOT NULL " +
                                 "PRIMARY KEY (v1, pk, ck)");
        ColumnFamilyStore viewCfs = getColumnFamilyStore(KEYSPACE, view);
        viewCfs.disableAutoCompaction();

        for (long pk = 0; pk < 6; pk++)
            for (long ck = 0; ck < 6; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, pk * 100 + ck, "x" + ck);
        flush(KEYSPACE, view);

        // view-partition moves (v1 changes: old view row dies via view tombstone, new row
        // appears) and base-row deletes (view row tombstones)
        for (long pk = 0; pk < 6; pk += 2)
        {
            execute("UPDATE %s SET v1 = ? WHERE pk = ? AND ck = ?", pk * 100 + 77, pk, 0L);
            execute("DELETE FROM %s WHERE pk = ? AND ck = ?", pk, 1L);
        }
        flush(KEYSPACE, view);

        // a second wave of moves on already-moved rows: tombstones meeting tombstones
        execute("UPDATE %s SET v1 = ? WHERE pk = ? AND ck = ?", 999L, 0L, 0L);
        flush(KEYSPACE, view);

        assertCursorMatchesIteratorAcrossGenerations(viewCfs);
    }

    /**
     * MV + TTL: the view-generated row liveness must track the base row's TTL, including
     * across a view-partition move (v1 changes -> old view row dies, new one carries its own
     * TTL). This interaction has produced bugs (CASSANDRA-21152).
     */
    @Test
    public void materializedViewWithTTL() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT pk, ck, v1 FROM %s " +
                                 "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v1 IS NOT NULL " +
                                 "PRIMARY KEY (v1, pk, ck)");
        ColumnFamilyStore viewCfs = getColumnFamilyStore(KEYSPACE, view);
        viewCfs.disableAutoCompaction();

        long writeTimeSec = FBUtilities.nowInSeconds();

        // short-TTL rows: base row AND its view row will have expired by fixedNow below
        for (long pk = 0; pk < 6; pk++)
            for (long ck = 0; ck < 6; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TTL 1", pk, ck, pk * 100 + ck, "x" + ck);
        flush(KEYSPACE, view);

        // long-TTL rows: still alive at fixedNow, exercises live-TTL view rows alongside dead ones
        for (long pk = 10; pk < 16; pk++)
            for (long ck = 0; ck < 6; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TTL 86400", pk, ck, pk * 100 + ck, "y" + ck);
        flush(KEYSPACE, view);

        // view-partition move on a short-TTL row, itself given a new (long) TTL: the old view
        // row's move-tombstone and its own now-irrelevant short TTL race the new view row's
        // fresh long TTL
        for (long pk = 0; pk < 6; pk += 2)
            execute("UPDATE %s USING TTL 86400 SET v1 = ? WHERE pk = ? AND ck = ?", pk * 100 + 77, pk, 0L);
        flush(KEYSPACE, view);

        long fixedNow = writeTimeSec + 3; // past the short TTLs' expiry; long TTLs still alive
        assertCursorMatchesIteratorAcrossGenerations(viewCfs, () -> fixedNow);
    }

    /**
     * A shadowable row deletion that the merged primary-key liveness SHADOWS. The compaction merge
     * ({@link Row.Merger#merge}) clears such a deletion to LIVE before it becomes the row's active
     * deletion. The row therefore keeps its liveness AND the cells the deletion would otherwise have
     * shadowed, and the output carries no row deletion and no {@code HAS_SHADOWABLE_DELETION}
     * extension flag.
     * <p>
     * The three pieces are in three sstables on purpose. A shadowable deletion never meets a
     * superseding liveness inside one memtable: {@code BTreeRow.Builder.build} applies
     * {@code isShadowedBy} itself, and {@code addRowDeletion}/{@code addPrimaryKeyLivenessInfo}
     * drop shadowed data outright. The compaction merge is the only place the two can meet.
     * <p>
     * The deletion's LOCAL deletion time is {@code nowInSeconds()} rather than a small constant so
     * it sits above {@code gcBefore} and is not purgeable — a purgeable deletion is cleared by the
     * purger on both paths, and the scenario would pass while covering nothing.
     */
    @Test
    public void shadowableRowDeletionShadowedByRowLiveness() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        writeShadowableRowDeletion(cfs, 1L, 1L, 100L, FBUtilities.nowInSeconds());
        flush();

        // a cell the deletion would drop: its timestamp is below the deletion's
        execute("UPDATE %s USING TIMESTAMP 50 SET v2 = 'keepme' WHERE pk = 1 AND ck = 1");
        flush();

        // the primary-key liveness that shadows the deletion (200 > 100)
        execute("INSERT INTO %s (pk, ck) VALUES (1, 1) USING TIMESTAMP 200");
        flush();

        assertEquals("the three pieces must meet in the compaction merge, not in a memtable",
                     3, cfs.getLiveSSTables().size());

        CapturedOutput out = assertCursorMatchesIterator(cfs);
        String json = allJson(out);
        // nothing else in this scenario writes a deletion, so any marked_deleted is the shadowed one
        assertFalse("the shadowed row deletion survived into the output: " + json,
                    json.contains("\"marked_deleted\""));
        assertEquals("the cell the shadowed deletion would have dropped is missing: " + json,
                     1, countOccurrences(json, cellValue("keepme")));
    }

    /**
     * Shadowable row deletions predate CASSANDRA-11500 and nothing since produces new ones, but
     * old data can still carry the flag on either a static or a non-static row. A non-static row
     * carries it at any position in the partition, not only as the partition's first unfiltered, so
     * the reader's flag dispatch must read extended flags at every position. Not constructible
     * through CQL — {@code Row.Deletion.shadowable(...)} has to be called directly.
     */
    @Test
    public void shadowableRowDeletion() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s1 bigint static, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // static row, shadowable deletion: the partition's first unfiltered
        writeShadowableStaticRowDeletion(cfs, 1L, 100L);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (1, 0, 0)");
        execute("INSERT INTO %s (pk, ck, v1) VALUES (1, 2, 2)");
        // regular row, shadowable deletion, NOT first in the partition (ck=1, between the two above)
        writeShadowableRowDeletion(cfs, 1L, 1L, 200L);
        flush();

        // second generation: a live write to the same static row, postdating the shadowable
        // deletion — the merge must keep both in the output across sstables, not copy a single
        // source through
        execute("INSERT INTO %s (pk, ck, s1, v1) VALUES (1, 0, 5, 99)");
        flush();

        assertCursorMatchesIterator(cfs);
    }

    /**
     * Strict liveness ({@code TableMetadata.enforceStrictLiveness()}, true exactly for a view whose
     * primary key contains a base non-PK column) is NOT "drop every row with no primary-key liveness".
     * {@link org.apache.cassandra.db.rows.BTreeRow#purge} reaches its strict-liveness drop only past
     * {@code hasDeletion(nowInSec)}, i.e. {@code nowInSec >= minLocalDeletionTime} of the merged row.
     * A row whose merged form has empty primary-key liveness, a live row deletion and only cells that
     * are live at {@code nowInSec} has {@code minLocalDeletionTime == Cell.MAX_DELETION_TIME}, so purge
     * returns it untouched — row and cells. One dead cell (a tombstone, or an expiring cell past its
     * expiry) pulls {@code minLocalDeletionTime} to or below {@code nowInSec} and the whole row goes,
     * live cells included.
     * <p>
     * Both halves are asserted, because "keep everything" and "drop everything" each satisfy only one
     * of them. Each half is built twice: once from a SINGLE sstable, and once merged across two, so the
     * cursor path's cell walk runs the multi-source case too. That case takes several rounds of the cell
     * sort, and cells from both sources reach one output row through the rewound cursors. The drop half
     * puts its dead cell in the SECOND source and a later column, so the walk has to get past a live
     * cell to find it.
     * <p>
     * The rows are constructed directly against the VIEW's ColumnFamilyStore because view maintenance
     * cannot produce this shape — no view row {@code ViewUpdateGenerator} writes has empty primary-key
     * liveness AND a live row deletion AND cells; every route to an empty merged liveness either carries
     * its own deletion or leaves the reference's {@code hasDeletion} guard open anyway. The shape still
     * has to merge correctly, though: the reader builds its column arrays from the sstable header, so
     * any row shape that can be on disk — legacy view data, a repaired or streamed sstable — reaches
     * this merge.
     */
    @Test
    public void strictLivenessKeepsARowWithNoDeletionAtNow() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, v3 text, v4 text, PRIMARY KEY (pk, ck))");
        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT pk, ck, v1, v2, v3, v4 FROM %s " +
                                 "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v1 IS NOT NULL " +
                                 "PRIMARY KEY (v1, pk, ck)");
        ColumnFamilyStore viewCfs = getColumnFamilyStore(KEYSPACE, view);
        viewCfs.disableAutoCompaction();
        assertTrue("v1 is a base non-PK column in the view PK, so the view must enforce strict liveness",
                   viewCfs.metadata().enforceStrictLiveness());

        // a normal view row, through real view maintenance, sharing the view partition below
        execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (1, 1, 5, 'normal')");
        flush(KEYSPACE, view);

        long nowInSec = FBUtilities.nowInSeconds();
        TableMetadata metadata = viewCfs.metadata();

        // FIRST source of the two-source rows, alongside both single-source rows
        Row.Builder keptOne = livenessFreeViewRow(2L, 2L);
        addLiveCell(keptOne, metadata, "v2", "keptFromOneSource");
        applyViewRow(viewCfs, 5L, keptOne.build());

        Row.Builder droppedOne = livenessFreeViewRow(3L, 3L);
        addLiveCell(droppedOne, metadata, "v2", "droppedFromOneSource");
        addCellTombstone(droppedOne, metadata, "v3", nowInSec);
        applyViewRow(viewCfs, 5L, droppedOne.build());

        Row.Builder keptMergedA = livenessFreeViewRow(4L, 4L);
        addLiveCell(keptMergedA, metadata, "v2", "keptMergedFirst");
        applyViewRow(viewCfs, 5L, keptMergedA.build());

        Row.Builder droppedMergedA = livenessFreeViewRow(6L, 6L);
        addLiveCell(droppedMergedA, metadata, "v2", "droppedMergedFirst");
        applyViewRow(viewCfs, 5L, droppedMergedA.build());
        flush(KEYSPACE, view);

        // SECOND source: same clusterings, different columns, so these rows merge two sources
        Row.Builder keptMergedB = livenessFreeViewRow(4L, 4L);
        addLiveCell(keptMergedB, metadata, "v3", "keptMergedSecond");
        addLiveCell(keptMergedB, metadata, "v4", "keptMergedThird");
        applyViewRow(viewCfs, 5L, keptMergedB.build());

        Row.Builder droppedMergedB = livenessFreeViewRow(6L, 6L);
        addLiveCell(droppedMergedB, metadata, "v3", "droppedMergedSecond");
        addCellTombstone(droppedMergedB, metadata, "v4", nowInSec);
        applyViewRow(viewCfs, 5L, droppedMergedB.build());
        flush(KEYSPACE, view);

        assertEquals("the constructed rows must reach the compaction merge from their own sstables",
                     3, viewCfs.getLiveSSTables().size());
        // non-vacuity for the DROP halves: a deletion time at or below nowInSec really is on disk, and
        // nothing else in this scenario writes one, so it is a cell tombstone; and it is above gcBefore,
        // so the purger cannot remove it before the merge decides the row
        assertSomethingExpiredAt(viewCfs, nowInSec);
        // This compares against the gcBefore the harness will actually derive, sampled the way it
        // derives it. Comparing against getDefaultGcBefore(nowInSec) instead would reduce to
        // gc_grace_seconds >= 0 and assert nothing at all.
        assertTrue("the cell tombstones must NOT be purgeable, or the purger removes them before the "
                   + "merge decides the row and they cannot decide a strict-liveness drop",
                   nowInSec >= viewCfs.getDefaultGcBefore(FBUtilities.nowInSeconds()));

        CapturedOutput out = assertCursorMatchesIterator(viewCfs);
        String json = allJson(out);
        assertEquals("the single-source row with no deletion at now lost its cell: " + json,
                     1, countOccurrences(json, cellValue("keptFromOneSource")));
        assertEquals("the two-source row with no deletion at now lost a cell: " + json,
                     1, countOccurrences(json, cellValue("keptMergedFirst")));
        assertEquals("the two-source row with no deletion at now lost a cell: " + json,
                     1, countOccurrences(json, cellValue("keptMergedSecond")));
        assertEquals("the two-source row with no deletion at now lost a cell: " + json,
                     1, countOccurrences(json, cellValue("keptMergedThird")));
        assertEquals("the single-source row carrying a cell tombstone survived strict liveness: " + json,
                     0, countOccurrences(json, cellValue("droppedFromOneSource")));
        assertEquals("the two-source row carrying a cell tombstone survived strict liveness: " + json,
                     0, countOccurrences(json, cellValue("droppedMergedFirst")));
        assertEquals("the two-source row carrying a cell tombstone survived strict liveness: " + json,
                     0, countOccurrences(json, cellValue("droppedMergedSecond")));
        assertEquals("the view-maintained row is missing: " + json,
                     1, countOccurrences(json, cellValue("normal")));
    }

    /**
     * The third way the {@code hasDeletion(nowInSec)} guard of the reference path opens: a
     * complex-column deletion.
     * {@link BTreeRow#minDeletionTime(org.apache.cassandra.db.rows.ComplexColumnData)} always folds
     * a non-live complex deletion in as {@code Long.MIN_VALUE}, above what the cells contribute.
     * The deletion therefore drops the whole row on its own, and takes a live cell of its own
     * column with it.
     * <p>
     * {@code keptWithLiveComplexDeletion} is the control: the same shape, but its column holds no
     * deletion, only a live cell, so strict liveness must keep the row.
     * <p>
     * Both rows go directly to the view's ColumnFamilyStore, for the reason
     * {@link #strictLivenessKeepsARowWithNoDeletionAtNow} gives.
     */
    @Test
    public void strictLivenessAccountsForComplexColumnDeletion() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, m map<text, bigint>, PRIMARY KEY (pk, ck))");
        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT pk, ck, v1, v2, m FROM %s " +
                                 "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v1 IS NOT NULL " +
                                 "PRIMARY KEY (v1, pk, ck)");
        ColumnFamilyStore viewCfs = getColumnFamilyStore(KEYSPACE, view);
        viewCfs.disableAutoCompaction();
        assertTrue("v1 is a base non-PK column in the view PK, so the view must enforce strict liveness",
                   viewCfs.metadata().enforceStrictLiveness());

        long nowInSec = FBUtilities.nowInSeconds();
        TableMetadata metadata = viewCfs.metadata();
        ColumnMetadata mapColumn = metadata.getColumn(ByteBufferUtil.bytes("m"));

        Row.Builder droppedByDeadComplexDeletion = livenessFreeViewRow(2L, 2L);
        droppedByDeadComplexDeletion.addComplexDeletion(mapColumn, DeletionTime.build(100L, nowInSec));
        droppedByDeadComplexDeletion.addCell(BufferCell.live(mapColumn, 150L, LongType.instance.decompose(1L),
                                                              CellPath.create(UTF8Type.instance.decompose("k"))));
        applyViewRow(viewCfs, 7L, droppedByDeadComplexDeletion.build());

        Row.Builder keptWithLiveComplexDeletion = livenessFreeViewRow(3L, 3L);
        keptWithLiveComplexDeletion.addCell(BufferCell.live(mapColumn, 150L, LongType.instance.decompose(2L),
                                                            CellPath.create(UTF8Type.instance.decompose("k"))));
        applyViewRow(viewCfs, 7L, keptWithLiveComplexDeletion.build());
        flush(KEYSPACE, view);

        assertEquals("the constructed rows must reach the compaction merge", 1, viewCfs.getLiveSSTables().size());
        assertTrue("the complex deletion must NOT be purgeable, or the purger removes it before the "
                   + "merge decides the row and it cannot decide a strict-liveness drop on its own",
                   nowInSec >= viewCfs.getDefaultGcBefore(FBUtilities.nowInSeconds()));

        assertCursorMatchesIterator(viewCfs);
    }

    /**
     * Strict liveness must drop a row when the merged winner of a complex cell is dead, and the two
     * cells come from different sstables. {@link #strictLivenessAccountsForComplexColumnDeletion}
     * is one sstable and short-circuits on a dead complex deletion, so it never enters the
     * {@code cellMergeLimit > 1} resolve loop in {@code anyMergedCellDeadAtNow}.
     * <p>
     * The dropped row has a live map cell in the first sstable and a tombstone on the same path in
     * the second. No complex deletion: that branch would skip the loop again. The control row has
     * two live cells on the same path and must be kept.
     */
    @Test
    public void strictLivenessAccountsForMergedComplexCell() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, m map<text, bigint>, PRIMARY KEY (pk, ck))");
        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT pk, ck, v1, v2, m FROM %s " +
                                 "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v1 IS NOT NULL " +
                                 "PRIMARY KEY (v1, pk, ck)");
        ColumnFamilyStore viewCfs = getColumnFamilyStore(KEYSPACE, view);
        viewCfs.disableAutoCompaction();
        assertTrue("v1 is a base non-PK column in the view PK, so the view must enforce strict liveness",
                   viewCfs.metadata().enforceStrictLiveness());

        execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (1, 1, 5, 'normal')");
        flush(KEYSPACE, view);

        long nowInSec = FBUtilities.nowInSeconds();
        TableMetadata metadata = viewCfs.metadata();
        ColumnMetadata mapColumn = metadata.getColumn(ByteBufferUtil.bytes("m"));

        Row.Builder droppedA = livenessFreeViewRow(2L, 2L);
        addLiveCell(droppedA, metadata, "v2", "droppedByMergedTombstone");
        addMapCell(droppedA, mapColumn, "k", 100L, 1L);
        applyViewRow(viewCfs, 7L, droppedA.build());

        Row.Builder keptA = livenessFreeViewRow(3L, 3L);
        addLiveCell(keptA, metadata, "v2", "keptMergedComplex");
        addMapCell(keptA, mapColumn, "k", 100L, 2L);
        applyViewRow(viewCfs, 7L, keptA.build());
        flush(KEYSPACE, view);

        Row.Builder droppedB = livenessFreeViewRow(2L, 2L);
        addMapTombstone(droppedB, mapColumn, "k", 200L, nowInSec);
        applyViewRow(viewCfs, 7L, droppedB.build());

        Row.Builder keptB = livenessFreeViewRow(3L, 3L);
        addMapCell(keptB, mapColumn, "k", 200L, 3L);
        applyViewRow(viewCfs, 7L, keptB.build());
        flush(KEYSPACE, view);

        assertEquals("the constructed rows must reach the compaction merge from their own sstables",
                     3, viewCfs.getLiveSSTables().size());
        assertSomethingExpiredAt(viewCfs, nowInSec);
        assertTrue("the map cell tombstone must NOT be purgeable, or the purger removes it before the "
                   + "merge decides the row and it cannot decide a strict-liveness drop on its own",
                   nowInSec >= viewCfs.getDefaultGcBefore(FBUtilities.nowInSeconds()));

        CapturedOutput out = assertCursorMatchesIterator(viewCfs);
        String json = allJson(out);
        assertEquals("the two-source row whose merged map cell is a tombstone survived strict liveness: " + json,
                     0, countOccurrences(json, cellValue("droppedByMergedTombstone")));
        assertEquals("the two-source row with two live map cells lost its cell: " + json,
                     1, countOccurrences(json, cellValue("keptMergedComplex")));
        assertEquals("the view-maintained row is missing: " + json,
                     1, countOccurrences(json, cellValue("normal")));
    }

    /**
     * Another way the reference's {@code hasDeletion(nowInSec)} guard opens for a row strict liveness
     * then drops: not through a cell, but because the PURGER cleared the row's liveness or its row
     * deletion. Whichever it cleared was, before it ran, a term of the merged row's
     * {@code minLocalDeletionTime} at or below {@code nowInSec} — an expired liveness contributes its
     * expiration time, a row deletion contributes {@code Long.MIN_VALUE} — so the row is dropped without
     * any cell needing to be dead.
     * <p>
     * Both clearances are built, each with a LIVE cell that must go with the row, and a control row that
     * neither clearance touches and that therefore keeps its cell. Without the purger half of the
     * cursor's condition, the cell walk finds only live cells in all three and keeps all three, so the
     * two dropped cells are what makes this scenario able to fail.
     * <p>
     * {@code gcBefore} is placed one second past the local deletion time the rows were built with, which
     * is what makes both clearances fire; the assertions are their own non-vacuity check, since a
     * clearance that did not fire leaves a non-empty liveness or a live-superseding deletion and the row
     * is written with its cell.
     */
    @Test
    public void strictLivenessDropsARowThePurgerEmptied() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT pk, ck, v1, v2 FROM %s " +
                                 "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v1 IS NOT NULL " +
                                 "PRIMARY KEY (v1, pk, ck)");
        ColumnFamilyStore viewCfs = getColumnFamilyStore(KEYSPACE, view);
        viewCfs.disableAutoCompaction();
        assertTrue("v1 is a base non-PK column in the view PK, so the view must enforce strict liveness",
                   viewCfs.metadata().enforceStrictLiveness());

        execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (1, 1, 5, 'normal')");
        flush(KEYSPACE, view);

        // every deletion time in this scenario, so one gcBefore purges all of them
        long localDeletionTime = FBUtilities.nowInSeconds();
        TableMetadata metadata = viewCfs.metadata();

        // expiring primary-key liveness, expired at the pinned now and purgeable at gcBefore
        Row.Builder purgedLiveness = livenessFreeViewRow(2L, 2L);
        purgedLiveness.addPrimaryKeyLivenessInfo(LivenessInfo.withExpirationTime(100L, 1, localDeletionTime));
        addLiveCell(purgedLiveness, metadata, "v2", "purgedLivenessCell");
        applyViewRow(viewCfs, 5L, purgedLiveness.build());

        // row deletion, purgeable at gcBefore, with a live cell above its timestamp so the cell survives
        // the active deletion and only the strict-liveness drop can remove it
        Row.Builder purgedDeletion = livenessFreeViewRow(3L, 3L);
        purgedDeletion.addRowDeletion(Row.Deletion.regular(DeletionTime.build(100L, localDeletionTime)));
        addLiveCell(purgedDeletion, metadata, "v2", "purgedDeletionCell");
        applyViewRow(viewCfs, 5L, purgedDeletion.build());

        // control: no liveness, no deletion, one live cell -- nothing for the purger to clear, so the
        // reference's guard stays shut and the row is kept
        Row.Builder untouched = livenessFreeViewRow(4L, 4L);
        addLiveCell(untouched, metadata, "v2", "untouchedCell");
        applyViewRow(viewCfs, 5L, untouched.build());
        flush(KEYSPACE, view);

        assertEquals("the constructed rows must reach the compaction merge from their own sstable",
                     2, viewCfs.getLiveSSTables().size());

        long gcBefore = localDeletionTime + 1;
        long pinnedNow = localDeletionTime + 60; // past the liveness expiry, below any live cell's deletion time
        CapturedOutput out = assertCursorMatchesIterator(viewCfs, viewCfs.getLiveSSTables(),
                                                         taskWithFixedNow(pinnedNow), gcBefore);
        String json = allJson(out);
        assertEquals("the row whose liveness the purger cleared kept its cell: " + json,
                     0, countOccurrences(json, cellValue("purgedLivenessCell")));
        assertEquals("the row whose deletion the purger cleared kept its cell: " + json,
                     0, countOccurrences(json, cellValue("purgedDeletionCell")));
        assertEquals("the control row, which the purger cleared nothing on, lost its cell: " + json,
                     1, countOccurrences(json, cellValue("untouchedCell")));
        assertEquals("the view-maintained row is missing: " + json,
                     1, countOccurrences(json, cellValue("normal")));
    }

    /** A view-row builder with no primary-key liveness and no row deletion, at view clustering (pk, ck). */
    private Row.Builder livenessFreeViewRow(long pk, long ck)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.make(LongType.instance.decompose(pk), LongType.instance.decompose(ck)));
        return builder;
    }

    private void addLiveCell(Row.Builder builder, TableMetadata metadata, String column, String value)
    {
        builder.addCell(BufferCell.live(metadata.getColumn(ByteBufferUtil.bytes(column)),
                                        200L, UTF8Type.instance.decompose(value)));
    }

    /**
     * A cell tombstone whose local deletion time is {@code nowInSec}, so it sits above the default
     * {@code gcBefore}. The purger cannot remove it before the merge decides its row.
     */
    private void addCellTombstone(Row.Builder builder, TableMetadata metadata, String column, long nowInSec)
    {
        builder.addCell(BufferCell.tombstone(metadata.getColumn(ByteBufferUtil.bytes(column)), 200L, nowInSec));
    }

    private void addMapCell(Row.Builder builder, ColumnMetadata mapColumn, String key, long timestamp, long value)
    {
        builder.addCell(BufferCell.live(mapColumn, timestamp, LongType.instance.decompose(value),
                                        CellPath.create(UTF8Type.instance.decompose(key))));
    }

    private void addMapTombstone(Row.Builder builder, ColumnMetadata mapColumn, String key, long timestamp, long nowInSec)
    {
        builder.addCell(BufferCell.tombstone(mapColumn, timestamp, nowInSec,
                                             CellPath.create(UTF8Type.instance.decompose(key))));
    }

    private void applyViewRow(ColumnFamilyStore viewCfs, long v1, Row row)
    {
        new Mutation(PartitionUpdate.singleRowUpdate(viewCfs.metadata(), LongType.instance.decompose(v1), row)).apply();
    }

    private void writeShadowableStaticRowDeletion(ColumnFamilyStore cfs, long pk, long timestamp)
    {
        Row staticRow = BTreeRow.create(Clustering.STATIC_CLUSTERING, LivenessInfo.EMPTY,
                                        Row.Deletion.shadowable(DeletionTime.build(timestamp, 1000)), BTree.empty());
        ByteBuffer key = LongType.instance.decompose(pk);
        new Mutation(PartitionUpdate.singleRowUpdate(cfs.metadata(), key, staticRow)).apply();
    }

    private void writeShadowableRowDeletion(ColumnFamilyStore cfs, long pk, long ck, long timestamp)
    {
        writeShadowableRowDeletion(cfs, pk, ck, timestamp, 1000L);
    }

    private void writeShadowableRowDeletion(ColumnFamilyStore cfs, long pk, long ck, long timestamp, long localDeletionTime)
    {
        Clustering<?> clustering = Clustering.make(ByteBufferUtil.bytes(ck));
        Row row = BTreeRow.create(clustering, LivenessInfo.EMPTY,
                                  Row.Deletion.shadowable(DeletionTime.build(timestamp, localDeletionTime)), BTree.empty());
        ByteBuffer key = LongType.instance.decompose(pk);
        new Mutation(PartitionUpdate.singleRowUpdate(cfs.metadata(), key, row)).apply();
    }
}
