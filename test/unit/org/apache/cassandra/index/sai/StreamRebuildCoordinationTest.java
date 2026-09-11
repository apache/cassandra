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

package org.apache.cassandra.index.sai;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableStreamRebuildState.State;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit coverage for the CASSANDRA-21520 coordination between an SAI rebuild and entire-sstable (zero-copy)
 * streaming, exercised through the real {@code StorageAttachedIndexBuildingSupport.getIndexBuildTask} path:
 *
 * <ul>
 *     <li>{@link #onNotExecutedReleasesReservedRebuildStatus()} - when the build task is created (reserving the
 *     per-sstable rebuild status) but its {@code build()} never runs (e.g. the executor rejected the submission
 *     during shutdown), {@code onNotExecuted()} must release every reserved status.</li>
 *     <li>{@link #partialReservationRollbackWhenAnSSTableIsBeingStreamed()} - when reserving the rebuild status
 *     fails partway (one target sstable is already being entire-sstable streamed), every status reserved before
 *     the failure must be rolled back, leaving only the streamed sstable in {@code ZCS_STREAMING}.</li>
 * </ul>
 */
public class StreamRebuildCoordinationTest extends SAITester
{
    private StorageAttachedIndex createIndexedTableWithSSTables(int sstableCount)
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        String indexName = createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        int pk = 0;
        for (int s = 0; s < sstableCount; s++)
        {
            for (int i = 0; i < 20; i++, pk++)
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, "v" + pk);
            flush();
        }
        waitForTableIndexesQueryable();

        assertEquals("expected one sstable per flush", sstableCount, cfs.getLiveSSTables().size());
        return (StorageAttachedIndex) cfs.indexManager.getIndexByName(indexName);
    }

    @Test
    public void onNotExecutedReleasesReservedRebuildStatus()
    {
        StorageAttachedIndex sai = createIndexedTableWithSSTables(2);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());

        // Real rebuild path: reserves the per-sstable rebuild status for every target sstable before returning.
        SecondaryIndexBuilder builder = sai.getBuildTaskSupport()
                                           .getIndexBuildTask(cfs, Collections.singleton(sai), sstables, true);

        for (SSTableReader sstable : sstables)
            assertEquals("rebuild status must be reserved for " + sstable.descriptor,
                         State.REBUILDING, sstable.streamRebuildState().state());

        // build() will never run (executor rejected the submission). onNotExecuted must release everything, so the
        // sstables do not stay stuck in REBUILDING and block future entire-sstable streaming.
        builder.onNotExecuted();

        for (SSTableReader sstable : sstables)
        {
            assertEquals("onNotExecuted must release the reserved rebuild status for " + sstable.descriptor,
                         State.NORMAL, sstable.streamRebuildState().state());
            // And a stream may now proceed.
            assertTrue(sstable.streamRebuildState().tryBeginStreaming());
            sstable.streamRebuildState().endStreaming();
        }
    }

    @Test
    public void secondRebuildRejectedWhileFirstRebuildInFlight()
    {
        StorageAttachedIndex sai = createIndexedTableWithSSTables(2);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());

        // First rebuild is "in flight": it reserved the per-sstable rebuild status for every target sstable but has
        // not run/released yet (the returned builder still owns the reservations).
        SecondaryIndexBuilder first = sai.getBuildTaskSupport()
                                         .getIndexBuildTask(cfs, Collections.singleton(sai), sstables, true);
        for (SSTableReader sstable : sstables)
            assertEquals(State.REBUILDING, sstable.streamRebuildState().state());

        try
        {
            // A second rebuild of the same sstables must fail fast (tryBeginRebuild returns false because they are
            // already REBUILDING) rather than mutating index components underneath the in-flight rebuild.
            assertThatThrownBy(() -> sai.getBuildTaskSupport()
                                        .getIndexBuildTask(cfs, Collections.singleton(sai), sstables, true))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot build SAI index");

            // The failed second attempt must not disturb the first rebuild's reservations: every sstable is still
            // REBUILDING and owned by the first builder (the second attempt rolled back only what it reserved).
            for (SSTableReader sstable : sstables)
            {
                assertEquals("first rebuild's reservation must be intact for " + sstable.descriptor,
                             State.REBUILDING, sstable.streamRebuildState().state());
                assertFalse("no third rebuild may begin while the first is in flight",
                            sstable.streamRebuildState().tryBeginRebuild());
                assertFalse("streaming must remain blocked while the first rebuild is in flight",
                            sstable.streamRebuildState().tryBeginStreaming());
            }
        }
        finally
        {
            first.onNotExecuted();
        }

        // Once the first rebuild releases, a fresh rebuild can reserve everything cleanly again.
        for (SSTableReader sstable : sstables)
            assertEquals(State.NORMAL, sstable.streamRebuildState().state());
        SecondaryIndexBuilder third = sai.getBuildTaskSupport()
                                         .getIndexBuildTask(cfs, Collections.singleton(sai), sstables, true);
        for (SSTableReader sstable : sstables)
            assertEquals(State.REBUILDING, sstable.streamRebuildState().state());
        third.onNotExecuted();
    }

    @Test
    public void partialReservationRollbackWhenAnSSTableIsBeingStreamed()
    {
        StorageAttachedIndex sai = createIndexedTableWithSSTables(3);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        List<SSTableReader> live = new ArrayList<>(cfs.getLiveSSTables());
        assertEquals(3, live.size());

        // Simulate an in-flight entire-sstable stream of ONE sstable, and make sure it is reserved LAST so the
        // rebuild reserves the other two first and must then roll them back when it hits the streamed one.
        SSTableReader streamed = live.get(0);
        List<SSTableReader> rollbackCandidates = new ArrayList<>(live.subList(1, live.size()));
        assertThat(rollbackCandidates).hasSize(2);
        assertTrue(streamed.streamRebuildState().tryBeginStreaming());

        List<SSTableReader> ordered = new ArrayList<>(rollbackCandidates);
        ordered.add(streamed); // streamed sstable reserved last

        try
        {
            assertThatThrownBy(() -> sai.getBuildTaskSupport()
                                        .getIndexBuildTask(cfs, Collections.singleton(sai), ordered, true))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("zero-copy")
                .hasMessageContaining("streaming is in progress");

            // The two sstables reserved before the failure must be fully rolled back to NORMAL...
            for (SSTableReader sstable : rollbackCandidates)
                assertEquals("reservation must be rolled back for " + sstable.descriptor,
                             State.NORMAL, sstable.streamRebuildState().state());

            // ...and the streamed sstable must be untouched (still streaming), not corrupted into REBUILDING.
            assertEquals(State.ZCS_STREAMING, streamed.streamRebuildState().state());
            assertEquals(1, streamed.streamRebuildState().zcsStreamCount());
        }
        finally
        {
            streamed.streamRebuildState().endStreaming();
        }

        // With the stream released, a rebuild can now reserve every sstable cleanly - proving the earlier partial
        // rollback left no lingering reservations.
        List<SSTableReader> all = new ArrayList<>(cfs.getLiveSSTables());
        SecondaryIndexBuilder builder = sai.getBuildTaskSupport()
                                           .getIndexBuildTask(cfs, Collections.singleton(sai), all, true);
        for (SSTableReader sstable : all)
            assertEquals(State.REBUILDING, sstable.streamRebuildState().state());
        builder.onNotExecuted();
        for (SSTableReader sstable : all)
            assertEquals(State.NORMAL, sstable.streamRebuildState().state());
    }
}
