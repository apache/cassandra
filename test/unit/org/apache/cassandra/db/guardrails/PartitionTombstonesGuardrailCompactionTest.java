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

package org.apache.cassandra.db.guardrails;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.differential.DifferentialCompactionTester;
import org.apache.cassandra.db.guardrails.GuardrailEvent.GuardrailEventType;
import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * The {@code partition_tombstones} guardrail must fire at the same count on both compaction pipelines.
 * <p>
 * {@code SortedTableWriter} counts the partition-level deletion when the partition starts and checks
 * the threshold when it ends, so the deletion is inside the count. The cursor writer does both in
 * {@code addPartitionMetadata}, and counting the deletion after the check would leave the partition
 * one tombstone short. This fixture sits exactly on that boundary: a partition deletion plus enough
 * row tombstones that the total crosses the threshold only if the partition deletion counts.
 */
public class PartitionTombstonesGuardrailCompactionTest extends DifferentialCompactionTester
{
    /** Row tombstones in the fixture. With the partition deletion the total is one more. */
    private static final int ROW_TOMBSTONES = 5;
    /** The total is ROW_TOMBSTONES + 1 and the guardrail fires above the threshold, not at it. */
    private static final long WARN_THRESHOLD = ROW_TOMBSTONES;
    private static final long FAIL_THRESHOLD = 1000;

    private final WarningCollector collector = new WarningCollector();
    private long originalWarn;
    private long originalFail;
    private boolean originalDiagnostics;

    @Before
    public void armGuardrails()
    {
        originalWarn = Guardrails.instance.getPartitionTombstonesWarnThreshold();
        originalFail = Guardrails.instance.getPartitionTombstonesFailThreshold();
        originalDiagnostics = DatabaseDescriptor.diagnosticEventsEnabled();

        Guardrails.instance.setPartitionTombstonesThreshold(WARN_THRESHOLD, FAIL_THRESHOLD);
        DatabaseDescriptor.setDiagnosticEventsEnabled(true);
        DiagnosticEventService.instance().subscribe(GuardrailEvent.class, collector);
    }

    @After
    public void disarmGuardrails()
    {
        DiagnosticEventService.instance().unsubscribe(collector);
        DatabaseDescriptor.setDiagnosticEventsEnabled(originalDiagnostics);
        Guardrails.instance.setPartitionTombstonesThreshold(originalWarn, originalFail);
    }

    /**
     * One partition holding a partition deletion and, above it in timestamp, {@link #ROW_TOMBSTONES}
     * row tombstones. The row deletions are newer, so the compaction keeps all of them, and the two
     * kinds arrive in separate sstables so neither flush can warn on its own.
     */
    private ColumnFamilyStore partitionDeletionOverRowTombstones()
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("DELETE FROM %s USING TIMESTAMP 1 WHERE k = 1");
        flush();
        for (int c = 0; c < ROW_TOMBSTONES; c++)
            execute("DELETE FROM %s USING TIMESTAMP ? WHERE k = 1 AND c = ?", 10L + c, c);
        flush();

        assertEquals("the fixture needs two sstables to merge", 2, cfs.getLiveSSTables().size());
        collector.drain();
        return cfs;
    }

    /** Compacts a fresh fixture down one pipeline and returns the warnings that compaction emitted. */
    private List<String> warningsFromOneCompaction(boolean cursor) throws Exception
    {
        ColumnFamilyStore cfs = partitionDeletionOverRowTombstones();
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        // gcBefore 0 keeps every tombstone: a purged one would never reach the guardrail.
        commitCompaction(cfs, inputs, cursor, 0);
        return collector.drain();
    }

    @Test
    public void bothPipelinesWarnAtTheSameTombstoneCount() throws Exception
    {
        assumeCursorSupportedFormatSelected();

        List<String> iterator = warningsFromOneCompaction(false);
        List<String> cursor = warningsFromOneCompaction(true);

        assertFalse("the iterator path must warn, or this says nothing about the cursor path",
                    iterator.isEmpty());
        assertEquals("the cursor path must count the same tombstones as the iterator path",
                     iterator, cursor);
    }

    /** Records the redacted text of each partition_tombstones warning, in arrival order. */
    private static final class WarningCollector implements Consumer<GuardrailEvent>
    {
        private final List<String> warnings = new CopyOnWriteArrayList<>();

        @Override
        public void accept(GuardrailEvent event)
        {
            if (event.getType() != GuardrailEventType.WARNED)
                return;
            Map<String, Serializable> map = event.toMap();
            if (Guardrails.partitionTombstones.name.equals(map.get("name")))
                warnings.add(String.valueOf(map.get("message")));
        }

        List<String> drain()
        {
            List<String> drained = new ArrayList<>(warnings);
            warnings.clear();
            return drained;
        }
    }
}
