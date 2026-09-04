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
import org.apache.cassandra.utils.FBUtilities;

import static java.nio.ByteBuffer.allocate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * The collection guardrails must reach the same verdict on both compaction pipelines.
 * <p>
 * {@code SortedTableWriter} applies them per row, from the merged {@code ComplexColumnData}.
 * {@code SSTableCursorWriter} never builds a row, so it measures each collection while it writes the
 * cells. This drives one merge that crosses the warning threshold down each pipeline and compares
 * what each one emitted. The messages are redacted, so they carry the measured size but neither the
 * table nor the key: equal messages therefore mean equal measurements, which holds only if the
 * cursor writer reproduces {@code Cell.dataSize} exactly.
 * <p>
 * A guardrail the cursor path never reached would emit nothing at all, so this needs the cursor path
 * to have really run rather than fallen back. {@code commitCompaction} asserts that.
 * <p>
 * This lives in the guardrails package for {@link GuardrailEvent}, which is package-private, and
 * extends the differential harness for its two-pipeline machinery.
 */
public class CollectionSizeGuardrailCompactionTest extends DifferentialCompactionTester
{
    /** Small enough that the two halves of one set cross it only once they merge. */
    private static final String WARN_THRESHOLD = "1024B";
    private static final String FAIL_THRESHOLD = "4096B";

    private final WarningCollector collector = new WarningCollector();
    private String originalWarn;
    private String originalFail;
    private boolean originalDiagnostics;

    @Before
    public void armGuardrails()
    {
        originalWarn = Guardrails.instance.getCollectionSizeWarnThreshold();
        originalFail = Guardrails.instance.getCollectionSizeFailThreshold();
        originalDiagnostics = DatabaseDescriptor.diagnosticEventsEnabled();

        Guardrails.instance.setCollectionSizeThreshold(WARN_THRESHOLD, FAIL_THRESHOLD);
        DatabaseDescriptor.setDiagnosticEventsEnabled(true);
        DiagnosticEventService.instance().subscribe(GuardrailEvent.class, collector);
    }

    @After
    public void disarmGuardrails()
    {
        DiagnosticEventService.instance().unsubscribe(collector);
        DatabaseDescriptor.setDiagnosticEventsEnabled(originalDiagnostics);
        Guardrails.instance.setCollectionSizeThreshold(originalWarn, originalFail);
    }

    /**
     * A set whose two halves land in separate sstables. Neither half crosses the threshold on its
     * own, so the flushes stay quiet and only the compaction has anything to report.
     */
    private ColumnFamilyStore twoHalvesOfOneCollection()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v set<blob>)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (k, v) VALUES (1, ?)", set(allocate(768)));
        flush();
        execute("UPDATE %s SET v = v + ? WHERE k = 1", set(allocate(256)));
        flush();

        assertEquals("the fixture needs two sstables to merge", 2, cfs.getLiveSSTables().size());
        collector.drain();
        return cfs;
    }

    /** Compacts a fresh fixture down one pipeline and returns the warnings that compaction emitted. */
    private List<String> warningsFromOneCompaction(boolean cursor) throws Exception
    {
        ColumnFamilyStore cfs = twoHalvesOfOneCollection();
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        commitCompaction(cfs, inputs, cursor, cfs.getDefaultGcBefore(FBUtilities.nowInSeconds()));
        return collector.drain();
    }

    /**
     * The merged set holds 1024 value bytes across two cells, which {@code Cell.dataSize} carries
     * past the 1KiB threshold. Each pipeline compacts its own copy of the fixture, so neither run
     * can disturb the other.
     */
    @Test
    public void bothPipelinesReportTheSameOversizedCollection() throws Exception
    {
        assumeBigFormatSelected();

        List<String> iterator = warningsFromOneCompaction(false);
        List<String> cursor = warningsFromOneCompaction(true);

        assertFalse("the iterator path must warn, or this says nothing about the cursor path",
                    iterator.isEmpty());
        assertEquals("the cursor path must report the same collection size as the iterator path",
                     iterator, cursor);
    }

    /** Records the redacted text of each collection_size warning, in the order the events arrive. */
    private static final class WarningCollector implements Consumer<GuardrailEvent>
    {
        private final List<String> warnings = new CopyOnWriteArrayList<>();

        @Override
        public void accept(GuardrailEvent event)
        {
            if (event.getType() != GuardrailEventType.WARNED)
                return;
            Map<String, Serializable> map = event.toMap();
            if (Guardrails.collectionSize.name.equals(map.get("name")))
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
