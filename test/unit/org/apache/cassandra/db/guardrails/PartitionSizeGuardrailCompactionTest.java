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

/** The {@code partition_size} guardrail reaches the same verdict on both compaction pipelines. */
public class PartitionSizeGuardrailCompactionTest extends DifferentialCompactionTester
{
    /** Small enough that one partition's two halves cross it only once merged; the other stays under. */
    private static final String WARN_THRESHOLD = "4096B";
    private static final String FAIL_THRESHOLD = "1048576B";

    private final WarningCollector collector = new WarningCollector();
    private String originalWarn;
    private String originalFail;
    private boolean originalDiagnostics;

    @Before
    public void armGuardrails()
    {
        originalWarn = Guardrails.instance.getPartitionSizeWarnThreshold();
        originalFail = Guardrails.instance.getPartitionSizeFailThreshold();
        originalDiagnostics = DatabaseDescriptor.diagnosticEventsEnabled();

        Guardrails.instance.setPartitionSizeThreshold(WARN_THRESHOLD, FAIL_THRESHOLD);
        DatabaseDescriptor.setDiagnosticEventsEnabled(true);
        DiagnosticEventService.instance().subscribe(GuardrailEvent.class, collector);
    }

    @After
    public void disarmGuardrails()
    {
        DiagnosticEventService.instance().unsubscribe(collector);
        DatabaseDescriptor.setDiagnosticEventsEnabled(originalDiagnostics);
        Guardrails.instance.setPartitionSizeThreshold(originalWarn, originalFail);
    }

    /**
     * Two partitions across two sstables:
     * k=1 has three 1 KiB rows in each sstable, under the threshold alone but over it merged;
     * k=2 has one small row, under the threshold either way.
     */
    private ColumnFamilyStore oneBigOneSmallPartition()
    {
        createTable("CREATE TABLE %s (k int, c int, v blob, PRIMARY KEY (k, c))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // Fixed timestamps keep both fixtures byte-identical: a wall-clock write timestamp is
        // vint-encoded in the data file, so it would otherwise nudge the partition size a byte or
        // two between the iterator-path build and the cursor-path build, and the reported sizes
        // would not compare equal.
        for (int c = 0; c < 3; c++)
            execute("INSERT INTO %s (k, c, v) VALUES (1, ?, ?) USING TIMESTAMP ?", c, allocate(1024), 100L + c);
        execute("INSERT INTO %s (k, c, v) VALUES (2, 0, ?) USING TIMESTAMP 200", allocate(64));
        flush();

        for (int c = 3; c < 6; c++)
            execute("INSERT INTO %s (k, c, v) VALUES (1, ?, ?) USING TIMESTAMP ?", c, allocate(1024), 100L + c);
        flush();

        assertEquals("the fixture needs two sstables to merge", 2, cfs.getLiveSSTables().size());
        // Nothing should have warned on flush: each sstable's k=1 partition is ~3 KiB, under 4 KiB.
        assertEquals("the fixture must not trip the guardrail before compaction",
                     List.of(), collector.drain());
        return cfs;
    }

    /** Compacts a fresh fixture down one pipeline and returns the warnings that compaction emitted. */
    private List<String> warningsFromOneCompaction(boolean cursor) throws Exception
    {
        ColumnFamilyStore cfs = oneBigOneSmallPartition();
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        commitCompaction(cfs, inputs, cursor, cfs.getDefaultGcBefore(FBUtilities.nowInSeconds()));
        return collector.drain();
    }

    /** Both pipelines report the same oversized partition, and neither flags the small one. */
    @Test
    public void bothPipelinesReportTheSameOversizedPartition() throws Exception
    {
        assumeCursorSupportedFormatSelected();

        List<String> iterator = warningsFromOneCompaction(false);
        List<String> cursor = warningsFromOneCompaction(true);

        assertFalse("the iterator path must warn, or this says nothing about the cursor path",
                    iterator.isEmpty());
        assertEquals("only the oversized partition may warn, not the small one",
                     1, iterator.size());
        assertEquals("the cursor path must report the same partition size as the iterator path",
                     iterator, cursor);
    }

    /**
     * Independent of the guardrail: the two pipelines must produce byte-identical output for the
     * identical fixture. If the Data.db files diverge, the partition size the guardrail reports
     * diverges with them, which is the root cause when {@link #bothPipelinesReportTheSameOversizedPartition}
     * disagrees on the size.
     */
    @Test
    public void differentialOnTheSameFixtureIsByteIdentical() throws Exception
    {
        assumeCursorSupportedFormatSelected();

        ColumnFamilyStore cfs = oneBigOneSmallPartition();
        assertCursorMatchesIterator(cfs);
    }

    /** Records the redacted text of each partition_size warning, in the order the events arrive. */
    private static final class WarningCollector implements Consumer<GuardrailEvent>
    {
        private final List<String> warnings = new CopyOnWriteArrayList<>();

        @Override
        public void accept(GuardrailEvent event)
        {
            if (event.getType() != GuardrailEventType.WARNED)
                return;
            Map<String, Serializable> map = event.toMap();
            if (Guardrails.partitionSize.name.equals(map.get("name")))
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
