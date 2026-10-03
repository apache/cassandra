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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.differential.DifferentialCompactionTester;
import org.apache.cassandra.db.guardrails.GuardrailEvent.GuardrailEventType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static java.nio.ByteBuffer.allocate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Each guardrail that compaction checks fires the same way on the cursor compaction path as on the
 * iterator path. A FAIL on a background compaction does not throw (it has no client state); it emits
 * a {@code FAILED} diagnostic event, which is what these tests compare.
 */
public class GuardrailCompactionTest extends DifferentialCompactionTester
{
    /** Row tombstones in the tombstone fixture. */
    private static final int ROW_TOMBSTONES = 5;

    @Rule
    public final GuardrailSettings guardrails = new GuardrailSettings();

    /**
     * items_per_collection, both thresholds in one compaction: k=1 crosses only warn, k=2 crosses
     * fail, and k=3 stays under both. Each list's two halves stay under the thresholds alone, so the
     * merge is what trips the guardrail.
     */
    @Test
    public void itemsPerCollectionWarnsAndFailsOnCursorPathLikeIterator() throws Exception
    {
        assumeCursorSupportedFormatSelected();

        // warn above 2 items, fail above 4.
        Guardrails.instance.setItemsPerCollectionThreshold(2, 4);
        EventCollector events = guardrails.collect(true, Guardrails.itemsPerCollection.name);

        List<String> iterator = eventsFromOneCompaction(events, false, this::listsCrossingWarnAndFail);
        List<String> cursor = eventsFromOneCompaction(events, true, this::listsCrossingWarnAndFail);

        // Events arrive in partition (token) order; k=3 has no event.
        String prefix = "|items_per_collection|Guardrail items_per_collection violated: Detected collection <redacted> with ";
        Map<Integer, String> eventByKey =
            Map.of(1, GuardrailEventType.WARNED + prefix + "3 items, this exceeds the warning threshold of 2.",
                   2, GuardrailEventType.FAILED + prefix + "6 items, this exceeds the failure threshold of 4.");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        List<String> expected = Stream.of(1, 2, 3)
                                      .sorted(Comparator.comparing((Integer k) -> cfs.decorateKey(Int32Type.instance.decompose(k))))
                                      .filter(eventByKey::containsKey)
                                      .map(eventByKey::get)
                                      .collect(Collectors.toList());

        assertEquals("the iterator path must warn for k=1, fail for k=2 and stay silent for k=3",
                     expected, iterator);
        assertEquals("the cursor path must emit the same items_per_collection events as the iterator path",
                     iterator, cursor);
    }

    /** collection_size fail threshold on a multi-cell map: the merged map crosses the fail byte threshold. */
    @Test
    public void collectionSizeFailsOnCursorPathLikeIterator() throws Exception
    {
        assumeCursorSupportedFormatSelected();

        // warn at 2048B, fail at 4096B: the merged map crosses fail.
        Guardrails.instance.setCollectionSizeThreshold("2048B", "4096B");
        EventCollector events = guardrails.collect(true, Guardrails.collectionSize.name, Guardrails.itemsPerCollection.name);

        List<String> iterator = eventsFromOneCompaction(events, false, this::mapThatFailsOnSize);
        List<String> cursor = eventsFromOneCompaction(events, true, this::mapThatFailsOnSize);

        assertContainsExactlyOneEvent(iterator, GuardrailEventType.FAILED, Guardrails.collectionSize.name);
        assertEquals("the cursor path must emit the same collection_size failure as the iterator path",
                     iterator, cursor);
    }

    /** Both pipelines report the same size for a merged set that crosses the collection_size warn threshold. */
    @Test
    public void bothPipelinesReportTheSameOversizedCollection() throws Exception
    {
        assumeCursorSupportedFormatSelected();

        // Small enough that the two halves of one set cross it only once merged.
        Guardrails.instance.setCollectionSizeThreshold("1024B", "4096B");
        EventCollector events = guardrails.collect(false, Guardrails.collectionSize.name);

        List<String> iterator = eventsFromOneCompaction(events, false, this::twoHalvesOfOneSet);
        List<String> cursor = eventsFromOneCompaction(events, true, this::twoHalvesOfOneSet);

        assertFalse("the iterator path must warn, or this says nothing about the cursor path",
                    iterator.isEmpty());
        assertEquals("the cursor path must report the same collection size as the iterator path",
                     iterator, cursor);
    }

    /** Both pipelines report the same oversized partition, and neither flags the small one. */
    @Test
    public void bothPipelinesReportTheSameOversizedPartition() throws Exception
    {
        assumeCursorSupportedFormatSelected();

        // Small enough that one partition's two halves cross it only once merged; the other stays under.
        Guardrails.instance.setPartitionSizeThreshold("4096B", "1048576B");
        EventCollector events = guardrails.collect(false, Guardrails.partitionSize.name);

        List<String> iterator = eventsFromOneCompaction(events, false, this::oneBigOneSmallPartition);
        List<String> cursor = eventsFromOneCompaction(events, true, this::oneBigOneSmallPartition);

        assertFalse("the iterator path must warn, or this says nothing about the cursor path",
                    iterator.isEmpty());
        assertEquals("only the oversized partition may warn, not the small one",
                     1, iterator.size());
        assertEquals("the cursor path must report the same partition size as the iterator path",
                     iterator, cursor);
    }

    /** The partition_tombstones guardrail fires at the same count on both pipelines. */
    @Test
    public void bothPipelinesWarnAtTheSameTombstoneCount() throws Exception
    {
        assumeCursorSupportedFormatSelected();

        // The fixture's tombstone total sits just above the warn threshold.
        Guardrails.instance.setPartitionTombstonesThreshold(ROW_TOMBSTONES, 1000);
        EventCollector events = guardrails.collect(false, Guardrails.partitionTombstones.name);

        // gcBefore 0 keeps every tombstone.
        List<String> iterator = eventsFromOneCompaction(events, false, this::partitionDeletionOverRowTombstones, cfs -> 0);
        List<String> cursor = eventsFromOneCompaction(events, true, this::partitionDeletionOverRowTombstones, cfs -> 0);

        assertFalse("the iterator path must warn, or this says nothing about the cursor path",
                    iterator.isEmpty());
        assertEquals("the cursor path must count the same tombstones as the iterator path",
                     iterator, cursor);
    }

    /** k=1 merges to 3 items, k=2 to 6 items, k=3 to 2 items. */
    private ColumnFamilyStore listsCrossingWarnAndFail(EventCollector events)
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v list<int>)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (k, v) VALUES (1, ?)", list(1, 2));
        execute("INSERT INTO %s (k, v) VALUES (2, ?)", list(1, 2, 3));
        execute("INSERT INTO %s (k, v) VALUES (3, ?)", list(1));
        flush();
        execute("UPDATE %s SET v = v + ? WHERE k = 1", list(3));
        execute("UPDATE %s SET v = v + ? WHERE k = 2", list(4, 5, 6));
        execute("UPDATE %s SET v = v + ? WHERE k = 3", list(2));
        flush();

        assertEquals("the fixture needs two sstables to merge", 2, cfs.getLiveSSTables().size());
        events.drain();
        return cfs;
    }

    /**
     * A multi-cell map whose merge crosses the 4096B fail threshold, alongside a small map that stays
     * under both thresholds.
     */
    private ColumnFamilyStore mapThatFailsOnSize(EventCollector events)
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v map<int, blob>)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // offending partition: 2 entries of ~1200B per half, ~4800B once merged, over the 4096B fail.
        execute("INSERT INTO %s (k, v) VALUES (1, ?)", map(1, allocate(1200), 2, allocate(1200)));
        // within-threshold partition: one small entry per half, well under 2048B once merged.
        execute("INSERT INTO %s (k, v) VALUES (2, ?)", map(1, allocate(100)));
        flush();
        execute("UPDATE %s SET v = v + ? WHERE k = 1", map(3, allocate(1200), 4, allocate(1200)));
        execute("UPDATE %s SET v = v + ? WHERE k = 2", map(2, allocate(100)));
        flush();

        assertEquals("the fixture needs two sstables to merge", 2, cfs.getLiveSSTables().size());
        events.drain();
        return cfs;
    }

    /** A set whose two halves land in separate sstables, neither crossing the threshold alone. */
    private ColumnFamilyStore twoHalvesOfOneSet(EventCollector events)
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v set<blob>)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (k, v) VALUES (1, ?)", set(allocate(768)));
        flush();
        execute("UPDATE %s SET v = v + ? WHERE k = 1", set(allocate(256)));
        flush();

        assertEquals("the fixture needs two sstables to merge", 2, cfs.getLiveSSTables().size());
        events.drain();
        return cfs;
    }

    /**
     * Two partitions across two sstables:
     * k=1 has three 1 KiB rows in each sstable, under the threshold alone but over it merged;
     * k=2 has one small row, under the threshold either way.
     */
    private ColumnFamilyStore oneBigOneSmallPartition(EventCollector events)
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
                     List.of(), events.drain());
        return cfs;
    }

    /** One partition with a partition deletion and, newer, {@link #ROW_TOMBSTONES} row tombstones, split across two sstables. */
    private ColumnFamilyStore partitionDeletionOverRowTombstones(EventCollector events)
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
        events.drain();
        return cfs;
    }

    /** Builds a fresh fixture, compacts it down one pipeline, and returns the events that compaction emitted. */
    private List<String> eventsFromOneCompaction(EventCollector events, boolean cursor,
                                                 Function<EventCollector, ColumnFamilyStore> fixture) throws Exception
    {
        return eventsFromOneCompaction(events, cursor, fixture, cfs -> cfs.getDefaultGcBefore(FBUtilities.nowInSeconds()));
    }

    private List<String> eventsFromOneCompaction(EventCollector events, boolean cursor,
                                                 Function<EventCollector, ColumnFamilyStore> fixture,
                                                 ToLongFunction<ColumnFamilyStore> gcBefore) throws Exception
    {
        ColumnFamilyStore cfs = fixture.apply(events);
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        commitCompaction(cfs, inputs, cursor, gcBefore.applyAsLong(cfs));
        return events.drain();
    }

    /**
     * Asserts the compaction emitted exactly one event of the given type and guardrail. The diagnostic
     * message is redacted (it carries user data), so the offending partition key is not in it; the count
     * of exactly one is what proves only the offending collection tripped and the within-threshold one in
     * the same fixture stayed silent.
     */
    private static void assertContainsExactlyOneEvent(List<String> events, GuardrailEventType type, String guardrail)
    {
        assertEquals("the iterator path must emit exactly one " + type + " event for " + guardrail +
                     " (the within-threshold partition must stay silent), or this says nothing about the " +
                     "cursor path; got " + events,
                     1, events.size());
        String only = events.get(0);
        assertTrue("the only event must be a " + type + " for " + guardrail + "; got " + only,
                   only.startsWith(type + "|" + guardrail + "|"));
    }

    /**
     * Saves the compaction guardrail thresholds and the diagnostic events setting before each test, and
     * restores them afterwards, even when the test fails. Also removes any collector the test subscribed.
     */
    public static final class GuardrailSettings extends ExternalResource
    {
        private final List<EventCollector> collectors = new ArrayList<>();
        private String collectionSizeWarn;
        private String collectionSizeFail;
        private int itemsWarn;
        private int itemsFail;
        private String partitionSizeWarn;
        private String partitionSizeFail;
        private long tombstonesWarn;
        private long tombstonesFail;
        private boolean diagnostics;

        @Override
        protected void before()
        {
            collectionSizeWarn = Guardrails.instance.getCollectionSizeWarnThreshold();
            collectionSizeFail = Guardrails.instance.getCollectionSizeFailThreshold();
            itemsWarn = Guardrails.instance.getItemsPerCollectionWarnThreshold();
            itemsFail = Guardrails.instance.getItemsPerCollectionFailThreshold();
            partitionSizeWarn = Guardrails.instance.getPartitionSizeWarnThreshold();
            partitionSizeFail = Guardrails.instance.getPartitionSizeFailThreshold();
            tombstonesWarn = Guardrails.instance.getPartitionTombstonesWarnThreshold();
            tombstonesFail = Guardrails.instance.getPartitionTombstonesFailThreshold();
            diagnostics = DatabaseDescriptor.diagnosticEventsEnabled();

            DatabaseDescriptor.setDiagnosticEventsEnabled(true);
        }

        @Override
        protected void after()
        {
            collectors.forEach(DiagnosticEventService.instance()::unsubscribe);
            collectors.clear();
            DatabaseDescriptor.setDiagnosticEventsEnabled(diagnostics);
            Guardrails.instance.setCollectionSizeThreshold(collectionSizeWarn, collectionSizeFail);
            Guardrails.instance.setItemsPerCollectionThreshold(itemsWarn, itemsFail);
            Guardrails.instance.setPartitionSizeThreshold(partitionSizeWarn, partitionSizeFail);
            Guardrails.instance.setPartitionTombstonesThreshold(tombstonesWarn, tombstonesFail);
        }

        /** Subscribes a collector for the named guardrails; FAIL events are kept only if {@code failures}. */
        EventCollector collect(boolean failures, String... guardrailNames)
        {
            EventCollector collector = new EventCollector(failures, Set.of(guardrailNames));
            collectors.add(collector);
            DiagnosticEventService.instance().subscribe(GuardrailEvent.class, collector);
            return collector;
        }
    }

    /**
     * Records each WARN (and optionally FAIL) event of the named guardrails as {@code TYPE|name|message},
     * in arrival order, so both compaction paths can be compared event for event.
     */
    static final class EventCollector implements Consumer<GuardrailEvent>
    {
        private final boolean failures;
        private final Set<String> guardrailNames;
        private final List<String> events = new CopyOnWriteArrayList<>();

        EventCollector(boolean failures, Set<String> guardrailNames)
        {
            this.failures = failures;
            this.guardrailNames = guardrailNames;
        }

        @Override
        public void accept(GuardrailEvent event)
        {
            Enum<GuardrailEventType> type = event.getType();
            if (type != GuardrailEventType.WARNED && !(failures && type == GuardrailEventType.FAILED))
                return;
            Map<String, Serializable> map = event.toMap();
            String name = String.valueOf(map.get("name"));
            if (guardrailNames.contains(name))
                events.add(type + "|" + name + "|" + map.get("message"));
        }

        List<String> drain()
        {
            List<String> drained = new ArrayList<>(events);
            events.clear();
            return drained;
        }
    }
}
