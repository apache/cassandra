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
import static org.junit.Assert.assertTrue;

/**
 * The sibling {@link CollectionSizeGuardrailCompactionTest} covers exactly one arm of the collection
 * guardrail family on the cursor compaction path: the {@code collection_size} guardrail, WARN threshold
 * only, on a multi-cell {@code set<blob>}. Audit Gap 9 is that the remaining arms are never exercised on
 * the cursor path. This test covers the missing arms, and each asserts the cursor path reaches the same
 * verdict as the iterator path:
 * <ul>
 *   <li>{@code items_per_collection} WARN, on a multi-cell {@code list<int>}.</li>
 *   <li>{@code items_per_collection} FAIL, on a multi-cell {@code list<int>}.</li>
 *   <li>{@code collection_size} FAIL, on a multi-cell {@code map<int, blob>}.</li>
 * </ul>
 * Both guardrails are disabled by default, so each scenario arms only the guardrail it exercises and
 * leaves the other off. The events compaction emits are captured through the same diagnostic-event
 * mechanism the sibling asserts on; a FAIL on a background write does not throw (its client state is
 * null and the guardrail does not throw on a null state), it emits a {@code FAILED} diagnostic event.
 */
public class CollectionGuardrailArmsCompactionTest extends DifferentialCompactionTester
{
    private final EventCollector collector = new EventCollector();
    private String originalSizeWarn;
    private String originalSizeFail;
    private int originalItemsWarn;
    private int originalItemsFail;
    private boolean originalDiagnostics;

    @Before
    public void armGuardrails()
    {
        originalSizeWarn = Guardrails.instance.getCollectionSizeWarnThreshold();
        originalSizeFail = Guardrails.instance.getCollectionSizeFailThreshold();
        originalItemsWarn = Guardrails.instance.getItemsPerCollectionWarnThreshold();
        originalItemsFail = Guardrails.instance.getItemsPerCollectionFailThreshold();
        originalDiagnostics = DatabaseDescriptor.diagnosticEventsEnabled();

        DatabaseDescriptor.setDiagnosticEventsEnabled(true);
        DiagnosticEventService.instance().subscribe(GuardrailEvent.class, collector);
    }

    @After
    public void disarmGuardrails()
    {
        DiagnosticEventService.instance().unsubscribe(collector);
        DatabaseDescriptor.setDiagnosticEventsEnabled(originalDiagnostics);
        Guardrails.instance.setCollectionSizeThreshold(originalSizeWarn, originalSizeFail);
        Guardrails.instance.setItemsPerCollectionThreshold(originalItemsWarn, originalItemsFail);
    }

    /**
     * items_per_collection WARN arm: a multi-cell list whose two halves each stay under the warn count
     * but whose merge crosses it. The merge, not the flush, is what trips the guardrail.
     */
    @Test
    public void itemsPerCollectionWarnsOnCursorPathLikeIterator() throws Exception
    {
        assumeCursorSupportedFormatSelected();

        // warn above 4 items, fail disabled: only the WARN arm can fire.
        Guardrails.instance.setItemsPerCollectionThreshold(4, -1);

        List<String> iterator = eventsFromOneCompaction(false, this::listFixtureThatWarnsOnItems);
        List<String> cursor = eventsFromOneCompaction(true, this::listFixtureThatWarnsOnItems);

        assertContainsExactlyOneEvent(iterator, GuardrailEventType.WARNED, Guardrails.itemsPerCollection.name);
        assertEquals("the cursor path must emit the same items_per_collection warning as the iterator path",
                     iterator, cursor);
    }

    /**
     * items_per_collection FAIL arm: the merged list crosses the fail count. On a background write the
     * FAIL does not throw; it emits a FAILED diagnostic event, and it supersedes the WARN.
     */
    @Test
    public void itemsPerCollectionFailsOnCursorPathLikeIterator() throws Exception
    {
        assumeCursorSupportedFormatSelected();

        // warn above 2, fail above 4: the 6-item merge crosses fail, so a single FAILED event fires.
        Guardrails.instance.setItemsPerCollectionThreshold(2, 4);

        List<String> iterator = eventsFromOneCompaction(false, this::listFixtureThatFailsOnItems);
        List<String> cursor = eventsFromOneCompaction(true, this::listFixtureThatFailsOnItems);

        assertContainsExactlyOneEvent(iterator, GuardrailEventType.FAILED, Guardrails.itemsPerCollection.name);
        assertEquals("the cursor path must emit the same items_per_collection failure as the iterator path",
                     iterator, cursor);
    }

    /**
     * collection_size FAIL arm on a multi-cell map: the sibling only covers the WARN arm, on a set. Here
     * the merged map crosses the fail byte threshold and a FAILED event fires on both paths.
     */
    @Test
    public void collectionSizeFailsOnCursorPathLikeIterator() throws Exception
    {
        assumeCursorSupportedFormatSelected();

        // warn at 2048B, fail at 4096B: the merged map crosses fail.
        Guardrails.instance.setCollectionSizeThreshold("2048B", "4096B");

        List<String> iterator = eventsFromOneCompaction(false, this::mapFixtureThatFailsOnSize);
        List<String> cursor = eventsFromOneCompaction(true, this::mapFixtureThatFailsOnSize);

        assertContainsExactlyOneEvent(iterator, GuardrailEventType.FAILED, Guardrails.collectionSize.name);
        assertEquals("the cursor path must emit the same collection_size failure as the iterator path",
                     iterator, cursor);
    }

    /**
     * A multi-cell list whose merge holds 6 elements (over a warn-of-4). A second partition's list holds
     * only 2 merged elements and must stay silent, proving the guardrail fires on the offending
     * collection and not on a within-threshold one.
     */
    private ColumnFamilyStore listFixtureThatWarnsOnItems()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v list<int>)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // offending partition: two halves of 3, neither over 4 alone, 6 once merged.
        execute("INSERT INTO %s (k, v) VALUES (1, ?)", list(1, 2, 3));
        // within-threshold partition: one element per half, 2 once merged.
        execute("INSERT INTO %s (k, v) VALUES (2, ?)", list(1));
        flush();
        execute("UPDATE %s SET v = v + ? WHERE k = 1", list(4, 5, 6));
        execute("UPDATE %s SET v = v + ? WHERE k = 2", list(2));
        flush();

        assertEquals("the fixture needs two sstables to merge", 2, cfs.getLiveSSTables().size());
        collector.drain();
        return cfs;
    }

    /** As above but the offending list merges to 6 elements, over a fail-of-4. */
    private ColumnFamilyStore listFixtureThatFailsOnItems()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v list<int>)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (k, v) VALUES (1, ?)", list(1, 2, 3));
        execute("INSERT INTO %s (k, v) VALUES (2, ?)", list(1));
        flush();
        execute("UPDATE %s SET v = v + ? WHERE k = 1", list(4, 5, 6));
        execute("UPDATE %s SET v = v + ? WHERE k = 2", list(2));
        flush();

        assertEquals("the fixture needs two sstables to merge", 2, cfs.getLiveSSTables().size());
        collector.drain();
        return cfs;
    }

    /**
     * A multi-cell map whose merge crosses the 4096B fail threshold, alongside a small map that stays
     * under both thresholds.
     */
    private ColumnFamilyStore mapFixtureThatFailsOnSize()
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
        collector.drain();
        return cfs;
    }

    /** Builds a fresh fixture, compacts it down one pipeline, and returns the events that compaction emitted. */
    private List<String> eventsFromOneCompaction(boolean cursor, java.util.function.Supplier<ColumnFamilyStore> fixture) throws Exception
    {
        ColumnFamilyStore cfs = fixture.get();
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        commitCompaction(cfs, inputs, cursor, cfs.getDefaultGcBefore(FBUtilities.nowInSeconds()));
        return collector.drain();
    }

    /**
     * Asserts the compaction emitted exactly one event of the given type and guardrail. The diagnostic
     * message is redacted (it carries user data), so the offending partition key is not in it; the count
     * of exactly one is what proves only the offending collection tripped and the within-threshold one in
     * the same fixture stayed silent — a second offending collection would add a second event.
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
     * Records every collection guardrail activation (warn or fail) as {@code TYPE|name|message}, in the
     * order the events arrive, so both compaction paths can be compared arm for arm.
     */
    private static final class EventCollector implements Consumer<GuardrailEvent>
    {
        private final List<String> events = new CopyOnWriteArrayList<>();

        @Override
        public void accept(GuardrailEvent event)
        {
            Map<String, Serializable> map = event.toMap();
            String name = String.valueOf(map.get("name"));
            if (Guardrails.collectionSize.name.equals(name) || Guardrails.itemsPerCollection.name.equals(name))
                events.add(event.getType() + "|" + name + "|" + map.get("message"));
        }

        List<String> drain()
        {
            List<String> drained = new ArrayList<>(events);
            events.clear();
            return drained;
        }
    }
}
