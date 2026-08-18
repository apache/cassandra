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
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Pins the cursor compaction support matrix.
 *
 * Each increment of the cursor completion work flips
 * its row here from unsupported to supported. A change in the gate's semantics — whether it decides on the
 * schema, on an input sstable's header, or on the compaction the controller describes — that silently
 * widens or narrows the fallback becomes a test failure instead of a silently different code path in
 * production.
 */
public class CursorSupportMatrixTest extends CQLTester
{
    private TableMetadata metadataFor(String createTable)
    {
        createTable(createTable);
        return getCurrentColumnFamilyStore().metadata();
    }

    private void assertSupported(String createTable)
    {
        TableMetadata metadata = metadataFor(createTable);
        assertFalse("expected cursor-supported metadata: " + metadata,
                    CursorCompactor.unsupportedMetadata(metadata));
    }

    private void assertUnsupported(String createTable)
    {
        TableMetadata metadata = metadataFor(createTable);
        assertTrue("expected cursor-UNsupported metadata: " + metadata,
                   CursorCompactor.unsupportedMetadata(metadata));
    }

    @Test
    public void simpleTableSupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
    }

    @Test
    public void staticColumnsSupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint, s text static, ck bigint, v text, PRIMARY KEY (pk, ck))");
    }

    @Test
    public void noClusteringSupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint PRIMARY KEY, v text)");
    }

    /** Frozen collections/tuples/UDTs are single cells: inside the supported surface. */
    @Test
    public void frozenCollectionsSupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, " +
                        "m frozen<map<text, bigint>>, l frozen<list<text>>, s frozen<set<int>>, " +
                        "t frozen<tuple<int, text>>, PRIMARY KEY (pk, ck))");
    }

    /** Frozen UDT as the CLUSTERING key: still a single cell, not a regular column. */
    @Test
    public void frozenUdtInClusteringKeySupported()
    {
        String udt = createType("CREATE TYPE %s (a int, b text)");
        assertSupported("CREATE TABLE %s (pk bigint, ck frozen<" + udt + ">, v text, PRIMARY KEY (pk, ck))");
    }

    /** Frozen UDT as (part of) the PARTITION key. */
    @Test
    public void frozenUdtInPartitionKeySupported()
    {
        String udt = createType("CREATE TYPE %s (a int, b text)");
        assertSupported("CREATE TABLE %s (pk frozen<" + udt + ">, ck bigint, v text, PRIMARY KEY (pk, ck))");
    }

    /** Frozen collections as (part of) the PRIMARY key, not just as regular columns. */
    @Test
    public void frozenCollectionInPrimaryKeySupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint, ck frozen<list<int>>, v text, PRIMARY KEY (pk, ck))");
        assertSupported("CREATE TABLE %s (pk frozen<set<text>>, ck bigint, v text, PRIMARY KEY (pk, ck))");
    }

    /** Increment 2 flips these to supported. */
    @Test
    public void multiCellCollectionsUnsupported()
    {
        assertUnsupported("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, PRIMARY KEY (pk, ck))");
        assertUnsupported("CREATE TABLE %s (pk bigint, ck bigint, l list<text>, PRIMARY KEY (pk, ck))");
        assertUnsupported("CREATE TABLE %s (pk bigint, ck bigint, s set<int>, PRIMARY KEY (pk, ck))");
    }

    /** Increment 2 flips this to supported. */
    @Test
    public void multiCellUdtUnsupported()
    {
        String udt = createType("CREATE TYPE %s (a int, b text)");
        assertUnsupported("CREATE TABLE %s (pk bigint, ck bigint, u " + udt + ", PRIMARY KEY (pk, ck))");
    }

    /** Vector and duration are inside the supported surface (single-cell types). */
    @Test
    public void vectorAndDurationSupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, vec vector<float, 3>, dur duration, " +
                        "PRIMARY KEY (pk, ck))");
    }

    /** Increment 5 flips this to supported. */
    @Test
    public void countersUnsupported()
    {
        assertUnsupported("CREATE TABLE %s (pk bigint, ck bigint, c counter, PRIMARY KEY (pk, ck))");
    }

    /** Out of scope for the current plan: indexes keep the iterator path. */
    @Test
    public void secondaryIndexUnsupported()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s (v)");
        assertTrue("expected index to disqualify cursor compaction",
                   CursorCompactor.unsupportedMetadata(getCurrentColumnFamilyStore().metadata()));
    }

    /**
     * Why the cursor path must refuse this is at the gate, in {@code CursorCompactor.isSupported}'s
     * ignore-gc-grace branch. What is test-specific is when the gate can be observed.
     * <p>
     * The set exists only for the duration of the force compaction:
     * {@code forceCompactionKeysIgnoringGcGrace} populates it, hands the keys to {@code CompactionManager},
     * and clears it in a finally block. The gate therefore has to be evaluated from inside that window,
     * which is what the tracker subscriber below is for — the compaction's own sstable swap is published on
     * the compaction thread while the caller is still blocked inside the force compaction, with the set
     * populated.
     */
    @Test
    public void ignoreGcGraceForAnyKeyUnsupported() throws Throwable
    {
        // cursor compaction only supports BIG output, so under a non-BIG format isSupported is false for
        // every table and the assertions below could not tell the ignore-gc-grace gate apart
        Assume.assumeTrue("requires the BIG sstable format", BigFormat.isSelected());

        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // pk 1 is the key forcecompact will name, and its row deletion is what that compaction is asked to
        // purge ahead of gc grace; pk 2 keeps the output non-empty whatever the purge decides about pk 1,
        // so the observer below always has an output sstable to gate on
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 'x')");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 1, 'y')");
        flush();
        execute("DELETE FROM %s WHERE pk = 1 AND ck = 1");
        flush();

        // control: the same table, the same two sstables, with nothing ignoring gc grace, is supported —
        // so the rejection observed below is attributable to the key set and to no other gate
        assertEquals("expected one sstable per flush", 2, cfs.getLiveSSTables().size());
        assertFalse("no key should ignore gc grace outside a force compaction",
                    cfs.shouldIgnoreGcGraceForAnyKey());
        assertTrue("expected a plain two-sstable table to be cursor-supported",
                   isSupportedNow(cfs));

        AtomicReference<Boolean> ignoredGcGraceInside = new AtomicReference<>();
        AtomicReference<Boolean> supportedInside = new AtomicReference<>();
        AtomicReference<Throwable> observerFailure = new AtomicReference<>();
        INotificationConsumer observer = (notification, sender) ->
        {
            if (!(notification instanceof SSTableListChangedNotification))
                return;
            // record the first sstable swap only, and record the key set before anything else, so that a
            // throw below cannot leave the window unaccounted for
            if (!ignoredGcGraceInside.compareAndSet(null, cfs.shouldIgnoreGcGraceForAnyKey()))
                return;
            try
            {
                // the compaction strategy manager subscribes to the tracker in the ColumnFamilyStore
                // constructor, so it precedes this observer and has already taken the swap and released
                // its lock: on the commit path the live set here is the compaction's output, and it is
                // safe to open scanners over it
                supportedInside.set(isSupportedNow(cfs));
            }
            catch (Throwable t)
            {
                // Tracker merges a subscriber's throw into the compaction's own failure, which would say
                // nothing about the gate; carry it out and rethrow it on the calling thread instead
                observerFailure.set(t);
            }
        };

        cfs.getTracker().subscribe(observer);
        try
        {
            cfs.forceCompactionKeysIgnoringGcGrace("1");
        }
        finally
        {
            cfs.getTracker().unsubscribe(observer);
        }

        if (observerFailure.get() != null)
            throw observerFailure.get();

        // the scenario is only covering its subject while the force compaction really compacts something
        // with the key set populated; assert both, so it cannot go quiet and still pass
        assertNotNull("expected the force compaction to change the sstable list while the observer was " +
                      "subscribed; with no compaction there is no window in which the gate is exercised",
                      ignoredGcGraceInside.get());
        assertTrue("expected the ignore-gc-grace key set to be populated for the duration of the force " +
                   "compaction; if it is not, the rejection below is not attributable to this gate",
                   ignoredGcGraceInside.get());

        assertNotNull("expected the observer to have evaluated the gate", supportedInside.get());
        assertFalse("cursor compaction must refuse a table while any key ignores gc grace: the iterator " +
                    "suppresses row-level purging wholesale there and a streaming cursor cannot",
                    supportedInside.get());

        // and the gate reopens once the force compaction has cleared the set, using the very same helper
        // that returned false inside the window, so that assertion cannot be vacuously false
        assertFalse("expected the key set to be cleared when the force compaction returned",
                    cfs.shouldIgnoreGcGraceForAnyKey());
        assertTrue("expected the table to be cursor-supported again after the force compaction",
                   isSupportedNow(cfs));
    }

    /**
     * A DROPPED non-frozen collection is gone from the schema but still present in the header of
     * every sstable written before the drop, still multi-cell. The metadata-level check cannot see
     * it, so the gate has to screen the input headers too — otherwise the cell cursor, which has no
     * complex framing, misparses those rows.
     */
    @Test
    public void droppedCollectionUnsupportedFromHeaders() throws Exception
    {
        assertDroppedCollectionUnsupported("CREATE TABLE %s (pk bigint, ck bigint, m map<text, text>, " +
                                           "v text, PRIMARY KEY (pk, ck))",
                                           "INSERT INTO %s (pk, ck, m, v) VALUES (1, 1, {'a':'b'}, 'x')",
                                           false);
    }

    /**
     * Same hole via a dropped STATIC collection, which lands in header.columns(true) only — so a
     * check that inspected regular columns alone would pass this test's regular-column sibling and
     * still admit the misparse.
     */
    @Test
    public void droppedStaticCollectionUnsupportedFromHeaders() throws Exception
    {
        assertDroppedCollectionUnsupported("CREATE TABLE %s (pk bigint, ck bigint, " +
                                           "m map<text, text> static, v text, PRIMARY KEY (pk, ck))",
                                           "INSERT INTO %s (pk, ck, m, v) VALUES (1, 1, {'a':'b'}, 'x')",
                                           true);
    }

    private void assertDroppedCollectionUnsupported(String ddl, String insert, boolean isStatic) throws Exception
    {
        // cursor compaction only supports BIG output, so under a non-BIG format isSupported is
        // false for every table and the assertion below could not tell the header check apart
        Assume.assumeTrue("requires the BIG sstable format", BigFormat.isSelected());

        createTable(ddl);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute(insert);
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 'y')");
        flush();

        execute("ALTER TABLE %s DROP m");

        // the schema-level check is satisfied: the collection is no longer a current column
        assertFalse("dropped collection should leave the metadata check satisfied, which is exactly " +
                    "why the header check is needed",
                    CursorCompactor.unsupportedMetadata(cfs.metadata()));

        // ... but the pre-drop sstable still carries it, multi-cell, in its own header
        boolean anyHeaderStillHasIt = false;
        for (SSTableReader reader : cfs.getLiveSSTables())
            for (ColumnMetadata column : reader.header.columns(isStatic))
                anyHeaderStillHasIt |= column.isComplex();
        assertTrue("expected a pre-drop sstable header to still list the collection as multi-cell",
                   anyHeaderStillHasIt);

        assertFalse("cursor compaction must refuse a table whose input headers still carry a " +
                    "multi-cell column; the cursor cannot parse complex framing",
                    isSupportedNow(cfs));

        // Positive control on a separate table: isSupportedNow returns true for an equivalent table
        // that never had a collection, so the rejection above is attributable to the dropped column
        // and not to the harness, the format or any other gate. (The table under test cannot serve
        // as its own pre-drop control: while the collection is still live the SCHEMA check rejects
        // it, which is the very check the drop defeats.)
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore plain = getCurrentColumnFamilyStore();
        plain.disableAutoCompaction();
        // same shape as the table under test after its drop — same surviving columns, same two
        // inserts and two flushes — so the only difference left is the dropped collection itself
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 'x')");
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 'y')");
        flush();
        assertTrue("expected a plain table with no dropped collection to be cursor-supported",
                   isSupportedNow(plain));
    }

    private boolean isSupportedNow(ColumnFamilyStore cfs) throws Exception
    {
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        try (CompactionController controller = new CompactionController(cfs, inputs, FBUtilities.nowInSeconds());
             AbstractCompactionStrategy.ScannerList scanners =
                 cfs.getCompactionStrategyManager().getScanners(new ArrayList<>(inputs), null))
        {
            return CursorCompactor.isSupported(scanners, controller);
        }
    }
}
