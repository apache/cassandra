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
 * <p>
 * The gate decides on three inputs: the schema, an input sstable's header, and the compaction the
 * controller describes. A change in its semantics that widens or narrows the fallback fails a test
 * here, instead of taking a different code path in production.
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

    @Test
    public void frozenUdtInPartitionKeySupported()
    {
        String udt = createType("CREATE TYPE %s (a int, b text)");
        assertSupported("CREATE TABLE %s (pk frozen<" + udt + ">, ck bigint, v text, PRIMARY KEY (pk, ck))");
    }

    @Test
    public void frozenCollectionInPrimaryKeySupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint, ck frozen<list<int>>, v text, PRIMARY KEY (pk, ck))");
        assertSupported("CREATE TABLE %s (pk frozen<set<text>>, ck bigint, v text, PRIMARY KEY (pk, ck))");
    }

    /** The cursor path can read, merge and write a multi-cell collection. */
    @Test
    public void multiCellCollectionsSupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, PRIMARY KEY (pk, ck))");
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, l list<text>, PRIMARY KEY (pk, ck))");
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, s set<int>, PRIMARY KEY (pk, ck))");
    }

    /** The cursor path can read, merge and write a multi-cell UDT. */
    @Test
    public void multiCellUdtSupported()
    {
        String udt = createType("CREATE TYPE %s (a int, b text)");
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, u " + udt + ", PRIMARY KEY (pk, ck))");
    }

    /** Vector and duration are inside the supported surface (single-cell types). */
    @Test
    public void vectorAndDurationSupported()
    {
        assertSupported("CREATE TABLE %s (pk bigint, ck bigint, vec vector<float, 3>, dur duration, " +
                        "PRIMARY KEY (pk, ck))");
    }

    /** Counter columns are a planned gap in the supported surface, not a permanent limit. */
    @Test
    public void countersUnsupported()
    {
        assertUnsupported("CREATE TABLE %s (pk bigint, ck bigint, c counter, PRIMARY KEY (pk, ck))");
    }

    /** An indexed table keeps the iterator path. */
    @Test
    public void secondaryIndexUnsupported()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s (v)");
        assertTrue("expected index to disqualify cursor compaction",
                   CursorCompactor.unsupportedMetadata(getCurrentColumnFamilyStore().metadata()));
    }

    /**
     * The refusal belongs to the gate, in {@code CursorCompactor.isSupported}'s ignore-gc-grace
     * branch. This test supplies only the window in which the gate can be observed.
     * <p>
     * The key set lives only for the duration of the force compaction.
     * {@code forceCompactionKeysIgnoringGcGrace} populates it, hands the keys to
     * {@code CompactionManager}, then clears it in a finally block. The tracker subscriber below
     * reads the gate from inside that window. The compaction publishes its sstable swap on the
     * compaction thread while the caller still blocks inside the force compaction, with the set
     * populated.
     */
    @Test
    public void ignoreGcGraceForAnyKeyUnsupported() throws Throwable
    {
        // cursor compaction only supports BIG output. Under another format isSupported is false for
        // every table, so the assertions below could not tell the ignore-gc-grace gate apart
        Assume.assumeTrue("requires the BIG sstable format", BigFormat.isSelected());

        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // pk 1 is the key forcecompact names, and its row deletion is what that compaction must purge
        // ahead of gc grace. pk 2 keeps the output non-empty whatever the purge decides about pk 1, so
        // the observer below always has an output sstable to gate on
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 'x')");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 1, 'y')");
        flush();
        execute("DELETE FROM %s WHERE pk = 1 AND ck = 1");
        flush();

        // control: the same table and the same two sstables, with nothing ignoring gc grace, is
        // supported. The rejection observed below is therefore attributable to the key set alone
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
            // record the first sstable swap only. Record the key set before anything else, so a
            // throw below cannot leave the window unaccounted for
            if (!ignoredGcGraceInside.compareAndSet(null, cfs.shouldIgnoreGcGraceForAnyKey()))
                return;
            try
            {
                // the compaction strategy manager subscribes to the tracker in the ColumnFamilyStore
                // constructor, so it precedes this observer. It has already taken the swap and
                // released its lock. On the commit path the live set here is the compaction's output,
                // so it is safe to open scanners over it
                supportedInside.set(isSupportedNow(cfs));
            }
            catch (Throwable t)
            {
                // Tracker merges a subscriber's throw into the compaction's own failure, which would
                // say nothing about the gate. Carry the throw out and rethrow it on the calling thread
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

        // the scenario covers its subject only while the force compaction really compacts something
        // with the key set populated. Assert both, so the test cannot go quiet and still pass
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

        // the gate reopens once the force compaction has cleared the set. The assertion below reuses
        // the helper that returned false inside the window, so the helper is not hardwired to one answer
        assertFalse("expected the key set to be cleared when the force compaction returned",
                    cfs.shouldIgnoreGcGraceForAnyKey());
        assertTrue("expected the table to be cursor-supported again after the force compaction",
                   isSupportedNow(cfs));
    }

    /**
     * A DROPPED non-frozen collection is gone from the schema. Every sstable written before the drop
     * still lists it in that sstable's own header, still multi-cell. The metadata-level check cannot
     * see it, so the gate has to screen the input headers too.
     * <p>
     * The cursor reads complex framing correctly, so this gate is not about parsing. It is held
     * closed because a cell written above the drop time survives the read, and the iterator cannot
     * merge a row that holds one. See {@code CursorCompactor.unsupportedHeaderColumns} and
     * {@code DroppedColumnDifferentialCompactionTest.droppedComplexColumnSurvivingCells}, which is
     * {@code @Ignore}d on CASSANDRA-21607.
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
     * The same shape through a dropped STATIC collection. A static column lands in
     * {@code header.columns(true)} only, so a gate that inspected regular columns alone would
     * reach a different verdict here than on this test's regular-column sibling.
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
        // cursor compaction only supports BIG output. Under another format isSupported is false
        // for every table, so the assertion below could not tell the header check apart
        Assume.assumeTrue("requires the BIG sstable format", BigFormat.isSelected());

        createTable(ddl);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute(insert);
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 'y')");
        flush();

        execute("ALTER TABLE %s DROP m");

        assertFalse("dropped collection should leave the metadata check satisfied, which is exactly " +
                    "why the header check is needed",
                    CursorCompactor.unsupportedMetadata(cfs.metadata()));

        boolean anyHeaderStillHasIt = false;
        for (SSTableReader reader : cfs.getLiveSSTables())
            for (ColumnMetadata column : reader.header.columns(isStatic))
                anyHeaderStillHasIt |= column.isComplex();
        assertTrue("expected a pre-drop sstable header to still list the collection as multi-cell",
                   anyHeaderStillHasIt);

        assertFalse("cursor compaction must refuse a table whose input headers still carry a " +
                    "dropped multi-cell column",
                    isSupportedNow(cfs));

        // positive control on a separate table: an equivalent table that never had a collection is
        // supported. The rejection above is therefore attributable to the dropped column alone. The
        // table under test cannot serve as its own pre-drop control, because the schema check rejects
        // it while the collection is still live
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore plain = getCurrentColumnFamilyStore();
        plain.disableAutoCompaction();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 'x')");
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 'y')");
        flush();
        assertTrue("expected a plain table with no dropped collection to be cursor-supported",
                   isSupportedNow(plain));
    }

    /**
     * A dropped COUNTER column stays gated. The cursor path has no counter merge at all, so the
     * header gate must still screen a counter the schema has dropped.
     * <p>
     * The dropped-collection tests above reach the same verdict through the same gate, but for a
     * different reason. This one is about the missing counter merge, not about the drop filter.
     */
    @Test
    public void droppedCounterUnsupportedFromHeaders() throws Exception
    {
        Assume.assumeTrue("requires the BIG sstable format", BigFormat.isSelected());

        // ONE counter column, so the drop leaves no counter in the schema. With a second counter
        // still live, unsupportedSchema would reject the table on its own and this test would pass
        // without the header gate doing anything.
        createTable("CREATE TABLE %s (pk bigint, ck bigint, c counter, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("UPDATE %s SET c = c + 1 WHERE pk = 1 AND ck = 1");
        flush();
        execute("UPDATE %s SET c = c + 1 WHERE pk = 1 AND ck = 2");
        flush();

        execute("ALTER TABLE %s DROP c");

        assertFalse("the dropped counter must leave the metadata check satisfied, which is exactly " +
                    "why the header check is needed",
                    CursorCompactor.unsupportedMetadata(cfs.metadata()));

        boolean anyHeaderStillHasIt = false;
        for (SSTableReader reader : cfs.getLiveSSTables())
            for (ColumnMetadata column : reader.header.columns(false))
                anyHeaderStillHasIt |= cfs.metadata().getColumn(column.name) == null;
        assertTrue("expected a pre-drop sstable header to still list the dropped counter",
                   anyHeaderStillHasIt);

        assertFalse("cursor compaction must refuse a table whose input headers still carry a " +
                    "counter column; the cursor has no counter merge",
                    isSupportedNow(cfs));
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
