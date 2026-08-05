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
package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * A zero-copy split copies Data.db chunks verbatim and rebuilds only the components
 * {@link org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter} knows about. Storage-attached index components are
 * not among them, and their absence from a child is not a loud failure: {@code SSTableContextManager.update} drops an
 * sstable whose per-sstable completion marker is missing out of the index view WITHOUT reporting it invalid, and
 * {@code SecondaryIndexManager.handleNotification} deliberately skips sstable-attached indexes. The child would
 * therefore be live and readable with every one of its rows invisible to every index predicate, and nothing would
 * log or throw.
 *
 * <p>So a table with a storage-attached index must take the rewriting path, which writes those components inline.
 * This pins that, on the sstable shape {@code AntiCompactionRunPlanner} actually calls eligible -- many small
 * compression chunks and an unrepaired run on each side of one full run. {@link ZeroCopyAntiCompactionTest} covers the
 * same shape without an index and is where the fixture below comes from.
 *
 * <p>There are two independent gates and one test each, because they protect different sstables and only one of them
 * is enough on its own. {@link #anticompactionOfAnIndexedTableTakesTheRewritePath} covers the parent whose components
 * name the index, which the planner's {@code unhandledComponents} BACKSTOP refuses on;
 * {@link #anticompactionOfATableIndexedAfterTheFactTakesTheRewritePath} covers the parent whose components do NOT,
 * where {@code SecondaryIndexManager.hasSSTableAttachedIndexes()} in {@code CompactionManager.zeroCopyAntiCompact}
 * is the only thing left -- and where the planner is asserted to say ELIGIBLE, so the test fails if that gate goes.
 *
 * <p>{@code SELECT ... WHERE v = ?} returning a third of its rows is what this bug looks like in production.
 */
public class ZeroCopyAntiCompactionSAITest extends CQLTester
{
    private static final int PARTITIONS = 200;
    private static final int ROWS_PER_PARTITION = 4;
    private static final int VALUE_BYTES = 512;
    /** The one indexed value every row carries, so a single predicate must return every row in the table. */
    private static final int INDEXED_VALUE = 7;

    private boolean savedZeroCopyEnabled;

    @Before
    public void enableZeroCopyAnticompaction()
    {
        savedZeroCopyEnabled = DatabaseDescriptor.getZeroCopyAnticompactionEnabled();
        DatabaseDescriptor.setZeroCopyAnticompactionEnabled(true);
    }

    @After
    public void restoreZeroCopyAnticompaction()
    {
        DatabaseDescriptor.setZeroCopyAnticompactionEnabled(savedZeroCopyEnabled);
    }

    /**
     * The regression. With all three gates removed this fixture really does reproduce the bug: the split runs
     * (~435 KB of chunks copied), all three children come out with
     * {@code IndexDescriptor.isPerSSTableIndexBuildComplete() == false}, and
     * {@code SELECT * FROM t WHERE v = 7} returns 0 rows instead of 800 -- no exception and no warning.
     */
    @Test
    public void anticompactionOfAnIndexedTableTakesTheRewritePath() throws Throwable
    {
        ColumnFamilyStore cfs = indexedTableWithOneEligibleSSTable();
        SSTableReader parent = onlySSTable(cfs);

        // Sanity: the shape really is one the splitter would have taken if the table had no index. Asserting this is
        // what stops the test from passing vacuously the day the fixture stops producing an eligible sstable.
        assertTrue("fixture no longer produces a splittable sstable", ZeroCopySSTableSplitter.isSupported(parent));
        assertTrue("fixture no longer spans enough compression chunks",
                   parent.getCompressionMetadata().chunkLength() * 4L < parent.uncompressedLength());

        long rowsBefore = rowCountByIndex();
        assertEquals(PARTITIONS * ROWS_PER_PARTITION, rowsBefore);

        List<DecoratedKey> keys = keysInTokenOrder(cfs);
        Range<Token> full = rangeCovering(keys, 50, 150);

        // The planner refuses, and refuses for the right reason: the shape is the eligible one, so what disqualifies
        // this sstable is the components it carries and nothing else. Also proves the walk was skipped (runCount 0).
        AntiCompactionRunPlanner.Plan plan =
            AntiCompactionRunPlanner.plan(parent, fullOnly(full), TimeUUID.Generator.nextTimeUUID());
        assertFalse(plan.toString(), plan.eligible);
        assertTrue(plan.ineligibleReason, plan.ineligibleReason.contains("components a child would not get"));
        assertTrue(plan.ineligibleReason, plan.ineligibleReason.contains("GroupMeta.db"));
        assertEquals("the Index.db walk should have been skipped", 0, plan.runCount);

        anticompact(cfs, fullOnly(full), TimeUUID.Generator.nextTimeUUID());

        // The witness of which path ran: marked only on a successful zero-copy commit.
        assertEquals("an indexed table must not be zero-copy anticompacted",
                     0, cfs.metric.bytesZeroCopyAnticompaction.table.getCount());

        // The anticompaction did happen, and produced the rewrite path's repaired/unrepaired split.
        assertTrue("expected the parent to have been anticompacted", cfs.getLiveSSTables().size() > 1);

        // Every output carries a complete index, and the index still sees every row.
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IndexDescriptor indexDescriptor = IndexDescriptor.create(sstable);
            assertTrue("per-sstable index components missing from " + sstable.descriptor,
                       indexDescriptor.isPerSSTableIndexBuildComplete());
        }
        assertEquals("the index lost rows to anticompaction", rowsBefore, rowCountByIndex());
    }

    /**
     * The case the primary gate UNIQUELY protects, and the hole in the test above: there, the index exists before
     * the inserts, so the parent carries {@code GroupMeta.db} and the planner's {@code unhandledComponents}
     * BACKSTOP refuses -- which means that test still passes with
     * {@code SecondaryIndexManager.hasSSTableAttachedIndexes()} deleted from
     * {@code CompactionManager.zeroCopyAntiCompact}.
     *
     * <p>Here the sstable's component set carries NOTHING a child would miss, so the backstop is structurally blind
     * and the CFS-level gate is the only protection left. That is the real production shape:
     * {@code CREATE INDEX} on an already-populated table, and an sstable the initial build has not reached yet --
     * {@code SSTableContextManager.update} drops such an sstable out of the index view without reporting it
     * invalid, and {@code Descriptor.discoverComponents} (which is what a parent with no SAI entries in its TOC
     * falls back to) enumerates only the format's singleton components, so it can never see per-index-named SAI
     * ones. The state is reached here by unregistering them, which rewrites TOC.txt exactly as
     * {@code StorageAttachedIndexGroup} does on invalidate.
     *
     * <p>What makes this test load-bearing: it asserts the planner says <b>eligible</b>. With the primary gate
     * removed, the split would run, every child would come out with no index components, and
     * {@code SELECT ... WHERE v = ?} would return 0 of 800 rows with nothing logged or thrown.
     */
    @Test
    public void anticompactionOfATableIndexedAfterTheFactTakesTheRewritePath() throws Throwable
    {
        ColumnFamilyStore cfs = populatedTableIndexedAfterTheFact();
        SSTableReader parent = onlySSTable(cfs);

        assertTrue("fixture no longer produces a splittable sstable", ZeroCopySSTableSplitter.isSupported(parent));
        assertTrue("fixture no longer spans enough compression chunks",
                   parent.getCompressionMetadata().chunkLength() * 4L < parent.uncompressedLength());

        // Read through the index while the fixture is still pristine, so the expected count cannot be an artefact
        // of the component surgery below.
        long rowsBefore = rowCountByIndex();
        assertEquals(PARTITIONS * ROWS_PER_PARTITION, rowsBefore);

        // Make the reader look like one the initial build has not reached: no SAI components in its set (and none in
        // its TOC.txt, which unregisterComponents rewrites), so the backstop has nothing to refuse on. The set is
        // copied first -- unhandledComponents() is a live view over the very set being mutated.
        Set<Component> saiComponents = ImmutableSet.copyOf(ZeroCopySSTableSplitter.unhandledComponents(parent));
        assertFalse("the fixture was supposed to leave SAI components on the parent", saiComponents.isEmpty());
        parent.unregisterComponents(saiComponents, cfs.getTracker());
        assertTrue("the backstop must be blind for this test to be about the primary gate",
                   ZeroCopySSTableSplitter.unhandledComponents(parent).isEmpty());

        List<DecoratedKey> keys = keysInTokenOrder(cfs);
        Range<Token> full = rangeCovering(keys, 50, 150);

        // The planner now says YES: it holds only a reader, and this reader looks exactly like one of an unindexed
        // table. Nothing below the ColumnFamilyStore can tell the difference, which is why the gate lives there.
        AntiCompactionRunPlanner.Plan plan =
            AntiCompactionRunPlanner.plan(parent, fullOnly(full), TimeUUID.Generator.nextTimeUUID());
        assertTrue("this test is only meaningful while the planner would allow the split: " + plan, plan.eligible);
        assertEquals("U F U", 3, plan.runCount);

        // The index is still attached to the table, which is the only thing standing between this sstable and a
        // split that would silently drop its rows out of every index predicate.
        assertTrue("the table must still report storage-attached indexes",
                   cfs.indexManager.hasSSTableAttachedIndexes());

        anticompact(cfs, fullOnly(full), TimeUUID.Generator.nextTimeUUID());

        assertEquals("an indexed table must not be zero-copy anticompacted, whatever its components look like",
                     0, cfs.metric.bytesZeroCopyAnticompaction.table.getCount());
        assertTrue("expected the parent to have been anticompacted", cfs.getLiveSSTables().size() > 1);

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IndexDescriptor indexDescriptor = IndexDescriptor.create(sstable);
            assertTrue("per-sstable index components missing from " + sstable.descriptor,
                       indexDescriptor.isPerSSTableIndexBuildComplete());
        }
        assertEquals("the index lost rows to anticompaction", rowsBefore, rowCountByIndex());
    }

    // ----------------------------------------------------------------------------------------------------
    // Fixture -- the shape ZeroCopyAntiCompactionTest proves the planner calls eligible
    // ----------------------------------------------------------------------------------------------------

    private ColumnFamilyStore indexedTableWithOneEligibleSSTable() throws Throwable
    {
        // small chunks plus near-incompressible values, so the sstable really spans many compression chunks; without
        // that there is nowhere to cut and the planner would refuse for reasons that have nothing to do with indexes
        createTable("CREATE TABLE %s (pk text, ck int, v int, val text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}");
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        for (int p = 0; p < PARTITIONS; p++)
            for (int c = 0; c < ROWS_PER_PARTITION; c++)
                execute("INSERT INTO %s (pk, ck, v, val) VALUES (?, ?, ?, ?)",
                        key(p), c, INDEXED_VALUE, randomText(VALUE_BYTES));
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertEquals("expected a single sstable to anticompact", 1, cfs.getLiveSSTables().size());
        return cfs;
    }

    /**
     * Same shape, but the index is created AFTER the data, which is what a real {@code CREATE INDEX} on a live table
     * does. {@code createIndex} waits for the index to become queryable, so the initial build has finished and has
     * registered its components on the parent -- the caller strips them to reach the state of an sstable the build
     * has not reached.
     */
    private ColumnFamilyStore populatedTableIndexedAfterTheFact() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, ck int, v int, val text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}");
        disableCompaction();

        for (int p = 0; p < PARTITIONS; p++)
            for (int c = 0; c < ROWS_PER_PARTITION; c++)
                execute("INSERT INTO %s (pk, ck, v, val) VALUES (?, ?, ?, ?)",
                        key(p), c, INDEXED_VALUE, randomText(VALUE_BYTES));
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertEquals("expected a single sstable to anticompact", 1, cfs.getLiveSSTables().size());

        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        // the build must not have compacted or replaced the sstable
        assertEquals("expected a single sstable to anticompact", 1, cfs.getLiveSSTables().size());
        return cfs;
    }

    private long rowCountByIndex() throws Throwable
    {
        return execute("SELECT * FROM %s WHERE v = ?", INDEXED_VALUE).size();
    }

    private static SSTableReader onlySSTable(ColumnFamilyStore cfs)
    {
        assertEquals(1, cfs.getLiveSSTables().size());
        return cfs.getLiveSSTables().iterator().next();
    }

    private static String key(int p)
    {
        return String.format("k%06d", p);
    }

    /** Near-incompressible payload, so chunk_length really bounds the chunk count. */
    private static String randomText(int length)
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        char[] chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = (char) ('!' + random.nextInt(94));
        return new String(chars);
    }

    private static List<DecoratedKey> keysInTokenOrder(ColumnFamilyStore cfs)
    {
        List<DecoratedKey> keys = new ArrayList<>(PARTITIONS);
        for (int p = 0; p < PARTITIONS; p++)
            keys.add(cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(key(p))));
        keys.sort(DecoratedKey::compareTo);
        return keys;
    }

    /**
     * A range covering exactly {@code keys[fromInclusive .. toExclusive - 1]}, leaving an unrepaired run on each
     * side -- which is both the eligible {@code UNREPAIRED, FULL, UNREPAIRED} shape and what keeps
     * {@code mutateFullyContainedSSTables} from claiming the sstable through the metadata-only path.
     */
    private static Range<Token> rangeCovering(List<DecoratedKey> keys, int fromInclusive, int toExclusive)
    {
        assertTrue("leave an unrepaired prefix", fromInclusive > 0);
        assertTrue("leave an unrepaired suffix", toExclusive < keys.size());
        return new Range<>(keys.get(fromInclusive - 1).getToken(), keys.get(toExclusive - 1).getToken());
    }

    private static RangesAtEndpoint fullOnly(Range<Token> range)
    {
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        return RangesAtEndpoint.builder(local).add(Replica.fullReplica(local, range)).build();
    }

    private void anticompact(ColumnFamilyStore cfs, RangesAtEndpoint ranges, TimeUUID sessionID) throws Exception
    {
        Set<SSTableReader> sstables = ImmutableSet.copyOf(cfs.getLiveSSTables());
        assertFalse("nothing to anticompact", sstables.isEmpty());

        ActiveRepairService.instance().registerParentRepairSession(sessionID,
                                                                  InetAddressAndPort.getByName("127.0.0.1"),
                                                                  Lists.newArrayList(cfs),
                                                                  ranges.ranges(),
                                                                  true,
                                                                  ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                  true,
                                                                  PreviewKind.NONE);
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            assertNotNull("could not mark the sstables compacting", txn);
            CompactionManager.instance.performAnticompaction(cfs, ranges, refs, txn, sessionID, () -> false);
        }
        finally
        {
            ActiveRepairService.instance().removeParentRepairSession(sessionID);
        }
        LifecycleTransaction.waitForDeletions();
    }
}
