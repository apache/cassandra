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
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.config.Config.PaxosStatePurging;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.schema.CompactionParams.TombstoneOption;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Pins the arms of {@code CursorCompactor.isSupported} that decide on the COMPACTION rather than on
 * the schema. {@link CursorSupportMatrixTest} covers the schema and the sstable headers.
 * <p>
 * Each gate falls back to the iterator path, so a gate that silently opened would not fail a
 * differential test: both pipelines would simply be the iterator. That is why these assert the gate
 * directly.
 */
public class CursorCompactionGateTest extends CQLTester
{
    private PaxosStatePurging originalPurging;

    @After
    public void restorePaxosStatePurging()
    {
        if (originalPurging != null)
        {
            DatabaseDescriptor.setPaxosStatePurging(originalPurging);
            originalPurging = null;
        }
    }

    /** Two sstables of a plain table, which every gate below then accepts or rejects. */
    private ColumnFamilyStore twoSSTableTable()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 'x')");
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 1, 'y')");
        flush();
        return cfs;
    }

    /**
     * Whether the gate can accept any compaction at all under the running configuration. The cursor
     * path writes the BIG format only, and {@code test/conf/latest_diff.yaml} selects BTI, so an
     * assertion that the gate opens has to read the format rather than assume it.
     */
    private static boolean cursorSupportsSelectedFormat()
    {
        return DatabaseDescriptor.getSelectedSSTableFormat() instanceof BigFormat;
    }

    private boolean isSupportedWith(ColumnFamilyStore cfs, TombstoneOption tombstoneOption) throws Exception
    {
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        try (CompactionController controller = new CompactionController(cfs, inputs, FBUtilities.nowInSeconds(),
                                                                        null, tombstoneOption);
             AbstractCompactionStrategy.ScannerList scanners =
                 cfs.getCompactionStrategyManager().getScanners(new ArrayList<>(inputs), null))
        {
            return CursorCompactor.isSupported(scanners, controller);
        }
    }

    /**
     * The control. Every rejection below has to be attributable to the one input it changes, so the
     * same table and the same sstables must be accepted first.
     */
    @Test
    public void aPlainTwoSSTableCompactionIsSupported() throws Exception
    {
        assertEquals("a plain two-sstable compaction is cursor-supported whenever the selected format is",
                     cursorSupportsSelectedFormat(), isSupportedWith(twoSSTableTable(), TombstoneOption.NONE));
    }

    /**
     * Garbage skipping is CompactionIterator.GarbageSkipper, which the cursor path does not
     * implement. Either non-NONE option must fall back.
     */
    @Test
    public void garbageSkippingIsUnsupported() throws Exception
    {
        ColumnFamilyStore cfs = twoSSTableTable();
        assertFalse("cursor compaction must refuse tombstone_compaction ROW: it has no GarbageSkipper",
                    isSupportedWith(cfs, TombstoneOption.ROW));
        assertFalse("cursor compaction must refuse tombstone_compaction CELL: it has no GarbageSkipper",
                    isSupportedWith(cfs, TombstoneOption.CELL));
    }

    /**
     * CompactionIterator swaps in PaxosPurger for system.paxos when purging is not legacy. The
     * cursor path has one purger, so it must decline rather than compact that table with the wrong
     * one.
     * <p>
     * This asserts the gate's own predicate rather than driving a compaction of system.paxos,
     * because the table under test here is an ordinary one: the point is that the gate reads the
     * setting, and that an ordinary table is unaffected by it.
     */
    @Test
    public void nonLegacyPaxosPurgingDoesNotAffectAnOrdinaryTable() throws Exception
    {
        originalPurging = DatabaseDescriptor.paxosStatePurging();
        ColumnFamilyStore cfs = twoSSTableTable();

        boolean expected = cursorSupportsSelectedFormat();

        DatabaseDescriptor.setPaxosStatePurging(PaxosStatePurging.legacy);
        assertEquals("an ordinary table is unaffected by legacy paxos purging",
                     expected, isSupportedWith(cfs, TombstoneOption.NONE));

        DatabaseDescriptor.setPaxosStatePurging(PaxosStatePurging.gc_grace);
        assertEquals("an ordinary table is not system.paxos, so the paxos gate must not close on it",
                     expected, isSupportedWith(cfs, TombstoneOption.NONE));

        DatabaseDescriptor.setPaxosStatePurging(PaxosStatePurging.repaired);
        assertEquals("an ordinary table is not system.paxos, so the paxos gate must not close on it",
                     expected, isSupportedWith(cfs, TombstoneOption.NONE));
    }

    /** One sstable of twenty partitions, so a token range can select an interior run of them. */
    private ColumnFamilyStore twentyPartitionTable()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < 20; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, 1, 'x')", pk);
        flush();
        return cfs;
    }

    /** The token range (keys[4], keys[12]] of the sstable's keys in file order: an interior run of eight partitions. */
    private static Range<Token> interiorRange(SSTableReader sstable)
    {
        List<DecoratedKey> keys = new ArrayList<>();
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    keys.add(partition.partitionKey());
                }
            }
        }
        return new Range<>(keys.get(4).getToken(), keys.get(12).getToken());
    }

    /**
     * A UCS shard task hands the strategy a token range, and an sstable that straddles the shard
     * boundary gets a partial scanner. The cursor reads that scanner's data-file segments, so the
     * gate must accept it rather than fall the whole task back to the iterator.
     */
    @Test
    public void aPartialRangeSimpleScannerIsSupported() throws Exception
    {
        ColumnFamilyStore cfs = twentyPartitionTable();
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        Range<Token> range = interiorRange(inputs.iterator().next());
        try (CompactionController controller = new CompactionController(cfs, inputs, FBUtilities.nowInSeconds(),
                                                                        null, TombstoneOption.NONE);
             AbstractCompactionStrategy.ScannerList scanners =
                 cfs.getCompactionStrategyManager().getScanners(new ArrayList<>(inputs), Collections.singleton(range)))
        {
            for (ISSTableScanner scanner : scanners.scanners)
                assertFalse("the range must give a partial scanner, or this test proves nothing", scanner.isFullRange());
            assertEquals("a partial SSTableSimpleScanner is cursor-supported whenever the selected format is",
                         cursorSupportsSelectedFormat(), CursorCompactor.isSupported(scanners, controller));
        }
    }

    /**
     * Only an SSTableSimpleScanner carries data-file bounds. A partial scanner of any other kind
     * filters by token as it iterates, which the cursor cannot, so the gate must still refuse it.
     */
    @Test
    public void aPartialScannerWithoutPositionBoundsIsUnsupported() throws Exception
    {
        ColumnFamilyStore cfs = twentyPartitionTable();
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        try (CompactionController controller = new CompactionController(cfs, inputs, FBUtilities.nowInSeconds(),
                                                                        null, TombstoneOption.NONE);
             AbstractCompactionStrategy.ScannerList scanners =
                 cfs.getCompactionStrategyManager().getScanners(new ArrayList<>(inputs), null))
        {
            List<ISSTableScanner> partial = new ArrayList<>();
            for (ISSTableScanner scanner : scanners.scanners)
                partial.add(new PartialScannerWithoutBounds(scanner));
            assertFalse("a partial scanner that is not an SSTableSimpleScanner must fall back",
                        CursorCompactor.isSupported(new AbstractCompactionStrategy.ScannerList(partial), controller));
        }
    }

    /** Delegates everything to a real full-range scanner and claims a partial range. */
    private static final class PartialScannerWithoutBounds implements ISSTableScanner
    {
        private final ISSTableScanner delegate;

        PartialScannerWithoutBounds(ISSTableScanner delegate)
        {
            this.delegate = delegate;
        }

        public boolean isFullRange()
        {
            return false;
        }

        public long getLengthInBytes()
        {
            return delegate.getLengthInBytes();
        }

        public long getCompressedLengthInBytes()
        {
            return delegate.getCompressedLengthInBytes();
        }

        public long getCurrentPosition()
        {
            return delegate.getCurrentPosition();
        }

        public long getBytesScanned()
        {
            return delegate.getBytesScanned();
        }

        public Set<SSTableReader> getBackingSSTables()
        {
            return delegate.getBackingSSTables();
        }

        public TableMetadata metadata()
        {
            return delegate.metadata();
        }

        public boolean hasNext()
        {
            return delegate.hasNext();
        }

        public UnfilteredRowIterator next()
        {
            return delegate.next();
        }

        public void close()
        {
            delegate.close();
        }
    }
}
