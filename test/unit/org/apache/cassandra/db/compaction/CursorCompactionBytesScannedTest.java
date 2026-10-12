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
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.differential.DifferentialCompactionTester;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The bytes a compaction reports as scanned, which CompactionTask passes to the compaction rate
 * limiter and which {@code nodetool compactionstats} shows. The cursor path must report the same
 * total as the iterator path, which takes it from the scanners.
 *
 * Each case writes the same data, with explicit timestamps, into two tables, so their input
 * sstables are byte-identical. It compacts one table on each path and compares the totals.
 */
@RunWith(Parameterized.class)
public class CursorCompactionBytesScannedTest extends DifferentialCompactionTester
{
    private static final long TIMESTAMP = 1_000_000L;

    @Parameterized.Parameter(0)
    public String format;

    @Parameterized.Parameter(1)
    public boolean compressed;

    @Parameterized.Parameters(name = "format={0} compressed={1}")
    public static Collection<Object[]> parameters()
    {
        List<Object[]> parameters = new ArrayList<>();
        for (String format : new String[]{ "big", "bti" })
            for (boolean compressed : new boolean[]{ false, true })
                parameters.add(new Object[]{ format, compressed });
        return parameters;
    }

    /** Whether {@link #bytesScanned} compacts only the middle of the token order instead of everything. */
    private boolean subrange;

    @Before
    public void selectFormat()
    {
        selectSSTableFormat(format);
    }

    @After
    public void restoreFormat()
    {
        restoreSelectedFormat();
    }

    /** Overlapping partitions across three sstables, one partition wide enough for progress refreshes. */
    @Test
    public void overlappingSSTables() throws Throwable
    {
        assertSameBytesScanned((table, round) -> {
            for (long pk = 0; pk < 20; pk++)
                for (long ck = 0; ck < 5; ck++)
                    insert(table, pk + round * 10, ck, round);
            // more than 128 merged unfiltereds in one partition
            for (long ck = 0; ck < 300; ck++)
                insert(table, 1000, ck, round);
        }, 3);
    }

    /** A single input sstable, so no partition is merged with another. */
    @Test
    public void singleSSTable() throws Throwable
    {
        assertSameBytesScanned((table, round) -> {
            for (long pk = 0; pk < 50; pk++)
                insert(table, pk, 0, round);
        }, 1);
    }

    /** Inputs whose key ranges do not overlap, so each input ends while others are still read. */
    @Test
    public void disjointSSTables() throws Throwable
    {
        assertSameBytesScanned((table, round) -> {
            for (long pk = 0; pk < 30; pk++)
                insert(table, round * 1000 + pk, pk, round);
        }, 4);
    }

    /** A task over a token subrange, so each scanner reads a segment that starts and ends inside the file. */
    @Test
    public void tokenSubrange() throws Throwable
    {
        subrange = true;
        assertSameBytesScanned((table, round) -> {
            for (long pk = 0; pk < 40; pk++)
                for (long ck = 0; ck < 3; ck++)
                    insert(table, pk, ck + round, round);
        }, 3);
    }

    private interface RoundWriter
    {
        void write(String table, int round);
    }

    private void assertSameBytesScanned(RoundWriter writer, int rounds) throws Throwable
    {
        String compression = compressed ? "" : " WITH compression = {'enabled': 'false'}";
        String iteratorTable = createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))" + compression);
        String cursorTable = createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))" + compression);
        ColumnFamilyStore iteratorCfs = getColumnFamilyStore(KEYSPACE, iteratorTable);
        ColumnFamilyStore cursorCfs = getColumnFamilyStore(KEYSPACE, cursorTable);
        iteratorCfs.disableAutoCompaction();
        cursorCfs.disableAutoCompaction();

        for (int round = 0; round < rounds; round++)
        {
            writer.write(iteratorTable, round);
            writer.write(cursorTable, round);
            iteratorCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            cursorCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        }
        assertEquals("both tables must have the same input data, or their totals cannot be compared",
                     uncompressedLength(iteratorCfs), uncompressedLength(cursorCfs));

        long inputBytes = uncompressedLength(cursorCfs);
        long iteratorBytes = bytesScanned(iteratorCfs, false);
        long cursorBytes = bytesScanned(cursorCfs, true);
        logger.info("bytes scanned: input={} iterator={} cursor={}", inputBytes, iteratorBytes, cursorBytes);
        assertTrue("the iterator path reported no bytes scanned", iteratorBytes > 0);
        assertEquals("bytes scanned, iterator vs cursor", iteratorBytes, cursorBytes);
    }

    /** Compacts the whole live set on one path and returns the bytes it scanned when it finished. */
    private long bytesScanned(ColumnFamilyStore cfs, boolean cursor) throws Exception
    {
        long[] scanned = { -1 };
        // CompactionTask calls finishCompaction after its last rate limiter acquire, which used the same total
        ActiveCompactionsTracker tracker = new ActiveCompactionsTracker()
        {
            public void beginCompaction(CompactionInfo.Holder ci) {}

            public void finishCompaction(CompactionInfo.Holder ci)
            {
                scanned[0] = ((AbstractCompactionPipeline) ci).getTotalBytesScanned();
            }
        };
        Range<Token> range = subrange ? middleOfTokenOrder(cfs) : null;
        commitThroughFactory(cfs, cursor, (c, txn, gcBefore) -> new CompactionTask(c, txn, gcBefore, false)
        {
            @Override
            protected Range<Token> tokenRange()
            {
                return range;
            }
        }, tracker);
        return scanned[0];
    }

    /** The range from the token of the quarter key to the token of the three-quarter key, in token order. */
    private static Range<Token> middleOfTokenOrder(ColumnFamilyStore cfs)
    {
        List<Token> tokens = new ArrayList<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        tokens.add(partition.partitionKey().getToken());
                    }
                }
            }
        }
        Collections.sort(tokens);
        return new Range<>(tokens.get(tokens.size() / 4), tokens.get(tokens.size() * 3 / 4));
    }

    private static long uncompressedLength(ColumnFamilyStore cfs)
    {
        long length = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
            length += sstable.uncompressedLength();
        return length;
    }

    private void insert(String table, long pk, long ck, int round)
    {
        execute(String.format("INSERT INTO %s.%s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP %d",
                              KEYSPACE, table, TIMESTAMP + round),
                pk, ck, "value-" + round + '-' + ck);
    }
}
