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
package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.dht.AbstractBounds.isEmpty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SSTableScannerTest
{
    public static final String KEYSPACE = "SSTableScannerTest";
    public static final String TABLE = "Standard1";

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }

    private static String toKey(int key)
    {
        return String.format("%03d", key);
    }

    // we produce all DataRange variations that produce an inclusive start and exclusive end range
    private static Iterable<DataRange> dataRanges(TableMetadata metadata, int start, int end)
    {
        if (end < start)
            return dataRanges(metadata, start, end, false, true);
        return Iterables.concat(dataRanges(metadata, start, end, false, false),
                                dataRanges(metadata, start, end, false, true),
                                dataRanges(metadata, start, end, true, false),
                                dataRanges(metadata, start, end, true, true)
        );
    }

    private static Iterable<DataRange> dataRanges(TableMetadata metadata, int start, int end, boolean inclusiveStart, boolean inclusiveEnd)
    {
        List<DataRange> ranges = new ArrayList<>();
        if (start == end + 1)
        {
            assert !inclusiveStart && inclusiveEnd;
            ranges.add(dataRange(metadata, min(start), false, max(end), true));
            ranges.add(dataRange(metadata, min(start), false, min(end + 1), true));
            ranges.add(dataRange(metadata, max(start - 1), false, max(end), true));
            ranges.add(dataRange(metadata, dk(start - 1), false, dk(start - 1), true));
        }
        else
        {
            for (PartitionPosition s : starts(start, inclusiveStart))
            {
                for (PartitionPosition e : ends(end, inclusiveEnd))
                {
                    if (end < start && e.compareTo(s) > 0)
                        continue;
                    if (!isEmpty(new AbstractBounds.Boundary<>(s, inclusiveStart), new AbstractBounds.Boundary<>(e, inclusiveEnd)))
                        continue;
                    ranges.add(dataRange(metadata, s, inclusiveStart, e, inclusiveEnd));
                }
            }
        }
        return ranges;
    }

    private static Iterable<PartitionPosition> starts(int key, boolean inclusive)
    {
        return Arrays.asList(min(key), max(key - 1), dk(inclusive ? key : key - 1));
    }

    private static Iterable<PartitionPosition> ends(int key, boolean inclusive)
    {
        return Arrays.asList(max(key), min(key + 1), dk(inclusive ? key : key + 1));
    }

    private static DecoratedKey dk(int key)
    {
        return Util.dk(toKey(key));
    }

    private static Token token(int key)
    {
        return key == Integer.MIN_VALUE ? ByteOrderedPartitioner.MINIMUM : new ByteOrderedPartitioner.BytesToken(toKey(key).getBytes());
    }

    private static PartitionPosition min(int key)
    {
        return token(key).minKeyBound();
    }

    private static PartitionPosition max(int key)
    {
        return token(key).maxKeyBound();
    }

    private static DataRange dataRange(TableMetadata metadata, PartitionPosition start, boolean startInclusive, PartitionPosition end, boolean endInclusive)
    {
        Slices.Builder sb = new Slices.Builder(metadata.comparator);
        ClusteringIndexSliceFilter filter = new ClusteringIndexSliceFilter(sb.build(), false);

        return new DataRange(AbstractBounds.bounds(start, startInclusive, end, endInclusive), filter);
    }

    private static Range<Token> rangeFor(int start, int end)
    {
        return new Range<Token>(new ByteOrderedPartitioner.BytesToken(toKey(start).getBytes()),
                                end == Integer.MIN_VALUE ? ByteOrderedPartitioner.MINIMUM : new ByteOrderedPartitioner.BytesToken(toKey(end).getBytes()));
    }

    private static Collection<Range<Token>> makeRanges(int ... keys)
    {
        Collection<Range<Token>> ranges = new ArrayList<Range<Token>>(keys.length / 2);
        for (int i = 0; i < keys.length; i += 2)
            ranges.add(rangeFor(keys[i], keys[i + 1]));
        return ranges;
    }

    private static void insertRowWithKey(TableMetadata metadata, int key)
    {
        long timestamp = System.currentTimeMillis();

        new RowUpdateBuilder(metadata, timestamp, toKey(key))
            .clustering("col")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();

    }

    private static void assertScanMatches(SSTableReader sstable, int scanStart, int scanEnd, int ... boundaries)
    {
        assert boundaries.length % 2 == 0;
        for (DataRange range : dataRanges(sstable.metadata(), scanStart, scanEnd))
        {
            try(UnfilteredPartitionIterator scanner = sstable.partitionIterator(ColumnFilter.all(sstable.metadata()),
                                                                                range,
                                                                                SSTableReadsListener.NOOP_LISTENER))
            {
                for (int b = 0; b < boundaries.length; b += 2)
                    for (int i = boundaries[b]; i <= boundaries[b + 1]; i++)
                        assertEquals(toKey(i), new String(scanner.next().partitionKey().getKey().array()));
                assertFalse(scanner.hasNext());
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private static void assertScanEmpty(SSTableReader sstable, int scanStart, int scanEnd)
    {
        assertScanMatches(sstable, scanStart, scanEnd);
    }

    @Test
    public void testSingleDataRange() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(TABLE);
        store.clearUnsafe();

        // disable compaction while flushing
        store.disableAutoCompaction();

        for (int i = 2; i < 10; i++)
            insertRowWithKey(store.metadata(), i);
        Util.flush(store);

        assertEquals(1, store.getLiveSSTables().size());
        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        // full range scan
        ISSTableScanner scanner = sstable.getScanner();
        for (int i = 2; i < 10; i++)
            assertEquals(toKey(i), new String(scanner.next().partitionKey().getKey().array()));

        scanner.close();

        // a simple read of a chunk in the middle
        assertScanMatches(sstable, 3, 6, 3, 6);

        // start of range edge conditions
        assertScanMatches(sstable, 1, 9, 2, 9);
        assertScanMatches(sstable, 2, 9, 2, 9);
        assertScanMatches(sstable, 3, 9, 3, 9);

        // end of range edge conditions
        assertScanMatches(sstable, 1, 8, 2, 8);
        assertScanMatches(sstable, 1, 9, 2, 9);
        assertScanMatches(sstable, 1, 99, 2, 9);

        // single item ranges
        assertScanMatches(sstable, 2, 2, 2, 2);
        assertScanMatches(sstable, 5, 5, 5, 5);
        assertScanMatches(sstable, 9, 9, 9, 9);

        // empty ranges
        assertScanEmpty(sstable, 0, 1);
        assertScanEmpty(sstable, 10, 11);

        // wrapping, starts in middle
        assertScanMatches(sstable, 5, 3, 2, 3, 5, 9);
        assertScanMatches(sstable, 5, 2, 2, 2, 5, 9);
        assertScanMatches(sstable, 5, 1, 5, 9);
        assertScanMatches(sstable, 5, Integer.MIN_VALUE, 5, 9);
        // wrapping, starts at end
        assertScanMatches(sstable, 9, 8, 2, 8, 9, 9);
        assertScanMatches(sstable, 9, 3, 2, 3, 9, 9);
        assertScanMatches(sstable, 9, 2, 2, 2, 9, 9);
        assertScanMatches(sstable, 9, 1, 9, 9);
        assertScanMatches(sstable, 9, Integer.MIN_VALUE, 9, 9);
        assertScanMatches(sstable, 8, 3, 2, 3, 8, 9);
        assertScanMatches(sstable, 8, 2, 2, 2, 8, 9);
        assertScanMatches(sstable, 8, 1, 8, 9);
        assertScanMatches(sstable, 8, Integer.MIN_VALUE, 8, 9);
        // wrapping, starts past end
        assertScanMatches(sstable, 10, 9, 2, 9);
        assertScanMatches(sstable, 10, 5, 2, 5);
        assertScanMatches(sstable, 10, 2, 2, 2);
        assertScanEmpty(sstable, 10, 1);
        assertScanEmpty(sstable, 10, Integer.MIN_VALUE);
        assertScanMatches(sstable, 11, 10, 2, 9);
        assertScanMatches(sstable, 11, 9, 2, 9);
        assertScanMatches(sstable, 11, 5, 2, 5);
        assertScanMatches(sstable, 11, 2, 2, 2);
        assertScanEmpty(sstable, 11, 1);
        assertScanEmpty(sstable, 11, Integer.MIN_VALUE);
        // wrapping, starts at start
        assertScanMatches(sstable, 3, 1, 3, 9);
        assertScanMatches(sstable, 3, Integer.MIN_VALUE, 3, 9);
        assertScanMatches(sstable, 2, 1, 2, 9);
        assertScanMatches(sstable, 2, Integer.MIN_VALUE, 2, 9);
        assertScanMatches(sstable, 1, 0, 2, 9);
        assertScanMatches(sstable, 1, Integer.MIN_VALUE, 2, 9);
        // wrapping, starts before
        assertScanMatches(sstable, 1, -1, 2, 9);
        assertScanMatches(sstable, 1, Integer.MIN_VALUE, 2, 9);
        assertScanMatches(sstable, 1, 0, 2, 9);
    }

    @Test
    public void testSingleDataRangeWithMovedStart() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(TABLE);
        store.clearUnsafe();

        // disable compaction while flushing
        store.disableAutoCompaction();

        for (int i = 2; i < 10; i++)
            insertRowWithKey(store.metadata(), i);
        Util.flush(store);

        assertEquals(1, store.getLiveSSTables().size());
        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        sstable = sstable.cloneWithNewStart(dk(4));

        // full range scan
        ISSTableScanner scanner = sstable.getScanner();
        for (int i = 4; i < 10; i++)
            assertEquals(toKey(i), new String(scanner.next().partitionKey().getKey().array()));

        scanner.close();

        // a simple read of a chunk in the middle
        assertScanMatches(sstable, 3, 6, 4, 6);

        // start of range edge conditions
        assertScanMatches(sstable, 3, 9, 4, 9);
        assertScanMatches(sstable, 4, 9, 4, 9);
        assertScanMatches(sstable, 5, 9, 4, 9);

        // end of range edge conditions
        assertScanMatches(sstable, 1, 8, 4, 8);
        assertScanMatches(sstable, 1, 9, 4, 9);
        assertScanMatches(sstable, 1, 99, 4, 9);

        // single item ranges
        assertScanMatches(sstable, 4, 4, 4, 4);
        assertScanMatches(sstable, 5, 5, 5, 5);
        assertScanMatches(sstable, 9, 9, 9, 9);

        // empty ranges
        assertScanEmpty(sstable, 2, 3);
        assertScanEmpty(sstable, 10, 11);

        // wrapping, starts in middle
        assertScanMatches(sstable, 7, 5, 4, 5, 7, 9);
        assertScanMatches(sstable, 5, 4, 4, 4, 5, 9);
        assertScanMatches(sstable, 5, 3, 5, 9);
        assertScanMatches(sstable, 5, Integer.MIN_VALUE, 5, 9);
        // wrapping, starts at end
        assertScanMatches(sstable, 9, 8, 4, 8, 9, 9);
        assertScanMatches(sstable, 9, 5, 4, 5, 9, 9);
        assertScanMatches(sstable, 9, 4, 4, 4, 9, 9);
        assertScanMatches(sstable, 9, 3, 9, 9);
        assertScanMatches(sstable, 9, Integer.MIN_VALUE, 9, 9);
        assertScanMatches(sstable, 8, 5, 4, 5, 8, 9);
        assertScanMatches(sstable, 8, 4, 4, 4, 8, 9);
        assertScanMatches(sstable, 8, 3, 8, 9);
        assertScanMatches(sstable, 8, Integer.MIN_VALUE, 8, 9);
        // wrapping, starts past end
        assertScanMatches(sstable, 10, 9, 4, 9);
        assertScanMatches(sstable, 10, 5, 4, 5);
        assertScanMatches(sstable, 10, 4, 4, 4);
        assertScanEmpty(sstable, 10, 3);
        assertScanEmpty(sstable, 10, Integer.MIN_VALUE);
        assertScanMatches(sstable, 11, 10, 4, 9);
        assertScanMatches(sstable, 11, 9, 4, 9);
        assertScanMatches(sstable, 11, 5, 4, 5);
        assertScanMatches(sstable, 11, 4, 4, 4);
        assertScanEmpty(sstable, 11, 3);
        assertScanEmpty(sstable, 11, Integer.MIN_VALUE);
        // wrapping, starts at start
        assertScanMatches(sstable, 5, 1, 5, 9);
        assertScanMatches(sstable, 5, Integer.MIN_VALUE, 5, 9);
        assertScanMatches(sstable, 4, 1, 4, 9);
        assertScanMatches(sstable, 4, Integer.MIN_VALUE, 4, 9);
        assertScanMatches(sstable, 1, 0, 4, 9);
        assertScanMatches(sstable, 1, Integer.MIN_VALUE, 4, 9);
        // wrapping, starts before
        assertScanMatches(sstable, 3, -1, 4, 9);
        assertScanMatches(sstable, 3, Integer.MIN_VALUE, 4, 9);
        assertScanMatches(sstable, 3, 0, 4, 9);
    }

    private static void assertScanContainsRanges(ISSTableScanner scanner, int ... rangePairs) throws IOException
    {
        assert rangePairs.length % 2 == 0;

        for (int pairIdx = 0; pairIdx < rangePairs.length; pairIdx += 2)
        {
            int rangeStart = rangePairs[pairIdx];
            int rangeEnd = rangePairs[pairIdx + 1];

            for (int expected = rangeStart; expected <= rangeEnd; expected++)
            {
                assertTrue(String.format("Expected to see key %03d", expected), scanner.hasNext());
                assertEquals(toKey(expected), new String(scanner.next().partitionKey().getKey().array()));
            }
        }
        assertFalse(scanner.hasNext());
        scanner.close();
    }

    @Test
    public void testMultipleRanges() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(TABLE);
        store.clearUnsafe();

        // disable compaction while flushing
        store.disableAutoCompaction();

        for (int i = 0; i < 3; i++)
            for (int j = 2; j < 10; j++)
                insertRowWithKey(store.metadata(), i * 100 + j);
        Util.flush(store);

        assertEquals(1, store.getLiveSSTables().size());
        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        // full range scan
        ISSTableScanner fullScanner = sstable.getScanner();
        assertScanContainsRanges(fullScanner,
                                 2, 9,
                                 102, 109,
                                 202, 209);


        // scan all three ranges separately
        ISSTableScanner scanner = sstable.getScanner(makeRanges(1, 9,
                                                                   101, 109,
                                                                   201, 209));
        assertScanContainsRanges(scanner,
                                 2, 9,
                                 102, 109,
                                 202, 209);

        // skip the first range
        scanner = sstable.getScanner(makeRanges(101, 109,
                                                201, 209));
        assertScanContainsRanges(scanner,
                                 102, 109,
                                 202, 209);

        // skip the second range
        scanner = sstable.getScanner(makeRanges(1, 9,
                                                201, 209));
        assertScanContainsRanges(scanner,
                                 2, 9,
                                 202, 209);


        // skip the last range
        scanner = sstable.getScanner(makeRanges(1, 9,
                                                101, 109));
        assertScanContainsRanges(scanner,
                                 2, 9,
                                 102, 109);

        // the first scanned range stops short of the actual data in the first range
        scanner = sstable.getScanner(makeRanges(1, 5,
                                                101, 109,
                                                201, 209));
        assertScanContainsRanges(scanner,
                                 2, 5,
                                 102, 109,
                                 202, 209);

        // the first scanned range requests data beyond actual data in the first range
        scanner = sstable.getScanner(makeRanges(1, 20,
                                                101, 109,
                                                201, 209));
        assertScanContainsRanges(scanner,
                                 2, 9,
                                 102, 109,
                                 202, 209);


        // the middle scan range splits the outside two data ranges
        scanner = sstable.getScanner(makeRanges(1, 5,
                                                6, 205,
                                                206, 209));
        assertScanContainsRanges(scanner,
                                 2, 5,
                                 7, 9,
                                 102, 109,
                                 202, 205,
                                 207, 209);

        // empty ranges
        scanner = sstable.getScanner(makeRanges(0, 1,
                                                2, 20,
                                                101, 109,
                                                150, 159,
                                                201, 209,
                                                1000, 1001));
        assertScanContainsRanges(scanner,
                                 3, 9,
                                 102, 109,
                                 202, 209);

        // out of order ranges
        scanner = sstable.getScanner(makeRanges(201, 209,
                                                1, 20,
                                                201, 209,
                                                101, 109,
                                                1000, 1001,
                                                150, 159));
        assertScanContainsRanges(scanner,
                                 2, 9,
                                 102, 109,
                                 202, 209);

        // only empty ranges
        scanner = sstable.getScanner(makeRanges(0, 1,
                                                150, 159,
                                                250, 259));
        assertFalse(scanner.hasNext());

        // no ranges is equivalent to a full scan
        scanner = sstable.getScanner(new ArrayList<Range<Token>>());
        assertFalse(scanner.hasNext());
    }

    @Test
    public void testSingleKeyMultipleRanges() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(TABLE);
        store.clearUnsafe();

        // disable compaction while flushing
        store.disableAutoCompaction();

        insertRowWithKey(store.metadata(), 205);
        Util.flush(store);

        assertEquals(1, store.getLiveSSTables().size());
        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        // full range scan
        ISSTableScanner fullScanner = sstable.getScanner();
        assertScanContainsRanges(fullScanner, 205, 205);

        // scan three ranges separately
        ISSTableScanner scanner = sstable.getScanner(makeRanges(101, 109,
                                                                   201, 209));

        // this will currently fail
        assertScanContainsRanges(scanner, 205, 205);
    }

    private static void testRequestNextRowIteratorWithoutConsumingPrevious(Consumer<ISSTableScanner> consumer)
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(TABLE);
        store.clearUnsafe();

        // disable compaction while flushing
        store.disableAutoCompaction();

        insertRowWithKey(store.metadata(), 0);
        store.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        assertEquals(1, store.getLiveSSTables().size());
        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        try (ISSTableScanner scanner = sstable.getScanner();
             UnfilteredRowIterator currentRowIterator = scanner.next())
        {
            assertTrue(currentRowIterator.hasNext());
            try
            {
                consumer.accept(scanner);
                fail("Should have thrown IllegalStateException");
            }
            catch (IllegalStateException e)
            {
                assertEquals("The UnfilteredRowIterator returned by the last call to next() was initialized: " +
                             "it must be closed before calling hasNext() or next() again.",
                             e.getMessage());
            }
        }
    }

    @Test
    public void testHasNextRowIteratorWithoutConsumingPrevious()
    {
        testRequestNextRowIteratorWithoutConsumingPrevious(ISSTableScanner::hasNext);
    }

    @Test
    public void testNextRowIteratorWithoutConsumingPrevious()
    {
        testRequestNextRowIteratorWithoutConsumingPrevious(ISSTableScanner::next);
    }
}
