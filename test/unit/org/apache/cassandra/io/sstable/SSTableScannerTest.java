/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.io.sstable;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.compaction.ICompactionScanner;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ByteOrderedPartitioner.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.*;

public class SSTableScannerTest
{
    public static final String KEYSPACE = "SSTableScannerTest";
    public static final String TABLE = "Standard1";

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }

    private static String toKey(int key)
    {
        return String.format("%03d", key);
    }

    private static Bounds<RowPosition> boundsFor(int start, int end)
    {
        return new Bounds<RowPosition>(new BytesToken(toKey(start).getBytes()).minKeyBound(),
                                       new BytesToken(toKey(end).getBytes()).maxKeyBound());
    }


    private static Range<Token> rangeFor(int start, int end)
    {
        return new Range<Token>(new BytesToken(toKey(start).getBytes()),
                                new BytesToken(toKey(end).getBytes()));
    }

    private static Collection<Range<Token>> makeRanges(int ... keys)
    {
        Collection<Range<Token>> ranges = new ArrayList<Range<Token>>(keys.length / 2);
        for (int i = 0; i < keys.length; i += 2)
            ranges.add(rangeFor(keys[i], keys[i + 1]));
        return ranges;
    }

    private static void insertRowWithKey(int key)
    {
        long timestamp = System.currentTimeMillis();
        DecoratedKey decoratedKey = Util.dk(toKey(key));
        Mutation rm = new Mutation(KEYSPACE, decoratedKey.getKey());
        rm.add(TABLE, Util.cellname("col"), ByteBufferUtil.EMPTY_BYTE_BUFFER, timestamp, 1000);
        rm.applyUnsafe();
    }

    private static void assertScanMatches(SSTableReader sstable, int scanStart, int scanEnd, int expectedStart, int expectedEnd)
    {
        ICompactionScanner scanner = sstable.getScanner(new DataRange(boundsFor(scanStart, scanEnd), new IdentityQueryFilter()));
        for (int i = expectedStart; i <= expectedEnd; i++)
            assertEquals(toKey(i), new String(scanner.next().getKey().getKey().array()));
        assertFalse(scanner.hasNext());
    }

    private static void assertScanEmpty(SSTableReader sstable, int scanStart, int scanEnd)
    {
        ICompactionScanner scanner = sstable.getScanner(new DataRange(boundsFor(scanStart, scanEnd), new IdentityQueryFilter()));
        assertFalse(String.format("scan of (%03d, %03d] should be empty", scanStart, scanEnd), scanner.hasNext());
    }

    @Test
    public void testSingleDataRange()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(TABLE);
        store.clearUnsafe();

        // disable compaction while flushing
        store.disableAutoCompaction();

        for (int i = 2; i < 10; i++)
            insertRowWithKey(i);
        store.forceBlockingFlush();

        assertEquals(1, store.getSSTables().size());
        SSTableReader sstable = store.getSSTables().iterator().next();

        // full range scan
        ICompactionScanner scanner = sstable.getScanner();
        for (int i = 2; i < 10; i++)
            assertEquals(toKey(i), new String(scanner.next().getKey().getKey().array()));

        // a simple read of a chunk in the middle
        assertScanMatches(sstable, 3, 6, 3, 6);

        // start of range edge conditions
        assertScanMatches(sstable, 1, 9, 2, 9);
        assertScanMatches(sstable, 2, 9, 2, 9);
        assertScanMatches(sstable, 3, 9, 3, 9);

        // end of range edge conditions
        assertScanMatches(sstable, 1, 8, 2, 8);
        assertScanMatches(sstable, 1, 9, 2, 9);
        assertScanMatches(sstable, 1, 9, 2, 9);

        // single item ranges
        assertScanMatches(sstable, 2, 2, 2, 2);
        assertScanMatches(sstable, 5, 5, 5, 5);
        assertScanMatches(sstable, 9, 9, 9, 9);

        // empty ranges
        assertScanEmpty(sstable, 0, 1);
        assertScanEmpty(sstable, 10, 11);
    }

    private static void assertScanContainsRanges(ICompactionScanner scanner, int ... rangePairs)
    {
        assert rangePairs.length % 2 == 0;

        for (int pairIdx = 0; pairIdx < rangePairs.length; pairIdx += 2)
        {
            int rangeStart = rangePairs[pairIdx];
            int rangeEnd = rangePairs[pairIdx + 1];

            for (int expected = rangeStart; expected <= rangeEnd; expected++)
            {
                assertTrue(String.format("Expected to see key %03d", expected), scanner.hasNext());
                assertEquals(toKey(expected), new String(scanner.next().getKey().getKey().array()));
            }
        }
        assertFalse(scanner.hasNext());
    }

    @Test
    public void testMultipleRanges()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(TABLE);
        store.clearUnsafe();

        // disable compaction while flushing
        store.disableAutoCompaction();

        for (int i = 0; i < 3; i++)
            for (int j = 2; j < 10; j++)
                insertRowWithKey(i * 100 + j);
        store.forceBlockingFlush();

        assertEquals(1, store.getSSTables().size());
        SSTableReader sstable = store.getSSTables().iterator().next();

        // full range scan
        ICompactionScanner fullScanner = sstable.getScanner();
        assertScanContainsRanges(fullScanner,
                                 2, 9,
                                 102, 109,
                                 202, 209);


        // scan all three ranges separately
        ICompactionScanner scanner = sstable.getScanner(makeRanges(1, 9,
                                                                   101, 109,
                                                                   201, 209),
                                                        null);
        assertScanContainsRanges(scanner,
                                 2, 9,
                                 102, 109,
                                 202, 209);

        // skip the first range
        scanner = sstable.getScanner(makeRanges(101, 109,
                                                201, 209),
                                     null);
        assertScanContainsRanges(scanner,
                                 102, 109,
                                 202, 209);

        // skip the second range
        scanner = sstable.getScanner(makeRanges(1, 9,
                                                201, 209),
                                     null);
        assertScanContainsRanges(scanner,
                                 2, 9,
                                 202, 209);


        // skip the last range
        scanner = sstable.getScanner(makeRanges(1, 9,
                                                101, 109),
                                     null);
        assertScanContainsRanges(scanner,
                                 2, 9,
                                 102, 109);

        // the first scanned range stops short of the actual data in the first range
        scanner = sstable.getScanner(makeRanges(1, 5,
                                                101, 109,
                                                201, 209),
                                     null);
        assertScanContainsRanges(scanner,
                                 2, 5,
                                 102, 109,
                                 202, 209);

        // the first scanned range requests data beyond actual data in the first range
        scanner = sstable.getScanner(makeRanges(1, 20,
                                                101, 109,
                                                201, 209),
                                     null);
        assertScanContainsRanges(scanner,
                                 2, 9,
                                 102, 109,
                                 202, 209);


        // the middle scan range splits the outside two data ranges
        scanner = sstable.getScanner(makeRanges(1, 5,
                                                6, 205,
                                                206, 209),
                                     null);
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
                                                1000, 1001),
                                     null);
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
                                                150, 159),
                                     null);
        assertScanContainsRanges(scanner,
                                 2, 9,
                                 102, 109,
                                 202, 209);

        // only empty ranges
        scanner = sstable.getScanner(makeRanges(0, 1,
                                                150, 159,
                                                250, 259),
                                     null);
        assertFalse(scanner.hasNext());

        // no ranges is equivalent to a full scan
        scanner = sstable.getScanner(new ArrayList<Range<Token>>(), null);
        assertFalse(scanner.hasNext());
    }

    @Test
    public void testSingleKeyMultipleRanges()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(TABLE);
        store.clearUnsafe();

        // disable compaction while flushing
        store.disableAutoCompaction();

        insertRowWithKey(205);
        store.forceBlockingFlush();

        assertEquals(1, store.getSSTables().size());
        SSTableReader sstable = store.getSSTables().iterator().next();

        // full range scan
        ICompactionScanner fullScanner = sstable.getScanner();
        assertScanContainsRanges(fullScanner, 205, 205);

        // scan three ranges separately
        ICompactionScanner scanner = sstable.getScanner(makeRanges(101, 109,
                                                                   201, 209), null);

        // this will currently fail
        assertScanContainsRanges(scanner, 205, 205);
    }
}
