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

package org.apache.cassandra.io.sstable.format.bti;

import java.util.Collections;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.TestDatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Covers the reuse of the data file reader between partition key verification and the row iterator.
 * <p>
 * A partition small enough to have no row index entry has its key verified by reading the data file at the position
 * the trie points to, which is where the row iterator then has to start. The reader is handed on rather than closed,
 * so the same position - and on a compressed table the same decompression and checksum - is not paid for twice.
 */
public class BtiTableReaderTest extends CQLTester
{
    /** Over the 4KiB default column_index_size, so this partition gets an entry in the row index. */
    private static final int ROWS_IN_WIDE_PARTITION = 400;
    private static final int NARROW_PARTITIONS = 100;
    private static final int WIDE_PARTITION_KEY = -1;

    @Before
    public void selectBtiFormatAndPopulate()
    {
        TestDatabaseDescriptor.setUnsafeSelectedSSTableFormat(new BtiFormat.BtiFormatFactory().getInstance(Collections.emptyMap()));

        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");

        String value = new String(new char[64]).replace('\0', 'x');
        for (int pk = 0; pk < NARROW_PARTITIONS; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 0, value);

        for (int ck = 0; ck < ROWS_IN_WIDE_PARTITION; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", WIDE_PARTITION_KEY, ck, value);

        flush();
    }

    private BtiTableReader sstable()
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> live = cfs.getLiveSSTables();
        assertEquals("expected the test data to be in a single sstable", 1, live.size());
        SSTableReader reader = live.iterator().next();
        assertTrue("expected a BTI sstable, got " + reader.getClass().getSimpleName(), reader instanceof BtiTableReader);
        return (BtiTableReader) reader;
    }

    private DecoratedKey key(int pk)
    {
        return sstable().decorateKey(ByteBufferUtil.bytes(pk));
    }

    private static void assertClosed(FileDataInput in)
    {
        assertFalse("reader should have been closed", isOpen(in));
    }

    private static void assertOpen(FileDataInput in)
    {
        assertTrue("reader should still be open", isOpen(in));
    }

    /**
     * A closed {@link RandomAccessReader} has dropped its buffer and refuses to seek; that is the externally visible
     * sign of closing it.
     */
    private static boolean isOpen(FileDataInput in)
    {
        RandomAccessReader reader = (RandomAccessReader) in;
        try
        {
            long position = reader.getFilePointer();
            reader.seek(0);          // accepted at any position while open, throws once closed
            reader.seek(position);   // leave the reader where we found it
            return true;
        }
        catch (IllegalStateException e)
        {
            return false;
        }
    }

    @Test
    public void testRetainsDataFileReaderForPartitionWithoutRowIndex() throws Exception
    {
        BtiTableReader sstable = sstable();
        DecoratedKey key = key(0);

        BtiTableReader.ExactPosition pos = sstable.getExactPosition(key, SSTableReadsListener.NOOP_LISTENER, false, true);

        assertNotNull(pos.entry);
        assertFalse("a narrow partition should have no row index entry", pos.entry.isIndexed());
        assertNotNull("the data file reader used for key verification should have been handed over", pos.dataInput);
        assertOpen(pos.dataInput);

        // it is left where the verification put it, just past the partition key, which is where the iterator seeks from
        assertTrue(pos.dataInput.getFilePointer() > pos.entry.position);

        pos.dataInput.close();
    }

    @Test
    public void testDoesNotRetainReaderForRowIndexedPartition()
    {
        BtiTableReader sstable = sstable();

        BtiTableReader.ExactPosition pos = sstable.getExactPosition(key(WIDE_PARTITION_KEY),
                                                                    SSTableReadsListener.NOOP_LISTENER,
                                                                    false,
                                                                    true);

        assertNotNull(pos.entry);
        assertTrue("a wide partition should have a row index entry", pos.entry.isIndexed());
        // that key was verified against the row index file, which is of no use to a data file iterator
        assertNull(pos.dataInput);
    }

    @Test
    public void testDoesNotRetainWhenNotRequested()
    {
        BtiTableReader sstable = sstable();

        BtiTableReader.ExactPosition pos = sstable.getExactPosition(key(0), SSTableReadsListener.NOOP_LISTENER, false, false);

        assertNotNull(pos.entry);
        assertNull(pos.dataInput);
    }

    @Test
    public void testMissingKeyRetainsNothing()
    {
        BtiTableReader sstable = sstable();

        BtiTableReader.ExactPosition pos = sstable.getExactPosition(key(NARROW_PARTITIONS + 1000),
                                                                    SSTableReadsListener.NOOP_LISTENER,
                                                                    false,
                                                                    true);

        assertNull(pos.entry);
        assertNull(pos.dataInput);
        assertSame(BtiTableReader.ExactPosition.NOT_FOUND, pos);
    }

    /**
     * The ownership half of the change: the iterator must close a reader handed to it, or every point read leaks a
     * reader and its buffer.
     */
    @Test
    public void testIteratorClosesRetainedReader()
    {
        for (boolean reversed : new boolean[]{ false, true })
        {
            BtiTableReader sstable = sstable();
            DecoratedKey key = key(1);

            BtiTableReader.ExactPosition pos = sstable.getExactPosition(key, SSTableReadsListener.NOOP_LISTENER, false, true);
            assertNotNull(pos.dataInput);

            int rows = 0;
            try (UnfilteredRowIterator iter = sstable.rowIterator(pos.dataInput, true, key, pos.entry,
                                                                  Slices.ALL, ColumnFilter.all(sstable.metadata()), reversed))
            {
                while (iter.hasNext())
                {
                    Unfiltered u = iter.next();
                    assertNotNull(u);
                    rows++;
                }
            }

            assertEquals("reversed=" + reversed, 1, rows);
            assertClosed(pos.dataInput);
        }
    }

    /**
     * A reader the caller keeps must survive the iterator (a scanner scenario).
     */
    @Test
    public void testIteratorLeavesCallerOwnedReaderOpen() throws Exception
    {
        BtiTableReader sstable = sstable();
        DecoratedKey key = key(2);

        BtiTableReader.ExactPosition pos = sstable.getExactPosition(key, SSTableReadsListener.NOOP_LISTENER, false, true);
        assertNotNull(pos.dataInput);

        try (UnfilteredRowIterator iter = sstable.rowIterator(pos.dataInput, false, key, pos.entry,
                                                              Slices.ALL, ColumnFilter.all(sstable.metadata()), false))
        {
            while (iter.hasNext())
                iter.next();
        }

        assertOpen(pos.dataInput);
        pos.dataInput.close();
    }

    @Test
    public void testNarrowPartitionsReadCorrectly()
    {
        String value = new String(new char[64]).replace('\0', 'x');
        for (int pk = 0; pk < NARROW_PARTITIONS; pk++)
            assertRows(execute("SELECT pk, ck, v FROM %s WHERE pk = ?", pk), row(pk, 0, value));
    }

    /**
     * A row-indexed partition is the case where nothing is handed over, so the iterator has to open its own reader
     * and close it again. Reversed as well as forward, since they build different readers.
     */
    @Test
    public void testWidePartitionReadsCorrectly()
    {
        assertEquals(ROWS_IN_WIDE_PARTITION,
                     execute("SELECT ck FROM %s WHERE pk = ?", WIDE_PARTITION_KEY).size());
        assertRows(execute("SELECT ck FROM %s WHERE pk = ? ORDER BY ck DESC LIMIT 1", WIDE_PARTITION_KEY),
                   row(ROWS_IN_WIDE_PARTITION - 1));
        assertRows(execute("SELECT ck FROM %s WHERE pk = ? ORDER BY ck ASC LIMIT 1", WIDE_PARTITION_KEY),
                   row(0));
    }

    @Test
    public void testMissingPartitionReturnsNothing()
    {
        assertEmpty(execute("SELECT * FROM %s WHERE pk = ?", NARROW_PARTITIONS + 1000));
    }

    @Test
    public void testFullScanStillWorks()
    {
        assertEquals(NARROW_PARTITIONS + ROWS_IN_WIDE_PARTITION, execute("SELECT pk, ck FROM %s").size());
    }

    /**
     * Repeated point reads through the normal entry point - the path that now hands the reader over every time.
     */
    @Test
    public void testRepeatedPointReadsThroughRowIterator()
    {
        BtiTableReader sstable = sstable();

        for (int round = 0; round < 5; round++)
        {
            for (int pk = 0; pk < NARROW_PARTITIONS; pk++)
            {
                DecoratedKey key = key(pk);
                int rows = 0;
                try (UnfilteredRowIterator iter = sstable.rowIterator(key, Slices.ALL, ColumnFilter.all(sstable.metadata()),
                                                                      false, SSTableReadsListener.NOOP_LISTENER))
                {
                    while (iter.hasNext())
                    {
                        iter.next();
                        rows++;
                    }
                }
                assertEquals("pk=" + pk, 1, rows);
            }
        }
    }
}
