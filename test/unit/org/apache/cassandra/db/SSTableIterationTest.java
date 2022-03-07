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

package org.apache.cassandra.db;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.compaction.SortedStringTableCursor;
import org.apache.cassandra.io.sstable.compaction.IteratorFromCursor;
import org.apache.cassandra.io.sstable.compaction.SSTableCursor;
import org.apache.cassandra.io.sstable.compaction.SSTableCursorMerger;
import org.apache.cassandra.io.sstable.compaction.SkipEmptyDataCursor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class SSTableIterationTest extends CQLTester
{

    public static final int PARTITIONS = 5;
    public static final int ROWS = 10;

    @Test
    public void testRowIteration() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        for (int i = 0; i < PARTITIONS; ++i)
            for (int j = 0; j < ROWS; j++)
                if (i != j)
                    execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", i, 0, j, i + j, (long) j);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        assertEquals((ROWS - 1) * PARTITIONS, execute("SELECT * FROM %s").size());

        SSTableReader reader = cfs.getLiveSSTables().iterator().next();
        assertCursorMatchesScanner(reader);
    }

    @Test
    public void testRowIterationWithComplexColumn() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, d set<int>, PRIMARY KEY (a, b, c))");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        for (int i = 0; i < PARTITIONS; ++i)
            for (int j = 0; j < ROWS; j++)
                if (i != j)
                    execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", i, 0, j, ImmutableSet.of(i, j, i + j), (long) j);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        assertEquals((ROWS - 1) * PARTITIONS, execute("SELECT * FROM %s").size());

        SSTableReader reader = cfs.getLiveSSTables().iterator().next();
        assertCursorMatchesScanner(reader);
    }

    @Test
    public void testRowIterationWithDeletion() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        for (int i = 0; i < PARTITIONS; ++i)
            for (int j = 0; j < ROWS; j++)
                if (i != j)
                    execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", i, 0, j, i + j, (long) j);
                else
                    execute("DELETE FROM %s USING TIMESTAMP ? WHERE a = ? AND b = ? AND c = ?", (long) j, i, 0, j);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        assertEquals((ROWS - 1) * PARTITIONS, execute("SELECT * FROM %s").size());

        SSTableReader reader = cfs.getLiveSSTables().iterator().next();
        assertCursorMatchesScanner(reader);
    }

    @Test
    public void testRowIterationWithRangeTombstone() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        for (int i = 0; i < PARTITIONS; ++i)
            for (int j = 0; j < ROWS; j++)
                if (i != j)
                    execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", i, 0, j, i + j, (long) j);
                else
                    execute("DELETE FROM %s USING TIMESTAMP ? WHERE a = ? AND b = ? AND c >= ? AND c <= ?", (long) j, i, 0, j - 2, j + 2);

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        // We deleted two before c=a for a>1, 1 for a=1 and 0 for a=0.
        assertEquals((ROWS - 3) * PARTITIONS + 3, execute("SELECT * FROM %s").size());

        SSTableReader reader = cfs.getLiveSSTables().iterator().next();
        assertCursorMatchesScanner(reader);
    }

    @Test
    public void testRowIterationWithStatic() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, d int, s int static, PRIMARY KEY (a, b, c))");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        for (int i = 0; i < PARTITIONS; ++i)
            for (int j = 0; j < ROWS; j++)
                if (i != j)
                    if (i % 3 == 1)
                        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", i, 0, j, i + j, (long) j);
                    else
                        execute("INSERT INTO %s (a, b, c, d, s) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?", i, 0, j, i + j, i + j, (long) j);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        assertEquals((ROWS - 1) * PARTITIONS, execute("SELECT * FROM %s").size());

        SSTableReader reader = cfs.getLiveSSTables().iterator().next();
        assertCursorMatchesScanner(reader);
    }


    @Test
    public void testIteratorMerge1() throws Throwable
    {
        testIteratorMerge(1);
    }

    @Test
    public void testIteratorMerge2() throws Throwable
    {
        testIteratorMerge(2);
    }

    @Test
    public void testIteratorMerge3() throws Throwable
    {
        testIteratorMerge(3);
    }

    @Test
    public void testIteratorMerge7() throws Throwable
    {
        testIteratorMerge(7);
    }

    @Test
    public void testIteratorMerge15() throws Throwable
    {
        testIteratorMerge(15);
    }

    public void testIteratorMerge(int sstableCount) throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, d text, s text static, cc set<int>, PRIMARY KEY (a, b, c))");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        cfs.disableAutoCompaction();

        for (int sstable = 0; sstable < sstableCount; ++sstable)
        {
            for (int i = 0; i < PARTITIONS; ++i)
                for (int j = 0; j < ROWS; j++)
                    if (i != j)
                    {
                        if (i % 3 != 1 || (sstable + i) % 5 == 4)
                            execute("INSERT INTO %s (a, b, c, d, cc) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?", i + sstable, 0, j * sstable, "" + i + ":" + j + ":" + sstable, ImmutableSet.of(i, j, sstable), (long) sstable);
                        else
                            execute("INSERT INTO %s (a, b, c, d, s) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?", i + sstable, 0, j * sstable, "" + i + ":" + j + ":" + sstable, "" + i + ":" + j + ":" + sstable, (long) sstable);
                    }
                    else
                    {
                        if (i % 3 != 1 || (sstable + i) % 5 == 4)
                            execute("DELETE FROM %s USING TIMESTAMP ? WHERE a = ? AND b = ? AND c >= ? AND c <= ?", (long) sstable, i, 0, j - 2, j + 2);
                        else
                            execute("DELETE FROM %s USING TIMESTAMP ? WHERE a = ? AND b = ? AND c = ?", (long) sstable, i, 0, j);
                    }
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        }

        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        for (SSTableReader rdr : sstables)
        {
            assertCursorMatchesScanner(rdr);
        }

        SSTableCursor mergedCursor = new SSTableCursorMerger(sstables.stream()
                                                                     .map(SortedStringTableCursor::new)
                                                                     .collect(Collectors.toList()),
                                                             cfs.metadata());
        mergedCursor = new SkipEmptyDataCursor(mergedCursor); // make sure we drop rows that end up empty

        UnfilteredPartitionIterator mergedScanner = UnfilteredPartitionIterators.merge(sstables.stream()
                                                                                               .map(SSTableReader::getScanner)
                                                                                               .collect(Collectors.toList()),
                                                                                       UnfilteredPartitionIterators.MergeListener.NOOP);
        assertIteratorsEqual(mergedScanner, new IteratorFromCursor(cfs.metadata(), mergedCursor), true);
    }



    private void dumpCursor(SSTableCursor cursor, TableMetadata metadata) throws IOException
    {
        while (true)
        {
            switch (cursor.advance())
            {
                case COMPLEX_COLUMN_CELL:
                    System.out.println("      " + cursor.cell());
                    break;
                case COMPLEX_COLUMN:
                    System.out.println("    " + cursor.column() + " " + cursor.complexColumnDeletion());
                    break;
                case SIMPLE_COLUMN:
                    System.out.println("    " + cursor.cell());
                    break;
                case ROW:
                    System.out.println("  Row @" + cursor.clusteringKey().toString(metadata) + " " + cursor.rowLevelDeletion());
                    break;
                case RANGE_TOMBSTONE:
                    System.out.println("  Range tombstone @" + cursor.clusteringKey().toString(metadata) + " " + cursor.rowLevelDeletion());
                    break;
                case PARTITION:
                    System.out.println("Partition @" + cursor.partitionKey().toString());
                    break;
                case EXHAUSTED:
                    System.out.println("Exhausted\n");
                    return;
            }
        }
    }

    private void assertCursorMatchesScanner(SSTableReader reader) throws IOException
    {
        dumpSSTableCursor(reader);
        try (UnfilteredPartitionIterator scanner = reader.getScanner();
             UnfilteredPartitionIterator cursor = new IteratorFromCursor(reader.metadata(), new SortedStringTableCursor(reader)))
        {
            assertIteratorsEqual(scanner, cursor, false);
        }
    }

    private void dumpSSTableCursor(SSTableReader reader) throws IOException
    {
        try (SSTableCursor cursor = new SortedStringTableCursor(reader))
        {
            dumpCursor(cursor, reader.metadata());
        }
    }

    private void assertIteratorsEqual(UnfilteredPartitionIterator scanner, UnfilteredPartitionIterator cursor, boolean dump)
    {
        assertEquals(scanner.metadata(), cursor.metadata());
        while (scanner.hasNext())
        {
            assertTrue(cursor.hasNext());
            try (UnfilteredRowIterator scannerRows = scanner.next();
                 UnfilteredRowIterator cursorRows = cursor.next())
            {
                assertEquals(scannerRows.isEmpty(), cursorRows.isEmpty());
                assertEquals(scannerRows.partitionKey(), cursorRows.partitionKey());
                assertEquals(scannerRows.partitionLevelDeletion(), cursorRows.partitionLevelDeletion());
                assertEquals(scannerRows.metadata(), cursorRows.metadata());
                assertEquals(scannerRows.staticRow(), cursorRows.staticRow());
                if (dump) System.out.println("Partition @" + cursorRows.partitionKey());

                if (dump && !cursorRows.staticRow().isEmpty())
                    System.out.println("  " + cursorRows.staticRow().toString(cursorRows.metadata()));

                while (scannerRows.hasNext())
                {
                    assertTrue(cursorRows.hasNext());
                    Unfiltered next = cursorRows.next();
                    if (dump) System.out.println("  " + next.toString(cursorRows.metadata()));
                    assertEquals(scannerRows.next(), next);
                }
                assertFalse(cursorRows.hasNext());
            }
        }
        assertFalse(cursor.hasNext());
    }
}
