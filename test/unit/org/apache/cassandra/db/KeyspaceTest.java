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

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.metrics.ClearSnapshotFilesMetrics;
import org.apache.cassandra.schema.Schema;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.ClearableHistogram;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import org.slf4j.Logger;

import org.mockito.Mockito;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;

import static java.lang.String.format;
import static org.junit.Assert.*;

public class KeyspaceTest extends CQLTester
{
    // Test needs synchronous table drop to avoid flushes causing flaky failures of testLimitSSTables

    @Override
    protected String createTable(String query)
    {
        return super.createTable(KEYSPACE_PER_TEST, query);
    }

    @Override
    protected UntypedResultSet execute(String query, Object... values) throws Throwable
    {
        return executeFormattedQuery(formatQuery(KEYSPACE_PER_TEST, query), values);
    }

    @Override
    public ColumnFamilyStore getCurrentColumnFamilyStore()
    {
        return super.getCurrentColumnFamilyStore(KEYSPACE_PER_TEST);
    }

    @Test
    public void testGetRowNoColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, PRIMARY KEY (a, b))");

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "0", 0, 0);

        final ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        for (int round = 0; round < 2; round++)
        {
            // slice with limit 0
            Util.assertEmpty(Util.cmd(cfs, "0").columns("c").withLimit(0).build());

            // slice with nothing in between the bounds
            Util.assertEmpty(Util.cmd(cfs, "0").columns("c").fromIncl(1).toIncl(1).build());

            // fetch a non-existent name
            Util.assertEmpty(Util.cmd(cfs, "0").columns("c").includeRow(1).build());

            if (round == 0)
                Util.flush(cfs);
        }
    }

    @Test
    public void testCreateCDCKeyspace() throws Throwable
    {
        String handlerClass = TestCDCHandler.class.getName();
        String keyspace = "test_create_cdc_keyspace1";
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", keyspace));
        assertRows(execute(format("select cdc_handler from %s.%s where keyspace_name = ?",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspaceTables.KEYSPACES),
                           keyspace),
                   row(ImmutableMap.of()));

        schemaChange(String.format("ALTER KEYSPACE %s WITH cdc_handler = {'class': '%s'}", keyspace, handlerClass));
        assertRows(execute(format("select cdc_handler from %s.%s where keyspace_name = ?",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspaceTables.KEYSPACES),
                           keyspace),
                   row(ImmutableMap.of("class", handlerClass)));

        keyspace = "test_create_cdc_keyspace2";

        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} and cdc_handler = {'class': '%s'}", keyspace, handlerClass));
        assertRows(execute(format("select cdc_handler from %s.%s where keyspace_name = ?",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspaceTables.KEYSPACES),
                           keyspace),
                   row(ImmutableMap.of("class", handlerClass)));

        schemaChange(String.format("ALTER KEYSPACE %s WITH cdc_handler = {'class': ''}", keyspace));
        assertRows(execute(format("select cdc_handler from %s.%s where keyspace_name = ?",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspaceTables.KEYSPACES),
                           keyspace),
                   row(ImmutableMap.of()));

        keyspace = "test_create_cdc_keyspace3";
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} and cdc_handler = {'class': '%s', 'option1': 'test1', 'option2': 'test2'}", keyspace, handlerClass));
        assertRows(execute(format("select cdc_handler from %s.%s where keyspace_name = ?",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspaceTables.KEYSPACES),
                           keyspace),
                   row(ImmutableMap.of("class", handlerClass,
                                       "option1", "test1",
                                       "option2", "test2")));
    }

    @Test(expected = RuntimeException.class)
    public void testCreateInvalidCDCKeyspace() throws Throwable
    {
        String keyspace = "test_create_invalid_cdc_keyspace";
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND cdc_handler = {'class': 'invalid'}", keyspace));
    }

    @Test(expected = RuntimeException.class)
    public void testAlterInvalidCDCKeyspace() throws Throwable
    {
        String keyspace = "test_alter_invalid_cdc_keyspace";
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", keyspace));

        assertRows(execute(format("select cdc_handler from %s.%s where keyspace_name = ?",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspaceTables.KEYSPACES),
                           keyspace),
                   row(ImmutableMap.of()));

        schemaChange(String.format("ALTER KEYSPACE %s WITH cdc_handler = {'class': 'invalid'}", keyspace));
    }

    @Test
    public void testGetRowSingleColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, PRIMARY KEY (a, b))");

        for (int i = 0; i < 2; i++)
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "0", i, i);

        final ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        for (int round = 0; round < 2; round++)
        {
            // slice with limit 1
            Row row = Util.getOnlyRow(Util.cmd(cfs, "0").columns("c").withLimit(1).build());
            assertEquals(ByteBufferUtil.bytes(0), row.getCell(cfs.metadata().getColumn(new ColumnIdentifier("c", false))).buffer());

            // fetch each row by name
            for (int i = 0; i < 2; i++)
            {
                row = Util.getOnlyRow(Util.cmd(cfs, "0").columns("c").includeRow(i).build());
                assertEquals(ByteBufferUtil.bytes(i), row.getCell(cfs.metadata().getColumn(new ColumnIdentifier("c", false))).buffer());
            }

            // fetch each row by slice
            for (int i = 0; i < 2; i++)
            {
                row = Util.getOnlyRow(Util.cmd(cfs, "0").columns("c").fromIncl(i).toIncl(i).build());
                assertEquals(ByteBufferUtil.bytes(i), row.getCell(cfs.metadata().getColumn(new ColumnIdentifier("c", false))).buffer());
            }

            if (round == 0)
                Util.flush(cfs);
        }
    }

    @Test
    public void testGetSliceBloomFilterFalsePositive() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, PRIMARY KEY (a, b))");

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "1", 1, 1);

        final ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        // check empty reads on the partitions before and after the existing one
        for (String key : new String[]{"0", "2"})
            Util.assertEmpty(Util.cmd(cfs, key).build());

        Util.flush(cfs);

        for (String key : new String[]{"0", "2"})
            Util.assertEmpty(Util.cmd(cfs, key).build());

        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        assertEquals(1, sstables.size());
        Util.disableBloomFilter(cfs);

        for (String key : new String[]{"0", "2"})
            Util.assertEmpty(Util.cmd(cfs, key).build());
    }

    private static void assertRowsInSlice(ColumnFamilyStore cfs, String key, int sliceStart, int sliceEnd, int limit, boolean reversed, String columnValuePrefix)
    {
        Clustering<?> startClustering = Clustering.make(ByteBufferUtil.bytes(sliceStart));
        Clustering<?> endClustering = Clustering.make(ByteBufferUtil.bytes(sliceEnd));
        Slices slices = Slices.with(cfs.getComparator(), Slice.make(startClustering, endClustering));
        ClusteringIndexSliceFilter filter = new ClusteringIndexSliceFilter(slices, reversed);
        SinglePartitionReadCommand command = singlePartitionSlice(cfs, key, filter, limit);

        try (ReadExecutionController executionController = command.executionController();
             PartitionIterator iterator = command.executeInternal(executionController))
        {
            try (RowIterator rowIterator = iterator.next())
            {
                if (reversed)
                {
                    for (int i = sliceEnd; i >= sliceStart; i--)
                    {
                        Row row = rowIterator.next();
                        Cell<?> cell = row.getCell(cfs.metadata().getColumn(new ColumnIdentifier("c", false)));
                        assertEquals(ByteBufferUtil.bytes(columnValuePrefix + i), cell.buffer());
                    }
                }
                else
                {
                    for (int i = sliceStart; i <= sliceEnd; i++)
                    {
                        Row row = rowIterator.next();
                        Cell<?> cell = row.getCell(cfs.metadata().getColumn(new ColumnIdentifier("c", false)));
                        assertEquals(ByteBufferUtil.bytes(columnValuePrefix + i), cell.buffer());
                    }
                }
                assertFalse(rowIterator.hasNext());
            }
        }
    }

    @Test
    public void testGetSliceWithCutoff() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c text, PRIMARY KEY (a, b))");
        String prefix = "omg!thisisthevalue!";

        for (int i = 0; i < 300; i++)
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "0", i, prefix + i);

        final ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        for (int round = 0; round < 2; round++)
        {
            assertRowsInSlice(cfs, "0", 96, 99, 4, false, prefix);
            assertRowsInSlice(cfs, "0", 96, 99, 4, true, prefix);

            assertRowsInSlice(cfs, "0", 100, 103, 4, false, prefix);
            assertRowsInSlice(cfs, "0", 100, 103, 4, true, prefix);

            assertRowsInSlice(cfs, "0", 0, 99, 100, false, prefix);
            assertRowsInSlice(cfs, "0", 288, 299, 12, true, prefix);

            if (round == 0)
                Util.flush(cfs);
        }
    }

    @Test
    public void testReversedWithFlushing() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, PRIMARY KEY (a, b)) WITH CLUSTERING ORDER BY (b DESC)");
        final ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "0", i, i);

        Util.flush(cfs);

        for (int i = 10; i < 20; i++)
        {
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "0", i, i);

            RegularAndStaticColumns columns = RegularAndStaticColumns.of(cfs.metadata().getColumn(new ColumnIdentifier("c", false)));
            ClusteringIndexSliceFilter filter = new ClusteringIndexSliceFilter(Slices.ALL, false);
            SinglePartitionReadCommand command = singlePartitionSlice(cfs, "0", filter, null);
            try (ReadExecutionController executionController = command.executionController();
                 PartitionIterator iterator = command.executeInternal(executionController))
            {
                try (RowIterator rowIterator = iterator.next())
                {
                    Row row = rowIterator.next();
                    Cell<?> cell = row.getCell(cfs.metadata().getColumn(new ColumnIdentifier("c", false)));
                    assertEquals(ByteBufferUtil.bytes(i), cell.buffer());
                }
            }
        }
    }

    private static void assertRowsInResult(ColumnFamilyStore cfs, SinglePartitionReadCommand command, int ... columnValues)
    {
        try (ReadExecutionController executionController = command.executionController();
             PartitionIterator iterator = command.executeInternal(executionController))
        {
            if (columnValues.length == 0)
            {
                if (iterator.hasNext())
                    fail("Didn't expect any results, but got rows starting with: " + iterator.next().next().toString(cfs.metadata()));
                return;
            }

            try (RowIterator rowIterator = iterator.next())
            {
                for (int expected : columnValues)
                {
                    Row row = rowIterator.next();
                    Cell<?> cell = row.getCell(cfs.metadata().getColumn(new ColumnIdentifier("c", false)));
                    assertEquals(
                            String.format("Expected %s, but got %s", ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(expected)), ByteBufferUtil.bytesToHex(cell.buffer())),
                            ByteBufferUtil.bytes(expected), cell.buffer());
                }
                assertFalse(rowIterator.hasNext());
            }
        }
    }

    private static ClusteringIndexSliceFilter slices(ColumnFamilyStore cfs, Integer sliceStart, Integer sliceEnd, boolean reversed)
    {
        ClusteringBound<ByteBuffer> startBound = sliceStart == null
                                                 ? BufferClusteringBound.create(ClusteringPrefix.Kind.INCL_START_BOUND, ByteBufferUtil.EMPTY_ARRAY)
                                                 : BufferClusteringBound.create(ClusteringPrefix.Kind.INCL_START_BOUND, new ByteBuffer[]{ByteBufferUtil.bytes(sliceStart)});
        ClusteringBound<ByteBuffer> endBound = sliceEnd == null
                                               ? BufferClusteringBound.create(ClusteringPrefix.Kind.INCL_END_BOUND, ByteBufferUtil.EMPTY_ARRAY)
                                               : BufferClusteringBound.create(ClusteringPrefix.Kind.INCL_END_BOUND, new ByteBuffer[]{ByteBufferUtil.bytes(sliceEnd)});
        Slices slices = Slices.with(cfs.getComparator(), Slice.make(startBound, endBound));
        return new ClusteringIndexSliceFilter(slices, reversed);
    }

    private static SinglePartitionReadCommand singlePartitionSlice(ColumnFamilyStore cfs, String key, ClusteringIndexSliceFilter filter, Integer rowLimit)
    {
        DataLimits limit = rowLimit == null
                         ? DataLimits.NONE
                         : DataLimits.cqlLimits(rowLimit);
        return SinglePartitionReadCommand.create(
                cfs.metadata(), FBUtilities.nowInSeconds(), ColumnFilter.all(cfs.metadata()), RowFilter.NONE, limit, Util.dk(key), filter);
    }

    @Test
    public void testGetSliceFromBasic() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, PRIMARY KEY (a, b))");
        final ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        for (int i = 1; i < 10; i++)
        {
            if (i == 6 || i == 8)
                continue;
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "0", i, i);
        }

        execute("DELETE FROM %s WHERE a = ? AND b = ?", "0", 4);

        for (int round = 0; round < 2; round++)
        {

            ClusteringIndexSliceFilter filter = slices(cfs, 5, null, false);
            SinglePartitionReadCommand command = singlePartitionSlice(cfs, "0", filter, 2);
            assertRowsInResult(cfs, command, 5, 7);

            command = singlePartitionSlice(cfs, "0", slices(cfs, 4, null, false), 2);
            assertRowsInResult(cfs, command, 5, 7);

            command = singlePartitionSlice(cfs, "0", slices(cfs, null, 5, true), 2);
            assertRowsInResult(cfs, command, 5, 3);

            command = singlePartitionSlice(cfs, "0", slices(cfs, null, 6, true), 2);
            assertRowsInResult(cfs, command, 5, 3);

            command = singlePartitionSlice(cfs, "0", slices(cfs, null, 6, true), 2);
            assertRowsInResult(cfs, command, 5, 3);

            command = singlePartitionSlice(cfs, "0", slices(cfs, null, null, true), 2);
            assertRowsInResult(cfs, command, 9, 7);

            command = singlePartitionSlice(cfs, "0", slices(cfs, 95, null, false), 2);
            assertRowsInResult(cfs, command);

            command = singlePartitionSlice(cfs, "0", slices(cfs, null, 0, true), 2);
            assertRowsInResult(cfs, command);

            if (round == 0)
                Util.flush(cfs);
        }
    }

    @Test
    public void testGetSliceWithExpiration() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, PRIMARY KEY (a, b))");
        final ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "0", 0, 0);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TTL 60", "0", 1, 1);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "0", 2, 2);

        for (int round = 0; round < 2; round++)
        {
            SinglePartitionReadCommand command = singlePartitionSlice(cfs, "0", slices(cfs, null, null, false), 2);
            assertRowsInResult(cfs, command, 0, 1);

            command = singlePartitionSlice(cfs, "0", slices(cfs, 1, null, false), 1);
            assertRowsInResult(cfs, command, 1);

            if (round == 0)
                Util.flush(cfs);
        }
    }

    @Test
    public void testGetSliceFromAdvanced() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, PRIMARY KEY (a, b))");
        final ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        for (int i = 1; i < 7; i++)
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "0", i, i);

        Util.flush(cfs);

        // overwrite three rows with -1
        for (int i = 1; i < 4; i++)
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "0", i, -1);

        for (int round = 0; round < 2; round++)
        {
            SinglePartitionReadCommand command = singlePartitionSlice(cfs, "0", slices(cfs, 2, null, false), 3);
            assertRowsInResult(cfs, command, -1, -1, 4);

            if (round == 0)
                Util.flush(cfs);
        }
    }

    @Test
    public void testGetSliceFromLarge() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, PRIMARY KEY (a, b))");
        final ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        for (int i = 1000; i < 2000; i++)
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "0", i, i);

        Util.flush(cfs);

        validateSliceLarge(cfs);

        // compact so we have a big row with more than the minimum index count
        if (cfs.getLiveSSTables().size() > 1)
            CompactionManager.instance.performMaximal(cfs, false);

        // verify that we do indeed have multiple index entries
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        RowIndexEntry<?> indexEntry = sstable.getPosition(Util.dk("0"), SSTableReader.Operator.EQ);
        assert indexEntry.columnsIndexCount() > 2;

        validateSliceLarge(cfs);
    }

    @Test
    public void testSnapshotCreation() throws Throwable {
        createTable("CREATE TABLE %s (a text, b int, c int, PRIMARY KEY (a, b))");

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "0", 0, 0);

        Keyspace ks = Keyspace.open(KEYSPACE_PER_TEST);
        String table = getCurrentColumnFamilyStore().name;
        ks.snapshot("test", table);

        assertTrue(ks.snapshotExists("test"));
        assertEquals(1, ks.getAllSnapshots().count());
    }

    @Test
    public void testClearSnapshotFiles() throws Throwable
    {
        String snapshotName = "test_snapshot_files";
        createTable("CREATE TABLE %s (a text, b int, c int, PRIMARY KEY (a, b))");
        final ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int j = 0; j < 10; j++)
        {
            for (int i = 1000 + (j * 100); i < 1000 + ((j + 1) * 100); i++)
                execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ?", "0", i, i, (long) i);

            Util.flush(cfs);
        }
        // take snapshot of this table
        Keyspace.open(KEYSPACE_PER_TEST).snapshot(snapshotName, cfs.name);

        for (File tableDir : cfs.getDirectories().getCFDirectories())
        {
            long noTableDirCountBefore;
            long noTableDirCountAfter;
            long ioExceptionBefore;
            long ioExceptionAfter;
            long successCountBefore;
            long successCountAfter;

            File snapshotDir = Directories.getSnapshotDirectory(tableDir, snapshotName);
            File[] files = snapshotDir.list();
            int count = files.length;
            assertTrue(count > 0);

            // delete one file
            noTableDirCountBefore = ClearSnapshotFilesMetrics.noTableDir.getCount();
            ioExceptionBefore = ClearSnapshotFilesMetrics.ioException.getCount();
            successCountBefore = ClearSnapshotFilesMetrics.success.latency.getCount();
            Keyspace.clearSnapshotFiles(snapshotName, KEYSPACE_PER_TEST, tableDir.name(), files[0].name());
            noTableDirCountAfter = ClearSnapshotFilesMetrics.noTableDir.getCount();
            ioExceptionAfter = ClearSnapshotFilesMetrics.ioException.getCount();
            successCountAfter = ClearSnapshotFilesMetrics.success.latency.getCount();
            files = Directories.getSnapshotDirectory(tableDir, snapshotName).list();
            int oldCount = count;
            count = files.length;
            assertEquals(1, oldCount - count);
            assertEquals(0, noTableDirCountAfter - noTableDirCountBefore);
            assertEquals(0, ioExceptionAfter - ioExceptionBefore);
            assertEquals(1, successCountAfter - successCountBefore);

            // delete two files
            noTableDirCountBefore = ClearSnapshotFilesMetrics.noTableDir.getCount();
            ioExceptionBefore = ClearSnapshotFilesMetrics.ioException.getCount();
            successCountBefore = ClearSnapshotFilesMetrics.success.latency.getCount();
            Keyspace.clearSnapshotFiles(snapshotName, KEYSPACE_PER_TEST, tableDir.name(), files[0].name(), files[1].name());
            noTableDirCountAfter = ClearSnapshotFilesMetrics.noTableDir.getCount();
            ioExceptionAfter = ClearSnapshotFilesMetrics.ioException.getCount();
            successCountAfter = ClearSnapshotFilesMetrics.success.latency.getCount();
            files = Directories.getSnapshotDirectory(tableDir, snapshotName).list();
            oldCount = count;
            count = files.length;
            assertEquals(2, oldCount - count);
            assertEquals(0, noTableDirCountAfter - noTableDirCountBefore);
            assertEquals(0, ioExceptionAfter - ioExceptionBefore);
            assertEquals(1, successCountAfter - successCountBefore);

            // delete non-existing files
            noTableDirCountBefore = ClearSnapshotFilesMetrics.noTableDir.getCount();
            ioExceptionBefore = ClearSnapshotFilesMetrics.ioException.getCount();
            successCountBefore = ClearSnapshotFilesMetrics.success.latency.getCount();
            String nonExistingTable = tableDir.name() + "nonexisting";
            try {
                Keyspace.clearSnapshotFiles(snapshotName, KEYSPACE_PER_TEST, nonExistingTable, files[0].name());
                fail(); // should not reach here
            } catch (Exception e) {
                assertTrue(e instanceof RuntimeException);
                assertTrue(e.getMessage().contains("No table dir found"));
            }
            noTableDirCountAfter = ClearSnapshotFilesMetrics.noTableDir.getCount();
            ioExceptionAfter = ClearSnapshotFilesMetrics.ioException.getCount();
            successCountAfter = ClearSnapshotFilesMetrics.success.latency.getCount();
            files = Directories.getSnapshotDirectory(tableDir, snapshotName).list();
            oldCount = count;
            count = files.length;
            assertEquals(0, oldCount - count);
            assertEquals(1, noTableDirCountAfter - noTableDirCountBefore);
            assertEquals(0, ioExceptionAfter - ioExceptionBefore);
            assertEquals(0, successCountAfter - successCountBefore);

            // delete all snapshot files in snapshot dir
            noTableDirCountBefore = ClearSnapshotFilesMetrics.noTableDir.getCount();
            ioExceptionBefore = ClearSnapshotFilesMetrics.ioException.getCount();
            successCountBefore = ClearSnapshotFilesMetrics.success.latency.getCount();
            Keyspace.clearSnapshotFiles(snapshotName, KEYSPACE_PER_TEST, tableDir.name());
            noTableDirCountAfter = ClearSnapshotFilesMetrics.noTableDir.getCount();
            ioExceptionAfter = ClearSnapshotFilesMetrics.ioException.getCount();
            successCountAfter = ClearSnapshotFilesMetrics.success.latency.getCount();
            files = Directories.getSnapshotDirectory(tableDir, snapshotName).list();
            assertEquals(0, files.length);
            assertEquals(0, noTableDirCountAfter - noTableDirCountBefore);
            assertEquals(0, ioExceptionAfter - ioExceptionBefore);
            assertEquals(1, successCountAfter - successCountBefore);
        }
    }

    @Test
    public void testClearSnapshotFilesWithLogger() throws Throwable
    {
        String snapshotName = "test_snapshot_files";
        createTable("CREATE TABLE %s (a text, b int, c int, PRIMARY KEY (a, b))");
        final ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int j = 0; j < 10; j++)
        {
            for (int i = 1000 + (j * 100); i < 1000 + ((j + 1) * 100); i++)
                execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ?", "0", i, i, (long) i);

            Util.flush(cfs);
        }
        // take snapshot of this table
        Keyspace.open(KEYSPACE_PER_TEST).snapshot(snapshotName, cfs.name);

        Logger mockLogger = Mockito.mock(Logger.class);
        for (File tableDir : cfs.getDirectories().getCFDirectories())
        {
            DatabaseDescriptor.setClearSnapshotFilesLogEnabled(false);
            File snapshotDir = Directories.getSnapshotDirectory(tableDir, snapshotName);
            File[] files = snapshotDir.list();
            Keyspace.clearSnapshotFilesWithLogger(mockLogger, snapshotName, KEYSPACE_PER_TEST, tableDir.name(), files[0].name());
            verify(mockLogger, never()).info(Mockito.anyString());

            DatabaseDescriptor.setClearSnapshotFilesLogEnabled(true);
            files = Directories.getSnapshotDirectory(tableDir, snapshotName).list();
            Keyspace.clearSnapshotFilesWithLogger(mockLogger, snapshotName, KEYSPACE_PER_TEST, tableDir.name(), files[0].name());
            verify(mockLogger).info(contains("clearSnapshotFiles success."));

            DatabaseDescriptor.setClearSnapshotFilesLogEnabled(false);
            files = Directories.getSnapshotDirectory(tableDir, snapshotName).list();
            String nonExistingTable = tableDir.name() + "nonexisting";
            try {
                Keyspace.clearSnapshotFilesWithLogger(mockLogger, snapshotName, KEYSPACE_PER_TEST, nonExistingTable, files[0].name());
                fail(); // should not reach here
            } catch (Exception e) {
                verify(mockLogger).error(Mockito.contains("clearSnapshotFiles noTableDir."));
            }
        }

        // always make sure it is set to false after the test
        DatabaseDescriptor.setClearSnapshotFilesLogEnabled(false);
    }

    @Test
    public void testConvertToSingleStringForLogging()
    {
        String res = Keyspace.convertToSingleStringForLogging(
        "snapshot1", "ks1", "tb1", new String[]{"file1", "file2"});
        assertEquals(res, "snapshotName=snapshot1 keyspace=ks1 tableDir=tb1 files=[ file1 file2 ]");
    }

    @Test
    public void testLimitSSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, PRIMARY KEY (a, b))");
        final ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int j = 0; j < 10; j++)
        {
            for (int i = 1000 + (j*100); i < 1000 + ((j+1)*100); i++)
                execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ?", "0", i, i, (long)i);

            Util.flush(cfs);
        }

        ((ClearableHistogram)cfs.metric.sstablesPerReadHistogram.cf).clear();

        SinglePartitionReadCommand command = singlePartitionSlice(cfs, "0", slices(cfs, null, 1499, false), 1000);
        int[] expectedValues = new int[500];
        for (int i = 0; i < 500; i++)
            expectedValues[i] = i + 1000;
        assertRowsInResult(cfs, command, expectedValues);

        assertEquals(5, cfs.metric.sstablesPerReadHistogram.cf.getSnapshot().getMax(), 0.1);
        ((ClearableHistogram)cfs.metric.sstablesPerReadHistogram.cf).clear();

        command = singlePartitionSlice(cfs, "0", slices(cfs, 1500, 2000, false), 1000);
        for (int i = 0; i < 500; i++)
            expectedValues[i] = i + 1500;
        assertRowsInResult(cfs, command, expectedValues);

        assertEquals(5, cfs.metric.sstablesPerReadHistogram.cf.getSnapshot().getMax(), 0.1);
        ((ClearableHistogram)cfs.metric.sstablesPerReadHistogram.cf).clear();

        // reverse
        command = singlePartitionSlice(cfs, "0", slices(cfs, 1500, 2000, true), 1000);
        for (int i = 0; i < 500; i++)
            expectedValues[i] = 1999 - i;
        assertRowsInResult(cfs, command, expectedValues);
    }

    private void validateSliceLarge(ColumnFamilyStore cfs)
    {
        ClusteringIndexSliceFilter filter = slices(cfs, 1000, null, false);
        SinglePartitionReadCommand command = SinglePartitionReadCommand.create(
                cfs.metadata(), FBUtilities.nowInSeconds(), ColumnFilter.all(cfs.metadata()), RowFilter.NONE, DataLimits.cqlLimits(3), Util.dk("0"), filter);
        assertRowsInResult(cfs, command, 1000, 1001, 1002);

        filter = slices(cfs, 1195, null, false);
        command = SinglePartitionReadCommand.create(
                cfs.metadata(), FBUtilities.nowInSeconds(), ColumnFilter.all(cfs.metadata()), RowFilter.NONE, DataLimits.cqlLimits(3), Util.dk("0"), filter);
        assertRowsInResult(cfs, command, 1195, 1196, 1197);

        filter = slices(cfs, null, 1996, true);
        command = SinglePartitionReadCommand.create(
                cfs.metadata(), FBUtilities.nowInSeconds(), ColumnFilter.all(cfs.metadata()), RowFilter.NONE, DataLimits.cqlLimits(1000), Util.dk("0"), filter);
        int[] expectedValues = new int[997];
        for (int i = 0, v = 1996; v >= 1000; i++, v--)
            expectedValues[i] = v;
        assertRowsInResult(cfs, command, expectedValues);

        filter = slices(cfs, 1990, null, false);
        command = SinglePartitionReadCommand.create(
                cfs.metadata(), FBUtilities.nowInSeconds(), ColumnFilter.all(cfs.metadata()), RowFilter.NONE, DataLimits.cqlLimits(3), Util.dk("0"), filter);
        assertRowsInResult(cfs, command, 1990, 1991, 1992);

        filter = slices(cfs, null, null, true);
        command = SinglePartitionReadCommand.create(
                cfs.metadata(), FBUtilities.nowInSeconds(), ColumnFilter.all(cfs.metadata()), RowFilter.NONE, DataLimits.cqlLimits(3), Util.dk("0"), filter);
        assertRowsInResult(cfs, command, 1999, 1998, 1997);

        filter = slices(cfs, null, 9000, true);
        command = SinglePartitionReadCommand.create(
                cfs.metadata(), FBUtilities.nowInSeconds(), ColumnFilter.all(cfs.metadata()), RowFilter.NONE, DataLimits.cqlLimits(3), Util.dk("0"), filter);
        assertRowsInResult(cfs, command, 1999, 1998, 1997);

        filter = slices(cfs, 9000, null, false);
        command = SinglePartitionReadCommand.create(
                cfs.metadata(), FBUtilities.nowInSeconds(), ColumnFilter.all(cfs.metadata()), RowFilter.NONE, DataLimits.cqlLimits(3), Util.dk("0"), filter);
        assertRowsInResult(cfs, command);
    }

    @Test
    public void shouldThrowOnMissingKeyspace()
    {
        String ksName = "MissingKeyspace";

        Assertions.assertThatThrownBy(() -> Keyspace.open(ksName, Schema.instance, false))
                  .isInstanceOf(AssertionError.class)
                  .hasMessage("Unknown keyspace " + ksName);
    }
}
