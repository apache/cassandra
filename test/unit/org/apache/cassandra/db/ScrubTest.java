package org.apache.cassandra.db;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.Scrubber;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.Util.cellname;
import static org.apache.cassandra.Util.column;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class ScrubTest extends SchemaLoader
{
    public String KEYSPACE = "Keyspace1";
    public String CF = "Standard1";
    public String CF3 = "Standard2";
    public String COUNTER_CF = "Counter1";
    private static Integer COMPRESSION_CHUNK_LENGTH = 4096;

    @BeforeClass
    public static void loadSchema() throws ConfigurationException
    {
        loadSchema(COMPRESSION_CHUNK_LENGTH);
    }

    @Test
    public void testScrubOneRow() throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.clearUnsafe();

        List<Row> rows;

        // insert data and verify we get it back w/ range query
        fillCF(cfs, 1);
        rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);
        assertEquals(1, rows.size());

        CompactionManager.instance.performScrub(cfs, false, true);

        // check data is still there
        rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);
        assertEquals(1, rows.size());
    }

    @Test
    public void testScrubCorruptedCounterRow() throws IOException, WriteTimeoutException
    {
        // When compression is enabled, for testing corrupted chunks we need enough partitions to cover
        // at least 3 chunks of size COMPRESSION_CHUNK_LENGTH
        int numPartitions = 1000;

        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF);
        cfs.clearUnsafe();

        fillCounterCF(cfs, numPartitions);

        List<Row> rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), numPartitions*10);
        assertEquals(numPartitions, rows.size());
        assertEquals(1, cfs.getSSTables().size());

        SSTableReader sstable = cfs.getSSTables().iterator().next();

        //make sure to override at most 1 chunk when compression is enabled
        overrideWithGarbage(sstable, ByteBufferUtil.bytes("0"), ByteBufferUtil.bytes("1"));

        // with skipCorrupted == false, the scrub is expected to fail
        Scrubber scrubber = new Scrubber(cfs, sstable, false, false, true);
        try
        {
            scrubber.scrub();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (IOError err) {}

        // with skipCorrupted == true, the corrupt row will be skipped
        Scrubber.ScrubResult scrubResult;
        scrubber = new Scrubber(cfs, sstable, true, false, true);
        scrubResult = scrubber.scrubWithResult();
        scrubber.close();

        assertNotNull(scrubResult);

        boolean compression = Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false"));
        if (compression)
        {
            assertEquals(0, scrubResult.emptyRows);
            assertEquals(numPartitions, scrubResult.badRows + scrubResult.goodRows);
            //because we only corrupted 1 chunk and we chose enough partitions to cover at least 3 chunks
            assertTrue(scrubResult.goodRows >= scrubResult.badRows * 2);
        }
        else
        {
            assertEquals(0, scrubResult.emptyRows);
            assertEquals(1, scrubResult.badRows);
            assertEquals(numPartitions-1, scrubResult.goodRows);
        }
        assertEquals(1, cfs.getSSTables().size());

        rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);
        assertEquals(scrubResult.goodRows, rows.size());
    }

    @Test
    public void testScrubCorruptedRowInSmallFile() throws IOException, WriteTimeoutException
    {
        // cannot test this with compression
        assumeTrue(!Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false")));

        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF);
        cfs.clearUnsafe();

        fillCounterCF(cfs, 2);

        List<Row> rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);
        assertEquals(2, rows.size());

        SSTableReader sstable = cfs.getSSTables().iterator().next();

        // overwrite one row with garbage
        overrideWithGarbage(sstable, ByteBufferUtil.bytes("0"), ByteBufferUtil.bytes("1"));

        // with skipCorrupted == false, the scrub is expected to fail
        Scrubber scrubber = new Scrubber(cfs, sstable, false, false, true);
        try
        {
            scrubber.scrub();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (IOError err) {}

        // with skipCorrupted == true, the corrupt row will be skipped
        scrubber = new Scrubber(cfs, sstable, true, false, true);
        scrubber.scrub();
        scrubber.close();
        assertEquals(1, cfs.getSSTables().size());

        // verify that we can read all of the rows, and there is now one less row
        rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);
        assertEquals(1, rows.size());
    }

    @Test
    public void testScrubOneRowWithCorruptedKey() throws IOException, ExecutionException, InterruptedException, ConfigurationException
    {
        // cannot test this with compression
        assumeTrue(!Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false")));

        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.clearUnsafe();

        List<Row> rows;

        // insert data and verify we get it back w/ range query
        fillCF(cfs, 4);
        rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);
        assertEquals(4, rows.size());

        SSTableReader sstable = cfs.getSSTables().iterator().next();
        overrideWithGarbage(sstable, 0, 2);

        CompactionManager.instance.performScrub(cfs, false, true);

        // check data is still there
        rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);
        assertEquals(4, rows.size());
    }

    @Test
    public void testScrubDeletedRow() throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF3);
        cfs.clearUnsafe();

        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, CF3);
        cf.delete(new DeletionInfo(0, 1)); // expired tombstone
        Mutation rm = new Mutation(KEYSPACE, ByteBufferUtil.bytes(1), cf);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();

        CompactionManager.instance.performScrub(cfs, false, true);
        assert cfs.getSSTables().isEmpty();
    }

    @Test
    public void testScrubMultiRow() throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.clearUnsafe();

        List<Row> rows;

        // insert data and verify we get it back w/ range query
        fillCF(cfs, 10);
        rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);
        assertEquals(10, rows.size());

        CompactionManager.instance.performScrub(cfs, false, true);

        // check data is still there
        rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);
        assertEquals(10, rows.size());
    }

    @Test
    public void testScrubOutOfOrder() throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        String columnFamily = "Standard3";
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(columnFamily);
        cfs.clearUnsafe();

        /*
         * Code used to generate an outOfOrder sstable. The test for out-of-order key in SSTableWriter must also be commented out.
         * The test also assumes an ordered partitioner.
         *
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cf.addColumn(new Cell(ByteBufferUtil.bytes("someName"), ByteBufferUtil.bytes("someValue"), 0L));

        SSTableWriter writer = new SSTableWriter(cfs.getTempSSTablePath(new File(System.getProperty("corrupt-sstable-root"))),
                                                 cfs.metadata.getIndexInterval(),
                                                 cfs.metadata,
                                                 cfs.partitioner,
                                                 SSTableMetadata.createCollector(BytesType.instance));
        writer.append(Util.dk("a"), cf);
        writer.append(Util.dk("b"), cf);
        writer.append(Util.dk("z"), cf);
        writer.append(Util.dk("c"), cf);
        writer.append(Util.dk("y"), cf);
        writer.append(Util.dk("d"), cf);
        writer.closeAndOpenReader();
        */

        String root = System.getProperty("corrupt-sstable-root");
        assert root != null;
        File rootDir = new File(root);
        assert rootDir.isDirectory();
        Descriptor desc = new Descriptor(new Descriptor.Version("jb"), rootDir, KEYSPACE, columnFamily, 1, Descriptor.Type.FINAL);
        CFMetaData metadata = Schema.instance.getCFMetaData(desc.ksname, desc.cfname);

        try
        {
            SSTableReader.open(desc, metadata);
            fail("SSTR validation should have caught the out-of-order rows");
        }
        catch (IllegalStateException ise) { /* this is expected */ }

        // open without validation for scrubbing
        Set<Component> components = new HashSet<>();
        components.add(Component.COMPRESSION_INFO);
        components.add(Component.DATA);
        components.add(Component.PRIMARY_INDEX);
        components.add(Component.FILTER);
        components.add(Component.STATS);
        components.add(Component.SUMMARY);
        components.add(Component.TOC);
        SSTableReader sstable = SSTableReader.openNoValidation(desc, components, metadata);

        Scrubber scrubber = new Scrubber(cfs, sstable, false, true, true);
        scrubber.scrub();

        cfs.loadNewSSTables();
        List<Row> rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);
        assert isRowOrdered(rows) : "Scrub failed: " + rows;
        assert rows.size() == 6 : "Got " + rows.size();
    }

    private void overrideWithGarbage(SSTableReader sstable, ByteBuffer key1, ByteBuffer key2) throws IOException
    {
        boolean compression = Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false"));
        long startPosition, endPosition;

        if (compression)
        { // overwrite with garbage the compression chunks from key1 to key2
            CompressionMetadata compData = CompressionMetadata.create(sstable.getFilename());

            CompressionMetadata.Chunk chunk1 = compData.chunkFor(
                    sstable.getPosition(RowPosition.ForKey.get(key1, sstable.partitioner), SSTableReader.Operator.EQ).position);
            CompressionMetadata.Chunk chunk2 = compData.chunkFor(
                    sstable.getPosition(RowPosition.ForKey.get(key2, sstable.partitioner), SSTableReader.Operator.EQ).position);

            startPosition = Math.min(chunk1.offset, chunk2.offset);
            endPosition = Math.max(chunk1.offset + chunk1.length, chunk2.offset + chunk2.length);
        }
        else
        { // overwrite with garbage from key1 to key2
            long row0Start = sstable.getPosition(RowPosition.ForKey.get(key1, sstable.partitioner), SSTableReader.Operator.EQ).position;
            long row1Start = sstable.getPosition(RowPosition.ForKey.get(key2, sstable.partitioner), SSTableReader.Operator.EQ).position;
            startPosition = Math.min(row0Start, row1Start);
            endPosition = Math.max(row0Start, row1Start);
        }

        overrideWithGarbage(sstable, startPosition, endPosition);
    }

    private void overrideWithGarbage(SSTableReader sstable, long startPosition, long endPosition) throws IOException
    {
        RandomAccessFile file = new RandomAccessFile(sstable.getFilename(), "rw");
        file.seek(startPosition);
        file.writeBytes(StringUtils.repeat('z', (int) (endPosition - startPosition)));
        file.close();
    }

    private static boolean isRowOrdered(List<Row> rows)
    {
        DecoratedKey prev = null;
        for (Row row : rows)
        {
            if (prev != null && prev.compareTo(row.key) > 0)
                return false;
            prev = row.key;
        }
        return true;
    }

    protected void fillCF(ColumnFamilyStore cfs, int rowsPerSSTable)
    {
        for (int i = 0; i < rowsPerSSTable; i++)
        {
            String key = String.valueOf(i);
            // create a row and update the birthdate value, test that the index query fetches the new version
            ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, CF);
            cf.addColumn(column("c1", "1", 1L));
            cf.addColumn(column("c2", "2", 1L));
            Mutation rm = new Mutation(KEYSPACE, ByteBufferUtil.bytes(key), cf);
            rm.applyUnsafe();
        }

        cfs.forceBlockingFlush();
    }

    protected void fillCounterCF(ColumnFamilyStore cfs, int rowsPerSSTable) throws WriteTimeoutException
    {
        for (int i = 0; i < rowsPerSSTable; i++)
        {
            String key = String.valueOf(i);
            ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, COUNTER_CF);
            Mutation rm = new Mutation(KEYSPACE, ByteBufferUtil.bytes(key), cf);
            rm.addCounter(COUNTER_CF, cellname("Column1"), 100);
            CounterMutation cm = new CounterMutation(rm, ConsistencyLevel.ONE);
            cm.apply();
        }

        cfs.forceBlockingFlush();
    }

    @Test
    public void testScrubColumnValidation() throws InterruptedException, RequestExecutionException, ExecutionException
    {
        QueryProcessor.process("CREATE TABLE \"Keyspace1\".test_compact_static_columns (a bigint, b timeuuid, c boolean static, d text, PRIMARY KEY (a, b))", ConsistencyLevel.ONE);

        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("test_compact_static_columns");

        QueryProcessor.executeInternal("INSERT INTO \"Keyspace1\".test_compact_static_columns (a, b, c, d) VALUES (123, c3db07e8-b602-11e3-bc6b-e0b9a54a6d93, true, 'foobar')");
        cfs.forceBlockingFlush();
        CompactionManager.instance.performScrub(cfs, false, true);

        QueryProcessor.process("CREATE TABLE \"Keyspace1\".test_scrub_validation (a text primary key, b int)", ConsistencyLevel.ONE);
        ColumnFamilyStore cfs2 = keyspace.getColumnFamilyStore("test_scrub_validation");
        Mutation mutation = new Mutation("Keyspace1", UTF8Type.instance.decompose("key"));
        CellNameType ct = cfs2.getComparator();
        mutation.add("test_scrub_validation", ct.makeCellName("b"), LongType.instance.decompose(1L), System.currentTimeMillis());
        mutation.apply();
        cfs2.forceBlockingFlush();

        CompactionManager.instance.performScrub(cfs2, false, false);
    }

    /**
     * Tests CASSANDRA-6892 (key aliases being used improperly for validation)
     */
    @Test
    public void testColumnNameEqualToDefaultKeyAlias() throws ExecutionException, InterruptedException
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("UUIDKeys");

        ColumnFamily cf = ArrayBackedSortedColumns.factory.create("Keyspace1", "UUIDKeys");
        cf.addColumn(column(CFMetaData.DEFAULT_KEY_ALIAS, "not a uuid", 1L));
        Mutation mutation = new Mutation("Keyspace1", ByteBufferUtil.bytes(UUIDGen.getTimeUUID()), cf);
        mutation.applyUnsafe();
        cfs.forceBlockingFlush();
        CompactionManager.instance.performScrub(cfs, false, true);

        assertEquals(1, cfs.getSSTables().size());
    }

    /**
     * For CASSANDRA-6892 too, check that for a compact table with one cluster column, we can insert whatever
     * we want as value for the clustering column, including something that would conflict with a CQL column definition.
     */
    @Test
    public void testValidationCompactStorage() throws Exception
    {
        QueryProcessor.process("CREATE TABLE \"Keyspace1\".test_compact_dynamic_columns (a int, b text, c text, PRIMARY KEY (a, b)) WITH COMPACT STORAGE", ConsistencyLevel.ONE);

        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("test_compact_dynamic_columns");

        QueryProcessor.executeInternal("INSERT INTO \"Keyspace1\".test_compact_dynamic_columns (a, b, c) VALUES (0, 'a', 'foo')");
        QueryProcessor.executeInternal("INSERT INTO \"Keyspace1\".test_compact_dynamic_columns (a, b, c) VALUES (0, 'b', 'bar')");
        QueryProcessor.executeInternal("INSERT INTO \"Keyspace1\".test_compact_dynamic_columns (a, b, c) VALUES (0, 'c', 'boo')");
        cfs.forceBlockingFlush();
        CompactionManager.instance.performScrub(cfs, true, true);

        // Scrub is silent, but it will remove broken records. So reading everything back to make sure nothing to "scrubbed away"
        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM \"Keyspace1\".test_compact_dynamic_columns");
        assertEquals(3, rs.size());

        Iterator<UntypedResultSet.Row> iter = rs.iterator();
        assertEquals("foo", iter.next().getString("c"));
        assertEquals("bar", iter.next().getString("c"));
        assertEquals("boo", iter.next().getString("c"));
    }
}
