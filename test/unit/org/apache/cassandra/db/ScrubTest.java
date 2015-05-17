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
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.Scrubber;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.Util;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

import static org.apache.cassandra.Util.cellname;
import static org.apache.cassandra.Util.column;

@RunWith(OrderedJUnit4ClassRunner.class)
public class ScrubTest
{
    public static final String KEYSPACE = "Keyspace1";
    public static final String CF = "Standard1";
    public static final String CF2 = "Standard2";
    public static final String CF3 = "Standard3";
    public static final String COUNTER_CF = "Counter1";
    public static final String CF_UUID = "UUIDKeys";
    public static final String CF_INDEX1 = "Indexed1";
    public static final String CF_INDEX2 = "Indexed2";

    public static final String COL_KEYS_INDEX = "birthdate";
    public static final String COL_COMPOSITES_INDEX = "col1";
    public static final String COL_NON_INDEX = "notanindexcol";

    public static final Integer COMPRESSION_CHUNK_LENGTH = 4096;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF2),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF3),
                                    SchemaLoader.standardCFMD(KEYSPACE, COUNTER_CF)
                                                .defaultValidator(CounterColumnType.instance)
                                                .compressionParameters(SchemaLoader.getCompressionParameters(COMPRESSION_CHUNK_LENGTH)),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_UUID).keyValidator(UUIDType.instance),
                                    SchemaLoader.indexCFMD(KEYSPACE, CF_INDEX1, true),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE, CF_INDEX2, true));
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

        CompactionManager.instance.performScrub(cfs, false);

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
        try(Scrubber scrubber = new Scrubber(cfs, sstable, false, false))
        {
            scrubber.scrub();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (IOError err) {}

        // with skipCorrupted == true, the corrupt rows will be skipped
        Scrubber.ScrubResult scrubResult;
        try(Scrubber scrubber = new Scrubber(cfs, sstable, true, false))
        {
            scrubResult = scrubber.scrubWithResult();
        }

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
        Scrubber scrubber = new Scrubber(cfs, sstable, false, false);
        try
        {
            scrubber.scrub();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (IOError err) {}

        // with skipCorrupted == true, the corrupt row will be skipped
        scrubber = new Scrubber(cfs, sstable, true, false);
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

        CompactionManager.instance.performScrub(cfs, false);

        // check data is still there
        rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);
        assertEquals(4, rows.size());
    }

    @Test
    public void testScrubCorruptedCounterRowNoEarlyOpen() throws IOException, WriteTimeoutException
    {
        long oldOpenVal = SSTableRewriter.getOpenInterval();
        try
        {
            SSTableRewriter.overrideOpenInterval(Long.MAX_VALUE);
            testScrubCorruptedCounterRow();
        }
        finally
        {
            SSTableRewriter.overrideOpenInterval(oldOpenVal);
        }
    }

    @Test
    public void testScrubDeletedRow() throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF2);
        cfs.clearUnsafe();

        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, CF2);
        cf.delete(new DeletionInfo(0, 1)); // expired tombstone
        Mutation rm = new Mutation(KEYSPACE, ByteBufferUtil.bytes(1), cf);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();

        CompactionManager.instance.performScrub(cfs, false);
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

        CompactionManager.instance.performScrub(cfs, false);

        // check data is still there
        rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);
        assertEquals(10, rows.size());
    }

    @Test
    public void testScrubOutOfOrder() throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        String columnFamily = CF3;
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(columnFamily);
        cfs.clearUnsafe();

        /*
         * Code used to generate an outOfOrder sstable. The test for out-of-order key in SSTableWriter must also be commented out.
         * The test also assumes an ordered partitioner.
         *
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cf.addColumn(new BufferCell(ByteBufferUtil.bytes("someName"), ByteBufferUtil.bytes("someValue"), 0L));

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
        writer.finish();
        */

        String root = System.getProperty("corrupt-sstable-root");
        assert root != null;

        File rootDir = new File(root);
        assert rootDir.isDirectory();
        Descriptor desc = new Descriptor("jb", rootDir, KEYSPACE, columnFamily, 1, Descriptor.Type.FINAL, SSTableFormat.Type.LEGACY);
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
        SSTableReader sstable = SSTableReader.openNoValidation(desc, components, cfs);

        try(Scrubber scrubber = new Scrubber(cfs, sstable, false, true))
        {
            scrubber.scrub();
        }
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
            ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, CF);
            cf.addColumn(column("c1", "1", 1L));
            cf.addColumn(column("c2", "2", 1L));
            Mutation rm = new Mutation(KEYSPACE, ByteBufferUtil.bytes(key), cf);
            rm.applyUnsafe();
        }

        cfs.forceBlockingFlush();
    }

    private void fillIndexCF(ColumnFamilyStore cfs, boolean composite, long ... values)
    {
        assertTrue(values.length % 2 == 0);
        for (int i = 0; i < values.length; i +=2)
        {
            String key = String.valueOf(i);
            ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, cfs.name);
            if (composite)
            {
                String clusterKey = "c" + key;
                cf.addColumn(column(clusterKey, COL_COMPOSITES_INDEX, values[i], 1L));
                cf.addColumn(column(clusterKey, COL_NON_INDEX, values[i + 1], 1L));
            }
            else
            {
                cf.addColumn(column(COL_KEYS_INDEX, values[i], 1L));
                cf.addColumn(column(COL_NON_INDEX, values[i + 1], 1L));
            }
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
        QueryProcessor.process(String.format("CREATE TABLE \"%s\".test_compact_static_columns (a bigint, b timeuuid, c boolean static, d text, PRIMARY KEY (a, b))", KEYSPACE), ConsistencyLevel.ONE);

        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("test_compact_static_columns");

        QueryProcessor.executeInternal(String.format("INSERT INTO \"%s\".test_compact_static_columns (a, b, c, d) VALUES (123, c3db07e8-b602-11e3-bc6b-e0b9a54a6d93, true, 'foobar')", KEYSPACE));
        cfs.forceBlockingFlush();
        CompactionManager.instance.performScrub(cfs, false);
    }

    /**
     * Tests CASSANDRA-6892 (key aliases being used improperly for validation)
     */
    @Test
    public void testColumnNameEqualToDefaultKeyAlias() throws ExecutionException, InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_UUID);

        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE, CF_UUID);
        cf.addColumn(column(CFMetaData.DEFAULT_KEY_ALIAS, "not a uuid", 1L));
        Mutation mutation = new Mutation(KEYSPACE, ByteBufferUtil.bytes(UUIDGen.getTimeUUID()), cf);
        mutation.applyUnsafe();
        cfs.forceBlockingFlush();
        CompactionManager.instance.performScrub(cfs, false);

        assertEquals(1, cfs.getSSTables().size());
    }

    /**
     * For CASSANDRA-6892 too, check that for a compact table with one cluster column, we can insert whatever
     * we want as value for the clustering column, including something that would conflict with a CQL column definition.
     */
    @Test
    public void testValidationCompactStorage() throws Exception
    {
        QueryProcessor.process(String.format("CREATE TABLE \"%s\".test_compact_dynamic_columns (a int, b text, c text, PRIMARY KEY (a, b)) WITH COMPACT STORAGE", KEYSPACE), ConsistencyLevel.ONE);

        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("test_compact_dynamic_columns");

        QueryProcessor.executeInternal(String.format("INSERT INTO \"%s\".test_compact_dynamic_columns (a, b, c) VALUES (0, 'a', 'foo')", KEYSPACE));
        QueryProcessor.executeInternal(String.format("INSERT INTO \"%s\".test_compact_dynamic_columns (a, b, c) VALUES (0, 'b', 'bar')", KEYSPACE));
        QueryProcessor.executeInternal(String.format("INSERT INTO \"%s\".test_compact_dynamic_columns (a, b, c) VALUES (0, 'c', 'boo')", KEYSPACE));
        cfs.forceBlockingFlush();
        CompactionManager.instance.performScrub(cfs, true);

        // Scrub is silent, but it will remove broken records. So reading everything back to make sure nothing to "scrubbed away"
        UntypedResultSet rs = QueryProcessor.executeInternal(String.format("SELECT * FROM \"%s\".test_compact_dynamic_columns", KEYSPACE));
        assertEquals(3, rs.size());

        Iterator<UntypedResultSet.Row> iter = rs.iterator();
        assertEquals("foo", iter.next().getString("c"));
        assertEquals("bar", iter.next().getString("c"));
        assertEquals("boo", iter.next().getString("c"));
    }

    @Test /* CASSANDRA-5174 */
    public void testScrubKeysIndex_preserveOrder() throws IOException, ExecutionException, InterruptedException
    {
        //If the partitioner preserves the order then SecondaryIndex uses BytesType comparator,
        // otherwise it uses LocalByPartitionerType
        setKeyComparator(BytesType.instance);
        testScrubIndex(CF_INDEX1, COL_KEYS_INDEX, false, true);
    }

    @Test /* CASSANDRA-5174 */
    public void testScrubCompositeIndex_preserveOrder() throws IOException, ExecutionException, InterruptedException
    {
        setKeyComparator(BytesType.instance);
        testScrubIndex(CF_INDEX2, COL_COMPOSITES_INDEX, true, true);
    }

    @Test /* CASSANDRA-5174 */
    public void testScrubKeysIndex() throws IOException, ExecutionException, InterruptedException
    {
        setKeyComparator(new LocalByPartionerType(StorageService.getPartitioner()));
        testScrubIndex(CF_INDEX1, COL_KEYS_INDEX, false, true);
    }

    @Test /* CASSANDRA-5174 */
    public void testScrubCompositeIndex() throws IOException, ExecutionException, InterruptedException
    {
        setKeyComparator(new LocalByPartionerType(StorageService.getPartitioner()));
        testScrubIndex(CF_INDEX2, COL_COMPOSITES_INDEX, true, true);
    }

    @Test /* CASSANDRA-5174 */
    public void testFailScrubKeysIndex() throws IOException, ExecutionException, InterruptedException
    {
        testScrubIndex(CF_INDEX1, COL_KEYS_INDEX, false, false);
    }

    @Test /* CASSANDRA-5174 */
    public void testFailScrubCompositeIndex() throws IOException, ExecutionException, InterruptedException
    {
        testScrubIndex(CF_INDEX2, COL_COMPOSITES_INDEX, true, false);
    }

    @Test /* CASSANDRA-5174 */
    public void testScrubTwice() throws IOException, ExecutionException, InterruptedException
    {
        testScrubIndex(CF_INDEX1, COL_KEYS_INDEX, false, true, true);
    }

    /** The SecondaryIndex class is used for custom indexes so to avoid
     * making a public final field into a private field with getters
     * and setters, we resort to this hack in order to test it properly
     * since it can have two values which influence the scrubbing behavior.
     * @param comparator - the key comparator we want to test
     */
    private void setKeyComparator(AbstractType<?> comparator)
    {
        try
        {
            Field keyComparator = SecondaryIndex.class.getDeclaredField("keyComparator");
            keyComparator.setAccessible(true);
            int modifiers = keyComparator.getModifiers();
            Field modifierField = keyComparator.getClass().getDeclaredField("modifiers");
            modifiers = modifiers & ~Modifier.FINAL;
            modifierField.setAccessible(true);
            modifierField.setInt(keyComparator, modifiers);

            keyComparator.set(null, comparator);
        }
        catch (Exception ex)
        {
            fail("Failed to change key comparator in secondary index : " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private void testScrubIndex(String cfName, String colName, boolean composite, boolean ... scrubs)
            throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        int numRows = 1000;
        long[] colValues = new long [numRows * 2]; // each row has two columns
        for (int i = 0; i < colValues.length; i+=2)
        {
            colValues[i] = (i % 4 == 0 ? 1L : 2L); // index column
            colValues[i+1] = 3L; //other column
        }
        fillIndexCF(cfs, composite, colValues);

        // check index
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(colName), Operator.EQ, ByteBufferUtil.bytes(1L));
        List<Row> rows = cfs.search(Util.range("", ""), Arrays.asList(expr), new IdentityQueryFilter(), numRows);
        assertNotNull(rows);
        assertEquals(numRows / 2, rows.size());

        // scrub index
        Set<ColumnFamilyStore> indexCfss = cfs.indexManager.getIndexesBackedByCfs();
        assertTrue(indexCfss.size() == 1);
        for(ColumnFamilyStore indexCfs : indexCfss)
        {
            for (int i = 0; i < scrubs.length; i++)
            {
                boolean failure = !scrubs[i];
                if (failure)
                { //make sure the next scrub fails
                    overrideWithGarbage(indexCfs.getSSTables().iterator().next(), ByteBufferUtil.bytes(1L), ByteBufferUtil.bytes(2L));
                }
                CompactionManager.AllSSTableOpStatus result = indexCfs.scrub(false, false, true);
                assertEquals(failure ?
                             CompactionManager.AllSSTableOpStatus.ABORTED :
                             CompactionManager.AllSSTableOpStatus.SUCCESSFUL,
                                result);
            }
        }


        // check index is still working
        rows = cfs.search(Util.range("", ""), Arrays.asList(expr), new IdentityQueryFilter(), numRows);
        assertNotNull(rows);
        assertEquals(numRows / 2, rows.size());
    }
}
