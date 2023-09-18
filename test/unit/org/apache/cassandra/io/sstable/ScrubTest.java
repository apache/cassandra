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

import java.io.IOError;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.core.util.ThrowingBiConsumer;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.Util;
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.CompressionInfoComponent;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat.Components;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Throwables;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.SchemaLoader.counterCFMD;
import static org.apache.cassandra.SchemaLoader.createKeyspace;
import static org.apache.cassandra.SchemaLoader.getCompressionParameters;
import static org.apache.cassandra.SchemaLoader.loadSchema;
import static org.apache.cassandra.SchemaLoader.standardCFMD;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_INVALID_LEGACY_SSTABLE_ROOT;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

@RunWith(BMUnitRunner.class)
public class ScrubTest
{
    private final static Logger logger = LoggerFactory.getLogger(ScrubTest.class);

    public static final String CF = "Standard1";
    public static final String COUNTER_CF = "Counter1";
    public static final String CF_UUID = "UUIDKeys";
    public static final String CF_INDEX1 = "Indexed1";
    public static final String CF_INDEX2 = "Indexed2";
    public static final String CF_INDEX1_BYTEORDERED = "Indexed1_ordered";
    public static final String CF_INDEX2_BYTEORDERED = "Indexed2_ordered";
    public static final String COL_INDEX = "birthdate";
    public static final String COL_NON_INDEX = "notbirthdate";

    public static final Integer COMPRESSION_CHUNK_LENGTH = 4096;

    private static final AtomicInteger seq = new AtomicInteger();

    static WithProperties properties;

    String ksName;
    Keyspace keyspace;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setFileCacheEnabled(false);
        loadSchema();
        assertNull(ChunkCache.instance);
    }

    @Before
    public void setup()
    {
        ksName = "scrub_test_" + seq.incrementAndGet();
        createKeyspace(ksName,
                       KeyspaceParams.simple(1),
                       standardCFMD(ksName, CF),
                       counterCFMD(ksName, COUNTER_CF).compression(getCompressionParameters(COMPRESSION_CHUNK_LENGTH)),
                       standardCFMD(ksName, CF_UUID, 0, UUIDType.instance),
                       SchemaLoader.keysIndexCFMD(ksName, CF_INDEX1, true),
                       SchemaLoader.compositeIndexCFMD(ksName, CF_INDEX2, true),
                       SchemaLoader.keysIndexCFMD(ksName, CF_INDEX1_BYTEORDERED, true).partitioner(ByteOrderedPartitioner.instance),
                       SchemaLoader.compositeIndexCFMD(ksName, CF_INDEX2_BYTEORDERED, true).partitioner(ByteOrderedPartitioner.instance));
        keyspace = Keyspace.open(ksName);

        CompactionManager.instance.disableAutoCompaction();
        properties = new WithProperties().set(TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST, true);
    }

    @AfterClass
    public static void clearClassEnv()
    {
        properties.close();
    }

    @Test
    public void testScrubOnePartition()
    {
        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        // insert data and verify we get it back w/ range query
        fillCF(cfs, 1);
        assertOrderedAll(cfs, 1);

        performScrub(cfs, false, true, false, 2);

        // check data is still there
        assertOrderedAll(cfs, 1);
    }

    @Test
    public void testScrubLastBrokenPartition() throws IOException
    {
        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(ksName, CF);

        // insert data and verify we get it back w/ range query
        fillCF(cfs, 1);
        assertOrderedAll(cfs, 1);

        Set<SSTableReader> liveSSTables = cfs.getLiveSSTables();
        assertThat(liveSSTables).hasSize(1);
        String fileName = liveSSTables.iterator().next().getFilename();
        Files.write(Paths.get(fileName), new byte[10], StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        performScrub(cfs, true, true, false, 2);

        // check data is still there
        assertOrderedAll(cfs, 0);
    }

    @Test
    public void testScrubCorruptedCounterPartition() throws IOException, WriteTimeoutException
    {
        // When compression is enabled, for testing corrupted chunks we need enough partitions to cover
        // at least 3 chunks of size COMPRESSION_CHUNK_LENGTH
        int numPartitions = 1000;

        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF);
        cfs.truncateBlocking();
        fillCounterCF(cfs, numPartitions);

        assertOrderedAll(cfs, numPartitions);

        assertEquals(1, cfs.getLiveSSTables().size());

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        //make sure to override at most 1 chunk when compression is enabled
        //use 0x00 instead of the usual 0x7A because if by any chance it's able to iterate over the corrupt
        //section, then we get many out-of-order errors, which we don't want
        overrideWithGarbage(sstable, ByteBufferUtil.bytes("0"), ByteBufferUtil.bytes("1"), (byte) 0x00);

        // with skipCorrupted == false, the scrub is expected to fail
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(Collections.singletonList(sstable), OperationType.SCRUB);
             IScrubber scrubber = sstable.descriptor.getFormat().getScrubber(cfs, txn, new OutputHandler.LogOutput(), new IScrubber.Options.Builder().checkData().build()))
        {
            scrubber.scrub();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (IOError err)
        {
            Throwables.assertAnyCause(err, CorruptSSTableException.class);
        }

        // with skipCorrupted == true, the corrupt rows will be skipped
        IScrubber.ScrubResult scrubResult;
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(Collections.singletonList(sstable), OperationType.SCRUB);
             IScrubber scrubber = sstable.descriptor.getFormat().getScrubber(cfs, txn, new OutputHandler.LogOutput(), new IScrubber.Options.Builder().skipCorrupted().checkData().build()))
        {
            scrubResult = scrubber.scrubWithResult();
        }

        assertNotNull(scrubResult);

        boolean compression = sstable.compression;
        assertEquals(0, scrubResult.emptyPartitions);
        if (compression)
        {
            assertEquals(numPartitions, scrubResult.badPartitions + scrubResult.goodPartitions);
            //because we only corrupted 1 chunk, and we chose enough partitions to cover at least 3 chunks
            assertTrue(scrubResult.goodPartitions >= scrubResult.badPartitions * 2);
        }
        else
        {
            assertEquals(1, scrubResult.badPartitions);
            assertEquals(numPartitions - 1, scrubResult.goodPartitions);
        }
        assertEquals(1, cfs.getLiveSSTables().size());

        assertOrderedAll(cfs, scrubResult.goodPartitions);
    }

    private List<File> sstableIndexPaths(SSTableReader reader)
    {
        if (BigFormat.is(reader.descriptor.getFormat()))
            return Arrays.asList(reader.descriptor.fileFor(BigFormat.Components.PRIMARY_INDEX));
        if (BtiFormat.is(reader.descriptor.getFormat()))
            return Arrays.asList(reader.descriptor.fileFor(BtiFormat.Components.PARTITION_INDEX),
                                 reader.descriptor.fileFor(BtiFormat.Components.ROW_INDEX));
        else
            throw Util.testMustBeImplementedForSSTableFormat();
    }

    @Test
    public void testScrubCorruptedRowInSmallFile() throws Throwable
    {
        // overwrite one row with garbage
        testCorruptionInSmallFile((sstable, keys) ->
                                  overrideWithGarbage(sstable,
                                                      ByteBufferUtil.bytes(keys[0]),
                                                      ByteBufferUtil.bytes(keys[1]),
                                                      (byte) 0x7A),
                                  false,
                                  4);
    }


    @Test
    public void testScrubCorruptedIndex() throws Throwable
    {
        // overwrite a part of the index with garbage
        testCorruptionInSmallFile((sstable, keys) ->
                                  overrideWithGarbage(sstableIndexPaths(sstable).get(0),
                                                      5,
                                                      6,
                                                      (byte) 0x7A),
                                  true,
                                  5);
    }

    @Test
    public void testScrubCorruptedIndexOnOpen() throws Throwable
    {
        // overwrite the whole index with garbage
        testCorruptionInSmallFile((sstable, keys) ->
                                  overrideWithGarbage(sstableIndexPaths(sstable).get(0),
                                                      0,
                                                      60,
                                                      (byte) 0x7A),
                                  true,
                                  5);
    }

    @Test
    public void testScrubCorruptedRowCorruptedIndex() throws Throwable
    {
        // overwrite one row, and the index with garbage
        testCorruptionInSmallFile((sstable, keys) ->
                                  {
                                      overrideWithGarbage(sstable,
                                                          ByteBufferUtil.bytes(keys[2]),
                                                          ByteBufferUtil.bytes(keys[3]),
                                                          (byte) 0x7A);
                                      overrideWithGarbage(sstableIndexPaths(sstable).get(0),
                                                          5,
                                                          6,
                                                          (byte) 0x7A);
                                  },
                                  false,
                                  2);   // corrupt after the second partition, no way to resync
    }

    public void testCorruptionInSmallFile(ThrowingBiConsumer<SSTableReader, String[], IOException> corrupt, boolean isFullyRecoverable, int expectedPartitions) throws IOException, WriteTimeoutException
    {
        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF);
        cfs.clearUnsafe();

        String[] keys = fillCounterCF(cfs, 5);

        assertOrderedAll(cfs, 5);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // cannot test this with compression
        assumeFalse(sstable.metadata().params.compression.isEnabled());

        // overwrite one row with garbage
        corrupt.accept(sstable, keys);

        // with skipCorrupted == false, the scrub is expected to fail
        if (!isFullyRecoverable)
        {
            try (LifecycleTransaction txn = cfs.getTracker().tryModify(Collections.singletonList(sstable), OperationType.SCRUB);
                 IScrubber scrubber = sstable.descriptor.getFormat().getScrubber(cfs, txn, new OutputHandler.LogOutput(), new IScrubber.Options.Builder().checkData().build()))
            {
                // with skipCorrupted == true, the corrupt row will be skipped
                scrubber.scrub();
                fail("Expected a CorruptSSTableException to be thrown");
            }
            catch (IOError expected)
            {
            }
        }

        try (LifecycleTransaction txn = cfs.getTracker().tryModify(Collections.singletonList(sstable), OperationType.SCRUB);
             IScrubber scrubber = sstable.descriptor.getFormat().getScrubber(cfs, txn, new OutputHandler.LogOutput(), new IScrubber.Options.Builder().checkData().skipCorrupted().build()))
        {
            // with skipCorrupted == true, the corrupt row will be skipped
            scrubber.scrub();
        }

        assertEquals(1, cfs.getLiveSSTables().size());
        // verify that we can read all the rows, and there is now the expected number of rows
        assertOrderedAll(cfs, expectedPartitions);
    }

    @Test
    public void testScrubOneRowWithCorruptedKey() throws IOException, ConfigurationException
    {
        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        // insert data and verify we get it back w/ range query
        fillCF(cfs, 4);
        assertOrderedAll(cfs, 4);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        // cannot test this with compression
        assumeFalse(sstable.metadata().params.compression.isEnabled());

        overrideWithGarbage(sstable, 0, 2, (byte) 0x7A);

        performScrub(cfs, false, true, false, 2);

        // check data is still there
        if (BigFormat.is(sstable.descriptor.getFormat()))
            assertOrderedAll(cfs, 4);
        else if (BtiFormat.is(sstable.descriptor.getFormat()))
            // For Trie format we won't be able to recover the damaged partition key (partion index doesn't store the whole key)
            assertOrderedAll(cfs, 3);
        else
            throw Util.testMustBeImplementedForSSTableFormat();
    }

    @Test
    public void testScrubCorruptedCounterRowNoEarlyOpen() throws IOException, WriteTimeoutException
    {
        boolean oldDisabledVal = SSTableRewriter.disableEarlyOpeningForTests;
        try
        {
            SSTableRewriter.disableEarlyOpeningForTests = true;
            testScrubCorruptedCounterPartition();
        }
        finally
        {
            SSTableRewriter.disableEarlyOpeningForTests = oldDisabledVal;
        }
    }

    @Test
    public void testScrubMultiRow()
    {
        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        // insert data and verify we get it back w/ range query
        fillCF(cfs, 10);
        assertOrderedAll(cfs, 10);

        performScrub(cfs, false, true, false, 2);

        // check data is still there
        assertOrderedAll(cfs, 10);
    }

    @Test
    public void testScrubNoIndex() throws ConfigurationException
    {
        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        // insert data and verify we get it back w/ range query
        fillCF(cfs, 10);
        assertOrderedAll(cfs, 10);

        for (SSTableReader sstable : cfs.getLiveSSTables())
            sstableIndexPaths(sstable).forEach(File::tryDelete);

        performScrub(cfs, false, true, false, 2);

        // check data is still there
        assertOrderedAll(cfs, 10);
    }

    @Test
    @BMRule(name = "skip partition order verification", targetClass = "SortedTableWriter", targetMethod = "verifyPartition", action = "return true")
    public void testScrubOutOfOrder()
    {
        // Run only for Big Table format because Big Table Format does not complain if partitions are given in invalid
        // order. Legacy SSTables with out-of-order partitions exist in production systems and must be corrected
        // by scrubbing. The trie index format does not permit such partitions.

        Assume.assumeTrue(BigFormat.isSelected());

        // This test assumes ByteOrderPartitioner to create out-of-order SSTable
        IPartitioner oldPartitioner = DatabaseDescriptor.getPartitioner();
        DatabaseDescriptor.setPartitionerUnsafe(new ByteOrderedPartitioner());

        // Create out-of-order SSTable
        File tempDir = FileUtils.createTempFile("ScrubTest.testScrubOutOfOrder", "").parent();
        // create ks/cf directory
        File tempDataDir = new File(tempDir, String.join(File.pathSeparator(), ksName, CF));
        assertTrue(tempDataDir.tryCreateDirectories());
        try
        {
            CompactionManager.instance.disableAutoCompaction();
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

            List<String> keys = Arrays.asList("t", "a", "b", "z", "c", "y", "d");
            Descriptor desc = cfs.newSSTableDescriptor(tempDataDir);

            try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);
                 SSTableTxnWriter writer = new SSTableTxnWriter(txn, createTestWriter(desc, keys.size(), cfs, txn)))
            {
                for (String k : keys)
                {
                    PartitionUpdate update = UpdateBuilder.create(cfs.metadata(), Util.dk(k))
                                                          .newRow("someName").add("val", "someValue")
                                                          .build();

                    writer.append(update.unfilteredIterator());
                }
                writer.finish(false);
            }

            try
            {
                SSTableReader.open(cfs, desc, cfs.metadata);
                fail("SSTR validation should have caught the out-of-order rows");
            }
            catch (CorruptSSTableException ise)
            { /* this is expected */ }

            // open without validation for scrubbing
            Set<Component> components = new HashSet<>();
            if (desc.fileFor(Components.COMPRESSION_INFO).exists())
                components.add(Components.COMPRESSION_INFO);
            components.add(Components.DATA);
            components.add(Components.PRIMARY_INDEX);
            components.add(Components.FILTER);
            components.add(Components.STATS);
            components.add(Components.SUMMARY);
            components.add(Components.TOC);

            SSTableReader sstable = SSTableReader.openNoValidation(desc, components, cfs);
//            if (sstable.last.compareTo(sstable.first) < 0)
//                sstable.last = sstable.first;

            try (LifecycleTransaction scrubTxn = LifecycleTransaction.offline(OperationType.SCRUB, sstable);
                 IScrubber scrubber = sstable.descriptor.getFormat().getScrubber(cfs, scrubTxn, new OutputHandler.LogOutput(), new IScrubber.Options.Builder().checkData().build()))
            {
                scrubber.scrub();
            }
            LifecycleTransaction.waitForDeletions();
            cfs.loadNewSSTables();
            assertOrderedAll(cfs, 7);
        }
        finally
        {
            FileUtils.deleteRecursive(tempDataDir);
            // reset partitioner
            DatabaseDescriptor.setPartitionerUnsafe(oldPartitioner);
        }
    }

    public static void overrideWithGarbage(SSTableReader sstable, ByteBuffer key1, ByteBuffer key2) throws IOException
    {
        overrideWithGarbage(sstable, key1, key2, (byte) 'z');
    }

    public static void overrideWithGarbage(SSTableReader sstable, ByteBuffer key1, ByteBuffer key2, byte junk) throws IOException
    {
        boolean compression = sstable.metadata().params.compression.isEnabled();
        long startPosition, endPosition;

        if (compression)
        { // overwrite with garbage the compression chunks from key1 to key2
            CompressionMetadata compData = CompressionInfoComponent.load(sstable.descriptor);

            CompressionMetadata.Chunk chunk1 = compData.chunkFor(
            sstable.getPosition(PartitionPosition.ForKey.get(key1, sstable.getPartitioner()), SSTableReader.Operator.EQ));
            CompressionMetadata.Chunk chunk2 = compData.chunkFor(
            sstable.getPosition(PartitionPosition.ForKey.get(key2, sstable.getPartitioner()), SSTableReader.Operator.EQ));

            startPosition = Math.min(chunk1.offset, chunk2.offset);
            endPosition = Math.max(chunk1.offset + chunk1.length, chunk2.offset + chunk2.length);

            compData.close();
        }
        else
        { // overwrite with garbage from key1 to key2
            long row0Start = sstable.getPosition(PartitionPosition.ForKey.get(key1, sstable.getPartitioner()), SSTableReader.Operator.EQ);
            long row1Start = sstable.getPosition(PartitionPosition.ForKey.get(key2, sstable.getPartitioner()), SSTableReader.Operator.EQ);
            startPosition = Math.min(row0Start, row1Start);
            endPosition = Math.max(row0Start, row1Start);
        }

        overrideWithGarbage(sstable, startPosition, endPosition, junk);
    }

    private static void overrideWithGarbage(SSTableReader sstable, long startPosition, long endPosition) throws IOException
    {
        overrideWithGarbage(sstable, startPosition, endPosition, (byte) 'z');
    }

    private static void overrideWithGarbage(SSTableReader sstable, long startPosition, long endPosition, byte junk) throws IOException
    {
        overrideWithGarbage(sstable.getDataChannel().file(), startPosition, endPosition, junk);
    }

    private static void overrideWithGarbage(File path, long startPosition, long endPosition, byte junk) throws IOException
    {
        try (RandomAccessFile file = new RandomAccessFile(path.toJavaIOFile(), "rw"))
        {
            file.seek(startPosition);
            int length = (int) (endPosition - startPosition);
            byte[] buff = new byte[length];
            Arrays.fill(buff, junk);
            file.write(buff, 0, length);
        }
        if (ChunkCache.instance != null)
            ChunkCache.instance.invalidateFile(path.toString());
    }

    public static void assertOrderedAll(ColumnFamilyStore cfs, int expectedSize)
    {
        assertOrdered(Util.cmd(cfs).build(), expectedSize);
    }

    private static void assertOrdered(ReadCommand cmd, int expectedSize)
    {
        int size = 0;
        DecoratedKey prev = null;
        logger.info("Reading data from " + cmd);
        for (Partition partition : Util.getAllUnfiltered(cmd))
        {
            DecoratedKey current = partition.partitionKey();
            logger.info("Read " + current.toString());
            if (!(prev == null || prev.compareTo(current) < 0))
                logger.error("key " + current + " does not sort after previous key " + prev);
            assertTrue("key " + current + " does not sort after previous key " + prev, prev == null || prev.compareTo(current) < 0);
            prev = current;
            ++size;
        }
        assertEquals(expectedSize, size);
    }

    public static void fillCF(ColumnFamilyStore cfs, int partitionsPerSSTable)
    {
        for (int i = 0; i < partitionsPerSSTable; i++)
        {
            PartitionUpdate update = UpdateBuilder.create(cfs.metadata(), String.valueOf(i))
                                                  .newRow("r1").add("val", "1")
                                                  .newRow("r1").add("val", "1")
                                                  .build();

            new Mutation(update).applyUnsafe();
        }

        Util.flush(cfs);
    }

    public static void fillIndexCF(ColumnFamilyStore cfs, boolean composite, long... values)
    {
        assertEquals(0, values.length % 2);
        for (int i = 0; i < values.length; i += 2)
        {
            UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), String.valueOf(i));
            if (composite)
            {
                builder.newRow("c" + i)
                       .add(COL_INDEX, values[i])
                       .add(COL_NON_INDEX, values[i + 1]);
            }
            else
            {
                builder.newRow()
                       .add(COL_INDEX, values[i])
                       .add(COL_NON_INDEX, values[i + 1]);
            }
            new Mutation(builder.build()).applyUnsafe();
        }

        Util.flush(cfs);
    }

    public static String[] fillCounterCF(ColumnFamilyStore cfs, int partitionsPerSSTable) throws WriteTimeoutException
    {
        SortedSet<String> tokenSorted = Sets.newTreeSet(Comparator.comparing(a -> cfs.getPartitioner()
                                                                                     .decorateKey(ByteBufferUtil.bytes(a))));
        for (int i = 0; i < partitionsPerSSTable; i++)
        {
            if (i < 10)
                Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
            PartitionUpdate update = UpdateBuilder.create(cfs.metadata(), String.valueOf(i))
                                                  .newRow("r1").add("val", 100L)
                                                  .build();
            tokenSorted.add(String.valueOf(i));
            new CounterMutation(new Mutation(update), ConsistencyLevel.ONE).apply();
        }

        Util.flush(cfs);

        return tokenSorted.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
    }

    @Test
    public void testScrubColumnValidation() throws RequestExecutionException
    {
        QueryProcessor.process(String.format("CREATE TABLE \"%s\".test_compact_static_columns (a bigint, b timeuuid, c boolean static, d text, PRIMARY KEY (a, b))", ksName), ConsistencyLevel.ONE);

        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("test_compact_static_columns");

        QueryProcessor.executeInternal(String.format("INSERT INTO \"%s\".test_compact_static_columns (a, b, c, d) VALUES (123, c3db07e8-b602-11e3-bc6b-e0b9a54a6d93, true, 'foobar')", ksName));
        Util.flush(cfs);
        performScrub(cfs, false, true, false, 2);

        QueryProcessor.process(String.format("CREATE TABLE \"%s\".test_scrub_validation (a text primary key, b int)", ksName), ConsistencyLevel.ONE);
        ColumnFamilyStore cfs2 = keyspace.getColumnFamilyStore("test_scrub_validation");

        new Mutation(UpdateBuilder.create(cfs2.metadata(), "key").newRow().add("b", Int32Type.instance.decompose(1)).build()).apply();
        Util.flush(cfs2);

        performScrub(cfs2, false, false, false, 2);
    }

    /**
     * For CASSANDRA-6892 too, check that for a compact table with one cluster column, we can insert whatever
     * we want as value for the clustering column, including something that would conflict with a CQL column definition.
     */
    @Test
    public void testValidationCompactStorage()
    {
        QueryProcessor.process(String.format("CREATE TABLE \"%s\".test_compact_dynamic_columns (a int, b text, c text, PRIMARY KEY (a, b)) WITH COMPACT STORAGE", ksName), ConsistencyLevel.ONE);

        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("test_compact_dynamic_columns");

        QueryProcessor.executeInternal(String.format("INSERT INTO \"%s\".test_compact_dynamic_columns (a, b, c) VALUES (0, 'a', 'foo')", ksName));
        QueryProcessor.executeInternal(String.format("INSERT INTO \"%s\".test_compact_dynamic_columns (a, b, c) VALUES (0, 'b', 'bar')", ksName));
        QueryProcessor.executeInternal(String.format("INSERT INTO \"%s\".test_compact_dynamic_columns (a, b, c) VALUES (0, 'c', 'boo')", ksName));
        Util.flush(cfs);
        performScrub(cfs, true, true, false, 2);

        // Scrub is silent, but it will remove broken records. So reading everything back to make sure nothing to "scrubbed away"
        UntypedResultSet rs = QueryProcessor.executeInternal(String.format("SELECT * FROM \"%s\".test_compact_dynamic_columns", ksName));
        assertNotNull(rs);
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
        testScrubIndex(CF_INDEX1_BYTEORDERED, COL_INDEX, false, true);
    }

    @Test /* CASSANDRA-5174 */
    public void testScrubCompositeIndex_preserveOrder() throws IOException, ExecutionException, InterruptedException
    {
        testScrubIndex(CF_INDEX2_BYTEORDERED, COL_INDEX, true, true);
    }

    @Test /* CASSANDRA-5174 */
    public void testScrubKeysIndex() throws IOException, ExecutionException, InterruptedException
    {
        testScrubIndex(CF_INDEX1, COL_INDEX, false, true);
    }

    @Test /* CASSANDRA-5174 */
    public void testScrubCompositeIndex() throws IOException, ExecutionException, InterruptedException
    {
        testScrubIndex(CF_INDEX2, COL_INDEX, true, true);
    }

    @Test /* CASSANDRA-5174 */
    public void testFailScrubKeysIndex() throws IOException, ExecutionException, InterruptedException
    {
        testScrubIndex(CF_INDEX1, COL_INDEX, false, false);
    }

    @Test /* CASSANDRA-5174 */
    public void testFailScrubCompositeIndex() throws IOException, ExecutionException, InterruptedException
    {
        testScrubIndex(CF_INDEX2, COL_INDEX, true, false);
    }

    @Test /* CASSANDRA-5174 */
    public void testScrubTwice() throws IOException, ExecutionException, InterruptedException
    {
        testScrubIndex(CF_INDEX2, COL_INDEX, true, true, true);
    }

    @SuppressWarnings("SameParameterValue")
    private void testScrubIndex(String cfName, String colName, boolean composite, boolean... scrubs)
    throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        int numRows = 1000;
        long[] colValues = new long[numRows * 2]; // each row has two columns
        for (int i = 0; i < colValues.length; i += 2)
        {
            colValues[i] = (i % 4 == 0 ? 1L : 2L); // index column
            colValues[i + 1] = 3L; //other column
        }
        fillIndexCF(cfs, composite, colValues);

        // check index

        assertOrdered(Util.cmd(cfs).filterOn(colName, Operator.EQ, 1L).build(), numRows / 2);

        // scrub index
        Set<ColumnFamilyStore> indexCfss = cfs.indexManager.getAllIndexColumnFamilyStores();
        assertEquals(1, indexCfss.size());
        for (ColumnFamilyStore indexCfs : indexCfss)
        {
            for (int i = 0; i < scrubs.length; i++)
            {
                boolean failure = !scrubs[i];
                if (failure)
                { //make sure the next scrub fails
                    overrideWithGarbage(indexCfs.getLiveSSTables().iterator().next(), ByteBufferUtil.bytes(1L), ByteBufferUtil.bytes(2L), (byte) 0x7A);
                }
                CompactionManager.AllSSTableOpStatus result = indexCfs.scrub(false, true, IScrubber.options().build(), 0);
                assertEquals(failure ?
                             CompactionManager.AllSSTableOpStatus.ABORTED :
                             CompactionManager.AllSSTableOpStatus.SUCCESSFUL,
                             result);
            }
        }


        // check index is still working
        assertOrdered(Util.cmd(cfs).filterOn(colName, Operator.EQ, 1L).build(), numRows / 2);
    }

    private static SSTableMultiWriter createTestWriter(Descriptor descriptor, long keyCount, ColumnFamilyStore cfs, LifecycleTransaction txn)
    {
        SerializationHeader header = new SerializationHeader(true, cfs.metadata(), cfs.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS);
        MetadataCollector collector = new MetadataCollector(cfs.metadata().comparator).sstableLevel(0);
        SSTableWriter writer = descriptor.getFormat()
                                         .getWriterFactory()
                                         .builder(descriptor)
                                         .setKeyCount(keyCount)
                                         .setRepairedAt(0)
                                         .setPendingRepair(null)
                                         .setTransientSSTable(false)
                                         .setTableMetadataRef(cfs.metadata)
                                         .setMetadataCollector(collector)
                                         .setSerializationHeader(header)
                                         .addDefaultComponents(Collections.emptySet())
                                         .build(txn, cfs);

        return new TestMultiWriter(writer, txn);
    }

    private static class TestMultiWriter extends SimpleSSTableMultiWriter
    {
        TestMultiWriter(SSTableWriter writer, LifecycleNewTracker lifecycleNewTracker)
        {
            super(writer, lifecycleNewTracker);
        }
    }

    /**
     * Tests with invalid sstables (containing duplicate entries in 2.0 and 3.0 storage format),
     * that were caused by upgrading from 2.x with duplicate range tombstones.
     * <p>
     * See CASSANDRA-12144 for details.
     */
    @Test
    public void testFilterOutDuplicates() throws Exception
    {
        Assume.assumeTrue(BigFormat.isSelected());

        IPartitioner oldPart = DatabaseDescriptor.getPartitioner();
        try
        {
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
            QueryProcessor.process(String.format("CREATE TABLE \"%s\".cf_with_duplicates_3_0 (a int, b int, c int, PRIMARY KEY (a, b))", ksName), ConsistencyLevel.ONE);

            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("cf_with_duplicates_3_0");

            Path legacySSTableRoot = Paths.get(TEST_INVALID_LEGACY_SSTABLE_ROOT.getString(),
                                               "Keyspace1",
                                               "cf_with_duplicates_3_0");

            for (String filename : new String[]{ "mb-3-big-CompressionInfo.db",
                                                 "mb-3-big-Digest.crc32",
                                                 "mb-3-big-Index.db",
                                                 "mb-3-big-Summary.db",
                                                 "mb-3-big-Data.db",
                                                 "mb-3-big-Filter.db",
                                                 "mb-3-big-Statistics.db",
                                                 "mb-3-big-TOC.txt" })
            {
                Files.copy(Paths.get(legacySSTableRoot.toString(), filename), cfs.getDirectories().getDirectoryForNewSSTables().toPath().resolve(filename));
            }

            cfs.loadNewSSTables();

            cfs.scrub(true, false, IScrubber.options().skipCorrupted().build(), 1);

            UntypedResultSet rs = QueryProcessor.executeInternal(String.format("SELECT * FROM \"%s\".cf_with_duplicates_3_0", ksName));
            assertNotNull(rs);
            assertEquals(1, rs.size());

            QueryProcessor.executeInternal(String.format("DELETE FROM \"%s\".cf_with_duplicates_3_0 WHERE a=1 AND b =2", ksName));
            rs = QueryProcessor.executeInternal(String.format("SELECT * FROM \"%s\".cf_with_duplicates_3_0", ksName));
            assertNotNull(rs);
            assertEquals(0, rs.size());
        }
        finally
        {
            DatabaseDescriptor.setPartitionerUnsafe(oldPart);
        }
    }

    private static CompactionManager.AllSSTableOpStatus performScrub(ColumnFamilyStore cfs, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL, int jobs)
    {
        IScrubber.Options options = IScrubber.options()
                                             .skipCorrupted(skipCorrupted)
                                             .checkData(checkData)
                                             .reinsertOverflowedTTLRows(reinsertOverflowedTTL)
                                             .build();
        return CompactionManager.instance.performScrub(cfs, options, jobs);
    }
}
