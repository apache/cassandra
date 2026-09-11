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
package org.apache.cassandra.io.sstable.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for {@code SSTableReader#canReuseDfile} / {@code SSTableReader#openDataReaderInternal}, and for
 * reader contents and resource lifetime across foreground reads and scans.
 */
@RunWith(BMUnitRunner.class)
public class SSTableReaderDataReaderTest
{
    private static final String KEYSPACE = "SSTableReaderDataReaderTest";
    private static final String CF_UNCOMPRESSED = "Uncompressed";
    private static final String CF_COMPRESSED = "Compressed";

    private static DiskAccessMode originalDiskAccessMode;
    private static final ThreadLocal<Set<Integer>> advisedDescriptors = new ThreadLocal<>();
    private static final ThreadLocal<Boolean> sequentialRead = new ThreadLocal<>();
    private final List<Ref<?>> refsToRelease = new ArrayList<>();

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        originalDiskAccessMode = DatabaseDescriptor.getDiskAccessMode();
        DatabaseDescriptor.setDiskAccessMode(DiskAccessMode.standard);
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_UNCOMPRESSED)
                                                .compression(CompressionParams.noCompression()),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_COMPRESSED)
                                                .compression(CompressionParams.DEFAULT));
        CompactionManager.instance.disableAutoCompaction();
    }

    @AfterClass
    public static void restoreConfiguration()
    {
        DatabaseDescriptor.setDiskAccessMode(originalDiskAccessMode);
    }

    @After
    public void teardown()
    {
        DatabaseDescriptor.setDiskAccessMode(DiskAccessMode.standard);
        Throwable exceptions = null;
        for (Ref<?> ref : refsToRelease)
        {
            try
            {
                ref.close();
            }
            catch (Throwable t)
            {
                exceptions = Throwables.merge(exceptions, t);
            }
        }
        refsToRelease.clear();
        Throwables.maybeFail(exceptions);
    }

    @Test
    public void testNullModeReusesExistingDfile()
    {
        SSTableReader sstable = createSSTable(CF_UNCOMPRESSED);

        try (RandomAccessReader reader = sstable.openDataReader())
        {
            assertReaderSharesDfileChannel(sstable, reader);
        }
    }

    @Test
    public void testSameModeReusesExistingDfile()
    {
        SSTableReader sstable = createSSTable(CF_UNCOMPRESSED);

        try (RandomAccessReader reader = sstable.openDataReader(sstable.dfile.diskAccessMode()))
        {
            assertReaderSharesDfileChannel(sstable, reader);
        }
    }

    @Test
    public void testDirectOnUnsupportedFallsBackToReuse()
    {
        SSTableReader sstable = createSSTable(CF_UNCOMPRESSED);
        assertFalse("Uncompressed SSTable should not support direct IO",
                     sstable.dfile.supportsDirectIO());

        try (RandomAccessReader reader = sstable.openDataReader(DiskAccessMode.direct))
        {
            assertReaderSharesDfileChannel(sstable, reader);
        }
    }

    @Test
    public void testDirectOnCompressedCreatesNewHandle()
    {
        SSTableReader sstable = createSSTable(CF_COMPRESSED);

        try (RandomAccessReader reader = sstable.openDataReader(DiskAccessMode.direct))
        {
            assertReaderHasOwnChannel(sstable, reader);
        }
    }

    @Test
    public void testNewHandleCloseDoesNotAffectOriginalDfile()
    {
        SSTableReader sstable = createSSTable(CF_COMPRESSED);

        RandomAccessReader reader = sstable.openDataReader(DiskAccessMode.direct);
        ChannelProxy newChannel = reader.getChannel();
        assertNotSame(sstable.dfile.channel, newChannel);

        reader.close();

        assertTrue("New handle's channel should be cleaned up after reader close",
                    newChannel.isCleanedUp());
        assertFalse("Original dfile channel should not be affected",
                     sstable.dfile.channel.isCleanedUp());

        try (RandomAccessReader reader2 = sstable.openDataReader())
        {
            assertReaderSharesDfileChannel(sstable, reader2);
        }
    }

    @Test
    public void testReusedReaderCloseDoesNotAffectDfile()
    {
        SSTableReader sstable = createSSTable(CF_UNCOMPRESSED);

        RandomAccessReader reader = sstable.openDataReader();
        ChannelProxy channel = reader.getChannel();
        assertSame(sstable.dfile.channel, channel);

        reader.close();

        assertFalse("Dfile channel should not be cleaned up after reused reader close",
                     channel.isCleanedUp());

        try (RandomAccessReader reader2 = sstable.openDataReader())
        {
            assertReaderSharesDfileChannel(sstable, reader2);
        }
    }

    @Test
    public void testMultipleNewHandleReadersDoNotLeakResources()
    {
        SSTableReader sstable = createSSTable(CF_COMPRESSED);

        ChannelProxy[] newChannels = new ChannelProxy[3];
        for (int i = 0; i < 3; i++)
        {
            try (RandomAccessReader reader = sstable.openDataReader(DiskAccessMode.direct))
            {
                newChannels[i] = reader.getChannel();
                assertNotSame(sstable.dfile.channel, newChannels[i]);
            }
        }

        for (int i = 0; i < 3; i++)
            assertTrue("New handle channel " + i + " should be cleaned up",
                        newChannels[i].isCleanedUp());

        assertFalse(sstable.dfile.channel.isCleanedUp());
        try (RandomAccessReader reader = sstable.openDataReader())
        {
            assertReaderSharesDfileChannel(sstable, reader);
            assertEquals(0, reader.getFilePointer());
        }
    }

    @Test
    public void testFlushedMmapScanRetainsLiveRegions() throws IOException
    {
        DatabaseDescriptor.setDiskAccessMode(DiskAccessMode.mmap);
        for (String table : new String[]{ CF_UNCOMPRESSED, CF_COMPRESSED })
        {
            SSTableReader sstable = createSSTable(table);
            assertEquals(DiskAccessMode.mmap, sstable.dfile.diskAccessMode());
            byte[] expected = readData(sstable.openDataReader());
            // Flush has already closed the writer and its MmappedRegionsCache.
            assertArrayEquals(expected, readData(sstable.openDataReaderForScan()));
            assertArrayEquals(expected, readData(sstable.openDataReaderForBulkScan(DiskAccessMode.mmap)));
            assertArrayEquals(expected, readData(sstable.openDataReader()));
        }
    }

    @Test
    public void testScanClosePreservesWarmedSharedCache() throws IOException
    {
        assertNotNull("This regression requires the enabled file cache in test/conf/cassandra.yaml", ChunkCache.instance);
        for (String table : new String[]{ CF_UNCOMPRESSED, CF_COMPRESSED })
        {
            SSTableReader sstable = createSSTable(table);
            byte[] expected = readData(sstable.openDataReader());
            long misses = ChunkCache.instance.metrics.misses.getCount();
            assertArrayEquals(expected, readData(sstable.openDataReaderForScan()));
            assertArrayEquals(expected, readData(sstable.openDataReaderForBulkScan(DiskAccessMode.standard)));
            assertArrayEquals(expected, readData(sstable.openDataReader()));
            assertEquals("Neither a user scan nor a closed bulk scan may evict chunks warmed by foreground readers",
                         misses, ChunkCache.instance.metrics.misses.getCount());
        }
    }

    @Test
    public void testUncompressedDirectScanFallsBack() throws IOException
    {
        assertScanContents(CF_UNCOMPRESSED, DiskAccessMode.direct);
    }

    @Test
    public void testOnlyBulkScanOpensItsOwnChannel()
    {
        SSTableReader sstable = createSSTable(CF_UNCOMPRESSED);
        try (RandomAccessReader reader = sstable.openDataReaderForScan())
        {
            assertSame("A user range read must not open the data file again", sstable.dfile.channel, reader.getChannel());
        }
        RandomAccessReader bulk = sstable.openDataReaderForBulkScan(DiskAccessMode.standard);
        ChannelProxy bulkChannel = bulk.getChannel();
        assertNotSame(sstable.dfile.channel, bulkChannel);
        bulk.close();
        assertTrue("A bulk scan must close its private descriptor", bulkChannel.isCleanedUp());
        assertFalse("A bulk scan must not close the shared data channel", sstable.dfile.channel.isCleanedUp());
    }

    @Test
    @BMRules(rules = {
        @BMRule(name = "record advice on active scan descriptors",
                targetClass = "org.apache.cassandra.utils.NativeLibraryLinux",
                targetMethod = "callPosixFadvise(int, long, int, int)",
                targetLocation = "AT ENTRY",
                condition = "org.apache.cassandra.io.sstable.format.SSTableReaderDataReaderTest.isRecordingReads()",
                action = "return org.apache.cassandra.io.sstable.format.SSTableReaderDataReaderTest.recordAdvice($1)"),
        @BMRule(name = "check advice at actual read boundary",
                targetClass = "org.apache.cassandra.io.util.ChannelProxy",
                targetMethod = "read",
                targetLocation = "AT ENTRY",
                condition = "org.apache.cassandra.io.sstable.format.SSTableReaderDataReaderTest.isRecordingReads()",
                action = "org.apache.cassandra.io.sstable.format.SSTableReaderDataReaderTest.checkReadAdvice($0.getFileDescriptor())")
    })
    public void testBulkScanAdviceDoesNotAffectOtherReads() throws IOException
    {
        assumeTrue("Sequential advice is Linux-only", FBUtilities.isLinux);
        SSTableReader sstable = createSSTable(CF_UNCOMPRESSED);
        byte[] expected = readData(sstable.openDataReader());
        // Force subsequent foreground reads through their actual descriptor, not the chunk cache.
        ChunkCache.instance.invalidateFile(sstable.dfile.path());
        advisedDescriptors.set(new HashSet<>());
        try
        {
            sequentialRead.set(true);
            assertArrayEquals(expected, readData(sstable.openDataReaderForBulkScan(DiskAccessMode.standard)));
            sequentialRead.set(false);
            int advised = advisedDescriptors.get().size();
            assertArrayEquals(expected, readData(sstable.openDataReaderForScan()));
            assertEquals("A user range read must not be advised", advised, advisedDescriptors.get().size());
            assertArrayEquals(expected, readData(sstable.openDataReader()));
        }
        finally
        {
            sequentialRead.remove();
            advisedDescriptors.remove();
        }
    }

    public static boolean isRecordingReads()
    {
        return advisedDescriptors.get() != null;
    }

    public static int recordAdvice(int fd)
    {
        advisedDescriptors.get().add(fd);
        return 0;
    }

    public static void checkReadAdvice(int fd)
    {
        assertEquals("Advice must apply to the descriptor performing scan reads, not foreground reads",
                     sequentialRead.get(), Boolean.valueOf(advisedDescriptors.get().contains(fd)));
    }

    private void assertScanContents(String table, DiskAccessMode mode) throws IOException
    {
        SSTableReader sstable = createSSTable(table);
        byte[] expected = readData(sstable.openDataReader());
        try (RandomAccessReader foreground = sstable.openDataReader())
        {
            assertEquals(expected[0], foreground.readByte());
            assertArrayEquals(expected, readData(sstable.openDataReaderForBulkScan(mode)));
            foreground.seek(0);
            byte[] actual = new byte[expected.length];
            foreground.readFully(actual);
            assertArrayEquals(expected, actual);
        }
        assertArrayEquals(expected, readData(sstable.openDataReader()));
    }

    private byte[] readData(RandomAccessReader reader) throws IOException
    {
        try (RandomAccessReader input = reader)
        {
            byte[] contents = new byte[Math.toIntExact(input.length())];
            input.readFully(contents);
            assertEquals(-1, input.read());
            return contents;
        }
    }

    private void assertReaderSharesDfileChannel(SSTableReader sstable, RandomAccessReader reader)
    {
        assertNotNull(reader);
        assertSame("Reader should share the dfile's channel (reuse path)",
                    sstable.dfile.channel, reader.getChannel());
    }

    private void assertReaderHasOwnChannel(SSTableReader sstable, RandomAccessReader reader)
    {
        assertNotNull(reader);
        assertNotSame("Reader should have its own channel (new handle path)",
                       sstable.dfile.channel, reader.getChannel());
    }

    private SSTableReader createSSTable(String cf)
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(cf);
        store.clearUnsafe();

        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < 10; i++)
        {
            new RowUpdateBuilder(store.metadata(), timestamp, String.valueOf(i))
                .clustering("col")
                .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                .build()
                .applyUnsafe();
        }
        store.forceBlockingFlush(UNIT_TESTS);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        refsToRelease.add(sstable.ref());
        return sstable;
    }
}
