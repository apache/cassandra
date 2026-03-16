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

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
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
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@code SSTableReader#canReuseDfile} / {@code SSTableReader#openDataReaderInternal}.
 */
public class SSTableReaderDataReaderTest
{
    private static final String KEYSPACE = "SSTableReaderDataReaderTest";
    private static final String CF_UNCOMPRESSED = "Uncompressed";
    private static final String CF_COMPRESSED = "Compressed";

    private static DiskAccessMode originalDiskAccessMode;
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
    public void testForScanReusesWithNullMode()
    {
        SSTableReader sstable = createSSTable(CF_UNCOMPRESSED);

        try (RandomAccessReader reader = sstable.openDataReaderForScan())
        {
            assertReaderSharesDfileChannel(sstable, reader);
        }
    }

    @Test
    public void testForScanCreatesNewHandleWithDirect()
    {
        SSTableReader sstable = createSSTable(CF_COMPRESSED);

        try (RandomAccessReader reader = sstable.openDataReaderForScan(DiskAccessMode.direct))
        {
            assertReaderHasOwnChannel(sstable, reader);
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
        refsToRelease.add(sstable.selfRef());
        return sstable;
    }
}
