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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db.streaming;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import net.jpountz.lz4.LZ4Factory;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamCoordinator;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;
import org.apache.cassandra.streaming.async.NettyStreamingConnectionFactory;
import org.apache.cassandra.streaming.async.StreamCompressionSerializer;
import org.apache.cassandra.utils.FBUtilities;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;

import static java.util.Collections.emptyList;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class BackgroundStreamingTest
{
    private static final String KEYSPACE = "BackgroundStreamingTest";
    private static SSTableReader compressed;
    private static SSTableReader uncompressed;

    @BeforeClass
    public static void setup()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, "compressed").compression(CompressionParams.lz4()),
                                    SchemaLoader.standardCFMD(KEYSPACE, "uncompressed").compression(CompressionParams.noCompression()));
        compressed = writeSSTable("compressed");
        uncompressed = writeSSTable("uncompressed");
    }

    private static SSTableReader writeSSTable(String table)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        byte[] value = new byte[3 * StreamingFileReader.BUFFER_SIZE + 37];
        new Random(1).nextBytes(value);
        new RowUpdateBuilder(cfs.metadata(), 1, "key").clustering("row").add("val", ByteBuffer.wrap(value)).build().applyUnsafe();
        Util.flush(cfs);
        return cfs.getLiveSSTables().iterator().next();
    }

    @Test
    public void rawReaderHonorsModeAcrossUnalignedBoundaries() throws Exception
    {
        DiskAccessMode previous = DatabaseDescriptor.getBackgroundReadDiskAccessMode();
        File file = FileUtils.createTempFile("background-stream", ".db", uncompressed.descriptor.fileFor(Components.DATA).parent());
        byte[] content = new byte[StreamingFileReader.STAGING_BUFFER_SIZE + StreamingFileReader.BUFFER_SIZE + 37];
        new Random(2).nextBytes(content);
        Files.write(file.toPath(), content);
        try
        {
            for (DiskAccessMode mode : new DiskAccessMode[]{ DiskAccessMode.standard, DiskAccessMode.direct })
            {
                DatabaseDescriptor.setBackgroundReadDiskAccessMode(mode);
                try (StreamingFileReader reader = StreamingFileReader.open(file))
                {
                    assertEquals(mode == DiskAccessMode.direct && FileUtils.isDirectIOSupported(file), reader.isDirect());
                    // Includes disjoint/backward reads, a staging-window crossing, and the unaligned EOF tail.
                    int[][] ranges = { { 17, 3 }, { 65533, 65543 }, { content.length - 39, 39 }, { StreamingFileReader.STAGING_BUFFER_SIZE - 5, 65541 }, { 1, 131077 } };
                    for (int[] range : ranges)
                    {
                        ByteBuffer actual = ByteBuffer.allocate(range[1]);
                        reader.readFully(actual, range[0]);
                        assertArrayEquals(Arrays.copyOfRange(content, range[0], range[0] + range[1]), actual.array());
                    }
                    reader.readFully(ByteBuffer.allocate(0), content.length);
                    assertThatThrownBy(() -> reader.readFully(ByteBuffer.allocate(2), content.length - 1))
                    .isInstanceOf(EOFException.class);
                }
            }
        }
        finally
        {
            DatabaseDescriptor.setBackgroundReadDiskAccessMode(previous);
            file.delete();
        }
    }

    @Test
    public void compressedSectionsPreserveStoredChunksAndChecksums() throws Exception
    {
        CompressionMetadata metadata = compressed.getCompressionMetadata();
        List<SSTableReader.PartitionPositionBounds> sections = Arrays.asList(new SSTableReader.PartitionPositionBounds(1, 2),
                                                                           new SSTableReader.PartitionPositionBounds(metadata.dataLength - 13, metadata.dataLength));
        CompressionInfo info = CompressionInfo.newLazyInstance(metadata, sections);
        byte[] stored = Files.readAllBytes(compressed.descriptor.fileFor(Components.DATA).toPath());
        try (DataOutputBuffer expected = new DataOutputBuffer())
        {
            for (CompressionMetadata.Chunk chunk : info.chunks())
                expected.write(stored, (int) chunk.offset, chunk.length + Integer.BYTES);
            assertPartialStream(compressed, sections, info, expected.toByteArray());
        }
    }

    @Test
    public void uncompressedSectionsPreserveChecksumBoundarySlices() throws Exception
    {
        byte[] stored = Files.readAllBytes(uncompressed.descriptor.fileFor(Components.DATA).toPath());
        List<SSTableReader.PartitionPositionBounds> sections = Arrays.asList(new SSTableReader.PartitionPositionBounds(17, 131079),
                                                                           new SSTableReader.PartitionPositionBounds(stored.length - 29, stored.length));
        try (DataOutputBuffer expected = new DataOutputBuffer())
        {
            for (SSTableReader.PartitionPositionBounds section : sections)
                expected.write(stored, (int) section.lowerPosition, (int) (section.upperPosition - section.lowerPosition));
            assertPartialStream(uncompressed, sections, null, expected.toByteArray());
        }
    }

    private void assertPartialStream(SSTableReader sstable, List<SSTableReader.PartitionPositionBounds> sections,
                                     CompressionInfo info, byte[] expected) throws Exception
    {
        DiskAccessMode previous = DatabaseDescriptor.getBackgroundReadDiskAccessMode();
        CassandraStreamHeader header = CassandraStreamHeader.builder().withSSTableVersion(sstable.descriptor.version)
                                                           .withSections(sections).withCompressionInfo(info)
                                                           .withSerializationHeader(sstable.header.toComponent())
                                                           .withTableId(sstable.metadata().id).build();
        try
        {
            for (DiskAccessMode mode : new DiskAccessMode[]{ DiskAccessMode.standard, DiskAccessMode.direct })
            {
                DatabaseDescriptor.setBackgroundReadDiskAccessMode(mode);
                try (CollectingOutput output = new CollectingOutput())
                {
                    CassandraStreamWriter writer = info == null ? new CassandraStreamWriter(sstable, header, session())
                                                               : new CassandraCompressedStreamWriter(sstable, header, session());
                    writer.write(output);
                    if (info != null)
                        assertArrayEquals(expected, output.toByteArray());
                    else
                    {
                        StreamCompressionSerializer serializer = new StreamCompressionSerializer(UnpooledByteBufAllocator.DEFAULT);
                        try (DataInputBuffer input = new DataInputBuffer(output.toByteArray());
                             DataOutputBuffer decoded = new DataOutputBuffer())
                        {
                            while (input.available() > 0)
                            {
                                ByteBuf chunk = serializer.deserialize(LZ4Factory.fastestInstance().safeDecompressor(), input, current_version);
                                try
                                {
                                    decoded.write(chunk.nioBuffer());
                                }
                                finally
                                {
                                    chunk.release();
                                }
                            }
                            assertArrayEquals(expected, decoded.toByteArray());
                        }
                    }
                }
            }
        }
        finally
        {
            DatabaseDescriptor.setBackgroundReadDiskAccessMode(previous);
        }
    }

    @Test
    public void entireDirectStreamPreservesEveryComponent() throws Exception
    {
        DiskAccessMode previous = DatabaseDescriptor.getBackgroundReadDiskAccessMode();
        DatabaseDescriptor.setBackgroundReadDiskAccessMode(DiskAccessMode.direct);
        try
        {
            for (SSTableReader sstable : new SSTableReader[]{ compressed, uncompressed })
            {
                try (ComponentContext context = ComponentContext.create(sstable);
                     CollectingOutput output = new CollectingOutput();
                     DataOutputBuffer expected = new DataOutputBuffer())
                {
                    for (Component component : context.manifest().components())
                        expected.write(Files.readAllBytes(context.file(sstable.descriptor, component).toPath()));
                    new CassandraEntireSSTableStreamWriter(sstable, session(), context).write(output);
                    assertArrayEquals(expected.toByteArray(), output.toByteArray());
                    try (StreamingFileReader reader = StreamingFileReader.open(context.file(sstable.descriptor, Components.DATA)))
                    {
                        // Only a reader that really obtained direct I/O may cost the zero-copy path.
                        assertEquals(!reader.isDirect(), output.sendfile);
                    }
                }
            }
        }
        finally
        {
            DatabaseDescriptor.setBackgroundReadDiskAccessMode(previous);
        }
    }

    private static StreamSession session()
    {
        StreamCoordinator coordinator = new StreamCoordinator(StreamOperation.BOOTSTRAP, 1, new NettyStreamingConnectionFactory(), false, false, null, PreviewKind.NONE);
        StreamResultFuture future = StreamResultFuture.createInitiator(nextTimeUUID(), StreamOperation.BOOTSTRAP, Collections.emptyList(), coordinator);
        coordinator.addSessionInfo(new SessionInfo(FBUtilities.getBroadcastAddressAndPort(), 0, FBUtilities.getBroadcastAddressAndPort(), emptyList(), emptyList(), StreamSession.State.INITIALIZED, null));
        StreamSession session = coordinator.getOrCreateOutboundSession(FBUtilities.getBroadcastAddressAndPort());
        session.init(future);
        return session;
    }

    private static class CollectingOutput extends DataOutputBuffer implements StreamingDataOutputPlus
    {
        private boolean sendfile;

        @Override
        public int writeToChannel(Write write, RateLimiter limiter) throws IOException
        {
            ByteBuffer[] supplied = new ByteBuffer[1];
            write.write(size -> supplied[0] = ByteBuffer.allocate(size + 8));
            int count = supplied[0].remaining();
            write(supplied[0]);
            return count;
        }

        @Override
        public long writeFileToChannel(FileChannel file, RateLimiter limiter) throws IOException
        {
            sendfile = true;
            ByteBuffer buffer = ByteBuffer.allocate((int) file.size());
            try (FileChannel closing = file)
            {
                while (buffer.hasRemaining())
                    closing.read(buffer);
            }
            buffer.flip();
            write(buffer);
            return buffer.limit();
        }
    }
}
