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
package org.apache.cassandra.io.compress;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.Adler32;


import com.google.common.primitives.Ints;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.CompressedPoolingSegmentedFile;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.PoolingSegmentedFile;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.FBUtilities;

/**
 * CRAR extends RAR to transparently uncompress blocks from the file into RAR.buffer.  Most of the RAR
 * "read bytes from the buffer, rebuffering when necessary" machinery works unchanged after that.
 */
public class CompressedRandomAccessReader extends RandomAccessReader
{
    private static final boolean useMmap = DatabaseDescriptor.getDiskAccessMode() == Config.DiskAccessMode.mmap;

    public static CompressedRandomAccessReader open(String dataFilePath, CompressionMetadata metadata)
    {
        return open(dataFilePath, metadata, null);
    }
    public static CompressedRandomAccessReader open(String path, CompressionMetadata metadata, CompressedPoolingSegmentedFile owner)
    {
        try
        {
            return new CompressedRandomAccessReader(path, metadata, owner);
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }


    private TreeMap<Long, MappedByteBuffer> chunkSegments;
    private int MAX_SEGMENT_SIZE = Integer.MAX_VALUE;

    private final CompressionMetadata metadata;

    // we read the raw compressed bytes into this buffer, then move the uncompressed ones into super.buffer.
    private ByteBuffer compressed;

    // re-use single crc object
    private final Adler32 checksum;

    // raw checksum bytes
    private ByteBuffer checksumBytes;

    protected CompressedRandomAccessReader(String dataFilePath, CompressionMetadata metadata, PoolingSegmentedFile owner) throws FileNotFoundException
    {
        super(new File(dataFilePath), metadata.chunkLength(), metadata.compressedFileLength, metadata.compressor().useDirectOutputByteBuffers(), owner);
        this.metadata = metadata;
        checksum = new Adler32();

        if (!useMmap)
        {
            compressed = ByteBuffer.wrap(new byte[metadata.compressor().initialCompressedBufferLength(metadata.chunkLength())]);
            checksumBytes = ByteBuffer.wrap(new byte[4]);
        }
        else
        {
            try
            {
                createMappedSegments();
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
    }

    private void createMappedSegments() throws IOException
    {
        chunkSegments = new TreeMap<>();
        long offset = 0;
        long lastSegmentOffset = 0;
        long segmentSize = 0;

        while (offset < metadata.dataLength)
        {
            CompressionMetadata.Chunk chunk = metadata.chunkFor(offset);

            //Reached a new mmap boundary
            if (segmentSize + chunk.length + 4 > MAX_SEGMENT_SIZE)
            {
                chunkSegments.put(lastSegmentOffset, channel.map(FileChannel.MapMode.READ_ONLY, lastSegmentOffset, segmentSize));
                lastSegmentOffset += segmentSize;
                segmentSize = 0;
            }

            segmentSize += chunk.length + 4; //checksum
            offset += metadata.chunkLength();
        }

        if (segmentSize > 0)
            chunkSegments.put(lastSegmentOffset, channel.map(FileChannel.MapMode.READ_ONLY, lastSegmentOffset, segmentSize));
    }

    protected ByteBuffer allocateBuffer(int bufferSize, boolean useDirect)
    {
        assert Integer.bitCount(bufferSize) == 1;
        return useMmap && useDirect
                ? ByteBuffer.allocateDirect(bufferSize)
                : ByteBuffer.allocate(bufferSize);
    }

    @Override
    public void deallocate()
    {
        super.deallocate();

        if (chunkSegments != null)
        {
            for (Map.Entry<Long, MappedByteBuffer> entry : chunkSegments.entrySet())
            {
                FileUtils.clean(entry.getValue());
            }
        }

        chunkSegments = null;
    }

    private void reBufferStandard()
    {
        try
        {
            long position = current();
            assert position < metadata.dataLength;

            CompressionMetadata.Chunk chunk = metadata.chunkFor(position);

            if (channel.position() != chunk.offset)
                channel.position(chunk.offset);

            if (compressed.capacity() < chunk.length)
                compressed = ByteBuffer.wrap(new byte[chunk.length]);
            else
                compressed.clear();
            compressed.limit(chunk.length);

            if (channel.read(compressed) != chunk.length)
                throw new CorruptBlockException(getPath(), chunk);

            // technically flip() is unnecessary since all the remaining work uses the raw array, but if that changes
            // in the future this will save a lot of hair-pulling
            compressed.flip();
            buffer.clear();
            int decompressedBytes;
            try
            {
                decompressedBytes = metadata.compressor().uncompress(compressed.array(), 0, chunk.length, buffer.array(), 0);
                buffer.limit(decompressedBytes);
            }
            catch (IOException e)
            {
                throw new CorruptBlockException(getPath(), chunk);
            }

            if (metadata.parameters.getCrcCheckChance() > ThreadLocalRandom.current().nextDouble())
            {

                checksum.update(compressed.array(), 0, chunk.length);

                if (checksum(chunk) != (int) checksum.getValue())
                    throw new CorruptBlockException(getPath(), chunk);

                // reset checksum object back to the original (blank) state
                checksum.reset();
            }

            // buffer offset is always aligned
            bufferOffset = position & ~(buffer.capacity() - 1);
            buffer.position((int) (position - bufferOffset));
            // the length() can be provided at construction time, to override the true (uncompressed) length of the file;
            // this is permitted to occur within a compressed segment, so we truncate validBufferBytes if we cross the imposed length
            if (bufferOffset + buffer.limit() > length())
                buffer.limit((int)(length() - bufferOffset));
        }
        catch (CorruptBlockException e)
        {
            throw new CorruptSSTableException(e, getPath());
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getPath());
        }
    }

    private void reBufferMmap()
    {
        try
        {
            long position = current();
            assert position < metadata.dataLength;

            CompressionMetadata.Chunk chunk = metadata.chunkFor(position);

            Map.Entry<Long, MappedByteBuffer> entry = chunkSegments.floorEntry(chunk.offset);
            long segmentOffset = entry.getKey();
            int chunkOffset = Ints.checkedCast(chunk.offset - segmentOffset);
            MappedByteBuffer compressedChunk = entry.getValue();

            compressedChunk.position(chunkOffset);
            compressedChunk.limit(chunkOffset + chunk.length);
            compressedChunk.mark();

            buffer.clear();
            int decompressedBytes;
            try
            {
                decompressedBytes = metadata.compressor().uncompress(compressedChunk, buffer);
                buffer.limit(decompressedBytes);
            }
            catch (IOException e)
            {
                throw new CorruptBlockException(getPath(), chunk);
            }
            finally
            {
                compressedChunk.limit(compressedChunk.capacity());
            }

            if (metadata.parameters.getCrcCheckChance() > ThreadLocalRandom.current().nextDouble())
            {
                compressedChunk.reset();
                compressedChunk.limit(chunkOffset + chunk.length);

                FBUtilities.directCheckSum(checksum, compressedChunk);

                compressedChunk.limit(compressedChunk.capacity());


                if (compressedChunk.getInt() != (int) checksum.getValue())
                    throw new CorruptBlockException(getPath(), chunk);

                // reset checksum object back to the original (blank) state
                checksum.reset();
            }

            // buffer offset is always aligned
            bufferOffset = position & ~(buffer.capacity() - 1);
            buffer.position((int) (position - bufferOffset));
            // the length() can be provided at construction time, to override the true (uncompressed) length of the file;
            // this is permitted to occur within a compressed segment, so we truncate validBufferBytes if we cross the imposed length
            if (bufferOffset + buffer.limit() > length())
                buffer.limit((int)(length() - bufferOffset));
        }
        catch (CorruptBlockException e)
        {
            throw new CorruptSSTableException(e, getPath());
        }

    }

    @Override
    protected void reBuffer()
    {
        if (useMmap)
        {
            reBufferMmap();
        }
        else
        {
            reBufferStandard();
        }
    }

    private int checksum(CompressionMetadata.Chunk chunk) throws IOException
    {
        assert channel.position() == chunk.offset + chunk.length;
        checksumBytes.clear();
        if (channel.read(checksumBytes) != checksumBytes.capacity())
            throw new CorruptBlockException(getPath(), chunk);
        return checksumBytes.getInt(0);
    }

    public int getTotalBufferSize()
    {
        return super.getTotalBufferSize() + (useMmap ? 0 : compressed.capacity());
    }

    @Override
    public long length()
    {
        return metadata.dataLength;
    }

    @Override
    public String toString()
    {
        return String.format("%s - chunk length %d, data length %d.", getPath(), metadata.chunkLength(), metadata.dataLength);
    }
}
