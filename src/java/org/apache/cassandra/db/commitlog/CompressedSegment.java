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
package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import net.openhft.chronicle.core.util.ThrowingFunction;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.SimpleCachedBufferPool;

/**
 * Compressed commit log segment. Provides an in-memory buffer for the mutation threads. On sync compresses the written
 * section of the buffer and writes it to the destination channel.
 *
 * The format of the compressed commit log is as follows:
 * - standard commit log header (as written by {@link CommitLogDescriptor#writeHeader(ByteBuffer, CommitLogDescriptor)})
 * - a series of 'sync segments' that are written every time the commit log is sync()'ed
 * -- a sync section header, see {@link CommitLogSegment#writeSyncMarker(long, ByteBuffer, int, int, int)}
 * -- total plain text length for this section
 * -- a block of compressed data
 */
public class CompressedSegment extends FileDirectSegment
{
    static final int COMPRESSED_MARKER_SIZE = SYNC_MARKER_SIZE + 4;
    final ICompressor compressor;

    /**
     * Constructs a new segment file.
     */
    CompressedSegment(AbstractCommitLogSegmentManager manager, ThrowingFunction<Path, FileChannel, IOException> channelFactory)
    {
        super(manager, channelFactory);
        this.compressor = manager.getConfiguration().getCompressor();
    }

    @Override
    void write(int startMarker, int nextMarker)
    {
        int contentStart = startMarker + SYNC_MARKER_SIZE;
        int length = nextMarker - contentStart;
        // The length may be 0 when the segment is being closed.
        assert length > 0 || length == 0 && !isStillAllocating();

        try
        {
            int neededBufferSize = compressor.initialCompressedBufferLength(length) + COMPRESSED_MARKER_SIZE;
            ByteBuffer compressedBuffer = manager.getBufferPool().getThreadLocalReusableBuffer(neededBufferSize);

            ByteBuffer inputBuffer = buffer.duplicate();
            inputBuffer.limit(contentStart + length).position(contentStart);
            compressedBuffer.limit(compressedBuffer.capacity()).position(COMPRESSED_MARKER_SIZE);
            compressor.compress(inputBuffer, compressedBuffer);

            compressedBuffer.flip();
            compressedBuffer.putInt(SYNC_MARKER_SIZE, length);

            // Only one thread can be here at a given time.
            // Protected by synchronization on CommitLogSegment.sync().
            writeSyncMarker(id, compressedBuffer, 0, (int) channel.position(), (int) channel.position() + compressedBuffer.remaining());
            manager.addSize(compressedBuffer.limit());
            channel.write(compressedBuffer);
            assert channel.position() - lastWrittenPos == compressedBuffer.limit();
            lastWrittenPos = channel.position();
        }
        catch (Exception e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    @Override
    public long onDiskSize()
    {
        return lastWrittenPos;
    }

    protected static class CompressedSegmentBuilder extends CommitLogSegment.Builder
    {
        public CompressedSegmentBuilder(AbstractCommitLogSegmentManager segmentManager)
        {
            super(segmentManager);
        }

        @Override
        public CompressedSegment build()
        {
            return new CompressedSegment(segmentManager,
                                         path -> FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE));
        }

        @Override
        public SimpleCachedBufferPool createBufferPool()
        {
            return new SimpleCachedBufferPool(DatabaseDescriptor.getCommitLogMaxCompressionBuffersInPool(),
                                              DatabaseDescriptor.getCommitLogSegmentSize(),
                                              segmentManager.getConfiguration().getCompressor().preferredBufferType());
        }
    }
}
