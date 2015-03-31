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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.ICompressor.WrappedByteBuffer;
import org.apache.cassandra.io.util.FileUtils;

/*
 * Compressed commit log segment. Provides an in-memory buffer for the mutation threads. On sync compresses the written
 * section of the buffer and writes it to the destination channel.
 */
public class CompressedSegment extends CommitLogSegment
{
    static private final ThreadLocal<WrappedByteBuffer> compressedBufferHolder = new ThreadLocal<WrappedByteBuffer>() {
        protected WrappedByteBuffer initialValue()
        {
            return new WrappedByteBuffer(ByteBuffer.allocate(0));
        }
    };
    static Queue<ByteBuffer> bufferPool = new ConcurrentLinkedQueue<>();

    /**
     * Maximum number of buffers in the compression pool. The default value is 3, it should not be set lower than that
     * (one segment in compression, one written to, one in reserve); delays in compression may cause the log to use
     * more, depending on how soon the sync policy stops all writing threads.
     */
    static final int MAX_BUFFERPOOL_SIZE = DatabaseDescriptor.getCommitLogMaxCompressionBuffersInPool();

    static final int COMPRESSED_MARKER_SIZE = SYNC_MARKER_SIZE + 4;
    final ICompressor compressor;

    /**
     * Constructs a new segment file.
     */
    CompressedSegment(CommitLog commitLog)
    {
        super(commitLog);
        this.compressor = commitLog.compressor;
        try
        {
            channel.write((ByteBuffer) buffer.duplicate().flip());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    static ByteBuffer allocate(ICompressor compressor, int size)
    {
        if (compressor.useDirectOutputByteBuffers())
            return ByteBuffer.allocateDirect(size);
        else
            return ByteBuffer.allocate(size);
    }
    
    ByteBuffer allocate(int size)
    {
        return allocate(compressor, size);
    }

    ByteBuffer createBuffer(CommitLog commitLog)
    {
        ByteBuffer buf = bufferPool.poll();
        if (buf == null)
        {
            // this.compressor is not yet set, so we must use the commitLog's one.
            buf = allocate(commitLog.compressor, DatabaseDescriptor.getCommitLogSegmentSize());
        } else
            buf.clear();
        return buf;
    }

    static long startMillis = System.currentTimeMillis();

    @Override
    void write(int startMarker, int nextMarker)
    {
        int contentStart = startMarker + SYNC_MARKER_SIZE;
        int length = nextMarker - contentStart;
        // The length may be 0 when the segment is being closed.
        assert length > 0 || length == 0 && !isStillAllocating();

        try {

            int compressedLength = compressor.initialCompressedBufferLength(length);
            WrappedByteBuffer wrappedCompressedBuffer = compressedBufferHolder.get();
            ByteBuffer compressedBuffer = wrappedCompressedBuffer.buffer;
            if (compressedBuffer.isDirect() != compressor.useDirectOutputByteBuffers() ||
                compressedBuffer.capacity() < compressedLength + COMPRESSED_MARKER_SIZE)
            {
                compressedBuffer = allocate(compressedLength + COMPRESSED_MARKER_SIZE);
                FileUtils.clean(wrappedCompressedBuffer.buffer);
                wrappedCompressedBuffer.buffer = compressedBuffer;
            }

            ByteBuffer inputBuffer = buffer.duplicate();
            inputBuffer.limit(contentStart + length).position(contentStart);
            compressedBuffer.limit(compressedBuffer.capacity()).position(COMPRESSED_MARKER_SIZE);
            compressedLength = compressor.compress(inputBuffer, wrappedCompressedBuffer);

            compressedBuffer.position(0);
            compressedBuffer.limit(COMPRESSED_MARKER_SIZE + compressedLength);
            compressedBuffer.putInt(SYNC_MARKER_SIZE, length);

            // Only one thread can be here at a given time.
            // Protected by synchronization on CommitLogSegment.sync().
            writeSyncMarker(compressedBuffer, 0, (int) channel.position(), (int) channel.position() + compressedBuffer.remaining());
            channel.write(compressedBuffer);
            channel.force(true);
        }
        catch (Exception e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    @Override
    protected void internalClose()
    {
        if (bufferPool.size() < MAX_BUFFERPOOL_SIZE)
            bufferPool.add(buffer);
        else
            FileUtils.clean(buffer);

        super.internalClose();
    }

    static void shutdown()
    {
        bufferPool.clear();
    }
}
