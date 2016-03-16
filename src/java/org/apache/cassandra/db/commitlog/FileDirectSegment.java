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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.FileUtils;

/**
 * Writes to the backing commit log file only on sync, allowing transformations of the mutations,
 * such as compression or encryption, before writing out to disk.
 */
public abstract class FileDirectSegment extends CommitLogSegment
{
    protected static final ThreadLocal<ByteBuffer> reusableBufferHolder = new ThreadLocal<ByteBuffer>()
    {
        protected ByteBuffer initialValue()
        {
            return ByteBuffer.allocate(0);
        }
    };

    static Queue<ByteBuffer> bufferPool = new ConcurrentLinkedQueue<>();

    /**
     * Maximum number of buffers in the compression pool. The default value is 3, it should not be set lower than that
     * (one segment in compression, one written to, one in reserve); delays in compression may cause the log to use
     * more, depending on how soon the sync policy stops all writing threads.
     */
    static final int MAX_BUFFERPOOL_SIZE = DatabaseDescriptor.getCommitLogMaxCompressionBuffersInPool();

    /**
     * The number of buffers in use
     */
    private static AtomicInteger usedBuffers = new AtomicInteger(0);

    volatile long lastWrittenPos = 0;

    private final Runnable onClose;

    FileDirectSegment(CommitLog commitLog, Runnable onClose)
    {
        super(commitLog);
        this.onClose = onClose;
    }

    void writeLogHeader()
    {
        super.writeLogHeader();
        try
        {
            channel.write((ByteBuffer) buffer.duplicate().flip());
            commitLog.allocator.addSize(lastWrittenPos = buffer.position());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    ByteBuffer createBuffer(BufferType bufferType)
    {
        usedBuffers.incrementAndGet();
        ByteBuffer buf = bufferPool.poll();
        if (buf != null)
        {
            buf.clear();
            return buf;
        }

        return bufferType.allocate(DatabaseDescriptor.getCommitLogSegmentSize());
    }

    @Override
    protected void internalClose()
    {
        usedBuffers.decrementAndGet();

        try
        {
            if (bufferPool.size() < MAX_BUFFERPOOL_SIZE)
                bufferPool.add(buffer);
            else
                FileUtils.clean(buffer);
            super.internalClose();
        }
        finally
        {
            onClose.run();
        }
    }

    static void shutdown()
    {
        bufferPool.clear();
    }

    /**
     * Checks if the number of buffers in use is greater or equals to the maximum number of buffers allowed in the pool.
     *
     * @return <code>true</code> if the number of buffers in use is greater or equals to the maximum number of buffers
     * allowed in the pool, <code>false</code> otherwise.
     */
    static boolean hasReachedPoolLimit()
    {
        return usedBuffers.get() >= MAX_BUFFERPOOL_SIZE;
    }
}
