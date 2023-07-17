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
package org.apache.cassandra.index.sai.utils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * A simple, thread-safe memory usage tracker, named to reflect a particular scope.
 */
@ThreadSafe
public final class SegmentMemoryLimiter
{
    private static final Logger logger = LoggerFactory.getLogger(SegmentMemoryLimiter.class);

    /**
     * Global limit on heap consumed by all index segment building that occurs outside the context of Memtable flush.
     * <p>
     * Note that to avoid flushing small index segments, a segment is only flushed when
     * both the global size of all building segments has breached the limit and the size of the
     * segment in question reaches (segment_write_buffer_space_mb / # currently building column indexes).
     * <p>
     * ex. If there is only one column index building, it can buffer up to segment_write_buffer_space_mb.
     * <p>
     * ex. If there is one column index building per table across 8 compactors, each index will be
     *     eligible to flush once it reaches (segment_write_buffer_space_mb / 8) MBs.
     */
    public static final long DEFAULT_SEGMENT_BUILD_MEMORY_LIMIT = DatabaseDescriptor.getSAISegmentWriteBufferSpace().toBytes();

    private static final AtomicLong bytesUsed = new AtomicLong(0);

    private static volatile long limitBytes = DEFAULT_SEGMENT_BUILD_MEMORY_LIMIT;

    private static volatile long minimumFlushSize = 0;

    /** The number of column indexes being built globally. (Starts at one to avoid divide by zero.) */
    private static final AtomicInteger activeBuilderCount = new AtomicInteger(0);

    public static int getActiveBuilderCount()
    {
        return activeBuilderCount.get();
    }

    public static void registerBuilder()
    {
        minimumFlushSize =  limitBytes / activeBuilderCount.incrementAndGet();
    }

    public static void unregisterBuilder()
    {
        int builders = activeBuilderCount.decrementAndGet();
        minimumFlushSize = builders == 0 ? limitBytes : limitBytes / builders;
    }

    /**
     * @return true if the current number of bytes allocated against the tracker has breached the limit, false otherwise
     */
    public static boolean usageExceedsLimit(long bytesUsed)
    {
        return currentBytesUsed() > limitBytes && bytesUsed > minimumFlushSize;
    }

    public static long increment(long bytes)
    {
        if (logger.isTraceEnabled())
            logger.trace("Incrementing tracked memory usage by {} bytes from current usage of {}...", bytes, currentBytesUsed());
        return bytesUsed.addAndGet(bytes);
    }

    public static long decrement(long bytes)
    {
        if (logger.isTraceEnabled())
            logger.trace("Decrementing tracked memory usage by {} bytes from current usage of {}...", bytes, currentBytesUsed());
        return bytesUsed.addAndGet(-bytes);
    }

    public static long currentBytesUsed()
    {
        return bytesUsed.get();
    }
    
    public static long limitBytes()
    {
        return limitBytes;
    }

    @VisibleForTesting
    public static void setLimitBytes(long bytes)
    {
        limitBytes = bytes;
    }

    @VisibleForTesting
    public static void reset()
    {
        bytesUsed.set(0);
        limitBytes = DEFAULT_SEGMENT_BUILD_MEMORY_LIMIT;
        activeBuilderCount.set(0);
    }
}
