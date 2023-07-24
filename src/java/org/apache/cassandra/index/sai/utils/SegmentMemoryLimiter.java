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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A simple, thread-safe memory usage tracker, named to reflect a particular scope.
 */
@ThreadSafe
public class SegmentMemoryLimiter
{
    private static final Logger logger = LoggerFactory.getLogger(SegmentMemoryLimiter.class);

    public static final SegmentMemoryLimiter instance =
        CassandraRelevantProperties.SAI_TEST_MEMORY_LIMITER_IMPLEMENTATION.isPresent()
        ? FBUtilities.construct(CassandraRelevantProperties.SAI_TEST_MEMORY_LIMITER_IMPLEMENTATION.getString(), "Test Memory Limiter")
        : new SegmentMemoryLimiter();

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
    protected static final long SEGMENT_BUILD_MEMORY_LIMIT = DatabaseDescriptor.getSAISegmentWriteBufferSpace().toBytes();

    protected volatile long minimumFlushSize = 0;

    protected final AtomicLong bytesUsed = new AtomicLong(0);

    // The number of column indexes being built globally.
    protected final AtomicInteger activeBuilderCount = new AtomicInteger(0);

    public int getActiveBuilderCount()
    {
        return activeBuilderCount.get();
    }

    public void registerBuilder()
    {
        minimumFlushSize =  limitBytes() / activeBuilderCount.incrementAndGet();
    }

    public void unregisterBuilder()
    {
        int builders = activeBuilderCount.decrementAndGet();
        minimumFlushSize = builders == 0 ? limitBytes() : limitBytes() / builders;
    }

    /**
     * @return true if the current number of bytes allocated against the tracker has breached the limit, false otherwise
     */
    public boolean usageExceedsLimit(long bytesUsed)
    {
        return currentBytesUsed() > limitBytes() && bytesUsed > minimumFlushSize;
    }

    public long increment(long bytes)
    {
        if (logger.isTraceEnabled())
            logger.trace("Incrementing tracked memory usage by {} bytes from current usage of {}...", bytes, currentBytesUsed());
        return bytesUsed.addAndGet(bytes);
    }

    public long decrement(long bytes)
    {
        if (logger.isTraceEnabled())
            logger.trace("Decrementing tracked memory usage by {} bytes from current usage of {}...", bytes, currentBytesUsed());
        return bytesUsed.addAndGet(-bytes);
    }

    public long currentBytesUsed()
    {
        return bytesUsed.get();
    }
    
    public long limitBytes()
    {
        return SEGMENT_BUILD_MEMORY_LIMIT;
    }
}
