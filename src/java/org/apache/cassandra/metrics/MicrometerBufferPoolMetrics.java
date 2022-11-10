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

package org.apache.cassandra.metrics;

import com.codahale.metrics.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.apache.cassandra.utils.memory.BufferPool;

public class MicrometerBufferPoolMetrics extends MicrometerMetrics implements BufferPoolMetrics
{
    private static final String METRICS_PREFIX = "buffer_pool";

    public static final String TOTAL_SIZE_BYTES = METRICS_PREFIX + "_total_size_bytes";
    public static final String USED_SIZE_BYTES = METRICS_PREFIX + "_used_size_bytes";
    public static final String OVERFLOW_SIZE_BYTES = METRICS_PREFIX + "_overflow_size_bytes";
    public static final String OVERFLOW_ALLOCATIONS = METRICS_PREFIX + "_overflow_allocations";
    public static final String POOL_ALLOCATIONS = METRICS_PREFIX + "_pool_allocations";
    public static final String NAME_TAG = "pool_name";

    private final String scope;
    private final BufferPool bufferPool;

    /** Total number of hits */
    private final Meter hits;

    /** Total number of misses */
    private final Meter misses;

    public MicrometerBufferPoolMetrics(String scope, BufferPool bufferPool)
    {
        super();

        this.scope = scope;
        this.bufferPool = bufferPool;
        this.hits = new Meter();
        this.misses = new Meter();
    }

    @Override
    public synchronized void register(MeterRegistry newRegistry, Tags newTags)
    {
        super.register(newRegistry, newTags.and(NAME_TAG, scope));

        gauge(TOTAL_SIZE_BYTES, bufferPool, BufferPool::sizeInBytes);
        gauge(USED_SIZE_BYTES, bufferPool, BufferPool::usedSizeInBytes);
        gauge(OVERFLOW_SIZE_BYTES, bufferPool, BufferPool::overflowMemoryInBytes);
        gauge(OVERFLOW_ALLOCATIONS, misses, Meter::getMeanRate);
        gauge(POOL_ALLOCATIONS, hits, Meter::getMeanRate);
    }

    @Override
    public void markHit()
    {
        hits.mark();
    }

    @Override
    public long hits()
    {
        return hits.getCount();
    }

    public void markMissed()
    {
        misses.mark();
    }

    @Override
    public long misses()
    {
        return misses.getCount();
    }

    @Override
    public long overflowSize()
    {
        return bufferPool.overflowMemoryInBytes();
    }

    @Override
    public long usedSize()
    {
        return bufferPool.usedSizeInBytes();
    }

    @Override
    public long size()
    {
        return bufferPool.sizeInBytes();
    }

    @Override
    public void register3xAlias()
    {
        // Not implemented
    }
}
