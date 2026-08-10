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

import com.codahale.metrics.Gauge;

import io.netty.util.internal.PlatformDependent;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Exposes the direct (off-heap) memory that Netty accounts for internally, process-wide.
 *
 * <p>Netty allocates direct memory through {@code Unsafe.allocateMemory} (the "no cleaner" path, enabled in Cassandra
 * by {@code -Dio.netty.tryReflectionSetAccessible=true}), which bypasses {@code java.nio.Bits} entirely. Netty
 * therefore maintains its own counter and its own limit, independent of the JDK's direct memory accounting. Both
 * budgets default to {@code -XX:MaxDirectMemorySize}, so the two can each be filled independently and the process can
 * hold considerably more direct memory than that setting alone suggests.
 *
 * <p>Consequences for interpreting these metrics:
 * <ul>
 *     <li>{@link #USED_DIRECT_MEMORY} aggregates across every Netty allocator in the process, but only covers memory
 *     Netty itself allocated. It does <em>not</em> include Cassandra's {@code BufferPool}, which calls
 *     {@code ByteBuffer.allocateDirect} directly and is reported separately by {@link BufferPoolMetrics}. Internode
 *     messaging, streaming and native protocol v5+ channel buffers all go through {@code BufferPool}, not here.</li>
 *     <li>The JDK-side counterpart ({@code java.nio.Bits}) is reported by {@code nodetool gcstats} as the allocated /
 *     max / reserved direct memory values. That bucket and this one are disjoint; a complete picture requires both.</li>
 * </ul>
 *
 * <p>Both gauges report {@code -1} when Netty's counter is unavailable, which happens when the no-cleaner path is
 * disabled.
 */
public final class NettyMemoryMetrics
{
    public static final String TYPE_NAME = "NettyMemory";

    /**
     * Direct memory currently allocated by Netty and not yet freed, in bytes. Reflects what Netty has reserved from
     * the OS rather than what is actively in use, since pooled arenas retain chunks for reuse.
     */
    public static final String USED_DIRECT_MEMORY = "UsedDirectMemory";

    /**
     * The direct memory ceiling Netty enforces against {@link #USED_DIRECT_MEMORY}, in bytes. Derived from
     * {@code -Dio.netty.maxDirectMemory} when set, otherwise from {@code -XX:MaxDirectMemorySize}. Exceeding it raises
     * Netty's {@code OutOfDirectMemoryError}, which is distinct from the JDK's
     * {@code OutOfMemoryError: Direct buffer memory}.
     */
    public static final String DIRECT_MEMORY_LIMIT = "DirectMemoryLimit";

    private NettyMemoryMetrics()
    {
    }
    
    public static void register()
    {
        MetricNameFactory factory = new DefaultNameFactory(TYPE_NAME);

        Metrics.register(factory.createMetricName(USED_DIRECT_MEMORY),
                         (Gauge<Long>) PlatformDependent::usedDirectMemory);
        Metrics.register(factory.createMetricName(DIRECT_MEMORY_LIMIT),
                         (Gauge<Long>) PlatformDependent::maxDirectMemory);
    }
}
