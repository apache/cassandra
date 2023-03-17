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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.utils.memory.MemoryUtil;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class CodehaleNativeMemoryMetrics implements NativeMemoryMetrics
{
    private static final Logger logger = LoggerFactory.getLogger(CodehaleNativeMemoryMetrics.class);

    private final MetricNameFactory factory;

    /** Total size of memory allocated directly by calling Native.malloc via {@link MemoryUtil}, bypassing the JVM.
     * This is in addition to nio direct memory, for example off-heap memtables will use this type of memory. */
    private final Gauge<Long> rawNativeMemory;

    /**
     * Total size of memory used by bloom filters, part of {@link this#rawNativeMemory}.
     */
    private final Gauge<Long> bloomFilterMemory;

    /** The memory allocated for direct byte buffers, aligned or otherwise, without counting any padding due to alignment.
     *  If {@code -Dio.netty.directMemory} is not set to {@code 0}, the direct memory used by Netty is <em>not</em> included in this value. */
    private final Gauge<Long> usedNioDirectMemory;

    /** The total memory allocated for direct byte buffers, aligned or otherwise, including any padding due to alignment.
     * If -Dsun.nio.PageAlignDirectMemory=true is not set then this will be identical to usedNioDirectMemory.
     * If {@code -Dio.netty.directMemory} is not set to {@code 0}, the direct memory used by Netty is <em>not</em> included in this value. */
    private final Gauge<Long> totalNioDirectMemory;

    /** The memory used by direct byte buffers allocated via the Netty library. These buffers are used for network communications.
     * A limit can be set with "-Dio.netty.maxDirectMemory". When this property is zero (the default in jvm.options), then
     * Netty will use the JVM NIO direct memory. Therefore, this value will be included in {@link #usedNioDirectMemory}
     * and {@link #totalNioDirectMemory} only when the property is set to zero, otherwise this value is extra. */
    private final Gauge<Long> networkDirectMemory;

    /** The total number of direct byte buffers allocated, aligned or otherwise. */
    private final Gauge<Long> nioDirectBufferCount;

    /** The total memory allocated, including direct byte buffers, network direct memory, and raw malloc memory */
    private final Gauge<Long> totalMemory;

    public CodehaleNativeMemoryMetrics()
    {
        factory = new DefaultNameFactory("NativeMemory");

        if (directBufferPool == null)
            logger.error("Direct memory buffer pool MBean not present, native memory metrics will be missing for nio buffers");

        rawNativeMemory = Metrics.register(factory.createMetricName("RawNativeMemory"), this::rawNativeMemory);
        bloomFilterMemory = Metrics.register(factory.createMetricName("BloomFilterMemory"), this::bloomFilterMemory);
        usedNioDirectMemory = Metrics.register(factory.createMetricName("UsedNioDirectMemory"), this::usedNioDirectMemory);
        totalNioDirectMemory = Metrics.register(factory.createMetricName("TotalNioDirectMemory"), this::totalNioDirectMemory);
        networkDirectMemory = Metrics.register(factory.createMetricName("NetworkDirectMemory"), this::networkDirectMemory);
        nioDirectBufferCount = Metrics.register(factory.createMetricName("NioDirectBufferCount"), this::nioDirectBufferCount);
        totalMemory = Metrics.register(factory.createMetricName("TotalMemory"), this::totalMemory);
    }

    @Override
    public long usedNioDirectMemoryValue()
    {
        return usedNioDirectMemory.getValue();
    }
}
