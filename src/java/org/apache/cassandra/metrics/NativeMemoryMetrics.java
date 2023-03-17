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

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;

import io.netty.util.internal.PlatformDependent;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.memory.MemoryUtil;

public interface NativeMemoryMetrics
{
    BufferPoolMXBean directBufferPool = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)
                                                         .stream()
                                                         .filter(bpMBean -> bpMBean.getName().equals("direct"))
                                                         .findFirst()
                                                         .orElse(null);

    NativeMemoryMetrics instance = CassandraRelevantProperties.USE_MICROMETER.getBoolean()
                                   ? new MicrometerNativeMemoryMetrics()
                                   : new CodehaleNativeMemoryMetrics();

    long usedNioDirectMemoryValue();

    default long rawNativeMemory()
    {
        return MemoryUtil.allocated();
    }

    default long bloomFilterMemory()
    {
        return BloomFilter.memoryLimiter.memoryAllocated();
    }

    default long usedNioDirectMemory()
    {
        return directBufferPool == null ? 0 : directBufferPool.getMemoryUsed();
    }

    default long totalNioDirectMemory()
    {
        return directBufferPool == null ? 0 : directBufferPool.getTotalCapacity();
    }

    default long nioDirectBufferCount()
    {
        return directBufferPool == null ? 0 : directBufferPool.getCount();
    }

    default long networkDirectMemory()
    {
        return PlatformDependent.usedDirectMemory();
    }

    default boolean usingNioMemoryForNetwork()
    {
        return !PlatformDependent.useDirectBufferNoCleaner();
    }

    default long totalMemory()
    {
        // Use totalNioDirectMemory() instead of usedNioDirectMemory() because without
        // -Dsun.nio.PageAlignDirectMemory=true the two are identical. If someone adds
        // this flag again, we would prefer to include the JVM padding in the total memory.
        // Also only add the network memory if it's not allocated as NIO direct memory
        return rawNativeMemory() + totalNioDirectMemory() + (usingNioMemoryForNetwork() ? 0 : networkDirectMemory());
    }
}
