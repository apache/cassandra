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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.internal.PlatformDependent;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.assertj.core.api.Assertions.assertThat;

public class NettyMemoryMetricsTest
{
    @BeforeClass
    public static void setup() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();
        NettyMemoryMetrics.register();
    }

    @Test
    public void testGaugesAreRegisteredAndReflectPlatformDependent()
    {
        assertThat(gauge(NettyMemoryMetrics.USED_DIRECT_MEMORY).getValue())
            .isEqualTo(PlatformDependent.usedDirectMemory());
        assertThat(gauge(NettyMemoryMetrics.DIRECT_MEMORY_LIMIT).getValue())
            .isEqualTo(PlatformDependent.maxDirectMemory());
    }

    @Test
    public void testRegisterIsIdempotent()
    {
        Gauge<Long> before = gauge(NettyMemoryMetrics.USED_DIRECT_MEMORY);

        NettyMemoryMetrics.register();

        // Re-registering must not replace or duplicate the existing gauge.
        assertThat(gauge(NettyMemoryMetrics.USED_DIRECT_MEMORY)).isSameAs(before);
    }

    @Test
    public void testUsedDirectMemoryTracksNettyAllocations()
    {
        long before = gauge(NettyMemoryMetrics.USED_DIRECT_MEMORY).getValue();

        // Skip when Netty cannot account for direct memory (no-cleaner path unavailable), where -1 is reported.
        if (before < 0)
            return;

        // Allocate well beyond a single chunk so the arena has to grow rather than serve from an existing one.
        PooledByteBufAllocator allocator = new PooledByteBufAllocator(true);
        ByteBuf buf = allocator.directBuffer(64 * 1024 * 1024);
        try
        {
            assertThat(gauge(NettyMemoryMetrics.USED_DIRECT_MEMORY).getValue()).isGreaterThan(before);
        }
        finally
        {
            buf.release();
        }
    }

    @Test
    public void testLimitIsPositiveWhenAccountingIsAvailable()
    {
        // Netty falls back to Runtime.maxMemory() when -XX:MaxDirectMemorySize is absent, so the limit should always
        // be a usable positive number in a normal JVM.
        assertThat(gauge(NettyMemoryMetrics.DIRECT_MEMORY_LIMIT).getValue()).isGreaterThan(0L);
    }

    @SuppressWarnings("unchecked")
    private static Gauge<Long> gauge(String name)
    {
        String metricName = new DefaultNameFactory(NettyMemoryMetrics.TYPE_NAME).createMetricName(name).getMetricName();
        Gauge<Long> gauge = (Gauge<Long>) Metrics.getGauges().get(metricName);
        assertThat(gauge).as(metricName).isNotNull();
        return gauge;
    }
}
