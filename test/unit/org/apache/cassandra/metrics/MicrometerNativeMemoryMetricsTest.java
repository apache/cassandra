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

import org.junit.Test;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MicrometerNativeMemoryMetricsTest
{
    @Test
    public void testRegisteringNativeMetrics()
    {
        MicrometerNativeMemoryMetrics metrics = new MicrometerNativeMemoryMetrics();

        MeterRegistry registry = mock(MeterRegistry.class);
        Tags tags = Tags.of("tag_key", "tag_value");
        metrics.register(registry, tags);

        verify(registry).gauge(eq(MicrometerNativeMemoryMetrics.RAW_NATIVE_MEMORY), eq(tags), any(), any());
        verify(registry).gauge(eq(MicrometerNativeMemoryMetrics.TOTAL_MEMORY), eq(tags), any(), any());
        verify(registry).gauge(eq(MicrometerNativeMemoryMetrics.BLOOM_FILTER_MEMORY), eq(tags), any(), any());
        verify(registry).gauge(eq(MicrometerNativeMemoryMetrics.NETWORK_DIRECT_MEMORY), eq(tags), any(), any());
        verify(registry).gauge(eq(MicrometerNativeMemoryMetrics.TOTAL_NIO_MEMORY), eq(tags), any(), any());
        verify(registry).gauge(eq(MicrometerNativeMemoryMetrics.NIO_DIRECT_BUFFER_COUNT), eq(tags), any(), any());
        verify(registry).gauge(eq(MicrometerNativeMemoryMetrics.USED_NIO_DIRECT_MEMORY), eq(tags), any(), any());
    }
}