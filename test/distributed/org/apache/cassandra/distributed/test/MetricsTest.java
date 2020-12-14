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

package org.apache.cassandra.distributed.test;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;

public class MetricsTest extends TestBaseImpl
{
    @Test
    public void testMetrics() throws Throwable
    {
        try (ICluster<IInvokableInstance> cluster = init(builder().withNodes(1)
                                                                  .start()))
        {
            cluster.get(1).runOnInstance(() -> {
                CassandraMetricsRegistry.Metrics.counter("test_counter").inc(100);
                CassandraMetricsRegistry.Metrics.counter("test_counter_2").inc(101);
            });

            Assert.assertEquals(100, cluster.get(1).metrics().getCounter("test_counter"));
            Assert.assertEquals(ImmutableMap.of("test_counter", 100L,
                                                "test_counter_2", 101L),
                                cluster.get(1).metrics().getCounters(s -> s.startsWith("test_counter")));
        }
    }
}
