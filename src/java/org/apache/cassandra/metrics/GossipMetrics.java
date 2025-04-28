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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import org.apache.cassandra.gms.Gossiper;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class GossipMetrics
{
    public static final String TYPE_NAME = "Gossip";
    private static final MetricNameFactory factory = new DefaultNameFactory(TYPE_NAME);

    // Heartbeat of the current Node
    public final Gauge<Long> heartbeat;
    // How many live nodes the current node sees
    public final Gauge<Long> live;
    // How many unreachable nodes the current node sees
    public final Gauge<Long> unreachable;

    public static final Counter sendSynToUnreachable = Metrics.counter(factory.createMetricName("SendSynToUnreachable"));
    public static final Counter sendSynToSeed = Metrics.counter(factory.createMetricName("SendSynToSeed"));
    public static final Counter sendSynToCMS = Metrics.counter(factory.createMetricName("SendSynToCMS"));

    public GossipMetrics()
    {
        heartbeat = Metrics.register(factory.createMetricName("Heartbeat"), () -> (long) Gossiper.instance.getLocalHeartbeatNumber());
        live = Metrics.register(factory.createMetricName("Live"), () -> (long) Gossiper.instance.getLiveMembers().size());
        unreachable = Metrics.register(factory.createMetricName("Unreachable"), () -> (long) Gossiper.instance.getUnreachableMembers().size());
    }
}
