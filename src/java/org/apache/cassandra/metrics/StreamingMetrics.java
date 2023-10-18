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

import java.util.concurrent.ConcurrentMap;


import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import org.apache.cassandra.locator.InetAddressAndPort;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for streaming.
 */
public class StreamingMetrics
{
    public static final String TYPE_NAME = "Streaming";

    private static final ConcurrentMap<InetAddressAndPort, StreamingMetrics> instances = new NonBlockingHashMap<>();

    /** @deprecated See CASSANDRA-11174 */
    @Deprecated(since = "4.0")
    public static final Counter activeStreamsOutbound = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "ActiveOutboundStreams", null));
    public static final Counter totalIncomingBytes = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "TotalIncomingBytes", null));
    public static final Counter totalOutgoingBytes = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "TotalOutgoingBytes", null));
    public static final Counter totalOutgoingRepairBytes = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "TotalOutgoingRepairBytes", null));
    public static final Counter totalOutgoingRepairSSTables = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "TotalOutgoingRepairSSTables", null));
    public final Counter incomingBytes;
    public final Counter outgoingBytes;
    /* Measures the time taken for processing the incoming stream message after being deserialized, including the time to flush to disk. */
    public final Timer incomingProcessTime;
    private final Counter entireSSTablesStreamedIn;
    private final Counter partialSSTablesStreamedIn;

    public static StreamingMetrics get(InetAddressAndPort ip)
    {
       /*
         computeIfAbsent doesn't work for this situation. Since JMX metrics register themselves in their ctor, we need
         to create the metric exactly once, otherwise we'll get duplicate name exceptions. Although computeIfAbsent is
         thread safe in the context of the map, it uses compare and swap to add the computed value to the map. This
         means it eagerly allocates new metric instances, which can cause the jmx name collision we're trying to avoid
         if multiple calls interleave. So here we use synchronized to ensure we only instantiate metrics exactly once.
        */
       StreamingMetrics metrics = instances.get(ip);
       if (metrics == null)
       {
           synchronized (instances)
           {
               metrics = instances.get(ip);
               if (metrics == null)
               {
                   metrics = new StreamingMetrics(ip);
                   instances.put(ip, metrics);
               }
           }
       }
       return metrics;
    }

    public StreamingMetrics(final InetAddressAndPort peer)
    {
        MetricNameFactory factory = new DefaultNameFactory("Streaming", peer.toString().replace(':', '.'));
        incomingBytes = Metrics.counter(factory.createMetricName("IncomingBytes"));
        outgoingBytes= Metrics.counter(factory.createMetricName("OutgoingBytes"));
        incomingProcessTime = Metrics.timer(factory.createMetricName("IncomingProcessTime"));

        entireSSTablesStreamedIn = Metrics.counter(factory.createMetricName("EntireSSTablesStreamedIn"));
        partialSSTablesStreamedIn = Metrics.counter(factory.createMetricName("PartialSSTablesStreamedIn"));
    }

    public void countStreamedIn(boolean isEntireSSTable)
    {
        (isEntireSSTable ? entireSSTablesStreamedIn : partialSSTablesStreamedIn).inc();
    }
}
