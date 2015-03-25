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

import java.net.InetAddress;
import java.util.concurrent.ConcurrentMap;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * Metrics for streaming.
 */
public class StreamingMetrics
{
    public static final String TYPE_NAME = "Streaming";

    private static final ConcurrentMap<InetAddress, StreamingMetrics> instances = new NonBlockingHashMap<InetAddress, StreamingMetrics>();

    public static final Counter activeStreamsOutbound = Metrics.newCounter(DefaultNameFactory.createMetricName(TYPE_NAME, "ActiveOutboundStreams", null));
    public static final Counter totalIncomingBytes = Metrics.newCounter(DefaultNameFactory.createMetricName(TYPE_NAME, "TotalIncomingBytes", null));
    public static final Counter totalOutgoingBytes = Metrics.newCounter(DefaultNameFactory.createMetricName(TYPE_NAME, "TotalOutgoingBytes", null));
    public final Counter incomingBytes;
    public final Counter outgoingBytes;

    public static StreamingMetrics get(InetAddress ip)
    {
       StreamingMetrics metrics = instances.get(ip);
       if (metrics == null)
       {
           metrics = new StreamingMetrics(ip);
           instances.put(ip, metrics);
       }
       return metrics;
    }

    public StreamingMetrics(final InetAddress peer)
    {
        MetricNameFactory factory = new DefaultNameFactory("Streaming", peer.getHostAddress().replace(':', '.'));
        incomingBytes = Metrics.newCounter(factory.createMetricName("IncomingBytes"));
        outgoingBytes= Metrics.newCounter(factory.createMetricName("OutgoingBytes"));
    }
}
