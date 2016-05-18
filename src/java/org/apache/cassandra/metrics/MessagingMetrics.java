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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for messages
 */
public class MessagingMetrics
{
    private static Logger logger = LoggerFactory.getLogger(MessagingMetrics.class);
    private static final MetricNameFactory factory = new DefaultNameFactory("Messaging");
    public final Timer crossNodeLatency;
    public final ConcurrentHashMap<String, Timer> dcLatency;

    public MessagingMetrics()
    {
        crossNodeLatency = Metrics.timer(factory.createMetricName("CrossNodeLatency"));
        dcLatency = new ConcurrentHashMap<>();
    }

    public void addTimeTaken(InetAddress from, long timeTaken)
    {
        String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(from);
        Timer timer = dcLatency.get(dc);
        if (timer == null)
        {
            timer = dcLatency.computeIfAbsent(dc, k -> Metrics.timer(factory.createMetricName(dc + "-Latency")));
        }
        timer.update(timeTaken, TimeUnit.MILLISECONDS);
        crossNodeLatency.update(timeTaken, TimeUnit.MILLISECONDS);
    }
}
