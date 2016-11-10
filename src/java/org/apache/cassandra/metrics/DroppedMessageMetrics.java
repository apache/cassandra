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

import com.codahale.metrics.Meter;
import org.apache.cassandra.net.MessagingService;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for dropped messages by verb.
 */
public class DroppedMessageMetrics
{
    /** Number of dropped messages */
    public final Meter dropped;

    public DroppedMessageMetrics(MessagingService.Verb verb)
    {
        MetricNameFactory factory = new DefaultNameFactory("DroppedMessage", verb.toString());
        dropped = Metrics.meter(factory.createMetricName("Dropped"));
    }
}
