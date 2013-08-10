/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;

public class ClientRequestMetrics extends LatencyMetrics
{
    @Deprecated public static final Counter readTimeouts = CassandraMetricRegistry.get().counter(MetricRegistry.name(ClientRequestMetrics.class, "ReadTimeouts"));
    @Deprecated public static final Counter writeTimeouts = CassandraMetricRegistry.get().counter(MetricRegistry.name(ClientRequestMetrics.class, "WriteTimeouts"));
    @Deprecated public static final Counter readUnavailables = CassandraMetricRegistry.get().counter(MetricRegistry.name(ClientRequestMetrics.class, "ReadUnavailables"));
    @Deprecated public static final Counter writeUnavailables = CassandraMetricRegistry.get().counter(MetricRegistry.name(ClientRequestMetrics.class, "WriteUnavailables"));

    public final Meter timeouts;
    public final Meter unavailables;

    public ClientRequestMetrics(String scope)
    {
        super("org.apache.cassandra.metrics", "ClientRequest", scope);

        timeouts = CassandraMetricRegistry.get().meter(factory.createMetricName("Timeouts"));
        unavailables = CassandraMetricRegistry.get().meter(factory.createMetricName("Unavailables"));
    }

    public void release()
    {
        super.release();
        CassandraMetricRegistry.unregister(factory.createMetricName("Timeouts"));
        CassandraMetricRegistry.unregister(factory.createMetricName("Unavailables"));
    }
}
