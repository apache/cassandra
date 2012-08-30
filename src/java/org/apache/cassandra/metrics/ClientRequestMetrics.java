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

import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

public class ClientRequestMetrics extends LatencyMetrics
{
    @Deprecated public static final Counter readTimeouts = Metrics.newCounter(ClientRequestMetrics.class, "ReadTimeouts");
    @Deprecated public static final Counter writeTimeouts = Metrics.newCounter(ClientRequestMetrics.class, "WriteTimeouts");
    @Deprecated public static final Counter readUnavailables = Metrics.newCounter(ClientRequestMetrics.class, "ReadUnavailables");
    @Deprecated public static final Counter writeUnavailables = Metrics.newCounter(ClientRequestMetrics.class, "WriteUnavailables");

    public final Meter timeouts;
    public final Meter unavailables;

    public ClientRequestMetrics(String scope)
    {
        super("org.apache.cassandra.metrics", "ClientRequest", scope);

        timeouts = Metrics.newMeter(factory.createMetricName("Timeouts"), "timeouts", TimeUnit.SECONDS);
        unavailables = Metrics.newMeter(factory.createMetricName("Unavailables"), "unavailables", TimeUnit.SECONDS);
    }

    public void release()
    {
        super.release();
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("Timeouts"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("Unavailables"));
    }
}
