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


import com.codahale.metrics.Meter;
import org.apache.cassandra.exceptions.ReadAbortException;
import org.apache.cassandra.exceptions.ReadSizeAbortException;
import org.apache.cassandra.exceptions.TombstoneAbortException;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;


public class ClientRequestMetrics extends LatencyMetrics
{
    public final Meter timeouts;
    public final Meter unavailables;
    public final Meter failures;
    public final Meter aborts;
    public final Meter tombstoneAborts;
    public final Meter readSizeAborts;
    public final Meter localRequests;
    public final Meter remoteRequests;

    public ClientRequestMetrics(String scope)
    {
        super("ClientRequest", scope);

        timeouts = Metrics.meter(factory.createMetricName("Timeouts"));
        unavailables = Metrics.meter(factory.createMetricName("Unavailables"));
        failures = Metrics.meter(factory.createMetricName("Failures"));
        aborts = Metrics.meter(factory.createMetricName("Aborts"));
        tombstoneAborts = Metrics.meter(factory.createMetricName("TombstoneAborts"));
        readSizeAborts = Metrics.meter(factory.createMetricName("ReadSizeAborts"));
        localRequests = Metrics.meter(factory.createMetricName("LocalRequests"));
        remoteRequests = Metrics.meter(factory.createMetricName("RemoteRequests"));
    }

    public void markAbort(Throwable cause)
    {
        aborts.mark();
        if (!(cause instanceof ReadAbortException))
            return;
        if (cause instanceof TombstoneAbortException)
        {
            tombstoneAborts.mark();
        }
        else if (cause instanceof ReadSizeAbortException)
        {
            readSizeAborts.mark();
        }
    }

    public void release()
    {
        super.release();
        Metrics.remove(factory.createMetricName("Timeouts"));
        Metrics.remove(factory.createMetricName("Unavailables"));
        Metrics.remove(factory.createMetricName("Failures"));
        Metrics.remove(factory.createMetricName("Aborts"));
        Metrics.remove(factory.createMetricName("TombstoneAborts"));
        Metrics.remove(factory.createMetricName("ReadSizeAborts"));
        Metrics.remove(factory.createMetricName("LocalRequests"));
        Metrics.remove(factory.createMetricName("RemoteRequests"));
    }
}
