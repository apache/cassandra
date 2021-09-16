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

import java.util.EnumMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.ConsistencyLevel;

public class ClientRequestsMetrics
{
    public final ClientRequestMetrics readMetrics;
    public final ClientRangeRequestMetrics rangeMetrics;
    public final ClientWriteRequestMetrics writeMetrics;
    public final CASClientWriteRequestMetrics casWriteMetrics;
    public final CASClientRequestMetrics casReadMetrics;
    public final ViewWriteMetrics viewWriteMetrics;
    private final Map<ConsistencyLevel, ClientRequestMetrics> readMetricsMap;
    private final Map<ConsistencyLevel, ClientWriteRequestMetrics> writeMetricsMap;

    public ClientRequestsMetrics()
    {
        this("");
    }

    /**
     * CassandraMetricsRegistry requires unique metrics name, otherwise it returns previous metrics.
     * CNDB will create coordinator metrics with unique name prefix for each tenant
     */
    public ClientRequestsMetrics(String namePrefix)
    {
        readMetrics = new ClientRequestMetrics("Read", namePrefix);
        rangeMetrics = new ClientRangeRequestMetrics("RangeSlice", namePrefix);
        writeMetrics = new ClientWriteRequestMetrics("Write", namePrefix);
        casWriteMetrics = new CASClientWriteRequestMetrics("CASWrite", namePrefix);
        casReadMetrics = new CASClientRequestMetrics("CASRead", namePrefix);
        viewWriteMetrics = new ViewWriteMetrics("ViewWrite", namePrefix);
        readMetricsMap = new EnumMap<>(ConsistencyLevel.class);
        writeMetricsMap = new EnumMap<>(ConsistencyLevel.class);
        for (ConsistencyLevel level : ConsistencyLevel.values())
        {
            readMetricsMap.put(level, new ClientRequestMetrics("Read-" + level.name(), namePrefix));
            writeMetricsMap.put(level, new ClientWriteRequestMetrics("Write-" + level.name(), namePrefix));
        }
    }

    public ClientRequestMetrics readMetricsForLevel(ConsistencyLevel level)
    {
        return readMetricsMap.get(level);
    }

    public ClientWriteRequestMetrics writeMetricsForLevel(ConsistencyLevel level)
    {
        return writeMetricsMap.get(level);
    }

    /**
     * When we want to reset metrics, say in a test env, it is not enough to create a new {@link ClientRequestsMetrics}
     * object because the internal histograms would be initialized with the already registered, existing instances.
     * In order to unregister and make the constructor really create new metrics histograms, we need to call this method
     * on the old instance first.
     */
    @VisibleForTesting
    public void release()
    {
        readMetrics.release();
        rangeMetrics.release();
        writeMetrics.release();
        casWriteMetrics.release();
        casReadMetrics.release();
        readMetricsMap.values().forEach(ClientRequestMetrics::release);
        writeMetricsMap.values().forEach(ClientRequestMetrics::release);
    }
}