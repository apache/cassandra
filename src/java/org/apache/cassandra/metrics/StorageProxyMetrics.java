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

import com.google.common.collect.ImmutableMap;

import com.codahale.metrics.Meter;
import org.apache.cassandra.config.Config.CLEnforcementLevel;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class StorageProxyMetrics
{
    private KeyspaceConsistencyLevelMetricsFactory factory;

    public ClientRequestMetrics readMetrics;
    public ClientRequestMetrics rangeMetrics;
    public ClientWriteRequestMetrics writeMetrics;
    public CASClientWriteRequestMetrics casWriteMetrics;
    public CASClientRequestMetrics casReadMetrics;
    public ViewWriteMetrics viewWriteMetrics;
    public Map<CLEnforcementLevel, Meter> writeCLEnforcementMeter = new EnumMap<>(CLEnforcementLevel.class);



    public void markWriteCLEnforced(CLEnforcementLevel level)
    {
        writeCLEnforcementMeter.get(level).mark();
    }

    public StorageProxyMetrics(String ksName, String consistency) {
        factory = new KeyspaceConsistencyLevelMetricsFactory(ksName, consistency);

        readMetrics = new ClientRequestMetrics(factory, "StorageProxyRead");
        rangeMetrics = new ClientRequestMetrics(factory, "StorageProxyRangeSlice");
        writeMetrics = new ClientWriteRequestMetrics(factory, "StorageProxyWrite");
        casWriteMetrics = new CASClientWriteRequestMetrics(factory, "StorageProxyCASWrite");
        casReadMetrics = new CASClientRequestMetrics(factory, "StorageProxyCASRead");
        viewWriteMetrics = new ViewWriteMetrics(factory, "StorageProxyViewWrite");
        for (CLEnforcementLevel level : CLEnforcementLevel.values())
        {
            writeCLEnforcementMeter.put(level, Metrics.meter(factory.createMetricName("StorageProxyWriteCLEnforced", ImmutableMap.of("enforcement", level.name()))));
        }
    }
}
