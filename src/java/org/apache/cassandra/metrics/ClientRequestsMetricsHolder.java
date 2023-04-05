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

import org.apache.cassandra.db.ConsistencyLevel;

public final class ClientRequestsMetricsHolder
{
    public static final ClientRequestMetrics readMetrics = new ClientRequestMetrics("Read");
    public static final ClientWriteRequestMetrics writeMetrics = new ClientWriteRequestMetrics("Write");
    public static final CASClientWriteRequestMetrics casWriteMetrics = new CASClientWriteRequestMetrics("CASWrite");
    public static final CASClientRequestMetrics casReadMetrics = new CASClientRequestMetrics("CASRead");
    public static final ViewWriteMetrics viewWriteMetrics = new ViewWriteMetrics("ViewWrite");

    public static final Map<ConsistencyLevel, ClientRequestMetrics> readMetricsMap = new EnumMap<>(ConsistencyLevel.class);
    public static final Map<ConsistencyLevel, ClientWriteRequestMetrics> writeMetricsMap = new EnumMap<>(ConsistencyLevel.class);

    static
    {
        for (ConsistencyLevel level : ConsistencyLevel.values())
        {
             readMetricsMap.put(level, new ClientRequestMetrics("Read-" + level.name()));
            writeMetricsMap.put(level, new ClientWriteRequestMetrics("Write-" + level.name()));
        }
    }

    public static ClientRequestMetrics readMetricsForLevel(ConsistencyLevel level)
    {
        return readMetricsMap.get(level);
    }

    public static ClientWriteRequestMetrics writeMetricsForLevel(ConsistencyLevel level)
    {
        return writeMetricsMap.get(level);
    }
}