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

package org.apache.cassandra.service.throttler.dynamic.metrics;

import org.apache.cassandra.metrics.AbstractMetricsManager;

public class KeyspaceThrottlingMetricsManager extends AbstractMetricsManager<String, KeyspaceThrottlingMetrics>
{
    private final static KeyspaceThrottlingMetricsManager instance = new KeyspaceThrottlingMetricsManager();

    @Override
    protected KeyspaceThrottlingMetrics createMetric(String ksName)
    {
        return new KeyspaceThrottlingMetrics(ksName);
    }

    @Override
    protected String buildKey(Object... parts)
    {
        if (parts.length != 1)
            throw new IllegalArgumentException("Expected 1 argument: keyspace name");
        return (String) parts[0];
    }

    public static KeyspaceThrottlingMetrics getMetrics(String ksName)
    {
        return instance.getMetricsSync(ksName);
    }
}
