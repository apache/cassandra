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
package org.apache.cassandra.index.sai.metrics;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public abstract class AbstractMetrics
{
    public static final String TYPE = "StorageAttachedIndex";

    protected final String keyspace;
    protected final String table;
    private final String index;
    private final String scope;
    protected final List<CassandraMetricsRegistry.MetricName> tracked = new ArrayList<>();

    AbstractMetrics(IndexIdentifier indexIdentifier, String scope)
    {
        this(indexIdentifier.keyspaceName, indexIdentifier.tableName, indexIdentifier.indexName, scope);
    }

    AbstractMetrics(String keyspace, String table, String scope)
    {
        this(keyspace, table, null, scope);
    }

    AbstractMetrics(String keyspace, String table, String index, String scope)
    {
        assert keyspace != null && table != null : "SAI metrics must include keyspace and table";
        this.keyspace = keyspace;
        this.table = table;
        this.index = index;
        this.scope = scope;
    }

    public void release()
    {
        tracked.forEach(Metrics::remove);
        tracked.clear();
    }

    protected CassandraMetricsRegistry.MetricName createMetricName(String name)
    {
        return createMetricName(name, scope);
    }

    protected CassandraMetricsRegistry.MetricName createMetricName(String name, String scope)
    {
        String metricScope = keyspace + '.' + table;
        if (index != null)
        {
            metricScope += '.' + index;
        }
        metricScope += '.' + scope + '.' + name;

        CassandraMetricsRegistry.MetricName metricName = new CassandraMetricsRegistry.MetricName(DefaultNameFactory.GROUP_NAME,
                                                                                                 TYPE, name, metricScope, createMBeanName(name, scope));
        tracked.add(metricName);
        return metricName;
    }

    private String createMBeanName(String name, String scope)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(DefaultNameFactory.GROUP_NAME);
        builder.append(":type=").append(TYPE);
        builder.append(',').append("keyspace=").append(keyspace);
        builder.append(',').append("table=").append(table);
        if (index != null)
            builder.append(',').append("index=").append(index);
        builder.append(',').append("scope=").append(scope);
        builder.append(',').append("name=").append(name);
        return builder.toString();
    }
}
