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

package org.apache.cassandra.index.accord;

import java.util.ArrayList;
import java.util.List;

import com.codahale.metrics.Timer;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

// Stolen from org.apache.cassandra.index.sai.metrics.AbstractMetrics
public class IndexMetrics
{
    public static final String TYPE = "RouteIndex";
    private static final String SCOPE = "IndexMetrics";

    private final List<CassandraMetricsRegistry.MetricName> tracked = new ArrayList<>();

    private final String ks;
    private final String table;
    private final String indexName;
    public final Timer memtableIndexWriteLatency;

    public IndexMetrics(RouteIndex index)
    {
        this.ks = index.baseCfs().getKeyspaceName();
        this.table = index.baseCfs().name;
        this.indexName = index.getIndexMetadata().name;
        memtableIndexWriteLatency = Metrics.timer(createMetricName("MemtableIndexWriteLatency"));
    }

    public void release()
    {
        tracked.forEach(Metrics::remove);
        tracked.clear();
    }

    private CassandraMetricsRegistry.MetricName createMetricName(String name)
    {
        String metricScope = ks + '.' + table;
        if (indexName != null)
        {
            metricScope += '.' + indexName;
        }
        metricScope += '.' + SCOPE + '.' + name;

        CassandraMetricsRegistry.MetricName metricName = new CassandraMetricsRegistry.MetricName(DefaultNameFactory.GROUP_NAME,
                                                                                                 TYPE, name, metricScope, createMBeanName(name, SCOPE));
        tracked.add(metricName);
        return metricName;
    }

    private String createMBeanName(String name, String scope)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(DefaultNameFactory.GROUP_NAME);
        builder.append(":type=").append(TYPE);
        builder.append(',').append("keyspace=").append(ks);
        builder.append(',').append("table=").append(table);
        if (indexName != null)
            builder.append(',').append("index=").append(indexName);
        builder.append(',').append("scope=").append(scope);
        builder.append(',').append("name=").append(name);
        return builder.toString();
    }
}
