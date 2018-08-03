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
package org.apache.cassandra.db.virtual;

import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;

import java.util.stream.Collectors;

import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.ThreadPoolMetrics.ThreadPoolMetric;
import org.apache.cassandra.schema.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ThreadPoolTable extends AbstractVirtualTable
{
    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolTable.class);

    private final static String POOL = "thread_pool";
    private final static String ACTIVE = "active_tasks";
    private final static String ACTIVE_MAX = "max_pool_size";
    private final static String PENDING = "pending_tasks";
    private final static String MAX_TASKS = "max_tasks_queued";
    private final static String COMPLETED = "completed_tasks";
    private final static String BLOCKED = "currently_blocked_tasks";
    private final static String TOTAL_BLOCKED = "total_blocked_tasks";

    ThreadPoolTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "thread_pools")
                           .comment("metrics of internal thread pools")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .addPartitionKeyColumn(POOL, UTF8Type.instance)
                           .addRegularColumn(ACTIVE, LongType.instance)
                           .addRegularColumn(ACTIVE_MAX, LongType.instance)
                           .addRegularColumn(PENDING, LongType.instance)
                           .addRegularColumn(COMPLETED, LongType.instance)
                           .addRegularColumn(BLOCKED, LongType.instance)
                           .addRegularColumn(MAX_TASKS, LongType.instance)
                           .addRegularColumn(TOTAL_BLOCKED, LongType.instance)
                           .build());
    }


    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        CassandraMetricsRegistry.Metrics.getMetrics().values().stream()
            .filter(m -> m instanceof ThreadPoolMetric)
            .map(m -> (ThreadPoolMetric) m)
            .collect(Collectors.groupingBy(m -> m.getPoolName()))
            .entrySet().forEach(e ->
            {
                result.row(e.getKey());
                for (ThreadPoolMetric m : e.getValue())
                {
                    String identifier = UPPER_CAMEL.to(LOWER_UNDERSCORE, m.getMetricName());
                    result.column(identifier, m.getLongValue());
                }
            });
        return result;
    }
}
