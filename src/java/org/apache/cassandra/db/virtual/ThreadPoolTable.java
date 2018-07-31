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

import java.lang.management.ManagementFactory;
import java.util.Map;

import javax.management.MBeanServer;

import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.metrics.ThreadPoolMetrics;
import org.apache.cassandra.schema.TableMetadata;

final class ThreadPoolTable extends AbstractVirtualTable
{
    private final static String POOL = "thread_pool";
    private final static String ACTIVE = "active";
    private final static String ACTIVE_MAX = "active_max";
    private final static String PENDING = "pending";
    private final static String COMPLETED = "completed";
    private final static String BLOCKED = "tasks_blocked";
    private final static String TOTAL_BLOCKED = "total_blocked";

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
                           .addRegularColumn(TOTAL_BLOCKED, LongType.instance)
                           .build());
    }

    private long getJmxMetric(Map.Entry<String, String> tpool, String key)
    {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        Object value = ThreadPoolMetrics.getJmxMetric(server, tpool.getKey(), tpool.getValue(), key);
        if (value instanceof Long)
            return ((Long) value).longValue();
        else if (value instanceof Integer)
            return ((Integer) value).longValue();
        throw new IllegalArgumentException(value + " of unexpected type " + value.getClass());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        for (Map.Entry<String, String> tpool : ThreadPoolMetrics.getJmxThreadPools(server).entries())
        {
            result.row(tpool.getValue())
                .column(ACTIVE, getJmxMetric(tpool, ThreadPoolMetrics.ACTIVE_TASKS))
                .column(ACTIVE_MAX, getJmxMetric(tpool, ThreadPoolMetrics.MAX_POOL_SIZE))
                .column(PENDING, getJmxMetric(tpool, ThreadPoolMetrics.PENDING_TASKS))
                .column(COMPLETED, getJmxMetric(tpool, ThreadPoolMetrics.COMPLETED_TASKS))
                .column(BLOCKED, getJmxMetric(tpool, ThreadPoolMetrics.CURRENTLY_BLOCKED_TASKS))
                .column(TOTAL_BLOCKED, getJmxMetric(tpool, ThreadPoolMetrics.TOTAL_BLOCKED_TASKS));
        }
        return result;
    }
}
