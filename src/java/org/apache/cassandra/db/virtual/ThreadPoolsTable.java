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

import java.util.Map;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.ThreadPoolMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.StatusLogger;

final class ThreadPoolsTable extends AbstractVirtualTable
{
    private final static String THREAD_POOL = "thread_pool";
    private final static String ACTIVE = "active_tasks";
    private final static String ACTIVE_MAX = "max_pool_size";
    private final static String PENDING = "pending_tasks";
    private final static String COMPLETED = "completed_tasks";
    private final static String BLOCKED = "blocked_tasks";
    private final static String TOTAL_BLOCKED = "blocked_tasks_all_time";

    ThreadPoolsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "thread_pools")
                           .comment("metrics of internal thread pools")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(THREAD_POOL, UTF8Type.instance)
                           .addRegularColumn(ACTIVE, Int32Type.instance)
                           .addRegularColumn(ACTIVE_MAX, Int32Type.instance)
                           .addRegularColumn(PENDING, LongType.instance)
                           .addRegularColumn(COMPLETED, LongType.instance)
                           .addRegularColumn(BLOCKED, LongType.instance)
                           .addRegularColumn(TOTAL_BLOCKED, LongType.instance)
                           .build());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (Map.Entry<String, ThreadPoolMetrics> tpool : CassandraMetricsRegistry.Metrics.getThreadPoolMetrics().entrySet())
        {
            ThreadPoolMetrics metrics = tpool.getValue();
            result.row(tpool.getKey())
                  .column(ACTIVE, metrics.activeTasks.getValue())
                  .column(ACTIVE_MAX, metrics.maxPoolSize.getValue())
                  .column(PENDING, metrics.pendingTasks.getValue())
                  .column(COMPLETED, metrics.completedTasks.getValue())
                  .column(BLOCKED, metrics.currentBlocked.getCount())
                  .column(TOTAL_BLOCKED, metrics.totalBlocked.getCount());
        }
        StatusLogger.log();
        return result;
    }
}
