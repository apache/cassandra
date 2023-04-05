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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.metrics.ThreadPoolMetrics;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

final class ThreadPoolsTable extends AbstractVirtualTable
{
    private static final String NAME = "name";
    private static final String ACTIVE_TASKS = "active_tasks";
    private static final String ACTIVE_TASKS_LIMIT = "active_tasks_limit";
    private static final String PENDING_TASKS = "pending_tasks";
    private static final String COMPLETED_TASKS = "completed_tasks";
    private static final String BLOCKED_TASKS = "blocked_tasks";
    private static final String BLOCKED_TASKS_ALL_TIME = "blocked_tasks_all_time";

    ThreadPoolsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "thread_pools")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME, UTF8Type.instance)
                           .addRegularColumn(ACTIVE_TASKS, Int32Type.instance)
                           .addRegularColumn(ACTIVE_TASKS_LIMIT, Int32Type.instance)
                           .addRegularColumn(PENDING_TASKS, Int32Type.instance)
                           .addRegularColumn(COMPLETED_TASKS, LongType.instance)
                           .addRegularColumn(BLOCKED_TASKS, LongType.instance)
                           .addRegularColumn(BLOCKED_TASKS_ALL_TIME, LongType.instance)
                           .build());
    }

    @Override
    public DataSet data(DecoratedKey partitionKey)
    {
        String poolName = UTF8Type.instance.compose(partitionKey.getKey());

        SimpleDataSet result = new SimpleDataSet(metadata());
        Metrics.getThreadPoolMetrics(poolName)
               .ifPresent(metrics -> addRow(result, metrics));
        return result;
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        Metrics.allThreadPoolMetrics()
               .forEach(metrics -> addRow(result, metrics));
        return result;
    }

    private void addRow(SimpleDataSet dataSet, ThreadPoolMetrics metrics)
    {
        dataSet.row(metrics.poolName)
               .column(ACTIVE_TASKS, metrics.activeTasks.getValue())
               .column(ACTIVE_TASKS_LIMIT, metrics.maxPoolSize.getValue())
               .column(PENDING_TASKS, metrics.pendingTasks.getValue())
               .column(COMPLETED_TASKS, metrics.completedTasks.getValue())
               .column(BLOCKED_TASKS, metrics.currentBlocked.getCount())
               .column(BLOCKED_TASKS_ALL_TIME, metrics.totalBlocked.getCount());
    }
}
