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

import org.apache.cassandra.concurrent.DebuggableTask;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;

import static java.lang.Long.max;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

/**
 * Virtual table that lists currently running queries on the NTR (coordinator) and Read/Mutation (local) stages
 *
 * Example:
 * <pre>
 * cqlsh> SELECT * FROM system_views.queries;
 *
 *  thread_id                   | queued_micros |  running_micros | task
 * ------------------------------+---------------+-----------------+--------------------------------------------------------------------------------
 *  Native-Transport-Requests-7 |         72923 |            7611 |                      QUERY select * from system_views.queries; [pageSize = 100]
 *              MutationStage-2 |         18249 |            2084 | Mutation(keyspace='distributed_test_keyspace', key='000000f8', modifications...
 *                  ReadStage-2 |         72447 |           10121 |                                         SELECT * FROM keyspace.table LIMIT 5000
 * </pre>
 */    
final class QueriesTable extends AbstractVirtualTable
{
    private static final String TABLE_NAME = "queries";
    private static final String ID = "thread_id";
    private static final String QUEUED = "queued_micros";
    private static final String RUNNING = "running_micros";
    private static final String DESC = "task";

    QueriesTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .comment("Lists currently running queries")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           // The thread name is unique since the id given to each SEPWorker is unique
                           .addPartitionKeyColumn(ID, UTF8Type.instance)
                           .addRegularColumn(QUEUED, LongType.instance)
                           .addRegularColumn(RUNNING, LongType.instance)
                           .addRegularColumn(DESC, UTF8Type.instance)
                           .build());
    }

    /**
     * Walks the {@link SharedExecutorPool} workers for any {@link DebuggableTask} instances and populates the table.
     */
    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        
        for (DebuggableTask.RunningDebuggableTask task : SharedExecutorPool.SHARED.runningTasks())
        {
            if (!task.hasTask()) continue;
            
            long creationTimeNanos = task.creationTimeNanos();
            long startTimeNanos = task.startTimeNanos();
            long now = approxTime.now();

            long queuedMicros = NANOSECONDS.toMicros(max((startTimeNanos > 0 ? startTimeNanos : now) - creationTimeNanos, 0));
            long runningMicros = startTimeNanos > 0 ? NANOSECONDS.toMicros(now - startTimeNanos) : 0;
            
            result.row(task.threadId())
                  .column(QUEUED, queuedMicros)
                  .column(RUNNING, runningMicros)
                  .column(DESC, task.description());
        }
        
        return result;
    }
}
