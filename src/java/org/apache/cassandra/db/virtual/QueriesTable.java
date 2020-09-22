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
import org.apache.cassandra.utils.MonotonicClock;

import static java.lang.Long.max;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Virtual table to list out the current running queries on the NTR (coordinator), Read and Mutation (local) stages
 *
 * Example:
 * <pre>
 * </pre>
 */
public class QueriesTable extends AbstractVirtualTable
{
    private static final String TABLE_NAME = "queries";
    private static final String ID = "thread_id";
    private static final String QUEUETIME = "queued_micros";
    private static final String RUNTIME = "running_micros";
    private static final String DESC = "task";

    QueriesTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           // The thread name is unique since the id given to each SEPWorker is unique
                           .addPartitionKeyColumn(ID, UTF8Type.instance)
                           .addRegularColumn(QUEUETIME, LongType.instance)
                           .addRegularColumn(RUNTIME, LongType.instance)
                           .addRegularColumn(DESC, UTF8Type.instance)
                           .build());
    }

    /**
     * Walks the SharedExecutorPool.SHARED SEPWorkers for any DebuggableTasks's and returns them
     * @see DebuggableTask
     */
    @Override
    public AbstractVirtualTable.DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        for (DebuggableTask.RunningDebuggableTask task : SharedExecutorPool.SHARED.runningTasks())
        {
            if(!task.hasTask()) continue;
            long approxTimeOfCreation = task.approxTimeOfCreation();
            long approxTimeOfStart = task.approxTimeOfStart();
            long nowNanos = MonotonicClock.approxTime.now();
            long queuedMicros = NANOSECONDS.toMicros(max((approxTimeOfStart > 0 ? approxTimeOfStart : nowNanos) - approxTimeOfCreation, 0));
            long runningMicros = NANOSECONDS.toMicros(approxTimeOfStart > 0 ? max(nowNanos - approxTimeOfStart, 0) : 0);
            result.row(task.threadId())
                  .column(QUEUETIME, queuedMicros)
                  .column(RUNTIME, runningMicros)
                  .column(DESC, task.debug());
        }
        return result;
    }

}
