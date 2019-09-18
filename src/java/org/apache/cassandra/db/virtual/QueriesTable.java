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


import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.DebuggableTask;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.MonotonicClock;

/**
 * Virtual table to list out the current running queries on the NTR (coordinator), Read and Mutation (local) stages
 *
 * Example:
 * <pre>
 *     cqlsh> select * from system_views.queries;
 *
 *  thread_id                    | duration_micros | task
 * ------------------------------+-----------------+--------------------------------------------------------------------
 *  Native-Transport-Requests-17 |            6325 |         QUERY select * from system_views.queries; [pageSize = 100]
 *   Native-Transport-Requests-4 |           14681 | EXECUTE f4...e452637f1f2444 with 0 values at consistency LOCAL_ONE
 *   Native-Transport-Requests-6 |           14678 | EXECUTE f4...e452637f1f2444 with 0 values at consistency LOCAL_ONE
 *                  ReadStage-10 |           16535 |                            SELECT * FROM keyspace.table LIMIT 5000
 * </pre>
 */
public class QueriesTable extends AbstractVirtualTable
{

    private static final String TABLE_NAME = "queries";
    private static final String ID = "thread_id";
    private static final String DURATION = "duration_micros";
    private static final String DESC = "task";

    QueriesTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           // The thread name is unique since the id given to each SEPWorker is unique
                           .addPartitionKeyColumn(ID, UTF8Type.instance)
                           .addRegularColumn(DURATION, LongType.instance)
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
        long now = TimeUnit.NANOSECONDS.toMicros(MonotonicClock.approxTime.now());
        for (DebuggableTask.RunningDebuggableTask task : SharedExecutorPool.SHARED.runningTasks())
        {
            if(!task.hasTask()) continue;
            long micros = TimeUnit.NANOSECONDS.toMicros(task.approxStartNanos());
            result.row(task.threadId())
                  // Since MonotonicClock is used for some but not all, we want to cap to make sure any drift between
                  // different clocks doesn't cause it to go negative which would just look impossible
                  .column(DURATION, Math.max(1, now - micros))
                  .column(DESC, task.debug());
        }
        return result;
    }

}
