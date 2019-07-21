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

public class QueriesTable extends AbstractVirtualTable
{

    private static final String ID = "thread_id";
    private static final String DURATION = "duration_micros";
    private static final String DESC = "task";

    QueriesTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "queries")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(ID, UTF8Type.instance) // strictly for uniqueness
                           .addRegularColumn(DURATION, LongType.instance)
                           .addRegularColumn(DESC, UTF8Type.instance)
                           .build());
    }

    @Override
    public AbstractVirtualTable.DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        long now = TimeUnit.NANOSECONDS.toMicros(MonotonicClock.approxTime.now());
        for (DebuggableTask.ThreadedDebuggableTask task : SharedExecutorPool.SHARED.runningTasks())
        {
            if(!task.hasTask()) continue;
            long micros = TimeUnit.NANOSECONDS.toMicros(task.startTimeNanos());
            result.row(task.threadId())
                  .column(DURATION, Math.max(1, now - micros))
                  .column(DESC, task.debug());
        }
        return result;
    }

}
