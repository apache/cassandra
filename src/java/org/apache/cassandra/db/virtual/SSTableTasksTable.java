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

import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;

final class SSTableTasksTable extends AbstractVirtualTable
{
    private final static String KEYSPACE_NAME = "keyspace_name";
    private final static String TABLE_NAME = "table_name";
    private final static String TASK_ID = "task_id";
    private final static String COMPLETION_RATIO = "completion_ratio";
    private final static String KIND = "kind";
    private final static String PROGRESS = "progress";
    private final static String SSTABLES = "sstables";
    private final static String TOTAL = "total";
    private final static String UNIT = "unit";
    private final static String TARGET_DIRECTORY = "target_directory";

    SSTableTasksTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "sstable_tasks")
                           .comment("current sstable tasks")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(KEYSPACE_NAME, UTF8Type.instance)
                           .addClusteringColumn(TABLE_NAME, UTF8Type.instance)
                           .addClusteringColumn(TASK_ID, TimeUUIDType.instance)
                           .addRegularColumn(COMPLETION_RATIO, DoubleType.instance)
                           .addRegularColumn(KIND, UTF8Type.instance)
                           .addRegularColumn(PROGRESS, LongType.instance)
                           .addRegularColumn(SSTABLES, Int32Type.instance)
                           .addRegularColumn(TOTAL, LongType.instance)
                           .addRegularColumn(UNIT, UTF8Type.instance)
                           .addRegularColumn(TARGET_DIRECTORY, UTF8Type.instance)
                           .build());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (CompactionInfo task : CompactionManager.instance.getSSTableTasks())
        {
            long completed = task.getCompleted();
            long total = task.getTotal();

            double completionRatio = total == 0L ? 1.0 : (((double) completed) / total);

            result.row(task.getKeyspace().orElse("*"),
                       task.getTable().orElse("*"),
                       task.getTaskId())
                  .column(COMPLETION_RATIO, completionRatio)
                  .column(KIND, task.getTaskType().toString().toLowerCase())
                  .column(PROGRESS, completed)
                  .column(SSTABLES, task.getSSTables().size())
                  .column(TOTAL, total)
                  .column(UNIT, task.getUnit().toString().toLowerCase())
                  .column(TARGET_DIRECTORY, task.targetDirectory());
        }

        return result;
    }
}
