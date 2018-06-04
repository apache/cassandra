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

import java.util.stream.Collectors;

import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInfo.Holder;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.schema.TableMetadata;

final class SSTableTasksTable extends AbstractVirtualTable
{
    private final static String COMPACTION_ID = "compaction_id";
    private final static String KIND = "kind";
    private final static String KEYSPACE = "keyspace_name";
    private final static String TABLE = "table_name";
    private final static String PROGRESS = "progress";
    private final static String TOTAL = "total";
    private final static String UNIT = "unit";

    SSTableTasksTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "sstable_tasks")
                           .comment("List of current sstable tasks")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .addPartitionKeyColumn(KEYSPACE, UTF8Type.instance)
                           .addClusteringColumn(TABLE, UTF8Type.instance)
                           .addClusteringColumn(COMPACTION_ID, UUIDType.instance)
                           .addRegularColumn(KIND, UTF8Type.instance)
                           .addRegularColumn(PROGRESS, LongType.instance)
                           .addRegularColumn(TOTAL, LongType.instance)
                           .addRegularColumn(UNIT, UTF8Type.instance)
                           .build());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (CompactionInfo c : CompactionMetrics.getCompactions().stream()
                .map(Holder::getCompactionInfo)
                .collect(Collectors.toList()))
        {
            result.row(c.getKeyspace().orElse("*"), c.getColumnFamily().orElse("*"), c.compactionId())
                .column(KIND, c.getTaskType().toString().toLowerCase())
                .column(PROGRESS, c.getCompleted())
                .column(TOTAL, c.getTotal())
                .column(UNIT, c.getUnit().toString().toLowerCase());
        }

        return result;
    }
}
