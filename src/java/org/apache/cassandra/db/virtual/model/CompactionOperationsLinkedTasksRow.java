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

package org.apache.cassandra.db.virtual.model;

import java.util.Map;

import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.db.compaction.CompactionInfo.COLUMNFAMILY;
import static org.apache.cassandra.db.compaction.CompactionInfo.COMPLETED;
import static org.apache.cassandra.db.compaction.CompactionInfo.KEYSPACE;
import static org.apache.cassandra.db.compaction.CompactionInfo.TARGET_DIRECTORY;
import static org.apache.cassandra.db.compaction.CompactionInfo.TOTAL;

/**
 * Representation of operation for a {@link org.apache.cassandra.db.virtual.CollectionVirtualTableAdapter}.
 */
public class CompactionOperationsLinkedTasksRow
{
    private final CompactionManager.OperationInfo operationInfo;
    private final CompactionInfo compactionInfo;
    private final Map<String, String> compactionInfoAsMap;

    public CompactionOperationsLinkedTasksRow(Map.Entry<CompactionManager.OperationInfo, CompactionInfo> entry)
    {
        this.operationInfo = entry.getKey();
        this.compactionInfo = entry.getValue();
        this.compactionInfoAsMap = entry.getValue().asMap();
    }

    @Column(type = Column.Type.PARTITION_KEY)
    public String operationType()
    {
        return operationInfo.operationType.toString();
    }

    @Column(type = Column.Type.CLUSTERING)
    public TimeUUID operationId()
    {
        return operationInfo.operationId;
    }

    @Column(type = Column.Type.CLUSTERING)
    public TimeUUID compactionId()
    {
        return compactionInfo.getTaskId();
    }

    @Column
    public String result()
    {
        return fromNullable(compactionInfoAsMap.get(KEYSPACE));
    }

    @Column
    public String keyspaceName()
    {
        return fromNullable(compactionInfoAsMap.get(KEYSPACE));
    }

    @Column
    public String columnFamily()
    {
        return fromNullable(compactionInfoAsMap.get(COLUMNFAMILY));
    }

    @Column
    public String completed()
    {
        return fromNullable(compactionInfoAsMap.get(COMPLETED));
    }

    @Column
    public String total()
    {
        return fromNullable(compactionInfoAsMap.get(TOTAL));
    }

    @Column
    public String unit()
    {
        return fromNullable(compactionInfoAsMap.get(CompactionInfo.UNIT));
    }

    @Column
    public String sstables()
    {
        return String.valueOf(compactionInfo.getSSTables().size());
    }

    @Column
    public String targetDirectory()
    {
        return fromNullable(compactionInfoAsMap.get(TARGET_DIRECTORY));
    }

    private static String fromNullable(Object value)
    {
        return value == null ? "" : value.toString();
    }
}
