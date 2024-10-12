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

package org.apache.cassandra.db.virtual.walker;

import org.apache.cassandra.db.virtual.model.Column;
import org.apache.cassandra.db.virtual.model.CompactionOperationsLinkedTasksRow;
import org.apache.cassandra.utils.TimeUUID;

/**
 * The {@link CompactionOperationsLinkedTasksRow} row metadata and data walker.
 *
 * @see CompactionOperationsLinkedTasksRow
 */
public class CompactionOperationsLinkedTasksWalker implements RowWalker<CompactionOperationsLinkedTasksRow>
{
    @Override
    public void visitMeta(MetadataVisitor visitor)
    {
        visitor.accept(Column.Type.PARTITION_KEY, "operation_type", String.class);
        visitor.accept(Column.Type.CLUSTERING, "operation_id", TimeUUID.class);
        visitor.accept(Column.Type.CLUSTERING, "compaction_id", TimeUUID.class);
        visitor.accept(Column.Type.REGULAR, "keyspace_name", String.class);
        visitor.accept(Column.Type.REGULAR, "column_family", String.class);
        visitor.accept(Column.Type.REGULAR, "completed", String.class);
        visitor.accept(Column.Type.REGULAR, "total", String.class);
        visitor.accept(Column.Type.REGULAR, "unit", String.class);
        visitor.accept(Column.Type.REGULAR, "sstables", String.class);
        visitor.accept(Column.Type.REGULAR, "target_directory", String.class);
    }

    @Override
    public void visitRow(CompactionOperationsLinkedTasksRow row, RowMetadataVisitor visitor)
    {
        visitor.accept(Column.Type.PARTITION_KEY, "operation_type", String.class, row::operationType);
        visitor.accept(Column.Type.CLUSTERING, "operation_id", TimeUUID.class, row::operationId);
        visitor.accept(Column.Type.CLUSTERING, "compaction_id", TimeUUID.class, row::compactionId);
        visitor.accept(Column.Type.REGULAR, "keyspace_name", String.class, row::keyspaceName);
        visitor.accept(Column.Type.REGULAR, "column_family", String.class, row::columnFamily);
        visitor.accept(Column.Type.REGULAR, "completed", String.class, row::completed);
        visitor.accept(Column.Type.REGULAR, "total", String.class, row::total);
        visitor.accept(Column.Type.REGULAR, "unit", String.class, row::unit);
        visitor.accept(Column.Type.REGULAR, "sstables", String.class, row::sstables);
        visitor.accept(Column.Type.REGULAR, "target_directory", String.class, row::targetDirectory);
    }

    @Override
    public int count(Column.Type type)
    {
        switch (type)
        {
            case PARTITION_KEY:
                return 1;
            case CLUSTERING:
                return 2;
            case REGULAR:
                return 7;
            default:
                throw new IllegalStateException("Unknown column type: " + type);
        }
    }
}
