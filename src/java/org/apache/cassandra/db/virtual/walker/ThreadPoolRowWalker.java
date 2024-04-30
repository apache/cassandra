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
import org.apache.cassandra.db.virtual.model.ThreadPoolRow;

/**
 * The {@link org.apache.cassandra.db.virtual.model.ThreadPoolRow} row metadata and data walker.
 *
 * @see org.apache.cassandra.db.virtual.model.ThreadPoolRow
 */
public class ThreadPoolRowWalker implements RowWalker<ThreadPoolRow>
{
    @Override
    public void visitMeta(MetadataVisitor visitor)
    {
        visitor.accept(Column.Type.PARTITION_KEY, "name", String.class);
        visitor.accept(Column.Type.REGULAR, "active_tasks", Integer.class);
        visitor.accept(Column.Type.REGULAR, "active_tasks_limit", Integer.class);
        visitor.accept(Column.Type.REGULAR, "blocked_tasks", Long.TYPE);
        visitor.accept(Column.Type.REGULAR, "blocked_tasks_all_time", Long.TYPE);
        visitor.accept(Column.Type.REGULAR, "completed_tasks", Long.class);
        visitor.accept(Column.Type.REGULAR, "core_pool_size", Integer.class);
        visitor.accept(Column.Type.REGULAR, "max_pool_size", Integer.class);
        visitor.accept(Column.Type.REGULAR, "max_tasks_queued", Integer.class);
        visitor.accept(Column.Type.REGULAR, "pending_tasks", Integer.class);
    }

    @Override
    public void visitRow(ThreadPoolRow row, RowMetadataVisitor visitor)
    {
        visitor.accept(Column.Type.PARTITION_KEY, "name", String.class, row::name);
        visitor.accept(Column.Type.REGULAR, "active_tasks", Integer.class, row::activeTasks);
        visitor.accept(Column.Type.REGULAR, "active_tasks_limit", Integer.class, row::activeTasksLimit);
        visitor.accept(Column.Type.REGULAR, "blocked_tasks", Long.TYPE, row::blockedTasks);
        visitor.accept(Column.Type.REGULAR, "blocked_tasks_all_time", Long.TYPE, row::blockedTasksAllTime);
        visitor.accept(Column.Type.REGULAR, "completed_tasks", Long.class, row::completedTasks);
        visitor.accept(Column.Type.REGULAR, "core_pool_size", Integer.class, row::corePoolSize);
        visitor.accept(Column.Type.REGULAR, "max_pool_size", Integer.class, row::maxPoolSize);
        visitor.accept(Column.Type.REGULAR, "max_tasks_queued", Integer.class, row::maxTasksQueued);
        visitor.accept(Column.Type.REGULAR, "pending_tasks", Integer.class, row::pendingTasks);
    }

    @Override
    public int count(Column.Type type)
    {
        switch (type)
        {
            case PARTITION_KEY:
                return 1;
            case CLUSTERING:
                return 0;
            case REGULAR:
                return 9;
            default:
                throw new IllegalStateException("Unknown column type: " + type);
        }
    }
}
