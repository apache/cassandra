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

import org.apache.cassandra.metrics.ThreadPoolMetrics;

/**
 * Thread pool metrics representation for a {@link org.apache.cassandra.db.virtual.CollectionVirtualTableAdapter}.
 */
public class ThreadPoolRow
{
    private final ThreadPoolMetrics entry;

    public ThreadPoolRow(ThreadPoolMetrics entry)
    {
        this.entry = entry;
    }

    @Column(type = Column.Type.PARTITION_KEY)
    public String name()
    {
        return entry.poolName;
    }

    @Column
    public Integer activeTasks()
    {
        return entry.activeTasks.getValue();
    }

    @Column
    public Integer activeTasksLimit()
    {
        return entry.maxPoolSize.getValue();
    }

    @Column
    public Integer pendingTasks()
    {
        return entry.pendingTasks.getValue();
    }

    @Column
    public Long completedTasks()
    {
        return entry.completedTasks.getValue();
    }

    @Column
    public long blockedTasks()
    {
        return entry.currentBlocked.getCount();
    }

    @Column
    public long blockedTasksAllTime()
    {
        return entry.totalBlocked.getCount();
    }

    @Column
    public Integer corePoolSize()
    {
        return entry.corePoolSize.getValue();
    }

    @Column
    public Integer maxPoolSize()
    {
        return entry.maxPoolSize.getValue();
    }

    @Column
    public Integer maxTasksQueued()
    {
        return entry.maxTasksQueued.getValue();
    }
}
