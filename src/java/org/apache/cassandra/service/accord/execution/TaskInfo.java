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

package org.apache.cassandra.service.accord.execution;

import javax.annotation.Nullable;

import accord.local.ExecutionContext;

import org.apache.cassandra.concurrent.DebuggableTask;

public class TaskInfo implements Comparable<TaskInfo>
{
    // sorted in name order for reporting to virtual tables
    public enum Status
    {
        LOADING, RUNNING, SCANNING_RANGES, WAITING_TO_LOAD, WAITING_TO_RUN
    }

    final Status status;
    final int commandStoreId;

    final Task task;

    public TaskInfo(Status status, int commandStoreId, Task task)
    {
        this.status = status;
        this.commandStoreId = commandStoreId;
        this.task = task;
    }

    public Status status()
    {
        return status;
    }

    public Integer commandStoreId()
    {
        return commandStoreId >= 0 ? commandStoreId : null;
    }

    public long position()
    {
        return task.position;
    }

    public @Nullable String describe()
    {
        if (task instanceof SafeTask)
            return ((SafeTask<?>) task).executionContext().reason();

        if (task instanceof DebuggableTask)
            return ((DebuggableTask) task).description();

        return null;
    }

    public @Nullable ExecutionContext preLoadContext()
    {
        if (task instanceof SafeTask)
            return ((SafeTask<?>) task).executionContext();
        if (task instanceof IOTaskWrapper && ((IOTaskWrapper) task).wrapped instanceof SafeTask.RangeTxnScanner)
            return ((SafeTask<?>.RangeTxnScanner) ((IOTaskWrapper) task).wrapped).preLoadContext();
        return null;
    }

    @Override
    public int compareTo(TaskInfo that)
    {
        int c = this.status.compareTo(that.status);
        if (c == 0) c = Long.compare(this.position(), that.position());
        return c;
    }
}
