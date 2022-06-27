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
package org.apache.cassandra.concurrent;

import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Recursive.INTERFACES;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * Interface to include on a Runnable or Callable submitted to the {@link SharedExecutorPool} to provide more
 * detailed diagnostics.
 */
@Shared(scope = SIMULATION, inner = INTERFACES)
public interface DebuggableTask
{
    public long creationTimeNanos();

    public long startTimeNanos();

    public String description();
    
    interface RunnableDebuggableTask extends Runnable, DebuggableTask {}

    /**
     * Wraps a {@link DebuggableTask} to include the name of the thread running it.
     */
    public static class RunningDebuggableTask implements DebuggableTask
    {
        private final DebuggableTask task;
        private final String threadId;

        public RunningDebuggableTask(String threadId, DebuggableTask task)
        {
            this.task = task;
            this.threadId = threadId;
        }

        public String threadId()
        {
            return threadId;
        }

        public boolean hasTask()
        {
            return task != null;
        }

        @Override
        public long creationTimeNanos()
        {
            assert hasTask();
            return task.creationTimeNanos();
        }

        @Override
        public long startTimeNanos()
        {
            assert hasTask();
            return task.startTimeNanos();
        }

        @Override
        public String description()
        {
            assert hasTask();
            return task.description();
        }
    }
}
