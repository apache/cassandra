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

/**
 * Interface to include on a Runnable or Callable submitted to the SharedExecutorPool to provide more
 * detailed diagnostics.
 */
public interface DebuggableTask
{
    /**
     * MonotonicClock.approxTime (or MonotonicClock.preciseTime) this task was created
     */
    public long approxTimeOfCreation();
    /**
     * MonotonicClock.approxTime (or MonotonicClock.preciseTime) this task was started
     */
    public long approxTimeOfStart();

    /**
     * String describing the task, this can be general thing or something very specific like the query string depending
     * on what is available
     */
    public String debug();

    /**
     * RunningDebuggableTask is created by the SharedExecutorPool to include the thread name of any DebuggableTask
     * running on a SEPWorker
     */
    public static class RunningDebuggableTask implements DebuggableTask
    {
        final DebuggableTask task;
        final String threadId;

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

        public long approxTimeOfCreation()
        {
            assert hasTask();
            return task.approxTimeOfCreation();
        }

        public long approxTimeOfStart()
        {
            assert hasTask();
            return task.approxTimeOfStart();
        }

        public String debug()
        {
            assert hasTask();
            return task.debug();
        }
    }
}
