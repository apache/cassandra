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

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION)
public interface ResizableThreadPool
{
    /**
     * Returns core pool size of thread pool, the minimum
     * number of workers (where that makes sense for a thread pool,
     * SEPExecutor does not have a minimum size).
     */
    public int getCorePoolSize();

    /**
     * Allows user to resize minimum size of the thread pool.
     */
    public void setCorePoolSize(int newCorePoolSize);

    /**
     * Returns maximum pool size of thread pool.
     */
    public int getMaximumPoolSize();

    /**
     * Allows user to resize maximum size of the thread pool.
     */
    public void setMaximumPoolSize(int newMaximumPoolSize);

    /**
     * Returns the approximate number of threads that are actively
     * executing tasks.
     *
     * @return the number of threads
     */
    int getActiveTaskCount();

    /**
     * Returns the approximate total number of tasks that have
     * completed execution. Because the states of tasks and threads
     * may change dynamically during computation, the returned value
     * is only an approximation, but one that does not ever decrease
     * across successive calls.
     *
     * @return the number of tasks
     */
    long getCompletedTaskCount();

    /**
     * Returns the approximate total of tasks waiting to be executed.
     * Because the states of tasks and threads may change dynamically
     * during computation, the returned value is only an approximation.
     *
     * @return the number of tasks
     */
    int getPendingTaskCount();

    default int getMaxTasksQueued()
    {
        return -1;
    }
}
