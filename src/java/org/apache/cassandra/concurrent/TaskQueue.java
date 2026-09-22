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

import org.apache.cassandra.config.DatabaseDescriptor;

public interface TaskQueue
{
    /**
     * Enqueue a task into the queue.
     */
    boolean add(Runnable task);

    /**
     * Dequeue the next available task for execution.
     * In the prioritizing implementation, skips tasks whose partition key is currently running.
     */
    Runnable poll();

    /**
     * Look at the head of the queue without removing it.
     */
    Runnable peek();

    /**
     * Invoked by SEPWorker immediately after task.run() completes.
     */
    default void onTaskCompleted(Runnable task) {}

    /**
     * Returns true if there are no tasks in the queue.
     */
    boolean isEmpty();

    /**
     * Estimated number of tasks in the queue.
     */
    int size();

    /**
     * Factory to instantiate the appropriate TaskQueue implementation.
     */
    static TaskQueue create(String executorName)
    {
        // We only enable LwtKeyPrioritizingTaskQueue on the Native-Transport-Requests pool
        if (executorName.contains("Native-Transport-Requests")
            && DatabaseDescriptor.getEnableLwtPartitionPrioritization())
            return new LwtKeyPrioritizingTaskQueue();

        return new StandardTaskQueue();
    }
}