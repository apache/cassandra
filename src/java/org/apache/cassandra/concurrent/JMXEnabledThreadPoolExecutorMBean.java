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
 * @see org.apache.cassandra.metrics.ThreadPoolMetrics
 */
@Deprecated
public interface JMXEnabledThreadPoolExecutorMBean extends IExecutorMBean
{
    /**
     * Get the number of tasks that had blocked before being accepted (or
     * rejected).
     */
    public int getTotalBlockedTasks();

    /**
     * Get the number of tasks currently blocked, waiting to be accepted by
     * the executor (because all threads are busy and the backing queue is full).
     */
    public int getCurrentlyBlockedTasks();

    /**
     * Returns core pool size of thread pool.
     */
    public int getCoreThreads();

    /**
     * Allows user to resize core pool size of the thread pool.
     */
    public void setCoreThreads(int number);

    /**
     * Returns maximum pool size of thread pool.
     */
    public int getMaximumThreads();

    /**
     * Allows user to resize maximum size of the thread pool.
     */
    public void setMaximumThreads(int number);
}
