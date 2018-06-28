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
package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;

import org.apache.cassandra.concurrent.SEPExecutor;

public class SEPMetrics extends ThreadPoolMetrics
{
    public static final String MAX_TASKS_QUEUED = "MaxTasksQueued";

    /** Maximum number of tasks queued before a task get blocked */
    public final Gauge<Integer> maxTasksQueued;

    /**
     * Create metrics for the given SEPExecutor.
     *
     * @param executor Thread pool
     * @param path Type of thread pool
     * @param poolName Name of thread pool to identify metrics
     */
    public SEPMetrics(final SEPExecutor executor, String path, String poolName)
    {
        super(executor, path, poolName);
        maxTasksQueued =  register(MAX_TASKS_QUEUED, () -> executor.maxTasksQueued);
    }

    public void release()
    {
        super.release();
        remove(MAX_TASKS_QUEUED);
    }
}
