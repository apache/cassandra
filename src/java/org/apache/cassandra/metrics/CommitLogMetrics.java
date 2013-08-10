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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Gauge;

import org.apache.cassandra.db.commitlog.CommitLogAllocator;
import org.apache.cassandra.db.commitlog.ICommitLogExecutorService;

/**
 * Metrics for commit log
 */
public class CommitLogMetrics
{
    public static final String GROUP_NAME = "org.apache.cassandra.metrics";
    public static final String TYPE_NAME = "CommitLog";

    /** Number of completed tasks */
    public final Gauge<Long> completedTasks;
    /** Number of pending tasks */
    public final Gauge<Long> pendingTasks;
    /** Current size used by all the commit log segments */
    public final Gauge<Long> totalCommitLogSize;

    public CommitLogMetrics(final ICommitLogExecutorService executor, final CommitLogAllocator allocator)
    {
        completedTasks = CassandraMetricRegistry.register(MetricRegistry.name(GROUP_NAME, TYPE_NAME, "CompletedTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return executor.getCompletedTasks();
            }
        });
        pendingTasks = CassandraMetricRegistry.register(MetricRegistry.name(GROUP_NAME, TYPE_NAME, "PendingTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return executor.getPendingTasks();
            }
        });
        totalCommitLogSize = CassandraMetricRegistry.register(MetricRegistry.name(GROUP_NAME, TYPE_NAME, "TotalCommitLogSize"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return allocator.bytesUsed();
            }
        });
    }
}
