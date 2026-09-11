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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorBuilder;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

/** Isolates best-effort history writes from compactions and shared maintenance schedulers. */
final class CompactionHistoryWriter
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionHistoryWriter.class);
    private final ExecutorPlus executor;

    CompactionHistoryWriter(int queueLimit, ExecutorBuilder<? extends ExecutorPlus> builder)
    {
        executor = builder.withQueueLimit(queueLimit)
                          .withRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy())
                          .build();
    }

    Future<Void> submit(Runnable write)
    {
        try
        {
            return executor.submit(write, null);
        }
        catch (RejectedExecutionException e)
        {
            // Drop the newest record, never block the compaction caller or run a write on its thread.
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES,
                             "Compaction history queue is full or shut down; dropping history update");
            return ImmediateFuture.failure(e);
        }
    }

    void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        // Drain accepted writes before the system memtables and commitlog are shut down. A timeout must
        // fail drain rather than allow a still-running history mutation to race commitlog shutdown.
        ExecutorUtils.shutdownAndWait(timeout, unit, executor);
    }
}
