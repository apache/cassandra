/**
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

package org.apache.cassandra.service;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;

public class PendingRangeCalculatorService
{
    public static final PendingRangeCalculatorService instance = new PendingRangeCalculatorService();

    private static Logger logger = LoggerFactory.getLogger(PendingRangeCalculatorService.class);

    // the executor will only run a single range calculation at a time while keeping at most one task queued in order
    // to trigger an update only after the most recent state change and not for each update individually
    private final JMXEnabledThreadPoolExecutor executor = new JMXEnabledThreadPoolExecutor(1, Integer.MAX_VALUE, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1), new NamedThreadFactory("PendingRangeCalculator"), "internal");

    private AtomicInteger updateJobs = new AtomicInteger(0);

    public PendingRangeCalculatorService()
    {
        executor.setRejectedExecutionHandler((r, e) ->
            {
                PendingRangeCalculatorServiceDiagnostics.taskRejected(instance, updateJobs);
                PendingRangeCalculatorService.instance.finishUpdate();
            }
        );
    }

    private static class PendingRangeTask implements Runnable
    {
        private final AtomicInteger updateJobs;

        PendingRangeTask(AtomicInteger updateJobs)
        {
            this.updateJobs = updateJobs;
        }

        public void run()
        {
            try
            {
                PendingRangeCalculatorServiceDiagnostics.taskStarted(instance, updateJobs);
                long start = System.currentTimeMillis();
                List<String> keyspaces = Schema.instance.getNonLocalStrategyKeyspaces();
                for (String keyspaceName : keyspaces)
                    calculatePendingRanges(Keyspace.open(keyspaceName).getReplicationStrategy(), keyspaceName);
                if (logger.isTraceEnabled())
                    logger.trace("Finished PendingRangeTask for {} keyspaces in {}ms", keyspaces.size(), System.currentTimeMillis() - start);
                PendingRangeCalculatorServiceDiagnostics.taskFinished(instance, updateJobs);
            }
            finally
            {
                PendingRangeCalculatorService.instance.finishUpdate();
            }
        }
    }

    private void finishUpdate()
    {
        int jobs = updateJobs.decrementAndGet();
        PendingRangeCalculatorServiceDiagnostics.taskCountChanged(instance, jobs);
    }

    public void update()
    {
        int jobs = updateJobs.incrementAndGet();
        PendingRangeCalculatorServiceDiagnostics.taskCountChanged(instance, jobs);
        executor.execute(new PendingRangeTask(updateJobs));
    }

    public void blockUntilFinished()
    {
        // We want to be sure the job we're blocking for is actually finished and we can't trust the TPE's active job count
        while (updateJobs.get() > 0)
        {
            try
            {
                Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
    }


    // public & static for testing purposes
    public static void calculatePendingRanges(AbstractReplicationStrategy strategy, String keyspaceName)
    {
        StorageService.instance.getTokenMetadata().calculatePendingRanges(strategy, keyspaceName);
    }

    @VisibleForTesting
    public void shutdownExecutor() throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
    }
}
