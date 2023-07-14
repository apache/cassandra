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

package org.apache.cassandra.service.throttler.dynamic;

import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.SEPExecutor;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.SharedExecutorPool;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;


public class CassandraResourceUtilization
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraResourceUtilization.class);
    private final ScheduledExecutorPlus reportThread = executorFactory().scheduled(false, "CassandraResourceUtilization", Thread.MAX_PRIORITY);
    private static final DecimalFormat df = new DecimalFormat("0");

    // TODO: make this configurable
    private static final IResourceUtilzation resourceUtilzation = new NativeResourceUtilization();

    private static final String READ_THREAD_POOL = "ReadStage";
    private static final String MUTATION_THREAD_POOL = "MutationStage";

    // Maintain 1 minute, 5 minutes, and 15 minutes history
    private long nrThrottled1Prev = -1;
    private long nrThrottled2Prev = -1;

    public ResourcesStats resourcesStats;
    public ThrottlingOptions throttlingOptions;

    public void setup()
    {
        resourcesStats = new ResourcesStats();
        throttlingOptions = new ThrottlingOptions();
        resourceUtilzation.setup();
        // TODO: make this configurable
        reportThread.scheduleAtFixedRate(() -> fetchCurrentHealth(), 10, 1, TimeUnit.SECONDS);
    }

    public void fetchCurrentHealth()
    {
        resourcesStats.setCpuUtil1(resourceUtilzation.getCurrentCpuUtil1());
        resourcesStats.setCpuUtil2(resourceUtilzation.getCurrentCpuUtil2());

        SEPExecutor readSEPTP = SharedExecutorPool.SHARED.getExecutor(READ_THREAD_POOL);
        if (readSEPTP != null)
        {
            resourcesStats.setPendingReads(readSEPTP.getPendingTaskCount());
        }
        SEPExecutor mutationSEPTP = SharedExecutorPool.SHARED.getExecutor(MUTATION_THREAD_POOL);
        if (mutationSEPTP != null)
        {
            resourcesStats.setPendingMutations(mutationSEPTP.getPendingTaskCount());
        }
        long nrThrottled1Now = resourceUtilzation.getCpuNRThrottled1();
        if (nrThrottled1Prev != -1)
        {
            resourcesStats.setNrThrottled1(nrThrottled1Now - nrThrottled1Prev);
        }
        nrThrottled1Prev = nrThrottled1Now;

        long nrThrottled2Now = resourceUtilzation.getCpuNRThrottled2();
        if (nrThrottled2Prev != -1)
        {
            resourcesStats.setNrThrottled2(nrThrottled2Now - nrThrottled2Prev);
        }
        nrThrottled2Prev = nrThrottled2Now;

        // TODO: Eventually, change this to Debug to avoid log flooding
        logger.info("CassandraResourceUtilization {}", this);
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("CpuUtil1: ").append(df.format(resourcesStats.getCpuUtil1Cur())).
          append("-").append(df.format(resourcesStats.getCpuUtil1OneMinute())).
          append("-").append(df.format(resourcesStats.getCpuUtil1FiveMinute())).
          append("-").append(df.format(resourcesStats.getCpuUtil1FifteenMinute())).

          append(", CpuUtil2: ").append(df.format(resourcesStats.getCpuUtil2Cur())).
          append("-").append(df.format(resourcesStats.getCpuUtil2OneMinute())).
          append("-").append(df.format(resourcesStats.getCpuUtil2FiveMinute())).
          append("-").append(df.format(resourcesStats.getCpuUtil2FifteenMinute())).

          append(", NrThrottled1: ").append(df.format(resourcesStats.getNrThrottled1Cur())).
          append("-").append(df.format(resourcesStats.getNrThrottled1OneMinute())).
          append("-").append(df.format(resourcesStats.getNrThrottled1FiveMinute())).
          append("-").append(df.format(resourcesStats.getNrThrottled1FifteenMinute())).

          append(", NrThrottled2: ").append(df.format(resourcesStats.getNrThrottled2Cur())).
          append("-").append(df.format(resourcesStats.getNrThrottled2OneMinute())).
          append("-").append(df.format(resourcesStats.getNrThrottled2FiveMinute())).
          append("-").append(df.format(resourcesStats.getNrThrottled2FifteenMinute())).

          append(", PendingReads: ").append(df.format(resourcesStats.getPendingReadsCur())).
          append("-").append(df.format(resourcesStats.getPendingReadsOneMinute())).
          append("-").append(df.format(resourcesStats.getPendingReadsFiveMinute())).
          append("-").append(df.format(resourcesStats.getPendingReadsFifteenMinute())).

          append(", PendingMutations: ").append(df.format(resourcesStats.getPendingMutationsCur())).
          append("-").append(df.format(resourcesStats.getPendingMutationsOneMinute())).
          append("-").append(df.format(resourcesStats.getPendingMutationsFiveMinute())).
          append("-").append(df.format(resourcesStats.getPendingMutationsFifteenMinute()));

        return sb.toString();
    }
}
