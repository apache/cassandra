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
package org.apache.cassandra.db.monitoring;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.BadQueryMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;

/**
 * Report badqueries in system log file. We cannot log each and every query in
 * system log file hence this class is designed to log some samples at periodic interval
 */
public class BadQueriesInSystemLog implements IBadQueryReporter
{
    private static final Logger logger = LoggerFactory.getLogger(BadQueriesInSystemLog.class);
    //Store different types of bad queries in this queue and limit this queue to fix size.
    private static final Map<BadQuery.BadQueryCategory, ConcurrentLinkedQueue<BadQueryTypes>> BAD_QUERY_CATEGORY_QUEUES = new HashMap<>();
    private static final Map<BadQuery.BadQueryCategory, AtomicInteger> CURRENT_SAMPLES = new HashMap<>();

    static
    {
        for (BadQuery.BadQueryCategory category : BadQuery.BadQueryCategory.values())
        {
            BAD_QUERY_CATEGORY_QUEUES.put(category, new ConcurrentLinkedQueue<BadQueryTypes>());
            CURRENT_SAMPLES.put(category, new AtomicInteger(0));
        }
    }

    @Override
    public void setup()
    {
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(() -> log(),
                5,
                DatabaseDescriptor.getBadQueryLoggingInterval(),
                TimeUnit.SECONDS);
    }

    @VisibleForTesting
    public Map<BadQuery.BadQueryCategory, ConcurrentLinkedQueue<BadQueryTypes>> getBadQueryCategoryQueues()
    {
        return Collections.unmodifiableMap(BAD_QUERY_CATEGORY_QUEUES);
    }

    @VisibleForTesting
    public void clear()
    {
        Iterator<Map.Entry<BadQuery.BadQueryCategory, ConcurrentLinkedQueue<BadQueryTypes>>> iter = BAD_QUERY_CATEGORY_QUEUES.entrySet().iterator();
        while (iter.hasNext())
        {
            Map.Entry<BadQuery.BadQueryCategory, ConcurrentLinkedQueue<BadQueryTypes>> entry = iter.next();
            entry.getValue().clear();
        }
        Iterator<Map.Entry<BadQuery.BadQueryCategory, AtomicInteger>> iterator =  CURRENT_SAMPLES.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<BadQuery.BadQueryCategory, AtomicInteger> entry = iterator.next();
            entry.getValue().set(0);
        }
    }

    @Override
    public void reportBadQuery(BadQuery.BadQueryCategory queryType, BadQueryTypes operationDetails)
    {
        if (CURRENT_SAMPLES.get(queryType).get() < DatabaseDescriptor.getBadQueryMaxSamplesPerIntervalInSyslog())
        {
            BAD_QUERY_CATEGORY_QUEUES.get(queryType).offer(operationDetails);
            CURRENT_SAMPLES.get(queryType).incrementAndGet();
            BadQueryMetrics.totalBadQueries.inc();
        }
    }

    /**
     * log bad query using BadQuery reporter.
     * by default it logs to system.log file but one can override logging behavior.
     */
    private static void log()
    {
        for (BadQuery.BadQueryCategory type : BadQuery.BadQueryCategory.values())
        {
            ConcurrentLinkedQueue<BadQueryTypes> badQueries = BAD_QUERY_CATEGORY_QUEUES.get(type);
            boolean cleanupDone = false;
            while (!badQueries.isEmpty())
            {
                BadQueryTypes bq = badQueries.poll();
                if (!cleanupDone)
                {
                    bq.cleanup();
                    cleanupDone = true;
                }
                logger.warn(String.format("%s detected: %s", type.toString(), bq.toString()));
                BadQueryMetrics.totalBadQueries.dec();
            }
            //reset sample count so we get next batch of bad queries
            CURRENT_SAMPLES.get(type).set(0);
        }
    }
}
