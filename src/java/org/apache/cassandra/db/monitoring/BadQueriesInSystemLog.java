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
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
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
public class BadQueriesInSystemLog extends BadQueryReporter
{
    private static final Logger logger = LoggerFactory.getLogger(BadQueriesInSystemLog.class);

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
        BadQueryMetrics.setup();
    }

    @Override
    public void reportBadQuery(BadQuery.BadQueryCategory queryType, BadQueryTypes operationDetails)
    {
        if (CURRENT_SAMPLES.get(queryType).get() < DatabaseDescriptor.getBadQueryMaxSamplesPerIntervalInSyslog())
        {
            BAD_QUERY_CATEGORY_QUEUES.get(queryType).offer(operationDetails);
            CURRENT_SAMPLES.get(queryType).incrementAndGet();

            // emit metric for large partition reads (table level)
            if (operationDetails instanceof LargePartition) {
                LargePartition op = (LargePartition) operationDetails;
                for (ColumnFamilyStore cfs : op.cfs)
                {
                    if (cfs == null)
                        continue;
                    if (queryType == BadQuery.BadQueryCategory.LARGE_PARTITION_READ) {
                        cfs.metric.largePartitionReadSize.inc(op.getSize());
                    }
                    if (queryType == BadQuery.BadQueryCategory.LARGE_PARTITION_WRITE) {
                        cfs.metric.largePartitionWriteSize.inc(op.getSize());
                    }
                }
            }
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
            }
            // reset sample count so we get next batch of bad queries
            CURRENT_SAMPLES.get(type).set(0);
        }
    }
}
