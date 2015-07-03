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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;

import static java.lang.System.getProperty;

/**
 * A task for monitoring in progress operations, currently only read queries, and aborting them if they time out.
 * We also log timed out operations, see CASSANDRA-7392.
 */
public class MonitoringTask
{
    private static final String LINE_SEPARATOR = getProperty("line.separator");
    private static final Logger logger = LoggerFactory.getLogger(MonitoringTask.class);

    /**
     * Defines the interval for reporting any operations that have timed out.
     */
    private static final int REPORT_INTERVAL_MS = Math.max(0, Integer.valueOf(System.getProperty(Config.PROPERTY_PREFIX + "monitoring_report_interval_ms", "5000")));

    /**
     * Defines the maximum number of unique timed out queries that will be reported in the logs.
     * Use a negative number to remove any limit.
     */
    private static final int MAX_OPERATIONS = Integer.valueOf(System.getProperty(Config.PROPERTY_PREFIX + "monitoring_max_operations", "50"));

    @VisibleForTesting
    static MonitoringTask instance = make(REPORT_INTERVAL_MS, MAX_OPERATIONS);

    private final int maxOperations;
    private final ScheduledFuture<?> reportingTask;
    private final BlockingQueue<FailedOperation> operationsQueue;
    private final AtomicLong numDroppedOperations;
    private long lastLogTime;

    @VisibleForTesting
    static MonitoringTask make(int reportIntervalMillis, int maxTimedoutOperations)
    {
        if (instance != null)
        {
            instance.cancel();
            instance = null;
        }

        return new MonitoringTask(reportIntervalMillis, maxTimedoutOperations);
    }

    private MonitoringTask(int reportIntervalMillis, int maxOperations)
    {
        this.maxOperations = maxOperations;
        this.operationsQueue = maxOperations > 0 ? new ArrayBlockingQueue<>(maxOperations) : new LinkedBlockingQueue<>();
        this.numDroppedOperations = new AtomicLong();
        this.lastLogTime = ApproximateTime.currentTimeMillis();

        logger.info("Scheduling monitoring task with report interval of {} ms, max operations {}", reportIntervalMillis, maxOperations);
        this.reportingTask = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(() -> logFailedOperations(ApproximateTime.currentTimeMillis()),
                                                                                     reportIntervalMillis,
                                                                                     reportIntervalMillis,
                                                                                     TimeUnit.MILLISECONDS);
    }

    public void cancel()
    {
        reportingTask.cancel(false);
    }

    public static void addFailedOperation(Monitorable operation, long now)
    {
        instance.innerAddFailedOperation(operation, now);
    }

    private void innerAddFailedOperation(Monitorable operation, long now)
    {
        if (maxOperations == 0)
            return; // logging of failed operations disabled

        if (!operationsQueue.offer(new FailedOperation(operation, now)))
            numDroppedOperations.incrementAndGet();
    }

    @VisibleForTesting
    FailedOperations aggregateFailedOperations()
    {
        Map<String, FailedOperation> operations = new HashMap<>();

        FailedOperation failedOperation;
        while((failedOperation = operationsQueue.poll()) != null)
        {
            FailedOperation existing = operations.get(failedOperation.name());
            if (existing != null)
                existing.addTimeout(failedOperation);
            else
                operations.put(failedOperation.name(), failedOperation);
        }

        return new FailedOperations(operations, numDroppedOperations.getAndSet(0L));
    }

    @VisibleForTesting
    List<String> getFailedOperations()
    {
        FailedOperations failedOperations = aggregateFailedOperations();
        String ret = failedOperations.getLogMessage();
        lastLogTime = ApproximateTime.currentTimeMillis();
        return ret.isEmpty() ? Collections.emptyList() : Arrays.asList(ret.split("\n"));
    }

    @VisibleForTesting
    void logFailedOperations(long now)
    {
        FailedOperations failedOperations = aggregateFailedOperations();
        if (!failedOperations.isEmpty())
        {
            long elapsed = now - lastLogTime;
            logger.warn("{} operations timed out in the last {} msecs, operation list available at debug log level",
                        failedOperations.num(),
                        elapsed);

            if (logger.isDebugEnabled())
                logger.debug("{} operations timed out in the last {} msecs:{}{}",
                            failedOperations.num(),
                            elapsed,
                            LINE_SEPARATOR,
                            failedOperations.getLogMessage());
        }

        lastLogTime = now;
    }

    private static final class FailedOperations
    {
        public final Map<String, FailedOperation> operations;
        public final long numDropped;

        FailedOperations(Map<String, FailedOperation> operations, long numDropped)
        {
            this.operations = operations;
            this.numDropped = numDropped;
        }

        public boolean isEmpty()
        {
            return operations.isEmpty() && numDropped == 0;
        }

        public long num()
        {
            return operations.size() + numDropped;
        }

        public String getLogMessage()
        {
            if (isEmpty())
                return "";

            final StringBuilder ret = new StringBuilder();
            operations.values().forEach(o -> addOperation(ret, o));

            if (numDropped > 0)
                ret.append(LINE_SEPARATOR)
                   .append("... (")
                   .append(numDropped)
                   .append(" were dropped)");

            return ret.toString();
        }

        private static void addOperation(StringBuilder ret, FailedOperation operation)
        {
            if (ret.length() > 0)
                ret.append(LINE_SEPARATOR);

            ret.append(operation.getLogMessage());
        }
    }

    private final static class FailedOperation
    {
        public final Monitorable operation;
        public int numTimeouts;
        public long totalTime;
        public long maxTime;
        public long minTime;
        private String name;

        FailedOperation(Monitorable operation, long failedAt)
        {
            this.operation = operation;
            numTimeouts = 1;
            totalTime = failedAt - operation.constructionTime().timestamp;
            minTime = totalTime;
            maxTime = totalTime;
        }

        public String name()
        {
            if (name == null)
                name = operation.name();
            return name;
        }

        void addTimeout(FailedOperation operation)
        {
            numTimeouts++;
            totalTime += operation.totalTime;
            maxTime = Math.max(maxTime, operation.maxTime);
            minTime = Math.min(minTime, operation.minTime);
        }

        public String getLogMessage()
        {
            if (numTimeouts == 1)
                return String.format("%s: total time %d msec - timeout %d %s",
                                     name(),
                                     totalTime,
                                     operation.timeout(),
                                     operation.constructionTime().isCrossNode ? "msec/cross-node" : "msec");
            else
                return String.format("%s (timed out %d times): total time avg/min/max %d/%d/%d msec - timeout %d %s",
                                     name(),
                                     numTimeouts,
                                     totalTime / numTimeouts,
                                     minTime,
                                     maxTime,
                                     operation.timeout(),
                                     operation.constructionTime().isCrossNode ? "msec/cross-node" : "msec");
        }
    }
}
