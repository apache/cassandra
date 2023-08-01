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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.MONITORING_MAX_OPERATIONS;
import static org.apache.cassandra.config.CassandraRelevantProperties.MONITORING_REPORT_INTERVAL_MS;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.apache.cassandra.utils.concurrent.BlockingQueues.newBlockingQueue;

/**
 * A task for monitoring in progress operations, currently only read queries, and aborting them if they time out.
 * We also log timed out operations, see CASSANDRA-7392.
 * Since CASSANDRA-12403 we also log queries that were slow.
 */
class MonitoringTask
{
    private static final String LINE_SEPARATOR = CassandraRelevantProperties.LINE_SEPARATOR.getString();
    private static final Logger logger = LoggerFactory.getLogger(MonitoringTask.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 5L, TimeUnit.MINUTES);

    /**
     * Defines the interval for reporting any operations that have timed out.
     */
    private static final int REPORT_INTERVAL_MS = Math.max(0, MONITORING_REPORT_INTERVAL_MS.getInt());

    /**
     * Defines the maximum number of unique timed out queries that will be reported in the logs.
     * Use a negative number to remove any limit.
     */
    private static final int MAX_OPERATIONS = MONITORING_MAX_OPERATIONS.getInt();

    @VisibleForTesting
    static MonitoringTask instance = make(REPORT_INTERVAL_MS, MAX_OPERATIONS);

    private final ScheduledFuture<?> reportingTask;
    private final OperationsQueue failedOperationsQueue;
    private final OperationsQueue slowOperationsQueue;
    private long approxLastLogTimeNanos;


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
        this.failedOperationsQueue = new OperationsQueue(maxOperations);
        this.slowOperationsQueue = new OperationsQueue(maxOperations);

        this.approxLastLogTimeNanos = approxTime.now();

        logger.info("Scheduling monitoring task with report interval of {} ms, max operations {}", reportIntervalMillis, maxOperations);
        this.reportingTask = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(() -> logOperations(approxTime.now()),
                                                                                     reportIntervalMillis,
                                                                                     reportIntervalMillis,
                                                                                     TimeUnit.MILLISECONDS);
    }

    public void cancel()
    {
        reportingTask.cancel(false);
    }

    static void addFailedOperation(Monitorable operation, long nowNanos)
    {
        instance.failedOperationsQueue.offer(new FailedOperation(operation, nowNanos));
    }

    static void addSlowOperation(Monitorable operation, long nowNanos)
    {
        instance.slowOperationsQueue.offer(new SlowOperation(operation, nowNanos));
    }

    @VisibleForTesting
    List<String> getFailedOperations()
    {
        return getLogMessages(failedOperationsQueue.popOperations());
    }

    @VisibleForTesting
    List<String> getSlowOperations()
    {
        return getLogMessages(slowOperationsQueue.popOperations());
    }

    private List<String> getLogMessages(AggregatedOperations operations)
    {
        String ret = operations.getLogMessage();
        return ret.isEmpty() ? Collections.emptyList() : Arrays.asList(ret.split("\n"));
    }

    @VisibleForTesting
    private void logOperations(long approxCurrentTimeNanos)
    {
        logSlowOperations(approxCurrentTimeNanos);
        logFailedOperations(approxCurrentTimeNanos);

        approxLastLogTimeNanos = approxCurrentTimeNanos;
    }

    @VisibleForTesting
    boolean logFailedOperations(long nowNanos)
    {
        AggregatedOperations failedOperations = failedOperationsQueue.popOperations();
        if (!failedOperations.isEmpty())
        {
            long elapsedNanos = nowNanos - approxLastLogTimeNanos;
            noSpamLogger.warn("Some operations timed out, details available at debug level (debug.log)");

            if (logger.isDebugEnabled())
                logger.debug("{} operations timed out in the last {} msecs:{}{}",
                            failedOperations.num(),
                             NANOSECONDS.toMillis(elapsedNanos),
                            LINE_SEPARATOR,
                            failedOperations.getLogMessage());
            return true;
        }

        return false;
    }

    @VisibleForTesting
    boolean logSlowOperations(long approxCurrentTimeNanos)
    {
        AggregatedOperations slowOperations = slowOperationsQueue.popOperations();
        if (!slowOperations.isEmpty())
        {
            long approxElapsedNanos = approxCurrentTimeNanos - approxLastLogTimeNanos;
            noSpamLogger.info("Some operations were slow, details available at debug level (debug.log)");

            if (logger.isDebugEnabled())
                logger.debug("{} operations were slow in the last {} msecs:{}{}",
                             slowOperations.num(),
                             NANOSECONDS.toMillis(approxElapsedNanos),
                             LINE_SEPARATOR,
                             slowOperations.getLogMessage());
            return true;
        }
        return false;
    }

    /**
     * A wrapper for a queue that can be either bounded, in which case
     * we increment a counter if we exceed the queue size, or unbounded.
     */
    private static final class OperationsQueue
    {
        /** The max operations on the queue. If this value is zero then logging is disabled
         * and the queue will always be empty. If this value is negative then the queue is unbounded.
         */
        private final int maxOperations;

        /**
         * The operations queue, it can be either bounded or unbounded depending on the value of maxOperations.
         */
        private final BlockingQueue<Operation> queue;

        /**
         * If we fail to add an operation to the queue then we increment this value. We reset this value
         * when the queue is emptied.
         */
        private final AtomicLong numDroppedOperations;

        OperationsQueue(int maxOperations)
        {
            this.maxOperations = maxOperations;
            this.queue = maxOperations > 0 ? newBlockingQueue(maxOperations) : newBlockingQueue();
            this.numDroppedOperations = new AtomicLong();
        }

        /**
         * Add an operation to the queue, if possible, or increment the dropped counter.
         *
         * @param operation - the operations to add
         */
        private void offer(Operation operation)
        {
            if (maxOperations == 0)
                return; // logging of operations is disabled

            if (!queue.offer(operation))
                numDroppedOperations.incrementAndGet();
        }


        /**
         * Return all operations in the queue, aggregated by name, and reset
         * the counter for dropped operations.
         *
         * @return - the aggregated operations
         */
        private AggregatedOperations popOperations()
        {
            Map<String, Operation> operations = new HashMap<>();

            Operation operation;
            while((operation = queue.poll()) != null)
            {
                Operation existing = operations.get(operation.name());
                if (existing != null)
                    existing.add(operation);
                else
                    operations.put(operation.name(), operation);
            }
            return new AggregatedOperations(operations, numDroppedOperations.getAndSet(0L));
        }
    }

    /**
     * Convert a map of aggregated operations into a log message that
     * includes the information of whether some operations were dropped.
     */
    private static final class AggregatedOperations
    {
        private final Map<String, Operation> operations;
        private final long numDropped;

        AggregatedOperations(Map<String, Operation> operations, long numDropped)
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

        String getLogMessage()
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

        private static void addOperation(StringBuilder ret, Operation operation)
        {
            if (ret.length() > 0)
                ret.append(LINE_SEPARATOR);

            ret.append(operation.getLogMessage());
        }
    }

    /**
     * A wrapper class for an operation that either failed (timed-out) or
     * was reported as slow. Because the same operation (query) may execute
     * multiple times, we aggregate the number of times an operation with the
     * same name (CQL query text) is reported and store the average, min and max
     * times.
     */
    protected abstract static class Operation
    {
        /** The operation that was reported as slow or timed out */
        final Monitorable operation;

        /** The number of times the operation was reported */
        int numTimesReported;

        /** The total time spent by this operation */
        long totalTimeNanos;

        /** The maximum time spent by this operation */
        long maxTime;

        /** The minimum time spent by this operation */
        long minTime;

        /** The name of the operation, i.e. the SELECT query CQL,
         * this is set lazily as it takes time to build the query CQL */
        private String name;

        Operation(Monitorable operation, long failedAtNanos)
        {
            this.operation = operation;
            numTimesReported = 1;
            totalTimeNanos = failedAtNanos - operation.creationTimeNanos();
            minTime = totalTimeNanos;
            maxTime = totalTimeNanos;
        }

        public String name()
        {
            if (name == null)
                name = operation.name();
            return name;
        }

        void add(Operation operation)
        {
            numTimesReported++;
            totalTimeNanos += operation.totalTimeNanos;
            maxTime = Math.max(maxTime, operation.maxTime);
            minTime = Math.min(minTime, operation.minTime);
        }

        public abstract String getLogMessage();
    }

    /**
     * An operation (query) that timed out.
     */
    private final static class FailedOperation extends Operation
    {
        FailedOperation(Monitorable operation, long failedAtNanos)
        {
            super(operation, failedAtNanos);
        }

        public String getLogMessage()
        {
            if (numTimesReported == 1)
                return String.format("<%s>, total time %d msec, timeout %d %s",
                                     name(),
                                     NANOSECONDS.toMillis(totalTimeNanos),
                                     NANOSECONDS.toMillis(operation.timeoutNanos()),
                                     operation.isCrossNode() ? "msec/cross-node" : "msec");
            else
                return String.format("<%s> timed out %d times, avg/min/max %d/%d/%d msec, timeout %d %s",
                                     name(),
                                     numTimesReported,
                                     NANOSECONDS.toMillis(totalTimeNanos / numTimesReported),
                                     NANOSECONDS.toMillis(minTime),
                                     NANOSECONDS.toMillis(maxTime),
                                     NANOSECONDS.toMillis(operation.timeoutNanos()),
                                     operation.isCrossNode() ? "msec/cross-node" : "msec");
        }
    }

    /**
     * An operation (query) that was reported as slow.
     */
    private final static class SlowOperation extends Operation
    {
        SlowOperation(Monitorable operation, long failedAt)
        {
            super(operation, failedAt);
        }

        public String getLogMessage()
        {
            if (numTimesReported == 1)
                return String.format("<%s>, time %d msec - slow timeout %d %s",
                                     name(),
                                     NANOSECONDS.toMillis(totalTimeNanos),
                                     NANOSECONDS.toMillis(operation.slowTimeoutNanos()),
                                     operation.isCrossNode() ? "msec/cross-node" : "msec");
            else
                return String.format("<%s>, was slow %d times: avg/min/max %d/%d/%d msec - slow timeout %d %s",
                                     name(),
                                     numTimesReported,
                                     NANOSECONDS.toMillis(totalTimeNanos/ numTimesReported),
                                     NANOSECONDS.toMillis(minTime),
                                     NANOSECONDS.toMillis(maxTime),
                                     NANOSECONDS.toMillis(operation.slowTimeoutNanos()),
                                     operation.isCrossNode() ? "msec/cross-node" : "msec");
        }
    }
}
