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
package org.apache.cassandra.service.reads;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.MessageParams;
import org.apache.cassandra.exceptions.ReadSizeAbortException;
import org.apache.cassandra.exceptions.TombstoneAbortException;
import org.apache.cassandra.locator.ReplicaPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.reads.trackwarnings.Shared;
import org.apache.cassandra.service.reads.trackwarnings.TrackWarningsSnapshot;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ReadCallback<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E>> implements RequestCallback<ReadResponse>
{
    protected static final Logger logger = LoggerFactory.getLogger( ReadCallback.class );

    private static class WarnAbortCounter
    {
        final AtomicInteger warnings = new AtomicInteger();
        // the highest number reported by a node's warning
        final AtomicLong maxWarningValue = new AtomicLong();

        final AtomicInteger aborts = new AtomicInteger();
        // the highest number reported by a node's rejection.
        final AtomicLong maxAbortsValue = new AtomicLong();

        void addWarning(long value)
        {
            warnings.incrementAndGet();
            maxWarningValue.accumulateAndGet(value, Math::max);
        }

        void addAbort(long value)
        {
            aborts.incrementAndGet();
            maxAbortsValue.accumulateAndGet(value, Math::max);
        }

        TrackWarningsSnapshot.Warnings snapshot()
        {
            return TrackWarningsSnapshot.Warnings.create(TrackWarningsSnapshot.Counter.create(warnings.get(), maxWarningValue.get()), TrackWarningsSnapshot.Counter.create(aborts.get(), maxAbortsValue.get()));
        }
    }

    private interface ToString
    {
        String apply(int count, long value, String cql);
    }

    public class WarningContext
    {
        final WarnAbortCounter tombstones = new WarnAbortCounter();
        final WarnAbortCounter localReadSize = new WarnAbortCounter();
        final WarnAbortCounter rowIndexTooLarge = new WarnAbortCounter();

        private String cql, loggableTokens;
        private String cql()
        {
            if (cql == null)
                cql = command.toCQLString();
            return cql;
        }
        private String loggableTokens()
        {
            if (loggableTokens == null)
                loggableTokens = command.loggableTokens();
            return loggableTokens;
        }

        public TrackWarningsSnapshot snapshot()
        {
            return TrackWarningsSnapshot.create(tombstones.snapshot(), localReadSize.snapshot(), rowIndexTooLarge.snapshot());
        }

        private void trackAborts(WarnAbortCounter counter, TableMetrics.TableMeter metric, ToString toString)
        {
            if (counter.aborts.get() > 0)
            {
                String msg = toString.apply(counter.aborts.get(), counter.maxAbortsValue.get(), cql());
                ClientWarn.instance.warn(msg + " with " + loggableTokens());
                logger.warn(msg);
                metric.mark();
            }
        }

        private void trackWarnings(WarnAbortCounter counter, TableMetrics.TableMeter metric, ToString toString)
        {
            if (counter.warnings.get() > 0)
            {
                String msg = toString.apply(counter.warnings.get(), counter.maxWarningValue.get(), cql());
                ClientWarn.instance.warn(msg + " with " + loggableTokens());
                logger.warn(msg);
                metric.mark();
            }
        }

        void track()
        {
            Shared.update(command, snapshot());
//            trackAborts(tombstones, cfs().metric.clientTombstoneAborts, ReadCallback::tombstoneAbortMessage);
//            trackWarnings(tombstones, cfs().metric.clientTombstoneWarnings, ReadCallback::tombstoneWarnMessage);
//
//            trackAborts(localReadSize, cfs().metric.localReadSizeAborts, ReadCallback::localReadSizeAbortMessage);
//            trackWarnings(localReadSize, cfs().metric.localReadSizeWarnings, ReadCallback::localReadSizeWarnMessage);
//
//            trackAborts(rowIndexTooLarge, cfs().metric.rowIndexSizeAborts, ReadCallback::rowIndexSizeAbortMessage);
//            trackWarnings(rowIndexTooLarge, cfs().metric.rowIndexSizeWarnings, ReadCallback::rowIndexSizeWarnMessage);
        }

//        private ColumnFamilyStore cfs()
//        {
//            return Schema.instance.getColumnFamilyStoreInstance(command.metadata().id);
//        }

        void mayAbort(int received)
        {
            if (tombstones.aborts.get() > 0)
                throw new TombstoneAbortException(tombstones.aborts.get(), tombstones.maxAbortsValue.get(), cql(), resolver.isDataPresent(),
                                                  replicaPlan.get().consistencyLevel(), received, blockFor, failureReasonByEndpoint);

            if (localReadSize.aborts.get() > 0)
                throw new ReadSizeAbortException(localReadSizeAbortMessage(localReadSize.aborts.get(), localReadSize.maxAbortsValue.get(), cql()),
                                                 replicaPlan.get().consistencyLevel(), received, blockFor, resolver.isDataPresent(), failureReasonByEndpoint);

            if (rowIndexTooLarge.aborts.get() > 0)
                throw new ReadSizeAbortException(rowIndexSizeAbortMessage(rowIndexTooLarge.aborts.get(), rowIndexTooLarge.maxAbortsValue.get(), cql()),
                                                 replicaPlan.get().consistencyLevel(), received, blockFor, resolver.isDataPresent(), failureReasonByEndpoint);
        }
    }

    public final ResponseResolver<E, P> resolver;
    final SimpleCondition condition = new SimpleCondition();
    private final long queryStartNanoTime;
    final int blockFor; // TODO: move to replica plan as well?
    // this uses a plain reference, but is initialised before handoff to any other threads; the later updates
    // may not be visible to the threads immediately, but ReplicaPlan only contains final fields, so they will never see an uninitialised object
    final ReplicaPlan.Shared<E, P> replicaPlan;
    private final ReadCommand command;
    private static final AtomicIntegerFieldUpdater<ReadCallback> failuresUpdater
            = AtomicIntegerFieldUpdater.newUpdater(ReadCallback.class, "failures");
    private volatile int failures = 0;
    private final Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint;
    private volatile WarningContext warningContext;
    private static final AtomicReferenceFieldUpdater<ReadCallback, ReadCallback.WarningContext> warningsUpdater
        = AtomicReferenceFieldUpdater.newUpdater(ReadCallback.class, ReadCallback.WarningContext.class, "warningContext");

    public ReadCallback(ResponseResolver<E, P> resolver, ReadCommand command, ReplicaPlan.Shared<E, P> replicaPlan, long queryStartNanoTime)
    {
        this.command = command;
        this.resolver = resolver;
        this.queryStartNanoTime = queryStartNanoTime;
        this.replicaPlan = replicaPlan;
        this.blockFor = replicaPlan.get().blockFor();
        this.failureReasonByEndpoint = new ConcurrentHashMap<>();
        // we don't support read repair (or rapid read protection) for range scans yet (CASSANDRA-6897)
        assert !(command instanceof PartitionRangeReadCommand) || blockFor >= replicaPlan().contacts().size();

        if (logger.isTraceEnabled())
            logger.trace("Blockfor is {}; setting up requests to {}", blockFor, this.replicaPlan);
    }

    protected P replicaPlan()
    {
        return replicaPlan.get();
    }

    public boolean await(long timePastStart, TimeUnit unit)
    {
        long time = unit.toNanos(timePastStart) - (System.nanoTime() - queryStartNanoTime);
        try
        {
            return condition.await(time, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }
    }

    @VisibleForTesting
    public static String tombstoneAbortMessage(int nodes, long tombstones, String cql)
    {
        return String.format("%s nodes scanned over %s tombstones and aborted the query %s (see tombstone_failure_threshold)", nodes, tombstones, cql);
    }

    @VisibleForTesting
    public static String tombstoneWarnMessage(int nodes, long tombstones, String cql)
    {
        return String.format("%s nodes scanned up to %s tombstones and issued tombstone warnings for query %s  (see tombstone_warn_threshold)", nodes, tombstones, cql);
    }

    @VisibleForTesting
    public static String localReadSizeAbortMessage(long nodes, long bytes, String cql)
    {
        return String.format("%s nodes loaded over %s bytes and aborted the query %s (see track_warnings.local_read_size.abort_threshold_kb)", nodes, bytes, cql);
    }

    @VisibleForTesting
    public static String localReadSizeWarnMessage(int nodes, long bytes, String cql)
    {
        return String.format("%s nodes loaded over %s bytes and issued local read size warnings for query %s  (see track_warnings.local_read_size.warn_threshold_kb)", nodes, bytes, cql);
    }

    @VisibleForTesting
    public static String rowIndexSizeAbortMessage(long nodes, long bytes, String cql)
    {
        return String.format("%s nodes loaded over %s bytes in RowIndexEntry and aborted the query %s (see track_warnings.row_index_size.abort_threshold_kb)", nodes, bytes, cql);
    }

    @VisibleForTesting
    public static String rowIndexSizeWarnMessage(int nodes, long bytes, String cql)
    {
        return String.format("%s nodes loaded over %s bytes in RowIndexEntry and issued warnings for query %s  (see track_warnings.row_index_size.warn_threshold_kb)", nodes, bytes, cql);
    }

    public void awaitResults() throws ReadFailureException, ReadTimeoutException
    {
        boolean signaled = await(command.getTimeout(MILLISECONDS), TimeUnit.MILLISECONDS);
        /**
         * Here we are checking isDataPresent in addition to the responses size because there is a possibility
         * that an asynchronous speculative execution request could be returning after a local failure already
         * signaled. Responses may have been set while the data reference is not yet.
         * See {@link DigestResolver#preprocess(Message)}
         * CASSANDRA-16097
         */
        int received = resolver.responses.size();
        boolean failed = failures > 0 && (blockFor > received || !resolver.isDataPresent());
        WarningContext warnings = warningContext;
        if (warnings != null)
            warnings.track();
        if (signaled && !failed)
            return;

        if (Tracing.isTracing())
        {
            String gotData = received > 0 ? (resolver.isDataPresent() ? " (including data)" : " (only digests)") : "";
            Tracing.trace("{}; received {} of {} responses{}", failed ? "Failed" : "Timed out", received, blockFor, gotData);
        }
        else if (logger.isDebugEnabled())
        {
            String gotData = received > 0 ? (resolver.isDataPresent() ? " (including data)" : " (only digests)") : "";
            logger.debug("{}; received {} of {} responses{}", failed ? "Failed" : "Timed out", received, blockFor, gotData);
        }

        if (warnings != null)
            warnings.mayAbort(received);

        // Same as for writes, see AbstractWriteResponseHandler
        throw failed
            ? new ReadFailureException(replicaPlan().consistencyLevel(), received, blockFor, resolver.isDataPresent(), failureReasonByEndpoint)
            : new ReadTimeoutException(replicaPlan().consistencyLevel(), received, blockFor, resolver.isDataPresent());
    }

    public int blockFor()
    {
        return blockFor;
    }

    private RequestFailureReason updateCounters(Map<ParamType, Object> params,
                                                ParamType abort, ParamType warn,
                                                RequestFailureReason reason,
                                                Function<WarningContext, WarnAbortCounter> fieldAccess)
    {
        // some checks use int32 others user int64; so rely on Number to handle both cases
        if (params.containsKey(abort))
        {
            fieldAccess.apply(getWarningContext()).addAbort(((Number) params.get(abort)).longValue());
            return reason;
        }
        else if (params.containsKey(warn))
        {
            fieldAccess.apply(getWarningContext()).addWarning(((Number) params.get(warn)).longValue());
        }
        return null;
    }

    @Override
    public void onResponse(Message<ReadResponse> message)
    {
        assertWaitingFor(message.from());
        Map<ParamType, Object> params = message.header.params();
        for (Supplier<RequestFailureReason> fn : Arrays.<Supplier<RequestFailureReason>>asList(
        () -> updateCounters(params, ParamType.TOMBSTONE_ABORT, ParamType.TOMBSTONE_WARNING, RequestFailureReason.READ_TOO_MANY_TOMBSTONES, ctx -> ctx.tombstones),
        () -> updateCounters(params, ParamType.LOCAL_READ_SIZE_ABORT, ParamType.LOCAL_READ_SIZE_WARN, RequestFailureReason.READ_SIZE, ctx -> ctx.localReadSize),
        () -> updateCounters(params, ParamType.ROW_INDEX_SIZE_ABORT, ParamType.ROW_INDEX_SIZE_WARN, RequestFailureReason.READ_SIZE, ctx -> ctx.rowIndexTooLarge)
        ))
        {
            RequestFailureReason reason = fn.get();
            if (reason != null)
            {
                onFailure(message.from(), reason);
                return;
            }
        }
        resolver.preprocess(message);

        /*
         * Ensure that data is present and the response accumulator has properly published the
         * responses it has received. This may result in not signaling immediately when we receive
         * the minimum number of required results, but it guarantees at least the minimum will
         * be accessible when we do signal. (see CASSANDRA-16807)
         */
        if (resolver.isDataPresent() && resolver.responses.size() >= blockFor)
            condition.signalAll();
    }

    private WarningContext getWarningContext()
    {
        WarningContext current;
        do {

            current = warningContext;
            if (current != null)
                return current;

            current = new WarningContext();
        } while (!warningsUpdater.compareAndSet(this, null, current));
        return current;
    }

    public void response(ReadResponse result)
    {
        Verb kind = command.isRangeRequest() ? Verb.RANGE_RSP : Verb.READ_RSP;
        Message<ReadResponse> message = Message.internalResponse(kind, result);
        message = MessageParams.addToMessage(message);
        onResponse(message);
    }


    @Override
    public boolean trackLatencyForSnitch()
    {
        return true;
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        assertWaitingFor(from);
                
        failureReasonByEndpoint.put(from, failureReason);

        if (blockFor + failuresUpdater.incrementAndGet(this) > replicaPlan().contacts().size())
            condition.signalAll();
    }

    @Override
    public boolean invokeOnFailure()
    {
        return true;
    }

    /**
     * Verify that a message doesn't come from an unexpected replica.
     */
    private void assertWaitingFor(InetAddressAndPort from)
    {
        assert !replicaPlan().consistencyLevel().isDatacenterLocal()
               || DatabaseDescriptor.getLocalDataCenter().equals(DatabaseDescriptor.getEndpointSnitch().getDatacenter(from))
               : "Received read response from unexpected replica: " + from;
    }
}
