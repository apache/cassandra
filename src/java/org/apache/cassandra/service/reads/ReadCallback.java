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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.MessageParams;
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
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ReadCallback<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E>> implements RequestCallback<ReadResponse>
{
    protected static final Logger logger = LoggerFactory.getLogger( ReadCallback.class );
    private class WarningCounter
    {
        // the highest number of tombstones reported by a node's warning
        final AtomicInteger tombstoneWarnings = new AtomicInteger();
        final AtomicInteger maxTombstoneWarningCount = new AtomicInteger();
        // the highest number of tombstones reported by a node's rejection. This should be the same as
        // our configured limit, but including to aid in diagnosing misconfigurations
        final AtomicInteger tombstoneAborts = new AtomicInteger();
        final AtomicInteger maxTombstoneAbortsCount = new AtomicInteger();

        // TODO: take message as arg and return boolean for 'had warning' etc
        void addTombstoneWarning(InetAddressAndPort from, int tombstones)
        {
            if (!waitingFor(from)) return;
            tombstoneWarnings.incrementAndGet();
            maxTombstoneWarningCount.accumulateAndGet(tombstones, Math::max);
        }

        void addTombstoneAbort(InetAddressAndPort from, int tombstones)
        {
            if (!waitingFor(from)) return;
            tombstoneAborts.incrementAndGet();
            maxTombstoneAbortsCount.accumulateAndGet(tombstones, Math::max);
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
    private volatile WarningCounter warningCounter;
    private static final AtomicReferenceFieldUpdater<ReadCallback, ReadCallback.WarningCounter> warningsUpdater
        = AtomicReferenceFieldUpdater.newUpdater(ReadCallback.class, ReadCallback.WarningCounter.class, "warningCounter");

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
    public static String tombstoneAbortMessage(int nodes, int tombstones, String cql)
    {
        return String.format("%s nodes scanned over %s tombstones and aborted the query %s (see tombstone_failure_threshold)", nodes, tombstones, cql);
    }

    @VisibleForTesting
    public static String tombstoneWarnMessage(int nodes, int tombstones, String cql)
    {
        return String.format("%s nodes scanned up to %s tombstones and issued tombstone warnings for query %s  (see tombstone_warn_threshold)", nodes, tombstones, cql);
    }

    private ColumnFamilyStore cfs()
    {
        return Schema.instance.getColumnFamilyStoreInstance(command.metadata().id);
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
        WarningCounter warnings = warningCounter;
        if (warnings != null)
        {
            if (warnings.tombstoneAborts.get() > 0)
            {
                String msg = tombstoneAbortMessage(warnings.tombstoneAborts.get(), warnings.maxTombstoneAbortsCount.get(), command.toCQLString());
                ClientWarn.instance.warn(msg + " with " + command.loggableTokens());
                logger.warn("{} with query {}", msg, command.toCQLString());
                cfs().metric.clientTombstoneAborts.mark();
            }

            if (warnings.tombstoneWarnings.get() > 0)
            {
                String msg = tombstoneWarnMessage(warnings.tombstoneWarnings.get(), warnings.maxTombstoneWarningCount.get(), command.toCQLString());
                ClientWarn.instance.warn(msg + " with " + command.loggableTokens());
                logger.warn("{} with query {}", msg, command.toCQLString());
                cfs().metric.clientTombstoneWarnings.mark();
            }
        }
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

        if (warnings != null && warnings.tombstoneAborts.get() > 0)
            throw new TombstoneAbortException(warnings.tombstoneAborts.get(), warnings.maxTombstoneAbortsCount.get(), command.toCQLString(), resolver.isDataPresent(),
                                              replicaPlan.get().consistencyLevel(), received, blockFor, failureReasonByEndpoint);

        // Same as for writes, see AbstractWriteResponseHandler
        throw failed
            ? new ReadFailureException(replicaPlan().consistencyLevel(), received, blockFor, resolver.isDataPresent(), failureReasonByEndpoint)
            : new ReadTimeoutException(replicaPlan().consistencyLevel(), received, blockFor, resolver.isDataPresent());
    }

    public int blockFor()
    {
        return blockFor;
    }

    @Override
    public void onResponse(Message<ReadResponse> message)
    {
        assertWaitingFor(message.from());
        Map<ParamType, Object> params = message.header.params();
        if (params.containsKey(ParamType.TOMBSTONE_ABORT))
        {
            getWarningCounter().addTombstoneAbort(message.from(), (Integer) params.get(ParamType.TOMBSTONE_ABORT));
            onFailure(message.from(), RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
            return;
        }
        else if (params.containsKey(ParamType.TOMBSTONE_WARNING))
        {
            getWarningCounter().addTombstoneWarning(message.from(), (Integer) params.get(ParamType.TOMBSTONE_WARNING));
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

    private WarningCounter getWarningCounter()
    {
        WarningCounter current;
        do {

            current = warningCounter;
            if (current != null)
                return current;

            current = new WarningCounter();
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
        assert waitingFor(from): "Received read response from unexpected replica: " + from;
    }

    private boolean waitingFor(InetAddressAndPort from)
    {
        return !replicaPlan().consistencyLevel().isDatacenterLocal()
               || DatabaseDescriptor.getLocalDataCenter().equals(DatabaseDescriptor.getEndpointSnitch().getDatacenter(from));
    }
}
