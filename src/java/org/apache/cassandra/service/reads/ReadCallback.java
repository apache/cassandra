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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.MessageParams;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.utils.concurrent.Condition;
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
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.reads.thresholds.CoordinatorWarnings;
import org.apache.cassandra.service.reads.thresholds.WarningContext;
import org.apache.cassandra.service.reads.thresholds.WarningsSnapshot;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;
import static org.apache.cassandra.tracing.Tracing.isTracing;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;

public class ReadCallback<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>> implements RequestCallback<ReadResponse>
{
    protected static final Logger logger = LoggerFactory.getLogger(ReadCallback.class);

    public final ResponseResolver<E, P> resolver;
    final Condition condition = newOneTimeCondition();
    private final long queryStartNanoTime;
    final int blockFor; // TODO: move to replica plan as well?
    // this uses a plain reference, but is initialised before handoff to any other threads; the later updates
    // may not be visible to the threads immediately, but ReplicaPlan only contains final fields, so they will never see an uninitialised object
    final ReplicaPlan.Shared<E, P> replicaPlan;
    private final ReadCommand command;
    private static final AtomicIntegerFieldUpdater<ReadCallback> failuresUpdater
            = newUpdater(ReadCallback.class, "failures");
    private volatile int failures = 0;
    private final Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint;
    private volatile WarningContext warningContext;
    private static final AtomicReferenceFieldUpdater<ReadCallback, WarningContext> warningsUpdater
        = AtomicReferenceFieldUpdater.newUpdater(ReadCallback.class, WarningContext.class, "warningContext");

    public ReadCallback(ResponseResolver<E, P> resolver, ReadCommand command, ReplicaPlan.Shared<E, P> replicaPlan, long queryStartNanoTime)
    {
        this.command = command;
        this.resolver = resolver;
        this.queryStartNanoTime = queryStartNanoTime;
        this.replicaPlan = replicaPlan;
        this.blockFor = replicaPlan.get().readQuorum();
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
        long time = unit.toNanos(timePastStart) - (nanoTime() - queryStartNanoTime);
        try
        {
            return condition.await(time, NANOSECONDS);
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
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
        // If all messages came back as a TIMEOUT then signaled=true and failed=true.
        // Need to distinguish between a timeout and a failure (network, bad data, etc.), so store an extra field.
        // see CASSANDRA-17828
        boolean timedout = !signaled;
        if (failed)
            timedout = RequestCallback.isTimeout(new HashMap<>(failureReasonByEndpoint));
        WarningContext warnings = warningContext;
        // save the snapshot so abort state is not changed between now and when mayAbort gets called
        WarningsSnapshot snapshot = null;
        if (warnings != null)
        {
            snapshot = warnings.snapshot();
            // this is possible due to a race condition between waiting and responding
            // network thread creates the WarningContext to update metrics, but we are actively reading and see it is empty
            // this is likely to happen when a timeout happens or from a speculative response
            if (!snapshot.isEmpty())
                CoordinatorWarnings.update(command, snapshot);
        }
        if (signaled && !failed)
            return;

        if (isTracing())
        {
            String gotData = received > 0 ? (resolver.isDataPresent() ? " (including data)" : " (only digests)") : "";
            Tracing.trace("{}; received {} of {} responses{}", !timedout ? "Failed" : "Timed out", received, blockFor, gotData);
        }
        else if (logger.isDebugEnabled())
        {
            String gotData = received > 0 ? (resolver.isDataPresent() ? " (including data)" : " (only digests)") : "";
            logger.debug("{}; received {} of {} responses{}", !timedout ? "Failed" : "Timed out", received, blockFor, gotData);
        }

        if (snapshot != null)
            snapshot.maybeAbort(command, replicaPlan().consistencyLevel(), received, blockFor, resolver.isDataPresent(), failureReasonByEndpoint);

        // Same as for writes, see AbstractWriteResponseHandler
        throw !timedout
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
        InetAddressAndPort from = message.from();
        if (WarningContext.isSupported(params.keySet()))
        {
            RequestFailureReason reason = getWarningContext().updateCounters(params, from);
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
