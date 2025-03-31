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
package org.apache.cassandra.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlan.ForWrite;
import org.apache.cassandra.net.CallbackResponseTracker;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.getCounterWriteRpcTimeout;
import static org.apache.cassandra.config.DatabaseDescriptor.getWriteRpcTimeout;
import static org.apache.cassandra.db.WriteType.COUNTER;
import static org.apache.cassandra.locator.Replicas.countInOurDc;
import static org.apache.cassandra.schema.Schema.instance;
import static org.apache.cassandra.service.StorageProxy.WritePerformer;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class WriteResponseHandler<T> implements RequestCallback<T>
{
    protected static final Logger logger = LoggerFactory.getLogger(WriteResponseHandler.class);

    private final Condition condition = newOneTimeCondition();
    protected final ReplicaPlan.ForWrite replicaPlan;

    protected final Runnable callback;
    protected final WriteType writeType;

    private static final AtomicIntegerFieldUpdater<WriteResponseHandler> failuresUpdater =
        AtomicIntegerFieldUpdater.newUpdater(WriteResponseHandler.class, "failureCount");
    private volatile int failureCount = 0;

    private final AtomicBoolean recordedMetric = new AtomicBoolean(false);

    protected final Dispatcher.RequestTime requestTime;
    protected final CallbackResponseTracker tracker;
    private @Nullable final Supplier<Mutation> hintOnFailure;

    /**
     * The count of all the replicas we'd need to hit across DC's to qualify as hitting our "ideal"
     *
     * Of note, this isn't a calculation of what would qualify as a quorum across all DC's, i.e. "EACH_QUORUM", but rather
     * a brute force calculation of what would equate to CL_ALL. This is how we originally calculated things on implementation
     * in CASSANDRA-13289 so we continue with that. It would be relatively trivial to extend this logic to include
     * a per-DC calculation similar to how we track different DC responses in {@link EachQuorumResponseHandler#pendingResponsesPerDC}
     */
    @SuppressWarnings("JavadocReference")
    protected final int idealCLReplicaCount;

    public WriteResponseHandler(ForWrite replicaPlan,
                                   WriteType writeType,
                                   Supplier<Mutation> hintOnFailure,
                                   Dispatcher.RequestTime requestTime)
    {
        this(replicaPlan, null, writeType, hintOnFailure, requestTime);
    }

    /**
     * @param callback           A callback to be called when the write is successful.
     * @param hintOnFailure      Enable/disable hinting on write failure
     * @param requestTime        Initial request time of the mutation to be used for timeouts and backpressure calculation
     */
    public WriteResponseHandler(ForWrite replicaPlan,
                                           Runnable callback,
                                           WriteType writeType,
                                           Supplier<Mutation> hintOnFailure,
                                           Dispatcher.RequestTime requestTime)
    {
        this.replicaPlan = replicaPlan;
        this.callback = callback;
        this.writeType = writeType;
        this.hintOnFailure = hintOnFailure;
        this.tracker = new CallbackResponseTracker(replicaPlan.contacts().endpoints(), WriteResponseHandler.blockFor(replicaPlan));
        this.requestTime = requestTime;
        this.idealCLReplicaCount = replicaPlan.contacts().size();
    }

    /**
     * Intended for use in DC-aware CL's, we'll track the DC responses in the response tracker.
     */
    public void trackIdealCL(ReplicaPlan.ForWrite replicaPlan)
    {
        tracker.enableIdealCLTracking(replicaPlan);
    }

    public void get() throws WriteTimeoutException, WriteFailureException
    {
        long timeoutNanos = currentTimeoutNanos();

        boolean signaled;
        Map<InetAddressAndPort, RequestFailureReason> failuresByEndpoint;
        try
        {
            signaled = condition.await(timeoutNanos, NANOSECONDS);
            failuresByEndpoint = tracker.endProcessing();
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }

        if (signaled && receivedSufficientResponses())
        {
            replicaPlan.checkStillAppliesTo(ClusterMetadata.current());
            return;
        }

        if (!signaled)
            throwTimeout(failuresByEndpoint);

        if (!receivedSufficientResponses())
        {
            // We only want to consider replicas that count towards our blockFor() / requiredResponses threshold here to flag as a timeout
            if (RequestCallback.isTimeout(failuresByEndpoint.keySet().stream()
                                                           .filter(this::waitingFor)
                                                           .collect(Collectors.toMap(Function.identity(), failuresByEndpoint::get))))
                throwTimeout(failuresByEndpoint);
            throw new WriteFailureException(writeType, replicaPlan.consistencyLevel(), tracker.responseCount(), blockFor(), failuresByEndpoint);
        }

        replicaPlan.checkStillAppliesTo(ClusterMetadata.current());
    }

    private void throwTimeout(Map<InetAddressAndPort, RequestFailureReason> failures)
    {
        int blockedFor = blockFor();
        int acks = tracker.responseCount();
        // It's pretty unlikely, but we can race between exiting await above and here, so
        // that we could now have enough acks. In that case, we "lie" on the acks count to
        // avoid sending confusing info to the user (see CASSANDRA-6491).
        if (acks >= blockedFor)
            acks = blockedFor - 1;
        throw WriteTimeoutException.withParticipants(writeType, replicaPlan.consistencyLevel(), acks, blockedFor, failures);
    }

    public final long currentTimeoutNanos()
    {
        long now = nanoTime();
        long requestTimeout = writeType == COUNTER
                              ? getCounterWriteRpcTimeout(NANOSECONDS)
                              : getWriteRpcTimeout(NANOSECONDS);
        return requestTime.computeTimeout(now, requestTimeout);
    }

    /**
     * @return the minimum number of endpoints that must respond.
     */
    @VisibleForTesting
    public int blockFor()
    {
        // During bootstrap, we have to include the pending endpoints or we may fail the consistency level
        // guarantees (see #833)
        return blockFor(replicaPlan);
    }

    public static int blockFor(ForWrite replicaPlan)
    {
        return replicaPlan.writeQuorum();
    }

    /**
     * TODO: this method is brittle for its purpose of deciding when we should fail a query;
     *       this needs to be aware of which nodes are live/down
     * @return the total number of endpoints the request can send to.
     */
    protected int candidateReplicaCount()
    {
        if (replicaPlan.consistencyLevel().isDatacenterLocal())
            return countInOurDc(replicaPlan.liveAndDown()).allReplicas();

        return replicaPlan.liveAndDown().size();
    }

    public ConsistencyLevel consistencyLevel()
    {
        return replicaPlan.consistencyLevel();
    }

    /**
     * @return true if the message counts towards the blockFor() threshold
     */
    protected boolean waitingFor(InetAddressAndPort from)
    {
        return true;
    }

    public Dispatcher.RequestTime getRequestTime()
    {
        return requestTime;
    }

    /**
     * null message means "response from local write"
     */
    public void onResponse(Message<T> msg)
    {
        InetAddressAndPort from = msg == null ? FBUtilities.getBroadcastAddressAndPort() : msg.from();
        replicaPlan.collectSuccess(from);
        tracker.recordResponse(from);
        if (receivedSufficientResponses())
            signal();
        maybeRecordIdealCLMetrics();
    }

    /**
     * Different write queries will have different criteria for "success" depending on their CL and DC locality. We
     * allow for specific callback implementations to signal whether they got what they needed.
     */
    public boolean receivedSufficientResponses()
    {
        return tracker.isSuccessful();
    }

    protected void signal()
    {
        condition.signalAll();
        if (callback != null)
            callback.run();
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        tracker.recordFailure(from, failureReason);

        int n = waitingFor(from)
                ? failuresUpdater.incrementAndGet(this)
                : failureCount;

        if (blockFor() + n > candidateReplicaCount())
            signal();

        // On the failure path, if we're hitting the point where this is our last replica update, we want to process ideal CL metrics.
        // We do this _after_ signaling so if this is the end of the query's functional life we can tell the clients before
        // any metric post-processing.
        if (tracker.gotAllResponses())
            maybeRecordIdealCLMetrics();

        if (hintOnFailure != null && StorageProxy.shouldHint(replicaPlan.lookup(from)) && requestTime.shouldSendHints())
            StorageProxy.submitHint(hintOnFailure.get(), replicaPlan.lookup(from), null);
    }

    /**
     * If we avoided sending a message entirely because we thought a node was down, we still want to record that failure
     * for purposes of tracking and logging, but we don't want to kick off any of the other hinting or signaling mechanisms
     * from a message we never sent out.
     */
    public void recordFailureNoMessageSent(InetAddressAndPort from)
    {
        tracker.recordFailure(from, RequestFailureReason.NODE_DOWN);
    }

    /**
     * We have a gap here between:
     * 1: When we got enough callback responses to satisfy primary CL, and
     * 2: When we get all callbacks back.
     *
     * We check at each incremental callback between 1 and 2 since any of them could tip us over the edge into success,
     * and we mark failure of the idealCL _only_ once we've processed all callbacks.
     */
    public void maybeRecordIdealCLMetrics()
    {
        if (!tracker.processingIdealCL())
            return;

        // If we've gotten responses indicating that there's no way for the query to succeed based on primary metrics, we
        // don't want to _also_ consider it an idealCL failure. We don't want to close down processing if we're at our
        // CL for the primary request since successes from other DC's might (likely will) come in _later_ than our regular
        // successes.
        if (!receivedSufficientResponses())
            return;

        // Small optimization check here to prevent repeat tracker.hitIdealConsistencyLevel checks after we've recorded success case
        if (recordedMetric.get())
            return;

        boolean hitIdealCL = tracker.hitIdealConsistencyLevel();

        // We want to record success as early as possible and not wait for all results to come in; only want to do this once.
        if (hitIdealCL)
        {
            if (recordedMetric.compareAndSet(false, true))
                replicaPlan.keyspace().metric.idealCLWriteLatency.addNano(nanoTime() - requestTime.startedAtNanos());
            return;
        }

        // We have to check for the CAS sentinel here since we can't otherwise distinguish between whether we had an
        // early success we already recorded and this is our final callback or whether this final response is the
        // success that tips us over the edge, but we didn't get enough to warrant idealCL.
        if (tracker.gotAllResponses() && recordedMetric.compareAndSet(false, true))
        {
            // By definition, if we hitDCQuorum we'll have already recorded it above and short-circuited out, so the only
            // way we can reach this point is a failure.
            replicaPlan.keyspace().metric.writeFailedIdealCL.inc();
        }
    }

    @Override
    public boolean invokeOnFailure()
    {
        return true;
    }

    /**
     * Cheap Quorum backup.  If we failed to reach quorum with our initial (full) nodes, reach out to other nodes.
     */
    public void maybeTryAdditionalReplicas(IMutation mutation, WritePerformer writePerformer, String localDC)
    {
        EndpointsForToken uncontacted = replicaPlan.liveUncontacted();
        if (uncontacted.isEmpty())
            return;

        long timeout = MAX_VALUE;
        List<ColumnFamilyStore> cfs = mutation.getTableIds().stream()
                                              .map(instance::getColumnFamilyStoreInstance)
                                              .toList();
        for (ColumnFamilyStore cf : cfs)
            timeout = min(timeout, cf.additionalWriteLatencyMicros);

        // no latency information, or we're overloaded
        if (timeout > mutation.getTimeout(MICROSECONDS))
            return;

        try
        {
            if (!condition.await(timeout, MICROSECONDS))
            {
                for (ColumnFamilyStore cf : cfs)
                    cf.metric.additionalWrites.inc();

                writePerformer.apply(mutation, replicaPlan.withContacts(uncontacted),
                                     (WriteResponseHandler<IMutation>) this,
                                     localDC,
                                     requestTime);
            }
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
    }
}