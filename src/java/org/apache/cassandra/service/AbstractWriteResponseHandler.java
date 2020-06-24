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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.stream.Collectors;

import org.apache.cassandra.db.ConsistencyLevel;

import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.ReplicaPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

import static java.util.concurrent.TimeUnit.NANOSECONDS;


public abstract class AbstractWriteResponseHandler<T> implements RequestCallback<T>
{
    protected static final Logger logger = LoggerFactory.getLogger(AbstractWriteResponseHandler.class);

    //Count down until all responses and expirations have occured before deciding whether the ideal CL was reached.
    private AtomicInteger responsesAndExpirations;
    private final SimpleCondition condition = new SimpleCondition();
    protected final ReplicaPlan.ForTokenWrite replicaPlan;

    protected final Runnable callback;
    protected final WriteType writeType;
    private static final AtomicIntegerFieldUpdater<AbstractWriteResponseHandler> failuresUpdater
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8592
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13289
    = AtomicIntegerFieldUpdater.newUpdater(AbstractWriteResponseHandler.class, "failures");
    private volatile int failures = 0;
    private final Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint;
    private final long queryStartNanoTime;
    private volatile boolean supportsBackPressure = true;

    /**
      * Delegate to another WriteResponseHandler or possibly this one to track if the ideal consistency level was reached.
      * Will be set to null if ideal CL was not configured
      * Will be set to an AWRH delegate if ideal CL was configured
      * Will be same as "this" if this AWRH is the ideal consistency level
      */
    private AbstractWriteResponseHandler idealCLDelegate;

    /**
     * We don't want to increment the writeFailedIdealCL if we didn't achieve the original requested CL
     */
    private boolean requestedCLAchieved = false;

    /**
     * @param callback           A callback to be called when the write is successful.
     * @param queryStartNanoTime
     */
    protected AbstractWriteResponseHandler(ReplicaPlan.ForTokenWrite replicaPlan,
                                           Runnable callback,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12256
                                           WriteType writeType,
                                           long queryStartNanoTime)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14404
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14705
        this.replicaPlan = replicaPlan;
        this.callback = callback;
        this.writeType = writeType;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12311
        this.failureReasonByEndpoint = new ConcurrentHashMap<>();
        this.queryStartNanoTime = queryStartNanoTime;
    }

    public void get() throws WriteTimeoutException, WriteFailureException
    {
        long timeoutNanos = currentTimeoutNanos();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15066

        boolean success;
        try
        {
            success = condition.await(timeoutNanos, NANOSECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }

        if (!success)
        {
            int blockedFor = blockFor();
            int acks = ackCount();
            // It's pretty unlikely, but we can race between exiting await above and here, so
            // that we could now have enough acks. In that case, we "lie" on the acks count to
            // avoid sending confusing info to the user (see CASSANDRA-6491).
            if (acks >= blockedFor)
                acks = blockedFor - 1;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14404
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14705
            throw new WriteTimeoutException(writeType, replicaPlan.consistencyLevel(), acks, blockedFor);
        }

        if (blockFor() + failures > candidateReplicaCount())
        {
            throw new WriteFailureException(replicaPlan.consistencyLevel(), ackCount(), blockFor(), writeType, failureReasonByEndpoint);
        }
    }

    public final long currentTimeoutNanos()
    {
        long requestTimeout = writeType == WriteType.COUNTER
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15066
                              ? DatabaseDescriptor.getCounterWriteRpcTimeout(NANOSECONDS)
                              : DatabaseDescriptor.getWriteRpcTimeout(NANOSECONDS);
        return requestTimeout - (System.nanoTime() - queryStartNanoTime);
    }

    /**
     * Set a delegate ideal CL write response handler. Note that this could be the same as this
     * if the ideal CL and requested CL are the same.
     */
    public void setIdealCLResponseHandler(AbstractWriteResponseHandler handler)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13289
        this.idealCLDelegate = handler;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14404
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14705
        idealCLDelegate.responsesAndExpirations = new AtomicInteger(replicaPlan.contacts().size());
    }

    /**
     * This logs the response but doesn't do any further processing related to this write response handler
     * on whether the CL was achieved. Only call this after the subclass has completed all it's processing
     * since the subclass instance may be queried to find out if the CL was achieved.
     */
    protected final void logResponseToIdealCLDelegate(Message<T> m)
    {
        //Tracking ideal CL was not configured
        if (idealCLDelegate == null)
        {
            return;
        }

        if (idealCLDelegate == this)
        {
            //Processing of the message was already done since this is the handler for the
            //ideal consistency level. Just decrement the counter.
            decrementResponseOrExpired();
        }
        else
        {
            //Let the delegate do full processing, this will loop back into the branch above
            //with idealCLDelegate == this, because the ideal write handler idealCLDelegate will always
            //be set to this in the delegate.
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15066
            idealCLDelegate.onResponse(m);
        }
    }

    public final void expired()
    {
        //Tracking ideal CL was not configured
        if (idealCLDelegate == null)
        {
            return;
        }

        //The requested CL matched ideal CL so reuse this object
        if (idealCLDelegate == this)
        {
            decrementResponseOrExpired();
        }
        else
        {
            //Have the delegate track the expired response
            idealCLDelegate.decrementResponseOrExpired();
        }
    }

    /**
     * @return the minimum number of endpoints that must respond.
     */
    protected int blockFor()
    {
        // During bootstrap, we have to include the pending endpoints or we may fail the consistency level
        // guarantees (see #833)
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14404
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14705
        return replicaPlan.blockFor();
    }

    /**
     * TODO: this method is brittle for its purpose of deciding when we should fail a query;
     *       this needs to be CL aware, and of which nodes are live/down
     * @return the total number of endpoints the request can been sent to.
     */
    protected int candidateReplicaCount()
    {
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

    /**
     * @return number of responses received
     */
    protected abstract int ackCount();

    /**
     * null message means "response from local write"
     */
    public abstract void onResponse(Message<T> msg);

    protected void signal()
    {
        //The ideal CL should only count as a strike if the requested CL was achieved.
        //If the requested CL is not achieved it's fine for the ideal CL to also not be achieved.
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15696
        if (idealCLDelegate != null)
        {
            idealCLDelegate.requestedCLAchieved = true;
        }

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5691
        condition.signalAll();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-4578
        if (callback != null)
            callback.run();
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        logger.trace("Got failure from {}", from);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8592

        int n = waitingFor(from)
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13289
                ? failuresUpdater.incrementAndGet(this)
                : failures;

        failureReasonByEndpoint.put(from, failureReason);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12311

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14404
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14705
        if (blockFor() + n > candidateReplicaCount())
            signal();
    }

    @Override
    public boolean invokeOnFailure()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15066
        return true;
    }

    @Override
    public boolean supportsBackPressure()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9318
        return supportsBackPressure;
    }

    public void setSupportsBackPressure(boolean supportsBackPressure)
    {
        this.supportsBackPressure = supportsBackPressure;
    }

    /**
     * Decrement the counter for all responses/expirations and if the counter
     * hits 0 check to see if the ideal consistency level (this write response handler)
     * was reached using the signal.
     */
    private final void decrementResponseOrExpired()
    {
        int decrementedValue = responsesAndExpirations.decrementAndGet();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13289
        if (decrementedValue == 0)
        {
            // The condition being signaled is a valid proxy for the CL being achieved
            // Only mark it as failed if the requested CL was achieved.
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15696
            if (!condition.isSignaled() && requestedCLAchieved)
            {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14404
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14705
                replicaPlan.keyspace().metric.writeFailedIdealCL.inc();
            }
            else
            {
                replicaPlan.keyspace().metric.idealCLWriteLatency.addNano(System.nanoTime() - queryStartNanoTime);
            }
        }
    }

    /**
     * Cheap Quorum backup.  If we failed to reach quorum with our initial (full) nodes, reach out to other nodes.
     */
    public void maybeTryAdditionalReplicas(IMutation mutation, StorageProxy.WritePerformer writePerformer, String localDC)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14404
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14705
        EndpointsForToken uncontacted = replicaPlan.liveUncontacted();
        if (uncontacted.isEmpty())
            return;

        long timeout = Long.MAX_VALUE;
        List<ColumnFamilyStore> cfs = mutation.getTableIds().stream()
                                              .map(Schema.instance::getColumnFamilyStoreInstance)
                                              .collect(Collectors.toList());
        for (ColumnFamilyStore cf : cfs)
            timeout = Math.min(timeout, cf.additionalWriteLatencyNanos);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14820

        // no latency information, or we're overloaded
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15066
        if (timeout > mutation.getTimeout(NANOSECONDS))
            return;

        try
        {
            if (!condition.await(timeout, NANOSECONDS))
            {
                for (ColumnFamilyStore cf : cfs)
                    cf.metric.additionalWrites.inc();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14820

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14404
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14705
                writePerformer.apply(mutation, replicaPlan.withContact(uncontacted),
                                     (AbstractWriteResponseHandler<IMutation>) this,
                                     localDC);
            }
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }
    }
}
