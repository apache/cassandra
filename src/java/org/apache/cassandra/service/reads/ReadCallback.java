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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaList;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

public class ReadCallback implements IAsyncCallbackWithFailure<ReadResponse>
{
    protected static final Logger logger = LoggerFactory.getLogger( ReadCallback.class );

    public final ResponseResolver resolver;
    final SimpleCondition condition = new SimpleCondition();
    private final long queryStartNanoTime;
    final int blockfor;
    final ReplicaList replicas;
    private final ReadCommand command;
    private final ConsistencyLevel consistencyLevel;
    private static final AtomicIntegerFieldUpdater<ReadCallback> recievedUpdater
            = AtomicIntegerFieldUpdater.newUpdater(ReadCallback.class, "received");
    private volatile int received = 0;
    private static final AtomicIntegerFieldUpdater<ReadCallback> failuresUpdater
            = AtomicIntegerFieldUpdater.newUpdater(ReadCallback.class, "failures");
    private volatile int failures = 0;
    private final Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint;

    private final Keyspace keyspace; // TODO push this into ConsistencyLevel?

    private final ReadRepair readRepair;

    /**
     * Constructor when response count has to be calculated and blocked for.
     */
    public ReadCallback(ResponseResolver resolver, ConsistencyLevel consistencyLevel, ReadCommand command, ReplicaList filteredReplicas, long queryStartNanoTime, ReadRepair readRepair)
    {
        this(resolver,
             consistencyLevel,
             consistencyLevel.blockFor(Keyspace.open(command.metadata().keyspace)),
             command,
             Keyspace.open(command.metadata().keyspace),
             filteredReplicas,
             queryStartNanoTime, readRepair);
    }

    public ReadCallback(ResponseResolver resolver, ConsistencyLevel consistencyLevel, int blockfor, ReadCommand command, Keyspace keyspace, ReplicaList replicas, long queryStartNanoTime, ReadRepair readRepair)
    {
        this.command = command;
        this.keyspace = keyspace;
        this.blockfor = blockfor;
        this.consistencyLevel = consistencyLevel;
        this.resolver = resolver;
        this.queryStartNanoTime = queryStartNanoTime;
        this.replicas = replicas;
        this.readRepair = readRepair;
        this.failureReasonByEndpoint = new ConcurrentHashMap<>();
        // we don't support read repair (or rapid read protection) for range scans yet (CASSANDRA-6897)
        assert !(command instanceof PartitionRangeReadCommand) || blockfor >= replicas.size();

        if (logger.isTraceEnabled())
            logger.trace("Blockfor is {}; setting up requests to {}", blockfor, StringUtils.join(this.replicas, ","));
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

    public void awaitResults() throws ReadFailureException, ReadTimeoutException
    {
        boolean signaled = await(command.getTimeout(), TimeUnit.MILLISECONDS);
        boolean failed = blockfor + failures > replicas.size();
        if (signaled && !failed)
            return;

        if (Tracing.isTracing())
        {
            String gotData = received > 0 ? (resolver.isDataPresent() ? " (including data)" : " (only digests)") : "";
            Tracing.trace("{}; received {} of {} responses{}", new Object[]{ (failed ? "Failed" : "Timed out"), received, blockfor, gotData });
        }
        else if (logger.isDebugEnabled())
        {
            String gotData = received > 0 ? (resolver.isDataPresent() ? " (including data)" : " (only digests)") : "";
            logger.debug("{}; received {} of {} responses{}", new Object[]{ (failed ? "Failed" : "Timed out"), received, blockfor, gotData });
        }

        // Same as for writes, see AbstractWriteResponseHandler
        throw failed
            ? new ReadFailureException(consistencyLevel, received, blockfor, resolver.isDataPresent(), failureReasonByEndpoint)
            : new ReadTimeoutException(consistencyLevel, received, blockfor, resolver.isDataPresent());
    }

    public int blockFor()
    {
        return blockfor;
    }

    public void response(MessageIn<ReadResponse> message)
    {
        resolver.preprocess(message);
        int n = waitingFor(message.from)
              ? recievedUpdater.incrementAndGet(this)
              : received;

        if (n >= blockfor && resolver.isDataPresent())
            condition.signalAll();
    }

    /**
     * @return true if the message counts towards the blockfor threshold
     */
    private boolean waitingFor(InetAddressAndPort from)
    {
        return consistencyLevel.isDatacenterLocal()
             ? DatabaseDescriptor.getLocalDataCenter().equals(DatabaseDescriptor.getEndpointSnitch().getDatacenter(from))
             : true;
    }

    /**
     * @return the current number of received responses
     */
    public int getReceivedCount()
    {
        return received;
    }

    public void response(ReadResponse result)
    {
        MessageIn<ReadResponse> message = MessageIn.create(FBUtilities.getBroadcastAddressAndPort(),
                                                           result,
                                                           Collections.emptyMap(),
                                                           MessagingService.Verb.INTERNAL_RESPONSE,
                                                           MessagingService.current_version);
        response(message);
    }

    public void assureSufficientLiveNodes() throws UnavailableException
    {
        consistencyLevel.assureSufficientLiveNodes(keyspace, replicas);
    }

    public boolean isLatencyForSnitch()
    {
        return true;
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        int n = waitingFor(from)
              ? failuresUpdater.incrementAndGet(this)
              : failures;

        failureReasonByEndpoint.put(from, failureReason);

        if (blockfor + n > replicas.size())
            condition.signalAll();
    }
}
