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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

public class ReadCallback<E extends Endpoints<E>, L extends ReplicaLayout<E, L>> implements IAsyncCallbackWithFailure<ReadResponse>
{
    protected static final Logger logger = LoggerFactory.getLogger( ReadCallback.class );

    public final ResponseResolver resolver;
    final SimpleCondition condition = new SimpleCondition();
    private final long queryStartNanoTime;
    // TODO: move to replica layout as well?
    final int blockfor;
    final L replicaLayout;
    private final ReadCommand command;
    private static final AtomicIntegerFieldUpdater<ReadCallback> recievedUpdater
            = AtomicIntegerFieldUpdater.newUpdater(ReadCallback.class, "received");
    private volatile int received = 0;
    private static final AtomicIntegerFieldUpdater<ReadCallback> failuresUpdater
            = AtomicIntegerFieldUpdater.newUpdater(ReadCallback.class, "failures");
    private volatile int failures = 0;
    private final Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint;

    public ReadCallback(ResponseResolver resolver, int blockfor, ReadCommand command, L replicaLayout, long queryStartNanoTime)
    {
        this.command = command;
        this.blockfor = blockfor;
        this.resolver = resolver;
        this.queryStartNanoTime = queryStartNanoTime;
        this.replicaLayout = replicaLayout;
        this.failureReasonByEndpoint = new ConcurrentHashMap<>();
        // we don't support read repair (or rapid read protection) for range scans yet (CASSANDRA-6897)
        assert !(command instanceof PartitionRangeReadCommand) || blockfor >= replicaLayout.selected().size();

        if (logger.isTraceEnabled())
            logger.trace("Blockfor is {}; setting up requests to {}", blockfor, this.replicaLayout);
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
        boolean failed = failures > 0 && blockfor + failures > replicaLayout.selected().size();
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
            ? new ReadFailureException(replicaLayout.consistencyLevel(), received, blockfor, resolver.isDataPresent(), failureReasonByEndpoint)
            : new ReadTimeoutException(replicaLayout.consistencyLevel(), received, blockfor, resolver.isDataPresent());
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
        return !replicaLayout.consistencyLevel().isDatacenterLocal() || DatabaseDescriptor.getLocalDataCenter().equals(DatabaseDescriptor.getEndpointSnitch().getDatacenter(from));
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
        replicaLayout.consistencyLevel().assureSufficientLiveNodesForRead(replicaLayout.keyspace(), replicaLayout.selected());
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

        if (blockfor + n > replicaLayout.selected().size())
            condition.signalAll();
    }
}
