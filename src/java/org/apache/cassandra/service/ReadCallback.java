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

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

public class ReadCallback implements IAsyncCallbackWithFailure<ReadResponse>
{
    protected static final Logger logger = LoggerFactory.getLogger( ReadCallback.class );

    public final ResponseResolver resolver;
    private final SimpleCondition condition = new SimpleCondition();
    private final long start;
    final int blockfor;
    final List<InetAddress> endpoints;
    private final ReadCommand command;
    private final ConsistencyLevel consistencyLevel;
    private static final AtomicIntegerFieldUpdater<ReadCallback> recievedUpdater
            = AtomicIntegerFieldUpdater.newUpdater(ReadCallback.class, "received");
    private volatile int received = 0;
    private static final AtomicIntegerFieldUpdater<ReadCallback> failuresUpdater
            = AtomicIntegerFieldUpdater.newUpdater(ReadCallback.class, "failures");
    private volatile int failures = 0;

    private final Keyspace keyspace; // TODO push this into ConsistencyLevel?

    /**
     * Constructor when response count has to be calculated and blocked for.
     */
    public ReadCallback(ResponseResolver resolver, ConsistencyLevel consistencyLevel, ReadCommand command, List<InetAddress> filteredEndpoints)
    {
        this(resolver,
             consistencyLevel,
             consistencyLevel.blockFor(Keyspace.open(command.metadata().ksName)),
             command,
             Keyspace.open(command.metadata().ksName),
             filteredEndpoints);
    }

    public ReadCallback(ResponseResolver resolver, ConsistencyLevel consistencyLevel, int blockfor, ReadCommand command, Keyspace keyspace, List<InetAddress> endpoints)
    {
        this.command = command;
        this.keyspace = keyspace;
        this.blockfor = blockfor;
        this.consistencyLevel = consistencyLevel;
        this.resolver = resolver;
        this.start = System.nanoTime();
        this.endpoints = endpoints;
        // we don't support read repair (or rapid read protection) for range scans yet (CASSANDRA-6897)
        assert !(command instanceof PartitionRangeReadCommand) || blockfor >= endpoints.size();

        if (logger.isTraceEnabled())
            logger.trace(String.format("Blockfor is %s; setting up requests to %s", blockfor, StringUtils.join(this.endpoints, ",")));
    }

    public boolean await(long timePastStart, TimeUnit unit)
    {
        long time = unit.toNanos(timePastStart) - (System.nanoTime() - start);
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
        boolean failed = blockfor + failures > endpoints.size();
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
            ? new ReadFailureException(consistencyLevel, received, failures, blockfor, resolver.isDataPresent())
            : new ReadTimeoutException(consistencyLevel, received, blockfor, resolver.isDataPresent());
    }

    public PartitionIterator get() throws ReadFailureException, ReadTimeoutException, DigestMismatchException
    {
        awaitResults();

        PartitionIterator result = blockfor == 1 ? resolver.getData() : resolver.resolve();
        if (logger.isTraceEnabled())
            logger.trace("Read: {} ms.", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        return result;
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
        {
            condition.signalAll();
            // kick off a background digest comparison if this is a result that (may have) arrived after
            // the original resolve that get() kicks off as soon as the condition is signaled
            if (blockfor < endpoints.size() && n == endpoints.size())
            {
                TraceState traceState = Tracing.instance.get();
                if (traceState != null)
                    traceState.trace("Initiating read-repair");
                StageManager.getStage(Stage.READ_REPAIR).execute(new AsyncRepairRunner(traceState));
            }
        }
    }

    /**
     * @return true if the message counts towards the blockfor threshold
     */
    private boolean waitingFor(InetAddress from)
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
        MessageIn<ReadResponse> message = MessageIn.create(FBUtilities.getBroadcastAddress(),
                                                           result,
                                                           Collections.<String, byte[]>emptyMap(),
                                                           MessagingService.Verb.INTERNAL_RESPONSE,
                                                           MessagingService.current_version,
                                                           MessageIn.createTimestamp());
        response(message);
    }

    public void assureSufficientLiveNodes() throws UnavailableException
    {
        consistencyLevel.assureSufficientLiveNodes(keyspace, endpoints);
    }

    public boolean isLatencyForSnitch()
    {
        return true;
    }

    private class AsyncRepairRunner implements Runnable
    {
        private final TraceState traceState;

        public AsyncRepairRunner(TraceState traceState)
        {
            this.traceState = traceState;
        }

        public void run()
        {
            // If the resolver is a DigestResolver, we need to do a full data read if there is a mismatch.
            // Otherwise, resolve will send the repairs directly if needs be (and in that case we should never
            // get a digest mismatch)
            try
            {
                resolver.resolve();
            }
            catch (DigestMismatchException e)
            {
                assert resolver instanceof DigestResolver;

                if (traceState != null)
                    traceState.trace("Digest mismatch: {}", e.toString());
                if (logger.isDebugEnabled())
                    logger.debug("Digest mismatch:", e);
                
                ReadRepairMetrics.repairedBackground.mark();
                
                final DataResolver repairResolver = new DataResolver(keyspace, command, consistencyLevel, endpoints.size());
                AsyncRepairCallback repairHandler = new AsyncRepairCallback(repairResolver, endpoints.size());

                for (InetAddress endpoint : endpoints)
                {
                    MessageOut<ReadCommand> message = command.createMessage(MessagingService.instance().getVersion(endpoint));
                    MessagingService.instance().sendRR(message, endpoint, repairHandler);
                }
            }
        }
    }

    @Override
    public void onFailure(InetAddress from)
    {
        int n = waitingFor(from)
              ? failuresUpdater.incrementAndGet(this)
              : failures;

        if (blockfor + n > endpoints.size())
            condition.signalAll();
    }
}
