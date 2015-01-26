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
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

public class ReadCallback<TMessage, TResolved> implements IAsyncCallbackWithFailure<TMessage>
{
    protected static final Logger logger = LoggerFactory.getLogger( ReadCallback.class );

    public final IResponseResolver<TMessage, TResolved> resolver;
    private final SimpleCondition condition = new SimpleCondition();
    final long start;
    final int blockfor;
    final List<InetAddress> endpoints;
    private final IReadCommand command;
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
    public ReadCallback(IResponseResolver<TMessage, TResolved> resolver, ConsistencyLevel consistencyLevel, IReadCommand command, List<InetAddress> filteredEndpoints)
    {
        this(resolver, consistencyLevel, consistencyLevel.blockFor(Keyspace.open(command.getKeyspace())), command, Keyspace.open(command.getKeyspace()), filteredEndpoints);
        if (logger.isTraceEnabled())
            logger.trace(String.format("Blockfor is %s; setting up requests to %s", blockfor, StringUtils.join(this.endpoints, ",")));
    }

    public ReadCallback(IResponseResolver<TMessage, TResolved> resolver, ConsistencyLevel consistencyLevel, int blockfor, IReadCommand command, Keyspace keyspace, List<InetAddress> endpoints)
    {
        this.command = command;
        this.keyspace = keyspace;
        this.blockfor = blockfor;
        this.consistencyLevel = consistencyLevel;
        this.resolver = resolver;
        this.start = System.nanoTime();
        this.endpoints = endpoints;
        // we don't support read repair (or rapid read protection) for range scans yet (CASSANDRA-6897)
        assert !(resolver instanceof RangeSliceResponseResolver) || blockfor >= endpoints.size();
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

    public TResolved get() throws ReadFailureException, ReadTimeoutException, DigestMismatchException
    {
        if (!await(command.getTimeout(), TimeUnit.MILLISECONDS))
        {
            // Same as for writes, see AbstractWriteResponseHandler
            ReadTimeoutException ex = new ReadTimeoutException(consistencyLevel, received, blockfor, resolver.isDataPresent());

            if (logger.isDebugEnabled())
                logger.debug("Read timeout: {}", ex.toString());
            throw ex;
        }

        if (blockfor + failures > endpoints.size())
        {
            ReadFailureException ex = new ReadFailureException(consistencyLevel, received, failures, blockfor, resolver.isDataPresent());

            if (logger.isDebugEnabled())
                logger.debug("Read failure: {}", ex.toString());
            throw ex;
        }

        return blockfor == 1 ? resolver.getData() : resolver.resolve();
    }

    public void response(MessageIn<TMessage> message)
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
                StageManager.getStage(Stage.READ_REPAIR).execute(new AsyncRepairRunner());
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

    public void response(TMessage result)
    {
        MessageIn<TMessage> message = MessageIn.create(FBUtilities.getBroadcastAddress(),
                                                       result,
                                                       Collections.<String, byte[]>emptyMap(),
                                                       MessagingService.Verb.INTERNAL_RESPONSE,
                                                       MessagingService.current_version);
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
        public void run()
        {
            // If the resolver is a RowDigestResolver, we need to do a full data read if there is a mismatch.
            // Otherwise, resolve will send the repairs directly if needs be (and in that case we should never
            // get a digest mismatch)
            try
            {
                resolver.resolve();
            }
            catch (DigestMismatchException e)
            {
                assert resolver instanceof RowDigestResolver;

                if (logger.isDebugEnabled())
                    logger.debug("Digest mismatch:", e);
                
                ReadRepairMetrics.repairedBackground.mark();
                
                ReadCommand readCommand = (ReadCommand) command;
                final RowDataResolver repairResolver = new RowDataResolver(readCommand.ksName, readCommand.key, readCommand.filter(), readCommand.timestamp, endpoints.size());
                AsyncRepairCallback repairHandler = new AsyncRepairCallback(repairResolver, endpoints.size());

                MessageOut<ReadCommand> message = ((ReadCommand) command).createMessage();
                for (InetAddress endpoint : endpoints)
                    MessagingService.instance().sendRR(message, endpoint, repairHandler);
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
