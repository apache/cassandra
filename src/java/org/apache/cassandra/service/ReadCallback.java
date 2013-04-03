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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SimpleCondition;

public class ReadCallback<TMessage, TResolved> implements IAsyncCallback<TMessage>
{
    protected static final Logger logger = LoggerFactory.getLogger( ReadCallback.class );

    public final IResponseResolver<TMessage, TResolved> resolver;
    private final SimpleCondition condition = new SimpleCondition();
    private final long startTime;
    private final int blockfor;
    final List<InetAddress> endpoints;
    private final IReadCommand command;
    private final ConsistencyLevel consistencyLevel;
    private final AtomicInteger received = new AtomicInteger(0);
    private final Table table; // TODO push this into ConsistencyLevel?

    /**
     * Constructor when response count has to be calculated and blocked for.
     */
    public ReadCallback(IResponseResolver<TMessage, TResolved> resolver, ConsistencyLevel consistencyLevel, IReadCommand command, List<InetAddress> filteredEndpoints)
    {
        this(resolver, consistencyLevel, consistencyLevel.blockFor(Table.open(command.getKeyspace())), command, Table.open(command.getKeyspace()), filteredEndpoints);
        if (logger.isTraceEnabled())
            logger.trace(String.format("Blockfor is %s; setting up requests to %s", blockfor, StringUtils.join(this.endpoints, ",")));
    }

    private ReadCallback(IResponseResolver<TMessage, TResolved> resolver, ConsistencyLevel consistencyLevel, int blockfor, IReadCommand command, Table table, List<InetAddress> endpoints)
    {
        this.command = command;
        this.table = table;
        this.blockfor = blockfor;
        this.consistencyLevel = consistencyLevel;
        this.resolver = resolver;
        this.startTime = System.currentTimeMillis();
        this.endpoints = endpoints;
    }

    public ReadCallback<TMessage, TResolved> withNewResolver(IResponseResolver<TMessage, TResolved> newResolver)
    {
        return new ReadCallback(newResolver, consistencyLevel, blockfor, command, table, endpoints);
    }

    public TResolved get() throws ReadTimeoutException, DigestMismatchException
    {
        long timeout = command.getTimeout() - (System.currentTimeMillis() - startTime);
        boolean success;
        try
        {
            success = condition.await(timeout, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }

        if (!success)
            throw new ReadTimeoutException(consistencyLevel, received.get(), blockfor, resolver.isDataPresent());

        return blockfor == 1 ? resolver.getData() : resolver.resolve();
    }

    public void response(MessageIn<TMessage> message)
    {
        resolver.preprocess(message);
        int n = waitingFor(message)
              ? received.incrementAndGet()
              : received.get();
        if (n >= blockfor && resolver.isDataPresent())
        {
            condition.signal();
            maybeResolveForRepair();
        }
    }

    /**
     * @return true if the message counts towards the blockfor threshold
     */
    private boolean waitingFor(MessageIn message)
    {
        return consistencyLevel == ConsistencyLevel.LOCAL_QUORUM
             ? DatabaseDescriptor.getLocalDataCenter().equals(DatabaseDescriptor.getEndpointSnitch().getDatacenter(message.from))
             : true;
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

    /**
     * Check digests in the background on the Repair stage if we've received replies
     * too all the requests we sent.
     */
    protected void maybeResolveForRepair()
    {
        if (blockfor < endpoints.size() && received.get() == endpoints.size())
        {
            assert resolver.isDataPresent();
            StageManager.getStage(Stage.READ_REPAIR).execute(new AsyncRepairRunner());
        }
    }

    public void assureSufficientLiveNodes() throws UnavailableException
    {
        consistencyLevel.assureSufficientLiveNodes(table, endpoints);
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

                ReadCommand readCommand = (ReadCommand) command;
                final RowDataResolver repairResolver = new RowDataResolver(readCommand.table, readCommand.key, readCommand.filter());
                IAsyncCallback repairHandler = new AsyncRepairCallback(repairResolver, endpoints.size());

                MessageOut<ReadCommand> message = ((ReadCommand) command).createMessage();
                for (InetAddress endpoint : endpoints)
                    MessagingService.instance().sendRR(message, endpoint, repairHandler);
            }
        }
    }
}
