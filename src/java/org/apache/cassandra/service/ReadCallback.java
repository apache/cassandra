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

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.Schema;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SimpleCondition;
import org.apache.cassandra.utils.WrappedRunnable;

import com.google.common.collect.Lists;

public class ReadCallback<TMessage, TResolved> implements IAsyncCallback<TMessage>
{
    protected static final Logger logger = LoggerFactory.getLogger( ReadCallback.class );

    protected static final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
    protected static final String localdc = snitch.getDatacenter(FBUtilities.getBroadcastAddress());

    public final IResponseResolver<TMessage, TResolved> resolver;
    protected final SimpleCondition condition = new SimpleCondition();
    private final long startTime;
    protected final int blockfor;
    final List<InetAddress> endpoints;
    private final IReadCommand command;
    protected final AtomicInteger received = new AtomicInteger(0);

    /**
     * Constructor when response count has to be calculated and blocked for.
     */
    public ReadCallback(IResponseResolver<TMessage, TResolved> resolver, ConsistencyLevel consistencyLevel, IReadCommand command, List<InetAddress> endpoints)
    {
        this.command = command;
        this.blockfor = determineBlockFor(consistencyLevel, command.getKeyspace());
        this.resolver = resolver;
        this.startTime = System.currentTimeMillis();
        sortForConsistencyLevel(endpoints);
        this.endpoints = resolver instanceof RowRepairResolver ? endpoints : filterEndpoints(endpoints);
        if (logger.isDebugEnabled())
            logger.debug(String.format("Blockfor is %s; setting up requests to %s", blockfor, StringUtils.join(this.endpoints, ",")));
    }

    /**
     * Endpoints is already restricted to live replicas, sorted by snitch preference.  This is a hook for
     * DatacenterReadCallback to move local-DC replicas to the front of the list.  We need this both
     * when doing read repair (because the first replica gets the data read) and otherwise (because
     * only the first 1..blockfor replicas will get digest reads).
     */
    protected void sortForConsistencyLevel(List<InetAddress> endpoints)
    {
        // no-op except in DRC
    }

    private List<InetAddress> filterEndpoints(List<InetAddress> ep)
    {
        if (resolver instanceof RowDigestResolver)
        {
            assert command instanceof ReadCommand : command;
            String table = ((RowDigestResolver) resolver).table;
            String columnFamily = ((ReadCommand) command).getColumnFamilyName();
            CFMetaData cfmd = Schema.instance.getTableMetaData(table).get(columnFamily);
            double chance = FBUtilities.threadLocalRandom().nextDouble();

            // if global repair then just return all the ep's
            if (cfmd.getReadRepairChance() > chance)
                return ep;

            // if local repair then just return localDC ep's
            if (cfmd.getDcLocalReadRepair() > chance)
            {
                List<InetAddress> local = Lists.newArrayList();
                List<InetAddress> other = Lists.newArrayList();
                for (InetAddress add : ep)
                {
                    if (snitch.getDatacenter(add).equals(localdc))
                        local.add(add);
                    else
                        other.add(add);
                }
                // check if blockfor more than we have localep's
                if (local.size() < blockfor)
                    local.addAll(other.subList(0, Math.min(blockfor - local.size(), other.size())));
                return local;
            }
        }
        // we don't read repair on range scans
        return ep.subList(0, Math.min(ep.size(), blockfor));
    }

    public TResolved get() throws TimeoutException, DigestMismatchException, IOException
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
        {
            StringBuilder sb = new StringBuilder("");
            for (MessageIn message : resolver.getMessages())
                sb.append(message.from).append(", ");
            throw new TimeoutException("Operation timed out - received only " + received.get() + " responses from " + sb.toString() + " .");
        }

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
     * TODO turn the Message into a response so we don't need two versions of this method
     */
    protected boolean waitingFor(MessageIn message)
    {
        return true;
    }

    /**
     * @return true if the response counts towards the blockfor threshold
     */
    protected boolean waitingFor(ReadResponse response)
    {
        return true;
    }

    public void response(ReadResponse result)
    {
        ((RowDigestResolver) resolver).injectPreProcessed(result);
        int n = waitingFor(result)
              ? received.incrementAndGet()
              : received.get();
        if (n >= blockfor && resolver.isDataPresent())
        {
            condition.signal();
            maybeResolveForRepair();
        }
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

    public int determineBlockFor(ConsistencyLevel consistencyLevel, String table)
    {
        switch (consistencyLevel)
        {
            case ONE:
                return 1;
            case TWO:
                return 2;
            case THREE:
                return 3;
            case QUORUM:
                return (Table.open(table).getReplicationStrategy().getReplicationFactor() / 2) + 1;
            case ALL:
                return Table.open(table).getReplicationStrategy().getReplicationFactor();
            default:
                throw new UnsupportedOperationException("invalid consistency level: " + consistencyLevel);
        }
    }

    public void assureSufficientLiveNodes() throws UnavailableException
    {
        if (endpoints.size() < blockfor)
        {
            logger.debug("Live nodes {} do not satisfy ConsistencyLevel ({} required)",
                         StringUtils.join(endpoints, ", "), blockfor);
            throw new UnavailableException();
        }
    }

    public boolean isLatencyForSnitch()
    {
        return true;
    }

    private class AsyncRepairRunner extends WrappedRunnable
    {
        protected void runMayThrow() throws IOException
        {
            try
            {
                resolver.resolve();
            }
            catch (DigestMismatchException e)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Digest mismatch:", e);

                ReadCommand readCommand = (ReadCommand) command;
                final RowRepairResolver repairResolver = new RowRepairResolver(readCommand.table, readCommand.key);
                IAsyncCallback repairHandler = new AsyncRepairCallback(repairResolver, endpoints.size());

                MessageOut<ReadCommand> message = ((ReadCommand) command).createMessage();
                for (InetAddress endpoint : endpoints)
                    MessagingService.instance().sendRR(message, endpoint, repairHandler);
            }
        }
    }
}
