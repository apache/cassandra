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

package org.apache.cassandra.service.reads.repair;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.AsyncOneResponse;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.reads.AsyncRepairCallback;
import org.apache.cassandra.service.reads.DataResolver;
import org.apache.cassandra.service.reads.DigestResolver;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.service.reads.ResponseResolver;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.Accumulator;

/**
 * 'Classic' read repair. Doesn't allow the client read to return until
 *  updates have been written to nodes needing correction.
 */
public class BlockingReadRepair implements ReadRepair, RepairListener
{
    private static final Logger logger = LoggerFactory.getLogger(BlockingReadRepair.class);

    private final ReadCommand command;
    private final List<InetAddressAndPort> endpoints;
    private final long queryStartNanoTime;
    private final ConsistencyLevel consistency;

    private final Queue<BlockingPartitionRepair> repairs = new ConcurrentLinkedQueue<>();

    private volatile DigestRepair digestRepair = null;

    private static class DigestRepair
    {
        private final DataResolver dataResolver;
        private final ReadCallback readCallback;
        private final Consumer<PartitionIterator> resultConsumer;

        public DigestRepair(DataResolver dataResolver, ReadCallback readCallback, Consumer<PartitionIterator> resultConsumer)
        {
            this.dataResolver = dataResolver;
            this.readCallback = readCallback;
            this.resultConsumer = resultConsumer;
        }
    }

    public BlockingReadRepair(ReadCommand command,
                              List<InetAddressAndPort> endpoints,
                              long queryStartNanoTime,
                              ConsistencyLevel consistency)
    {
        this.command = command;
        this.endpoints = endpoints;
        this.queryStartNanoTime = queryStartNanoTime;
        this.consistency = consistency;
    }

    public UnfilteredPartitionIterators.MergeListener getMergeListener(InetAddressAndPort[] endpoints)
    {
        return new PartitionIteratorMergeListener(endpoints, command, this);
    }

    public static class BlockingPartitionRepair extends AbstractFuture<Object> implements RepairListener.PartitionRepair
    {

        final List<AsyncOneResponse<?>> responses;

        public BlockingPartitionRepair(int expectedResponses)
        {
            this.responses = new ArrayList<>(expectedResponses);
        }

        protected AsyncOneResponse sendMutation(InetAddressAndPort endpoint, Mutation mutation)
        {
            // use a separate verb here because we don't want these to be get the white glove hint-
            // on-timeout behavior that a "real" mutation gets
            Tracing.trace("Sending read-repair-mutation to {}", endpoint);
            MessageOut<Mutation> msg = mutation.createMessage(MessagingService.Verb.READ_REPAIR);
            return MessagingService.instance().sendRR(msg, endpoint);
        }

        public void reportMutation(InetAddressAndPort endpoint, Mutation mutation)
        {
            responses.add(sendMutation(endpoint, mutation));
        }

        public void finish()
        {
            Futures.addCallback(Futures.allAsList(responses), new FutureCallback<List<Object>>()
            {
                public void onSuccess(@Nullable List<Object> result)
                {
                    set(result);
                }

                public void onFailure(Throwable t)
                {
                    setException(t);
                }
            });
        }
    }

    public void awaitRepairs(long timeout)
    {
        try
        {
            Futures.allAsList(repairs).get(timeout, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException ex)
        {
            // We got all responses, but timed out while repairing
            Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
            int blockFor = consistency.blockFor(keyspace);
            if (Tracing.isTracing())
                Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", blockFor);
            else
                logger.debug("Timeout while read-repairing after receiving all {} data and digest responses", blockFor);

            throw new ReadTimeoutException(consistency, blockFor - 1, blockFor, true);
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }

    }

    public PartitionRepair startPartitionRepair()
    {
        BlockingPartitionRepair repair = new BlockingPartitionRepair(endpoints.size());
        repairs.add(repair);
        return repair;
    }

    public void startForegroundRepair(DigestResolver digestResolver, List<InetAddressAndPort> allEndpoints, List<InetAddressAndPort> contactedEndpoints, Consumer<PartitionIterator> resultConsumer)
    {
        ReadRepairMetrics.repairedBlocking.mark();

        // Do a full data read to resolve the correct response (and repair node that need be)
        Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
        DataResolver resolver = new DataResolver(keyspace, command, ConsistencyLevel.ALL, allEndpoints.size(), queryStartNanoTime, this);
        ReadCallback readCallback = new ReadCallback(resolver, ConsistencyLevel.ALL, contactedEndpoints.size(), command,
                                                     keyspace, allEndpoints, queryStartNanoTime, this);

        digestRepair = new DigestRepair(resolver, readCallback, resultConsumer);

        for (InetAddressAndPort endpoint : contactedEndpoints)
        {
            Tracing.trace("Enqueuing full data read to {}", endpoint);
            MessagingService.instance().sendRRWithFailure(command.createMessage(), endpoint, readCallback);
        }
    }

    public void awaitForegroundRepairFinish() throws ReadTimeoutException
    {
        if (digestRepair != null)
        {
            digestRepair.readCallback.awaitResults();
            digestRepair.resultConsumer.accept(digestRepair.dataResolver.resolve());
        }
    }

    public void maybeStartBackgroundRepair(ResponseResolver resolver)
    {
        TraceState traceState = Tracing.instance.get();
        if (traceState != null)
            traceState.trace("Initiating read-repair");
        StageManager.getStage(Stage.READ_REPAIR).execute(() -> resolver.evaluateAllResponses(traceState));
    }

    public void backgroundDigestRepair(TraceState traceState)
    {
        if (traceState != null)
            traceState.trace("Digest mismatch");
        if (logger.isDebugEnabled())
            logger.debug("Digest mismatch");

        ReadRepairMetrics.repairedBackground.mark();

        Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
        final DataResolver repairResolver = new DataResolver(keyspace, command, consistency, endpoints.size(), queryStartNanoTime, this);
        AsyncRepairCallback repairHandler = new AsyncRepairCallback(repairResolver, endpoints.size());

        for (InetAddressAndPort endpoint : endpoints)
            MessagingService.instance().sendRR(command.createMessage(), endpoint, repairHandler);
    }
}
