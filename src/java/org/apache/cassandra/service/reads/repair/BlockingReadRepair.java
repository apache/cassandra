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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaList;
import org.apache.cassandra.locator.Replicas;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.AsyncOneResponse;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.reads.DataResolver;
import org.apache.cassandra.service.reads.DigestResolver;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.tracing.Tracing;

/**
 * 'Classic' read repair. Doesn't allow the client read to return until
 *  updates have been written to nodes needing correction.
 */
public class BlockingReadRepair implements ReadRepair, RepairListener
{
    private static final Logger logger = LoggerFactory.getLogger(BlockingReadRepair.class);

    private static final boolean DROP_OVERSIZED_READ_REPAIR_MUTATIONS =
        Boolean.getBoolean("cassandra.drop_oversized_readrepair_mutations");

    private final ReadCommand command;
    private final ReplicaList replicas;
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
                              ReplicaList replicas,
                              long queryStartNanoTime,
                              ConsistencyLevel consistency)
    {
        Replicas.checkFull(replicas);
        this.command = command;
        this.replicas = replicas;
        this.queryStartNanoTime = queryStartNanoTime;
        this.consistency = consistency;
    }

    public UnfilteredPartitionIterators.MergeListener getMergeListener(Replica[] replicas)
    {
        return new PartitionIteratorMergeListener(replicas, command, this);
    }

    public static class BlockingPartitionRepair extends AbstractFuture<Object> implements RepairListener.PartitionRepair
    {

        final List<AsyncOneResponse<?>> responses;
        final ReadCommand command;
        final ConsistencyLevel consistency;

        public BlockingPartitionRepair(int expectedResponses, ReadCommand command, ConsistencyLevel consistency)
        {
            this.responses = new ArrayList<>(expectedResponses);
            this.command = command;
            this.consistency = consistency;
        }

        private AsyncOneResponse sendRepairMutation(Mutation mutation, InetAddressAndPort destination)
        {
            DecoratedKey key = mutation.key();
            Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());
            int messagingVersion = MessagingService.instance().getVersion(destination);

            int    mutationSize = (int) Mutation.serializer.serializedSize(mutation, messagingVersion);
            int maxMutationSize = DatabaseDescriptor.getMaxMutationSize();

            AsyncOneResponse callback = null;

            if (mutationSize <= maxMutationSize)
            {
                Tracing.trace("Sending read-repair-mutation to {}", destination);
                // use a separate verb here to avoid writing hints on timeouts
                MessageOut<Mutation> message = mutation.createMessage(MessagingService.Verb.READ_REPAIR);
                callback = MessagingService.instance().sendRR(message, destination);
                ColumnFamilyStore.metricsFor(command.metadata().id).readRepairRequests.mark();
            }
            else if (DROP_OVERSIZED_READ_REPAIR_MUTATIONS)
            {
                logger.debug("Encountered an oversized ({}/{}) read repair mutation for table {}, key {}, node {}",
                             mutationSize,
                             maxMutationSize,
                             command.metadata(),
                             command.metadata().partitionKeyType.getString(key.getKey()),
                             destination);
            }
            else
            {
                logger.warn("Encountered an oversized ({}/{}) read repair mutation for table {}, key {}, node {}",
                            mutationSize,
                            maxMutationSize,
                            command.metadata(),
                            command.metadata().partitionKeyType.getString(key.getKey()),
                            destination);

                int blockFor = consistency.blockFor(keyspace);
                Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", blockFor);
                throw new ReadTimeoutException(consistency, blockFor - 1, blockFor, true);
            }
            return callback;
        }

        public void reportMutation(Replica replica, Mutation mutation)
        {
            Replicas.checkFull(replica);
            AsyncOneResponse<?> response = sendRepairMutation(mutation, replica.getEndpoint());

            if (response != null)
                responses.add(response);
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
        BlockingPartitionRepair repair = new BlockingPartitionRepair(replicas.size(), command, consistency);
        repairs.add(repair);
        return repair;
    }

    public void startRepair(DigestResolver digestResolver, ReplicaList allReplicas, ReplicaList contactedReplicas, Consumer<PartitionIterator> resultConsumer)
    {
        ReadRepairMetrics.repairedBlocking.mark();

        // Do a full data read to resolve the correct response (and repair node that need be)
        Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
        DataResolver resolver = new DataResolver(keyspace, command, ConsistencyLevel.ALL, allReplicas,
                                                 allReplicas.size(), queryStartNanoTime, this);
        ReadCallback readCallback = new ReadCallback(resolver, ConsistencyLevel.ALL, contactedReplicas.size(), command,
                                                     keyspace, allReplicas, queryStartNanoTime, this);

        digestRepair = new DigestRepair(resolver, readCallback, resultConsumer);

        for (Replica replica : contactedReplicas)
        {
            Tracing.trace("Enqueuing full data read to {}", replica);
            MessagingService.instance().sendRRWithFailure(command.createMessage(), replica.getEndpoint(), readCallback);
        }
    }

    public void awaitRepair() throws ReadTimeoutException
    {
        if (digestRepair != null)
        {
            digestRepair.readCallback.awaitResults();
            digestRepair.resultConsumer.accept(digestRepair.dataResolver.resolve());
        }
    }
}
