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

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.reads.DataResolver;
import org.apache.cassandra.service.reads.DigestResolver;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.tracing.Tracing;

/**
 * 'Classic' read repair. Doesn't allow the client read to return until
 *  updates have been written to nodes needing correction.
 */
public class BlockingReadRepair implements ReadRepair
{
    private static final Logger logger = LoggerFactory.getLogger(BlockingReadRepair.class);

    private final ReadCommand command;
    private final long queryStartNanoTime;
    private final ConsistencyLevel consistency;
    private final ColumnFamilyStore cfs;

    private final Queue<BlockingPartitionRepair> repairs = new ConcurrentLinkedQueue<>();

    private volatile DigestRepair digestRepair = null;

    private static class DigestRepair
    {
        private final DataResolver dataResolver;
        private final ReadCallback readCallback;
        private final Consumer<PartitionIterator> resultConsumer;
        private final List<InetAddressAndPort> initialContacts;

        public DigestRepair(DataResolver dataResolver, ReadCallback readCallback, Consumer<PartitionIterator> resultConsumer, List<InetAddressAndPort> initialContacts)
        {
            this.dataResolver = dataResolver;
            this.readCallback = readCallback;
            this.resultConsumer = resultConsumer;
            this.initialContacts = initialContacts;
        }
    }

    public BlockingReadRepair(ReadCommand command,
                              long queryStartNanoTime,
                              ConsistencyLevel consistency)
    {
        this.command = command;
        this.queryStartNanoTime = queryStartNanoTime;
        this.consistency = consistency;
        this.cfs = Keyspace.openAndGetStore(command.metadata());
    }

    public UnfilteredPartitionIterators.MergeListener getMergeListener(InetAddressAndPort[] endpoints)
    {
        return new PartitionIteratorMergeListener(endpoints, command, consistency, this);
    }

    private int getMaxResponses()
    {
        AbstractReplicationStrategy strategy = cfs.keyspace.getReplicationStrategy();
        if (consistency.isDatacenterLocal() && strategy instanceof NetworkTopologyStrategy)
        {
            NetworkTopologyStrategy nts = (NetworkTopologyStrategy) strategy;
            return nts.getReplicationFactor(DatabaseDescriptor.getLocalDataCenter());
        }
        else
        {
            return strategy.getReplicationFactor();
        }
    }

    // digestResolver isn't used here because we resend read requests to all participants
    public void startRepair(DigestResolver digestResolver, List<InetAddressAndPort> allEndpoints, List<InetAddressAndPort> contactedEndpoints, Consumer<PartitionIterator> resultConsumer)
    {
        ReadRepairMetrics.repairedBlocking.mark();

        // Do a full data read to resolve the correct response (and repair node that need be)
        Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
        DataResolver resolver = new DataResolver(keyspace, command, ConsistencyLevel.ALL, getMaxResponses(), queryStartNanoTime, this);
        ReadCallback readCallback = new ReadCallback(resolver, ConsistencyLevel.ALL, consistency.blockFor(cfs.keyspace), command,
                                                     keyspace, allEndpoints, queryStartNanoTime);

        digestRepair = new DigestRepair(resolver, readCallback, resultConsumer, contactedEndpoints);

        for (InetAddressAndPort endpoint : contactedEndpoints)
        {
            Tracing.trace("Enqueuing full data read to {}", endpoint);
            MessagingService.instance().sendRRWithFailure(command.createMessage(), endpoint, readCallback);
        }
    }

    public void awaitReads() throws ReadTimeoutException
    {
        DigestRepair repair = digestRepair;
        if (repair == null)
            return;

        repair.readCallback.awaitResults();
        repair.resultConsumer.accept(digestRepair.dataResolver.resolve());
    }

    private boolean shouldSpeculate()
    {
        ConsistencyLevel speculativeCL = consistency.isDatacenterLocal() ? ConsistencyLevel.LOCAL_QUORUM : ConsistencyLevel.QUORUM;
        return  consistency != ConsistencyLevel.EACH_QUORUM
               && consistency.satisfies(speculativeCL, cfs.keyspace)
               && cfs.sampleLatencyNanos <= TimeUnit.MILLISECONDS.toNanos(command.getTimeout());
    }

    public void maybeSendAdditionalReads()
    {
        Preconditions.checkState(command instanceof SinglePartitionReadCommand,
                                 "maybeSendAdditionalReads can only be called for SinglePartitionReadCommand");
        DigestRepair repair = digestRepair;
        if (repair == null)
            return;

        if (shouldSpeculate() && !repair.readCallback.await(cfs.sampleLatencyNanos, TimeUnit.NANOSECONDS))
        {
            Set<InetAddressAndPort> contacted = Sets.newHashSet(repair.initialContacts);
            Token replicaToken = ((SinglePartitionReadCommand) command).partitionKey().getToken();
            Iterable<InetAddressAndPort> candidates = BlockingReadRepairs.getCandidateEndpoints(cfs.keyspace, replicaToken, consistency);
            boolean speculated = false;
            for (InetAddressAndPort endpoint: Iterables.filter(candidates, e -> !contacted.contains(e)))
            {
                speculated = true;
                Tracing.trace("Enqueuing speculative full data read to {}", endpoint);
                MessagingService.instance().sendRR(command.createMessage(), endpoint, repair.readCallback);
                break;
            }

            if (speculated)
                ReadRepairMetrics.speculatedRead.mark();
        }
    }

    @Override
    public void maybeSendAdditionalWrites()
    {
        for (BlockingPartitionRepair repair: repairs)
        {
            repair.maybeSendAdditionalWrites(cfs.sampleLatencyNanos, TimeUnit.NANOSECONDS);
        }
    }

    @Override
    public void awaitWrites()
    {
        boolean timedOut = false;
        for (BlockingPartitionRepair repair: repairs)
        {
            if (!repair.awaitRepairs(DatabaseDescriptor.getWriteRpcTimeout(), TimeUnit.MILLISECONDS))
            {
                timedOut = true;
            }
        }
        if (timedOut)
        {
            // We got all responses, but timed out while repairing
            int blockFor = consistency.blockFor(cfs.keyspace);
            if (Tracing.isTracing())
                Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", blockFor);
            else
                logger.debug("Timeout while read-repairing after receiving all {} data and digest responses", blockFor);

            throw new ReadTimeoutException(consistency, blockFor-1, blockFor, true);
        }
    }

    @Override
    public void repairPartition(DecoratedKey key, Map<InetAddressAndPort, Mutation> mutations, InetAddressAndPort[] destinations)
    {
        BlockingPartitionRepair blockingRepair = new BlockingPartitionRepair(cfs.keyspace, key, consistency, mutations, consistency.blockFor(cfs.keyspace), destinations);
        blockingRepair.sendInitialRepairs();
        repairs.add(blockingRepair);
    }
}
