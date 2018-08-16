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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import com.codahale.metrics.Meter;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
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

public abstract class AbstractReadRepair implements ReadRepair
{
    protected final ReadCommand command;
    protected final long queryStartNanoTime;
    protected final ConsistencyLevel consistency;
    protected final ColumnFamilyStore cfs;

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

    public AbstractReadRepair(ReadCommand command,
                              long queryStartNanoTime,
                              ConsistencyLevel consistency)
    {
        this.command = command;
        this.queryStartNanoTime = queryStartNanoTime;
        this.consistency = consistency;
        this.cfs = Keyspace.openAndGetStore(command.metadata());
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

    void sendReadCommand(InetAddressAndPort to, ReadCallback readCallback)
    {
        MessagingService.instance().sendRRWithFailure(command.createMessage(), to, readCallback);
    }

    abstract Meter getRepairMeter();

    // digestResolver isn't used here because we resend read requests to all participants
    public void startRepair(DigestResolver digestResolver, List<InetAddressAndPort> allEndpoints, List<InetAddressAndPort> contactedEndpoints, Consumer<PartitionIterator> resultConsumer)
    {
        getRepairMeter().mark();

        // Do a full data read to resolve the correct response (and repair node that need be)
        Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
        DataResolver resolver = new DataResolver(keyspace, command, ConsistencyLevel.ALL, getMaxResponses(), queryStartNanoTime, this);
        ReadCallback readCallback = new ReadCallback(resolver, ConsistencyLevel.ALL, consistency.blockFor(cfs.keyspace), command,
                                                     keyspace, allEndpoints, queryStartNanoTime);

        digestRepair = new DigestRepair(resolver, readCallback, resultConsumer, contactedEndpoints);

        for (InetAddressAndPort endpoint : contactedEndpoints)
        {
            Tracing.trace("Enqueuing full data read to {}", endpoint);
            sendReadCommand(endpoint, readCallback);
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

    Iterable<InetAddressAndPort> getCandidatesForToken(Token token)
    {
        return BlockingReadRepairs.getCandidateEndpoints(cfs.keyspace, token, consistency);
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
            Iterable<InetAddressAndPort> candidates = getCandidatesForToken(replicaToken);

            Optional<InetAddressAndPort> endpoint = Iterables.tryFind(candidates, e -> !contacted.contains(e));
            if (endpoint.isPresent())
            {
                Tracing.trace("Enqueuing speculative full data read to {}", endpoint);
                sendReadCommand(endpoint.get(), repair.readCallback);
                ReadRepairMetrics.speculatedRead.mark();
            }
        }
    }
}
