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

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;

import com.codahale.metrics.Meter;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParameterType;
import org.apache.cassandra.service.reads.DataResolver;
import org.apache.cassandra.service.reads.DigestResolver;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.tracing.Tracing;

public abstract class AbstractReadRepair<E extends Endpoints<E>, L extends ReplicaLayout<E, L>> implements ReadRepair<E, L>
{
    protected final ReadCommand command;
    protected final long queryStartNanoTime;
    protected final L replicaLayout;
    protected final ColumnFamilyStore cfs;

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

    public AbstractReadRepair(ReadCommand command,
                              L replicaLayout,
                              long queryStartNanoTime)
    {
        this.command = command;
        this.queryStartNanoTime = queryStartNanoTime;
        this.replicaLayout = replicaLayout;
        this.cfs = Keyspace.openAndGetStore(command.metadata());
    }

    void sendReadCommand(Replica to, ReadCallback readCallback)
    {
        MessageOut<ReadCommand> message = command.createMessage();
        // if enabled, request additional info about repaired data from any full replicas
        if (command.isTrackingRepairedStatus() && to.isFull())
            message = message.withParameter(ParameterType.TRACK_REPAIRED_DATA, MessagingService.ONE_BYTE);

        MessagingService.instance().sendRRWithFailure(message, to.endpoint(), readCallback);
    }

    abstract Meter getRepairMeter();

    // digestResolver isn't used here because we resend read requests to all participants
    public void startRepair(DigestResolver<E, L> digestResolver, Consumer<PartitionIterator> resultConsumer)
    {
        getRepairMeter().mark();

        // Do a full data read to resolve the correct response (and repair node that need be)
        DataResolver<E, L> resolver = new DataResolver<>(command, replicaLayout, this, queryStartNanoTime);
        ReadCallback<E, L> readCallback = new ReadCallback<>(resolver, replicaLayout.consistencyLevel().blockFor(cfs.keyspace),
                                                             command, replicaLayout, queryStartNanoTime);

        digestRepair = new DigestRepair(resolver, readCallback, resultConsumer);

        // if enabled, request additional info about repaired data from any full replicas
        if (DatabaseDescriptor.getRepairedDataTrackingForPartitionReadsEnabled())
            command.trackRepairedStatus();

        for (Replica replica : replicaLayout.selected())
        {
            Tracing.trace("Enqueuing full data read to {}", replica);
            sendReadCommand(replica, readCallback);
        }
        ReadRepairDiagnostics.startRepair(this, replicaLayout, digestResolver);
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
        ConsistencyLevel consistency = replicaLayout.consistencyLevel();
        ConsistencyLevel speculativeCL = consistency.isDatacenterLocal() ? ConsistencyLevel.LOCAL_QUORUM : ConsistencyLevel.QUORUM;
        return  consistency != ConsistencyLevel.EACH_QUORUM
                && consistency.satisfies(speculativeCL, cfs.keyspace)
                && cfs.sampleReadLatencyNanos <= TimeUnit.MILLISECONDS.toNanos(command.getTimeout());
    }

    public void maybeSendAdditionalReads()
    {
        Preconditions.checkState(command instanceof SinglePartitionReadCommand,
                                 "maybeSendAdditionalReads can only be called for SinglePartitionReadCommand");
        DigestRepair repair = digestRepair;
        if (repair == null)
            return;

        if (shouldSpeculate() && !repair.readCallback.await(cfs.sampleReadLatencyNanos, TimeUnit.NANOSECONDS))
        {
            L uncontacted = replicaLayout.forNaturalUncontacted();
            if (uncontacted.selected().isEmpty())
                return;

            Replica replica = uncontacted.selected().iterator().next();
            Tracing.trace("Enqueuing speculative full data read to {}", replica);
            sendReadCommand(replica, repair.readCallback);
            ReadRepairMetrics.speculatedRead.mark();
            ReadRepairDiagnostics.speculatedRead(this, replica.endpoint(), uncontacted);
        }
    }
}
