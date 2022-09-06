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

import java.util.function.Consumer;

import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
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
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.reads.DataResolver;
import org.apache.cassandra.service.reads.DigestResolver;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.tracing.Tracing;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public abstract class AbstractReadRepair<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>>
        implements ReadRepair<E, P>
{
    protected static final Logger logger = LoggerFactory.getLogger(AbstractReadRepair.class);

    protected final ReadCommand command;
    protected final long queryStartNanoTime;
    protected final ReplicaPlan.Shared<E, P> replicaPlan;
    protected final ColumnFamilyStore cfs;

    private volatile DigestRepair<E, P> digestRepair = null;

    private static class DigestRepair<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>>
    {
        private final DataResolver<E, P> dataResolver;
        private final ReadCallback<E, P> readCallback;
        private final Consumer<PartitionIterator> resultConsumer;

        public DigestRepair(DataResolver<E, P> dataResolver, ReadCallback<E, P> readCallback, Consumer<PartitionIterator> resultConsumer)
        {
            this.dataResolver = dataResolver;
            this.readCallback = readCallback;
            this.resultConsumer = resultConsumer;
        }
    }

    public AbstractReadRepair(ReadCommand command,
                              ReplicaPlan.Shared<E, P> replicaPlan,
                              long queryStartNanoTime)
    {
        this.command = command;
        this.queryStartNanoTime = queryStartNanoTime;
        this.replicaPlan = replicaPlan;
        this.cfs = Keyspace.openAndGetStore(command.metadata());
    }

    protected P replicaPlan()
    {
        return replicaPlan.get();
    }

    void sendReadCommand(Replica to, ReadCallback<E, P> readCallback, boolean speculative, boolean trackRepairedStatus)
    {
        ReadCommand command = this.command;
        
        if (to.isSelf())
        {
            Stage.READ.maybeExecuteImmediately(new StorageProxy.LocalReadRunnable(command, readCallback, trackRepairedStatus));
            return;
        }

        if (to.isTransient())
        {
            // It's OK to send queries to transient nodes during RR, as we may have contacted them for their data request initially
            // So long as we don't use these to generate repair mutations, we're fine, and this is enforced by requiring
            // ReadOnlyReadRepair for transient keyspaces.
            command = command.copyAsTransientQuery(to);
        }

        if (Tracing.isTracing())
        {
            String type;
            if (speculative) type = to.isFull() ? "speculative full" : "speculative transient";
            else type = to.isFull() ? "full" : "transient";
            Tracing.trace("Enqueuing {} data read to {}", type, to);
        }

        Message<ReadCommand> message = command.createMessage(trackRepairedStatus && to.isFull());
        MessagingService.instance().sendWithCallback(message, to.endpoint(), readCallback);
    }

    abstract Meter getRepairMeter();

    // digestResolver isn't used here because we resend read requests to all participants
    public void startRepair(DigestResolver<E, P> digestResolver, Consumer<PartitionIterator> resultConsumer)
    {
        getRepairMeter().mark();

        /*
         * When repaired data tracking is enabled, a digest will be created from data reads from repaired SSTables.
         * The digests from each replica can then be compared on the coordinator to detect any divergence in their
         * repaired datasets. In this context, an SSTable is considered repaired if it is marked repaired or has a 
         * pending repair session which has been committed. In addition to the digest, a set of ids for any pending but 
         * as yet uncommitted repair sessions is recorded and returned to the coordinator. This is to help reduce false 
         * positives caused by compaction lagging which can leave sstables from committed sessions in the pending state
         * for a time.
         */
        boolean trackRepairedStatus = DatabaseDescriptor.getRepairedDataTrackingForPartitionReadsEnabled();

        // Do a full data read to resolve the correct response (and repair node that need be)
        DataResolver<E, P> resolver = new DataResolver<>(command, replicaPlan, this, queryStartNanoTime, trackRepairedStatus);
        ReadCallback<E, P> readCallback = new ReadCallback<>(resolver, command, replicaPlan, queryStartNanoTime);

        digestRepair = new DigestRepair<>(resolver, readCallback, resultConsumer);

        // if enabled, request additional info about repaired data from any full replicas
        for (Replica replica : replicaPlan().contacts())
        {
            sendReadCommand(replica, readCallback, false, trackRepairedStatus);
        }

        ReadRepairDiagnostics.startRepair(this, replicaPlan(), digestResolver);
    }

    public void awaitReads() throws ReadTimeoutException
    {
        DigestRepair<E, P> repair = digestRepair;
        if (repair == null)
            return;

        try
        {
            repair.readCallback.awaitResults();
        }
        catch (ReadTimeoutException e)
        {
            ReadRepairMetrics.timedOut.mark();
            if (logger.isDebugEnabled() )
                logger.debug("Timed out merging read repair responses", e);
            throw e;
        }
        repair.resultConsumer.accept(digestRepair.dataResolver.resolve());
    }

    private boolean shouldSpeculate()
    {
        ConsistencyLevel consistency = replicaPlan().consistencyLevel();
        ConsistencyLevel speculativeCL = consistency.isDatacenterLocal() ? ConsistencyLevel.LOCAL_QUORUM : ConsistencyLevel.QUORUM;
        return  consistency != ConsistencyLevel.EACH_QUORUM
                && consistency.satisfies(speculativeCL, replicaPlan.get().replicationStrategy())
                && cfs.sampleReadLatencyMicros <= command.getTimeout(MICROSECONDS);
    }

    public void maybeSendAdditionalReads()
    {
        Preconditions.checkState(command instanceof SinglePartitionReadCommand,
                                 "maybeSendAdditionalReads can only be called for SinglePartitionReadCommand");
        DigestRepair<E, P> repair = digestRepair;
        if (repair == null)
            return;

        if (shouldSpeculate() && !repair.readCallback.await(cfs.sampleReadLatencyMicros, MICROSECONDS))
        {
            Replica uncontacted = replicaPlan().firstUncontactedCandidate(replica -> true);
            if (uncontacted == null)
                return;

            replicaPlan.addToContacts(uncontacted);
            sendReadCommand(uncontacted, repair.readCallback, true, false);
            ReadRepairMetrics.speculatedRead.mark();
            ReadRepairDiagnostics.speculatedRead(this, uncontacted.endpoint(), replicaPlan());
        }
    }
}
