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

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.primitives.Keys;
import accord.primitives.Txn;
import com.codahale.metrics.Meter;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlan.ForWrite;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.txn.TxnKeyRead;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.service.accord.txn.UnrecoverableRepairUpdate;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationMutationHelper;
import org.apache.cassandra.service.consensus.migration.TransactionalMigrationFromMode;
import org.apache.cassandra.service.reads.ReadCoordinator;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * 'Classic' read repair. Doesn't allow the client read to return until
 *  updates have been written to nodes needing correction. Breaks write
 *  atomicity in some situations
 */
public class BlockingReadRepair<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>>
        extends AbstractReadRepair<E, P>
{
    private static final Logger logger = LoggerFactory.getLogger(BlockingReadRepair.class);

    protected final Queue<PendingPartitionRepair> repairs = new ConcurrentLinkedQueue<>();

    interface PendingPartitionRepair
    {

        /**
         * Wait for the repair to complete util a future time
         * If the {@param timeoutAt} is a past time, the method returns immediately with the repair result.
         * @param timeoutAt future time
         * @param timeUnit the time unit of the future time
         * @return true if repair is done; otherwise, false.
         */
        default boolean awaitRepairsUntil(long timeoutAt, TimeUnit timeUnit)
        {
            long timeoutAtNanos = timeUnit.toNanos(timeoutAt);
            long remaining = timeoutAtNanos - nanoTime();
            try
            {
                return awaitRepairs(remaining, timeUnit);
            }
            catch (InterruptedException e)
            {
                throw new UncheckedInterruptedException(e);
            }
            catch (ExecutionException e)
            {
                throw new UncheckedExecutionException(e);
            }
        }

        boolean awaitRepairs(long remaining, TimeUnit timeUnit) throws InterruptedException, ExecutionException;

        /**
         * If it looks like we might not receive acks for all the repair mutations we sent out, combine all
         * the unacked mutations and send them to the minority of nodes not involved in the read repair data
         * read / write cycle. We will accept acks from them in lieu of acks from the initial mutations sent
         * out, so long as we receive the same number of acks as repair mutations transmitted. This prevents
         * misbehaving nodes from killing a quorum read, while continuing to guarantee monotonic quorum reads
         */
        default void maybeSendAdditionalWrites(long timeout, TimeUnit timeoutUnit) {}

        default int blockFor()
        {
            return -1;
        }

        default int waitingOn()
        {
            return -1;
        }

        ForWrite repairPlan();
    }

    BlockingReadRepair(ReadCoordinator coordinator, ReadCommand command, ReplicaPlan.Shared<E, P> replicaPlan, Dispatcher.RequestTime requestTime)
    {
        super(coordinator, command, replicaPlan, requestTime);
    }

    @Override
    public UnfilteredPartitionIterators.MergeListener getMergeListener(P replicaPlan)
    {
        return new PartitionIteratorMergeListener<>(replicaPlan, command, this);
    }

    @Override
    Meter getRepairMeter()
    {
        return ReadRepairMetrics.repairedBlocking;
    }

    @Override
    public void maybeSendAdditionalWrites()
    {
        for (PendingPartitionRepair repair: repairs)
        {
            repair.maybeSendAdditionalWrites(cfs.additionalWriteLatencyMicros, MICROSECONDS);
        }
    }

    @Override
    public void awaitWrites()
    {
        PendingPartitionRepair timedOut = null;
        ReplicaPlan.ForWrite repairPlan = null;

        for (PendingPartitionRepair repair : repairs)
        {
            long deadline = requestTime.computeDeadline(DatabaseDescriptor.getReadRpcTimeout(NANOSECONDS));

            if (!repair.awaitRepairsUntil(deadline, NANOSECONDS))
            {
                timedOut = repair;
                break;
            }
            repairPlan = repair.repairPlan();
        }
        if (timedOut != null)
        {
            // We got all responses, but timed out while repairing;
            // pick one of the repairs to throw, as this is better than completely manufacturing the error message
            int blockFor = timedOut.blockFor();
            int received = Math.min(blockFor - timedOut.waitingOn(), blockFor - 1);
            if (Tracing.isTracing())
                Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", blockFor);
            else
                logger.debug("Timeout while read-repairing after receiving all {} data and digest responses", blockFor);

            throw new ReadTimeoutException(replicaPlan().consistencyLevel(), received, blockFor, true);
        }

        if (repairs.isEmpty() || repairPlan.stillAppliesTo(ClusterMetadata.current()))
            return;
    }

    @Override
    public void repairPartition(DecoratedKey dk, Map<Replica, Mutation> mutations, ReplicaPlan.ForWrite writePlan, ReadRepairSource rrSource)
    {
        // non-Accord reads only ever touch one table and key so all mutations need to be applied either transactionally
        // or non-transactionally (not a mix). There is no retry loop here because read repair is relatively rare so it racing
        // with changes to migrating ranges should also be pretty rare so it isn't worth the added complexity. If you were
        // to add a retry loop you would need to be careful to correctly set/unset allowPotentialTransactionConflicts in the mutations
        // since that is set if it is routed to Accord
        //
        // If this is an Accord transaction that is in interoperability mode and executing a read repair
        // then we take the non-transactional path and the mutations are intercepted in ReadCoordinator.sendRepairMutation
        // which will ensure the repair mutation runs in the command store thread after preceding transactions are done
        ClusterMetadata cm = ClusterMetadata.current();
        if (coordinator.isEventuallyConsistent() && ConsensusMigrationMutationHelper.tokenShouldBeWrittenThroughAccord(cm, command.metadata().id, dk.getToken(), TransactionalMode::readRepairsThroughAccord, TransactionalMigrationFromMode::readRepairsThroughAccord))
            repairViaAccordTransaction(dk, mutations, writePlan);
        else
            repairViaReadCoordinator(dk, mutations, writePlan, rrSource);
    }

    /*
     * Create a new Accord transaction to apply this blocking read repair ensuring that any data being written
     * consists of already committed Accord writes just by virtue of creating a new transaction which must occur
     * after any already partially applied transactions whose writes might be present in the repair mutation.
     */
    private void repairViaAccordTransaction(DecoratedKey dk, Map<Replica, Mutation> accordMutations, ForWrite writePlan)
    {
        checkState(coordinator.isEventuallyConsistent(), "Should only repair transactionally for an eventually consistent read coordinator");
        ReadRepairMetrics.repairedBlockingViaAccord.mark();
        PartitionKey partitionKey = new PartitionKey(command.metadata().id, dk);
        Keys key = Keys.of(partitionKey);
        // This is going create a new BlockingReadRepair inside an Accord transaction which will go down
        // the !isEventuallyConsistent path and apply the repairs through Accord command stores using AccordInteropExecution
        UnrecoverableRepairUpdate<E, P> repairUpdate = new UnrecoverableRepairUpdate(AccordService.instance().nodeId(), this, key, dk, accordMutations, writePlan);

        /*
         * The motivation for using a read to apply read repair is that we want to apply the writes in the execute phase
         * so it takes fewer roundtrips and re-use a lot of the AccordInteropExecution code. We don't want to wait for
         * the extra roundtrip for apply since this is blocking a read.
         *
         * The reason this is safe/correct even though read transactions commute with each other is that read transactions
         * don't return a result when they are recovered so there is no race with recovery coordinators to worry about.
         * The remaining concern of a Read transaction seeing a torn write from an Accord transaction can't happen because
         * this RR mutation only contains already applied Accord writes and possibly some non-transactional writes
         * that need to be read repaired.
         *
         * Really the partialy applied Accord writes could just be barriered instead of read repaired, but we use this
         * approach so we can read repair non-transactional writes as well. This doesn't make that any more deterministic
         * since overlapping non-transactional writes with transactional reads will never be deterministic, but it combines
         * the two things into the same mechanism and we can't tell the origin of the writes needing read repair anyways.
         */
        Txn txn = new Txn.InMemory(Txn.Kind.Read, key, TxnKeyRead.createNoOpRead(key), TxnQuery.NONE, repairUpdate);
        Future<TxnResult> repairFuture = Stage.ACCORD_MIGRATION.submit(() -> AccordService.instance().coordinate(command.metadata().epoch.getEpoch(), txn, ConsistencyLevel.ANY, requestTime));

        repairs.add(new PendingPartitionRepair()
        {
            @Override
            public boolean awaitRepairs(long remaining, TimeUnit timeUnit) throws InterruptedException, ExecutionException
            {
                try
                {
                    repairFuture.get(remaining, timeUnit);
                    return true;
                }
                catch (TimeoutException e)
                {

                    return false;
                }
            }

            @Override
            public ForWrite repairPlan()
            {
                return writePlan;
            }
        });
    }

    /*
     * ReadCoordinator could be an Accord transaction if this is already in an Accord transaction or a regular
     * non-transactional read coordinator. We might take this path because transactional repair is not needed, or this
     * is an Accord transaction and the Accord read coordinator will take care of proxying the mutations through command
     * stores
     */
    private void repairViaReadCoordinator(DecoratedKey dk, Map<Replica, Mutation> mutations, ForWrite writePlan, ReadRepairSource rrSource)
    {
        // Accord read at QUORUM and found it needed to read repair, this means txn recovery is non-deterministic
        if (rrSource == ReadRepairSource.OTHER && !coordinator.isEventuallyConsistent())
            ReadRepairMetrics.repairedBlockingFromAccord.mark();
        BlockingPartitionRepair blockingRepair = new BlockingPartitionRepair(coordinator, dk, mutations, writePlan);
        blockingRepair.sendInitialRepairs();
        repairs.add(blockingRepair);
    }

    public void repairPartitionDirectly(ReadCoordinator readCoordinator, DecoratedKey dk, Map<Replica, Mutation> mutations, ForWrite writePlan)
    {
        ReadRepair delegateRR = ReadRepairStrategy.BLOCKING.create(readCoordinator, command, replicaPlan, requestTime);
        delegateRR.repairPartition(dk, mutations, writePlan, ReadRepairSource.REPAIR_VIA_ACCORD);
        delegateRR.maybeSendAdditionalWrites();
        delegateRR.awaitWrites();
    }

    @Override
    public boolean coordinatorAllowsPotentialTransactionConflicts()
    {
        return coordinator.allowsPotentialTransactionConflicts();
    }
}
