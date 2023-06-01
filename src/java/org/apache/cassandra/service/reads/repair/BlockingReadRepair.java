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
import java.util.Collection;
import java.util.List;
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
import org.apache.cassandra.config.Config.LWTStrategy;
import org.apache.cassandra.config.Config.NonSerialWriteStrategy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.txn.TxnCondition;
import org.apache.cassandra.service.accord.txn.TxnDataResolver;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.TxnReferenceOperations;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.service.accord.txn.TxnUpdate;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.service.accord.txn.TxnWrite.Fragment;
import org.apache.cassandra.service.reads.MessagingServiceFollowupReader;
import org.apache.cassandra.tracing.Tracing;
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
         * @param timeoutAt, future time
         * @param timeUnit, the time unit of the future time
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
    }

    BlockingReadRepair(ReadCommand command, ReplicaPlan.Shared<E, P> replicaPlan, long queryStartNanoTime)
    {
        super(command, replicaPlan, queryStartNanoTime, MessagingServiceFollowupReader.instance);
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
        for (PendingPartitionRepair repair : repairs)
        {
            if (!repair.awaitRepairsUntil(DatabaseDescriptor.getReadRpcTimeout(NANOSECONDS) + queryStartNanoTime, NANOSECONDS))
            {
                timedOut = repair;
                break;
            }
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
    }

    @Override
    public void repairPartition(DecoratedKey dk, Map<Replica, Mutation> mutations, ReplicaPlan.ForWrite writePlan)
    {
        NonSerialWriteStrategy nonSerialWriteStrategy = DatabaseDescriptor.getNonSerialWriteStrategy();
        if (DatabaseDescriptor.getLWTStrategy() == LWTStrategy.accord
            || nonSerialWriteStrategy.blockingReadRepairThroughAccord)
        {
            ConsistencyLevel commitConsistencyLevel = nonSerialWriteStrategy.commitCLForStrategy(replicaPlan.get().consistencyLevel());
            Collection<PartitionUpdate> partitionUpdates = Mutation.merge(mutations.values()).getPartitionUpdates();
            checkState(partitionUpdates.size() == 1, "Expect only one PartitionUpdate");
            PartitionUpdate update = partitionUpdates.iterator().next();
            PartitionKey partitionKey = PartitionKey.of(update);
            Keys key = Keys.of(partitionKey);
            List<Fragment> fragment = new ArrayList<>(1);
            fragment.add(new TxnWrite.Fragment(0, update, TxnReferenceOperations.empty()));
            TxnUpdate txnUpdate = new TxnUpdate(fragment, TxnCondition.none(), commitConsistencyLevel);
            Txn txn = new Txn.InMemory(key, TxnRead.EMPTY_READ, new TxnDataResolver(), TxnQuery.NONE, txnUpdate);
            Future<TxnResult> repairFuture = Stage.ACCORD_MIGRATION.submit(() -> AccordService.instance().coordinate(txn, commitConsistencyLevel, queryStartNanoTime));
            repairs.add((remaining, timeUnit) -> {
                try
                {
                    repairFuture.get(remaining, timeUnit);
                    return true;
                }
                catch (TimeoutException e)
                {
                    return false;
                }
            });
        }
        else
        {
            BlockingPartitionRepair blockingRepair = new BlockingPartitionRepair(dk, mutations, writePlan);
            blockingRepair.sendInitialRepairs();
            repairs.add(blockingRepair);
        }
    }
}
