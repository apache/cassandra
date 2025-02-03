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

package org.apache.cassandra.service.reads.range;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.CoordinatorBehindException;
import org.apache.cassandra.exceptions.ReadAbortException;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RetryOnDifferentSystemException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.metrics.ClientRangeRequestMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter;
import org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.RangeReadTarget;
import org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.RangeReadWithTarget;
import org.apache.cassandra.service.reads.DataResolver;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.service.reads.ReadCoordinator;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.readMetrics;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.readMetricsForLevel;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

@VisibleForTesting
public class RangeCommandIterator extends AbstractIterator<RowIterator> implements PartitionIterator
{
    private static final Logger logger = LoggerFactory.getLogger(RangeCommandIterator.class);

    public static final ClientRangeRequestMetrics rangeMetrics = new ClientRangeRequestMetrics("RangeSlice");

    final CloseableIterator<ReplicaPlan.ForRangeRead> replicaPlans;
    final int totalRangeCount;
    final PartitionRangeReadCommand command;
    final boolean enforceStrictLiveness;
    final Dispatcher.RequestTime requestTime;
    final ReadCoordinator readCoordinator;

    int rangesQueried;
    int batchesRequested = 0;

    private DataLimits.Counter counter;
    private PartitionIterator sentQueryIterator;

    private final int maxConcurrencyFactor;
    private int concurrencyFactor;
    // The two following "metric" are maintained to improve the concurrencyFactor
    // when it was not good enough initially.
    private int liveReturned;

    RangeCommandIterator(CloseableIterator<ReplicaPlan.ForRangeRead> replicaPlans,
                         PartitionRangeReadCommand command,
                         ReadCoordinator readCoordinator,
                         int concurrencyFactor,
                         int maxConcurrencyFactor,
                         int totalRangeCount,
                         Dispatcher.RequestTime requestTime)
    {
        this.replicaPlans = replicaPlans;
        this.command = command;
        this.readCoordinator = readCoordinator;
        this.concurrencyFactor = concurrencyFactor;
        this.maxConcurrencyFactor = maxConcurrencyFactor;
        this.totalRangeCount = totalRangeCount;
        this.requestTime = requestTime;
        enforceStrictLiveness = command.metadata().enforceStrictLiveness();
    }

    @Override
    protected RowIterator computeNext()
    {
        try
        {
            while (sentQueryIterator == null || !sentQueryIterator.hasNext())
            {
                // If we don't have more range to handle, we're done
                if (!replicaPlans.hasNext())
                    return endOfData();

                // else, sends the next batch of concurrent queries (after having close the previous iterator)
                if (sentQueryIterator != null)
                {
                    liveReturned += counter.counted();
                    sentQueryIterator.close();

                    // It's not the first batch of queries and we're not done, so we we can use what has been
                    // returned so far to improve our rows-per-range estimate and update the concurrency accordingly
                    updateConcurrencyFactor();
                }
                sentQueryIterator = sendNextRequests();
            }

            return sentQueryIterator.next();
        }
        catch (UnavailableException e)
        {
            rangeMetrics.unavailables.mark();
            StorageProxy.logRequestException(e, Collections.singleton(command));
            throw e;
        }
        catch (ReadTimeoutException e)
        {
            rangeMetrics.timeouts.mark();
            StorageProxy.logRequestException(e, Collections.singleton(command));
            throw e;
        }
        catch (ReadAbortException e)
        {
            rangeMetrics.markAbort(e);
            throw e;
        }
        catch (ReadFailureException e)
        {
            rangeMetrics.failures.mark();
            throw e;
        }
    }

    private void updateConcurrencyFactor()
    {
        liveReturned += counter.counted();

        concurrencyFactor = computeConcurrencyFactor(totalRangeCount, rangesQueried, maxConcurrencyFactor, command.limits().count(), liveReturned);
    }

    @VisibleForTesting
    static int computeConcurrencyFactor(int totalRangeCount, int rangesQueried, int maxConcurrencyFactor, int limit, int liveReturned)
    {
        maxConcurrencyFactor = Math.max(1, Math.min(maxConcurrencyFactor, totalRangeCount - rangesQueried));
        if (liveReturned == 0)
        {
            // we haven't actually gotten any results, so query up to the limit if not results so far
            Tracing.trace("Didn't get any response rows; new concurrent requests: {}", maxConcurrencyFactor);
            return maxConcurrencyFactor;
        }

        // Otherwise, compute how many rows per range we got on average and pick a concurrency factor
        // that should allow us to fetch all remaining rows with the next batch of (concurrent) queries.
        int remainingRows = limit - liveReturned;
        float rowsPerRange = (float) liveReturned / (float) rangesQueried;
        int concurrencyFactor = Math.max(1, Math.min(maxConcurrencyFactor, Math.round(remainingRows / rowsPerRange)));
        if (logger.isTraceEnabled())
            logger.trace("Didn't get enough response rows; actual rows per range: {}; remaining rows: {}, new concurrent requests: {}",
                         rowsPerRange, remainingRows, concurrencyFactor);
        return concurrencyFactor;
    }

    private PartitionIterator executeAccord(ClusterMetadata cm, PartitionRangeReadCommand rangeCommand, ConsistencyLevel cl)
    {
        //TODO (expected): https://issues.apache.org/jira/browse/CASSANDRA-20210 More efficient reads across command stores
        IAccordService.IAccordResult<TxnResult> result = StorageProxy.readWithAccord(cm, rangeCommand, rangeCommand.dataRange().keyRange(), cl, requestTime);
        return new AccordRangeResponse(result, rangeCommand.isReversed());
    }

    private SingleRangeResponse executeNormal(ReplicaPlan.ForRangeRead replicaPlan, PartitionRangeReadCommand rangeCommand, ReadCoordinator readCoordinator)
    {
        rangeCommand = (PartitionRangeReadCommand) readCoordinator.maybeAllowOutOfRangeReads(rangeCommand, replicaPlan.consistencyLevel());
        // If enabled, request repaired data tracking info from full replicas, but
        // only if there are multiple full replicas to compare results from.
        boolean trackRepairedStatus = DatabaseDescriptor.getRepairedDataTrackingForRangeReadsEnabled()
                                      && replicaPlan.contacts().filter(Replica::isFull).size() > 1;

        ReplicaPlan.SharedForRangeRead sharedReplicaPlan = ReplicaPlan.shared(replicaPlan);
        ReadRepair<EndpointsForRange, ReplicaPlan.ForRangeRead> readRepair =
        ReadRepair.create(readCoordinator, command, sharedReplicaPlan, requestTime);
        DataResolver<EndpointsForRange, ReplicaPlan.ForRangeRead> resolver =
        new DataResolver<>(readCoordinator, rangeCommand, sharedReplicaPlan, readRepair, requestTime, trackRepairedStatus);
        ReadCallback<EndpointsForRange, ReplicaPlan.ForRangeRead> handler =
        new ReadCallback<>(resolver, rangeCommand, sharedReplicaPlan, requestTime);

        if (replicaPlan.contacts().size() == 1 && replicaPlan.contacts().get(0).isSelf() && readCoordinator.localReadSupported())
        {
            Stage.READ.execute(new StorageProxy.LocalReadRunnable(rangeCommand, handler, requestTime, trackRepairedStatus));
        }
        else
        {
            for (Replica replica : replicaPlan.contacts())
            {
                Tracing.trace("Enqueuing request to {}", replica);
                ReadCommand command = replica.isFull() ? rangeCommand : rangeCommand.copyAsTransientQuery(replica);
                Message<ReadCommand> message = command.createMessage(trackRepairedStatus && replica.isFull(), requestTime);
                readCoordinator.sendReadCommand(message, replica.endpoint(), handler);
            }
        }
        return new SingleRangeResponse(resolver, handler, readRepair);
    }


    /**
     * Queries the provided sub-range.
     *
     * @param replicaPlan the subRange to query.
     * @param isFirst in the case where multiple queries are sent in parallel, whether that's the first query on
     * that batch or not. The reason it matters is that whe paging queries, the command (more specifically the
     * {@code DataLimits}) may have "state" information and that state may only be valid for the first query (in
     * that it's the query that "continues" whatever we're previously queried).
     */
    private PartitionIterator query(ClusterMetadata cm, ReplicaPlan.ForRangeRead replicaPlan, ReadCoordinator readCoordinator, List<ReadRepair<?, ?>> readRepairs, boolean isFirst)
    {
        PartitionRangeReadCommand rangeCommand = command.forSubRange(replicaPlan.range(), isFirst);

        // Accord interop execution should always be coordinated through the C* plumbing
        if (!readCoordinator.isEventuallyConsistent())
        {
            SingleRangeResponse response = executeNormal(replicaPlan, rangeCommand, readCoordinator);
            readRepairs.add(response.getReadRepair());
            return response;
        }

        List<RangeReadWithTarget> reads = ConsensusRequestRouter.splitReadIntoAccordAndNormal(cm, rangeCommand, readCoordinator, requestTime);
        // Special case returning directly to avoid wrapping the iterator and applying the limits an extra time
        if (reads.size() == 1)
        {
            RangeReadWithTarget rangeReadWithTarget = reads.get(0);
            checkState(rangeReadWithTarget.read.dataRange().keyRange().equals(rangeCommand.dataRange().keyRange()));
            if (rangeReadWithTarget.target == RangeReadTarget.accord && readCoordinator.isEventuallyConsistent())
            {
                return executeAccord(cm,
                                     rangeReadWithTarget.read,
                                     replicaPlan.consistencyLevel());
            }
            else
            {
                SingleRangeResponse response = executeNormal(replicaPlan, rangeReadWithTarget.read, readCoordinator);
                readRepairs.add(response.getReadRepair());
                return response;
            }
        }

        // TODO (review): Should this be reworked to execute the queries serially from the iterator? It would respect
        // any provided limits better but the number of queries created will generally be low (2-3)
        List<PartitionIterator> responses = new ArrayList<>(reads.size() + 1);
        // Dummy iterator that checks all the responses for retry on different system hasNext so we don't read
        // from the first iterator when the second needs to be retried because the split was wrong
        responses.add(new PartitionIterator()
        {
            @Override
            public void close()
            {

            }

            @Override
            public boolean hasNext()
            {
                for (int i = 1; i < responses.size(); i++)
                    responses.get(i).hasNext();
                return false;
            }

            @Override
            public RowIterator next()
            {
                throw new NoSuchElementException();
            }
        });

        for (RangeReadWithTarget rangeReadWithTarget : reads)
        {
            if (rangeReadWithTarget.target == RangeReadTarget.accord && readCoordinator.isEventuallyConsistent())
                responses.add(executeAccord(cm, rangeReadWithTarget.read, replicaPlan.consistencyLevel()));
            else
            {
                SingleRangeResponse response = executeNormal(replicaPlan, rangeReadWithTarget.read, readCoordinator);
                responses.add(response);
                readRepairs.add(response.getReadRepair());
            }
        }

        /*
         * We have to apply limits here if the query spans different systems because each subquery we created
         * could have gaps in the results since the limit is pushed down independently to each subquery.
         * So if we don't meet the limit in the first subquery, it's not safe to go to the next one unless
         * we fully exhausted the data the first subquery might have reached
         */
        return command.limits().filter(PartitionIterators.concat(responses),
                                       0,
                                       command.selectsFullPartition(),
                                       command.metadata().enforceStrictLiveness());
    }

    PartitionIterator sendNextRequests()
    {
        List<PartitionIterator> concurrentQueries = new ArrayList<>(concurrencyFactor);
        List<ReadRepair<?, ?>> readRepairs = new ArrayList<>(concurrencyFactor);

        ClusterMetadata cm = ClusterMetadata.current();
        try
        {
            for (int i = 0; i < concurrencyFactor && replicaPlans.hasNext(); )
            {
                ReplicaPlan.ForRangeRead replicaPlan = replicaPlans.next();
                boolean isFirst = i == 0;
                PartitionIterator response;
                // Only add the retry wrapper to reroute for the top level coordinator execution
                // not Accord's interop execution
                if (readCoordinator.isEventuallyConsistent())
                {
                    Function<ClusterMetadata, PartitionIterator> querySupplier = clusterMetadata -> query(clusterMetadata, replicaPlan, readCoordinator, readRepairs, isFirst);
                    response = retryingPartitionIterator(querySupplier, replicaPlan.consistencyLevel());
                }
                else
                {
                    response = query(cm, replicaPlan, readCoordinator, readRepairs, isFirst);
                }
                concurrentQueries.add(response);
                // due to RangeMerger, coordinator may fetch more ranges than required by concurrency factor.
                rangesQueried += replicaPlan.vnodeCount();
                i += replicaPlan.vnodeCount();
            }
            batchesRequested++;
        }
        catch (Throwable t)
        {
            for (PartitionIterator response : concurrentQueries)
                response.close();
            throw t;
        }

        Tracing.trace("Submitted {} concurrent range requests", concurrentQueries.size());
        // We want to count the results for the sake of updating the concurrency factor (see updateConcurrencyFactor)
        // but we don't want to enforce any particular limit at this point (this could break code than rely on
        // postReconciliationProcessing), hence the DataLimits.NONE.
        counter = DataLimits.NONE.newCounter(command.nowInSec(), true, command.selectsFullPartition(), enforceStrictLiveness);
        return counter.applyTo(StorageProxy.concatAndBlockOnRepair(concurrentQueries, readRepairs));
    }

    // Wrap the iterator to retry if request routing is incorrect
    private PartitionIterator retryingPartitionIterator(Function<ClusterMetadata, PartitionIterator> attempt, ConsistencyLevel cl)
    {
        return new PartitionIterator()
        {
            private ClusterMetadata lastClusterMetadata = ClusterMetadata.current();
            private PartitionIterator delegate = attempt.apply(lastClusterMetadata);

            @Override
            public void close()
            {
                delegate.close();
            }

            @Override
            public boolean hasNext()
            {
                while (true)
                {
                    try
                    {
                        return delegate.hasNext();
                    }
                    catch (RetryOnDifferentSystemException e)
                    {
                        readMetrics.retryDifferentSystem.mark();
                        readMetricsForLevel(cl).retryDifferentSystem.mark();
                        logger.debug("Retrying range read on different system because some reads were misrouted according to Accord");
                        Tracing.trace("Got {} from range reads, will retry", e);
                    }
                    catch (CoordinatorBehindException e)
                    {
                        readMetrics.retryCoordinatorBehind.mark();
                        readMetricsForLevel(cl).retryCoordinatorBehind.mark();
                        logger.debug("Retrying range read now that coordinator has caught up to cluster metadata");
                        Tracing.trace("Got {} from range reads, will retry", e);
                    }
                    // Fetch the next epoch to retry
                    lastClusterMetadata = ClusterMetadata.current();
                    delegate = attempt.apply(lastClusterMetadata);
                }
            }

            @Override
            public RowIterator next()
            {
                return delegate.next();
            }
        };
    }

    @Override
    public void close()
    {
        try
        {
            if (sentQueryIterator != null)
                sentQueryIterator.close();

            replicaPlans.close();
        }
        finally
        {
            // We track latency based on request processing time, since the amount of time that request spends in the queue
            // is not a representative metric of replica performance.
            long latency = nanoTime() - requestTime.startedAtNanos();
            rangeMetrics.addNano(latency);
            rangeMetrics.roundTrips.update(batchesRequested);
            Keyspace.openAndGetStore(command.metadata()).metric.coordinatorScanLatency.update(latency, TimeUnit.NANOSECONDS);
        }
    }

    @VisibleForTesting
    int rangesQueried()
    {
        return rangesQueried;
    }

    @VisibleForTesting
    int batchesRequested()
    {
        return batchesRequested;
    }

    @VisibleForTesting
    int maxConcurrencyFactor()
    {
        return maxConcurrencyFactor;
    }

    @VisibleForTesting
    int concurrencyFactor()
    {
        return concurrencyFactor;
    }
}
