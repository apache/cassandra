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

package org.apache.cassandra.service.reads;

import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.MorePartitions;
import org.apache.cassandra.db.transform.MoreRows;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.reads.repair.NoopReadRepair;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.tracing.Tracing;

public class ShortReadPartitionsProtection extends Transformation<UnfilteredRowIterator> implements MorePartitions<UnfilteredPartitionIterator>
{
    private static final Logger logger = LoggerFactory.getLogger(ShortReadPartitionsProtection.class);
    private final ReadCommand command;
    private final Replica source;

    private final Runnable preFetchCallback; // called immediately before fetching more contents

    private final DataLimits.Counter singleResultCounter; // unmerged per-source counter
    private final DataLimits.Counter mergedResultCounter; // merged end-result counter

    private DecoratedKey lastPartitionKey; // key of the last observed partition

    private boolean partitionsFetched; // whether we've seen any new partitions since iteration start or last moreContents() call

    private final long queryStartNanoTime;

    public ShortReadPartitionsProtection(ReadCommand command,
                                         Replica source,
                                         Runnable preFetchCallback,
                                         DataLimits.Counter singleResultCounter,
                                         DataLimits.Counter mergedResultCounter,
                                         long queryStartNanoTime)
    {
        this.command = command;
        this.source = source;
        this.preFetchCallback = preFetchCallback;
        this.singleResultCounter = singleResultCounter;
        this.mergedResultCounter = mergedResultCounter;
        this.queryStartNanoTime = queryStartNanoTime;
    }

    @Override
    public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
    {
        partitionsFetched = true;

        lastPartitionKey = partition.partitionKey();

        /*
         * Extend for moreContents() then apply protection to track lastClustering by applyToRow().
         *
         * If we don't apply the transformation *after* extending the partition with MoreRows,
         * applyToRow() method of protection will not be called on the first row of the new extension iterator.
         */
        ReplicaPlan.ForTokenRead replicaPlan = ReplicaPlans.forSingleReplicaRead(Keyspace.open(command.metadata().keyspace), partition.partitionKey().getToken(), source);
        ReplicaPlan.SharedForTokenRead sharedReplicaPlan = ReplicaPlan.shared(replicaPlan);
        ShortReadRowsProtection protection = new ShortReadRowsProtection(partition.partitionKey(),
                                                                         command, source,
                                                                         (cmd) -> executeReadCommand(cmd, sharedReplicaPlan),
                                                                         singleResultCounter,
                                                                         mergedResultCounter);
        return Transformation.apply(MoreRows.extend(partition, protection), protection);
    }

    /*
     * We only get here once all the rows and partitions in this iterator have been iterated over, and so
     * if the node had returned the requested number of rows but we still get here, then some results were
     * skipped during reconciliation.
     */
    public UnfilteredPartitionIterator moreContents()
    {
        // never try to request additional partitions from replicas if our reconciled partitions are already filled to the limit
        assert !mergedResultCounter.isDone();

        // we do not apply short read protection when we have no limits at all
        assert !command.limits().isUnlimited();

        /*
         * If this is a single partition read command or an (indexed) partition range read command with
         * a partition key specified, then we can't and shouldn't try fetch more partitions.
         */
        assert !command.isLimitedToOnePartition();

        /*
         * If the returned result doesn't have enough rows/partitions to satisfy even the original limit, don't ask for more.
         *
         * Can only take the short cut if there is no per partition limit set. Otherwise it's possible to hit false
         * positives due to some rows being uncounted for in certain scenarios (see CASSANDRA-13911).
         */
        if (command.limits().isExhausted(singleResultCounter) && command.limits().perPartitionCount() == DataLimits.NO_LIMIT)
            return null;

        /*
         * Either we had an empty iterator as the initial response, or our moreContents() call got us an empty iterator.
         * There is no point to ask the replica for more rows - it has no more in the requested range.
         */
        if (!partitionsFetched)
            return null;
        partitionsFetched = false;

        /*
         * We are going to fetch one partition at a time for thrift and potentially more for CQL.
         * The row limit will either be set to the per partition limit - if the command has no total row limit set, or
         * the total # of rows remaining - if it has some. If we don't grab enough rows in some of the partitions,
         * then future ShortReadRowsProtection.moreContents() calls will fetch the missing ones.
         */
        int toQuery = command.limits().count() != DataLimits.NO_LIMIT
                      ? command.limits().count() - mergedResultCounter.rowsCounted()
                      : command.limits().perPartitionCount();

        ColumnFamilyStore.metricsFor(command.metadata().id).shortReadProtectionRequests.mark();
        Tracing.trace("Requesting {} extra rows from {} for short read protection", toQuery, source);
        logger.info("Requesting {} extra rows from {} for short read protection", toQuery, source);

        // If we've arrived here, all responses have been consumed, and we're about to request more.
        preFetchCallback.run();

        return makeAndExecuteFetchAdditionalPartitionReadCommand(toQuery);
    }

    private UnfilteredPartitionIterator makeAndExecuteFetchAdditionalPartitionReadCommand(int toQuery)
    {
        PartitionRangeReadCommand cmd = (PartitionRangeReadCommand) command;

        DataLimits newLimits = cmd.limits().forShortReadRetry(toQuery);

        AbstractBounds<PartitionPosition> bounds = cmd.dataRange().keyRange();
        AbstractBounds<PartitionPosition> newBounds = bounds.inclusiveRight()
                                                      ? new Range<>(lastPartitionKey, bounds.right)
                                                      : new ExcludingBounds<>(lastPartitionKey, bounds.right);
        DataRange newDataRange = cmd.dataRange().forSubRange(newBounds);

        ReplicaPlan.ForRangeRead replicaPlan = ReplicaPlans.forSingleReplicaRead(Keyspace.open(command.metadata().keyspace), cmd.dataRange().keyRange(), source, 1);
        return executeReadCommand(cmd.withUpdatedLimitsAndDataRange(newLimits, newDataRange), ReplicaPlan.shared(replicaPlan));
    }

    private <E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>>
    UnfilteredPartitionIterator executeReadCommand(ReadCommand cmd, ReplicaPlan.Shared<E, P> replicaPlan)
    {
        DataResolver<E, P> resolver = new DataResolver<>(cmd, replicaPlan, (NoopReadRepair<E, P>)NoopReadRepair.instance, queryStartNanoTime);
        ReadCallback<E, P> handler = new ReadCallback<>(resolver, cmd, replicaPlan, queryStartNanoTime);

        if (source.isSelf())
        {
            Stage.READ.maybeExecuteImmediately(new StorageProxy.LocalReadRunnable(cmd, handler));
        }
        else
        {
            if (source.isTransient())
                cmd = cmd.copyAsTransientQuery(source);
            MessagingService.instance().sendWithCallback(cmd.createMessage(false), source.endpoint(), handler);
        }

        // We don't call handler.get() because we want to preserve tombstones since we're still in the middle of merging node results.
        handler.awaitResults();
        assert resolver.getMessages().size() == 1;
        return resolver.getMessages().get(0).payload.makeIterator(command);
    }
}
