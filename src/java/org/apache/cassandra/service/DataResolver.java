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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.net.AsyncOneResponse;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

public class DataResolver extends ResponseResolver
{
    private final List<AsyncOneResponse> repairResults = Collections.synchronizedList(new ArrayList<AsyncOneResponse>());

    public DataResolver(Keyspace keyspace, ReadCommand command, ConsistencyLevel consistency, int maxResponseCount)
    {
        super(keyspace, command, consistency, maxResponseCount);
    }

    public PartitionIterator getData()
    {
        ReadResponse response = responses.iterator().next().payload;
        return UnfilteredPartitionIterators.filter(response.makeIterator(), command.nowInSec());
    }

    public PartitionIterator resolve()
    {
        // We could get more responses while this method runs, which is ok (we're happy to ignore any response not here
        // at the beginning of this method), so grab the response count once and use that through the method.
        int count = responses.size();
        List<UnfilteredPartitionIterator> iters = new ArrayList<>(count);
        InetAddress[] sources = new InetAddress[count];
        for (int i = 0; i < count; i++)
        {
            MessageIn<ReadResponse> msg = responses.get(i);
            iters.add(msg.payload.makeIterator());
            sources[i] = msg.from;
        }

        // Even though every responses should honor the limit, we might have more than requested post reconciliation,
        // so ensure we're respecting the limit.
        DataLimits.Counter counter = command.limits().newCounter(command.nowInSec(), true);
        return new CountingPartitionIterator(mergeWithShortReadProtection(iters, sources, counter), counter);
    }

    private PartitionIterator mergeWithShortReadProtection(List<UnfilteredPartitionIterator> results, InetAddress[] sources, DataLimits.Counter resultCounter)
    {
        // If we have only one results, there is no read repair to do and we can't get short reads
        if (results.size() == 1)
            return UnfilteredPartitionIterators.filter(results.get(0), command.nowInSec());

        UnfilteredPartitionIterators.MergeListener listener = new RepairMergeListener(sources);

        // So-called "short reads" stems from nodes returning only a subset of the results they have for a partition due to the limit,
        // but that subset not being enough post-reconciliation. So if we don't have limit, don't bother.
        if (command.limits().isUnlimited())
            return UnfilteredPartitionIterators.mergeAndFilter(results, command.nowInSec(), listener);

        for (int i = 0; i < results.size(); i++)
            results.set(i, new ShortReadProtectedIterator(sources[i], results.get(i), resultCounter));

        return UnfilteredPartitionIterators.mergeAndFilter(results, command.nowInSec(), listener);
    }

    private class RepairMergeListener implements UnfilteredPartitionIterators.MergeListener
    {
        private final InetAddress[] sources;

        public RepairMergeListener(InetAddress[] sources)
        {
            this.sources = sources;
        }

        public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
        {
            return new MergeListener(partitionKey, columns(versions), isReversed(versions));
        }

        private PartitionColumns columns(List<UnfilteredRowIterator> versions)
        {
            Columns statics = Columns.NONE;
            Columns regulars = Columns.NONE;
            for (UnfilteredRowIterator iter : versions)
            {
                if (iter == null)
                    continue;

                PartitionColumns cols = iter.columns();
                statics = statics.mergeTo(cols.statics);
                regulars = regulars.mergeTo(cols.regulars);
            }
            return new PartitionColumns(statics, regulars);
        }

        private boolean isReversed(List<UnfilteredRowIterator> versions)
        {
            assert !versions.isEmpty();
            // Everything will be in the same order
            return versions.get(0).isReverseOrder();
        }

        public void close()
        {
            try
            {
                FBUtilities.waitOnFutures(repairResults, DatabaseDescriptor.getWriteRpcTimeout());
            }
            catch (TimeoutException ex)
            {
                // We got all responses, but timed out while repairing
                int blockFor = consistency.blockFor(keyspace);
                if (Tracing.isTracing())
                    Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", blockFor);
                else
                    logger.debug("Timeout while read-repairing after receiving all {} data and digest responses", blockFor);

                throw new ReadTimeoutException(consistency, blockFor-1, blockFor, true);
            }
        }

        private class MergeListener implements UnfilteredRowIterators.MergeListener
        {
            private final DecoratedKey partitionKey;
            private final PartitionColumns columns;
            private final boolean isReversed;
            private final PartitionUpdate[] repairs = new PartitionUpdate[sources.length];

            private final Row.Writer[] currentRows = new Row.Writer[sources.length];
            private Clustering currentClustering;
            private ColumnDefinition currentColumn;

            private final Slice.Bound[] markerOpen = new Slice.Bound[sources.length];
            private final DeletionTime[] markerTime = new DeletionTime[sources.length];

            public MergeListener(DecoratedKey partitionKey, PartitionColumns columns, boolean isReversed)
            {
                this.partitionKey = partitionKey;
                this.columns = columns;
                this.isReversed = isReversed;
            }

            private PartitionUpdate update(int i)
            {
                PartitionUpdate upd = repairs[i];
                if (upd == null)
                {
                    upd = new PartitionUpdate(command.metadata(), partitionKey, columns, 1);
                    repairs[i] = upd;
                }
                return upd;
            }

            private Row.Writer currentRow(int i)
            {
                Row.Writer row = currentRows[i];
                if (row == null)
                {
                    row = currentClustering == Clustering.STATIC_CLUSTERING ? update(i).staticWriter() : update(i).writer();
                    currentClustering.writeTo(row);
                    currentRows[i] = row;
                }
                return row;
            }

            public void onMergePartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions)
            {
                for (int i = 0; i < versions.length; i++)
                {
                    DeletionTime version = versions[i];
                    if (mergedDeletion.supersedes(versions[i]))
                        update(i).addPartitionDeletion(mergedDeletion);
                }
            }

            public void onMergingRows(Clustering clustering,
                                      LivenessInfo mergedInfo,
                                      DeletionTime mergedDeletion,
                                      Row[] versions)
            {
                currentClustering = clustering;
                for (int i = 0; i < versions.length; i++)
                {
                    Row version = versions[i];

                    if (version == null || mergedInfo.supersedes(version.primaryKeyLivenessInfo()))
                        currentRow(i).writePartitionKeyLivenessInfo(mergedInfo);

                    if (version == null || mergedDeletion.supersedes(version.deletion()))
                        currentRow(i).writeRowDeletion(mergedDeletion);
                }
            }

            public void onMergedComplexDeletion(ColumnDefinition c, DeletionTime mergedCompositeDeletion, DeletionTime[] versions)
            {
                currentColumn = c;
                for (int i = 0; i < versions.length; i++)
                {
                    DeletionTime version = versions[i] == null ? DeletionTime.LIVE : versions[i];
                    if (mergedCompositeDeletion.supersedes(version))
                        currentRow(i).writeComplexDeletion(c, mergedCompositeDeletion);
                }
            }

            public void onMergedCells(Cell mergedCell, Cell[] versions)
            {
                for (int i = 0; i < versions.length; i++)
                {
                    Cell version = versions[i];
                    Cell toAdd = version == null ? mergedCell : Cells.diff(mergedCell, version);
                    if (toAdd != null)
                        toAdd.writeTo(currentRow(i));
                }
            }

            public void onRowDone()
            {
                for (int i = 0; i < currentRows.length; i++)
                {
                    if (currentRows[i] != null)
                        currentRows[i].endOfRow();
                }
                Arrays.fill(currentRows, null);
            }

            public void onMergedRangeTombstoneMarkers(RangeTombstoneMarker merged, RangeTombstoneMarker[] versions)
            {
                for (int i = 0; i < versions.length; i++)
                {
                    RangeTombstoneMarker marker = versions[i];
                    // Note that boundaries are both close and open, so it's not one or the other
                    if (merged.isClose(isReversed) && markerOpen[i] != null)
                    {
                        Slice.Bound open = markerOpen[i];
                        Slice.Bound close = merged.isBoundary() ? ((RangeTombstoneBoundaryMarker)merged).createCorrespondingCloseBound(isReversed).clustering() : merged.clustering();
                        update(i).addRangeTombstone(Slice.make(isReversed ? close : open, isReversed ? open : close), markerTime[i]);
                    }
                    if (merged.isOpen(isReversed) && (marker == null || merged.openDeletionTime(isReversed).supersedes(marker.openDeletionTime(isReversed))))
                    {
                        markerOpen[i] = merged.isBoundary() ? ((RangeTombstoneBoundaryMarker)merged).createCorrespondingOpenBound(isReversed).clustering() : merged.clustering();
                        markerTime[i] = merged.openDeletionTime(isReversed);
                    }
                }
            }

            public void close()
            {
                for (int i = 0; i < repairs.length; i++)
                {
                    if (repairs[i] == null)
                        continue;

                    // use a separate verb here because we don't want these to be get the white glove hint-
                    // on-timeout behavior that a "real" mutation gets
                    Tracing.trace("Sending read-repair-mutation to {}", sources[i]);
                    MessageOut<Mutation> msg = new Mutation(repairs[i]).createMessage(MessagingService.Verb.READ_REPAIR);
                    repairResults.add(MessagingService.instance().sendRR(msg, sources[i]));
                }
            }
        }
    }

    private class ShortReadProtectedIterator extends CountingUnfilteredPartitionIterator
    {
        private final InetAddress source;
        private final DataLimits.Counter postReconciliationCounter;

        private ShortReadProtectedIterator(InetAddress source, UnfilteredPartitionIterator iterator, DataLimits.Counter postReconciliationCounter)
        {
            super(iterator, command.limits().newCounter(command.nowInSec(), false));
            this.source = source;
            this.postReconciliationCounter = postReconciliationCounter;
        }

        @Override
        public UnfilteredRowIterator next()
        {
            return new ShortReadProtectedRowIterator(super.next());
        }

        private class ShortReadProtectedRowIterator extends WrappingUnfilteredRowIterator
        {
            private boolean initialReadIsDone;
            private UnfilteredRowIterator shortReadContinuation;
            private Clustering lastClustering;

            ShortReadProtectedRowIterator(UnfilteredRowIterator iter)
            {
                super(iter);
            }

            @Override
            public boolean hasNext()
            {
                if (super.hasNext())
                    return true;

                initialReadIsDone = true;

                if (shortReadContinuation != null && shortReadContinuation.hasNext())
                    return true;

                return checkForShortRead();
            }

            @Override
            public Unfiltered next()
            {
                Unfiltered next = initialReadIsDone ? shortReadContinuation.next() : super.next();

                if (next.kind() == Unfiltered.Kind.ROW)
                    lastClustering = ((Row)next).clustering();

                return next;
            }

            @Override
            public void close()
            {
                try
                {
                    super.close();
                }
                finally
                {
                    if (shortReadContinuation != null)
                        shortReadContinuation.close();
                }
            }

            private boolean checkForShortRead()
            {
                assert shortReadContinuation == null || !shortReadContinuation.hasNext();

                // We have a short read if the node this is the result of has returned the requested number of
                // rows for that partition (i.e. it has stopped returning results due to the limit), but some of
                // those results haven't made it in the final result post-reconciliation due to other nodes
                // tombstones. If that is the case, then the node might have more results that we should fetch
                // as otherwise we might return less results than required, or results that shouldn't be returned
                // (because the node has tombstone that hides future results from other nodes but that haven't
                // been returned due to the limit).
                // Also note that we only get here once all the results for this node have been returned, and so
                // if the node had returned the requested number but we still get there, it imply some results were
                // skipped during reconciliation.
                if (!counter.isDoneForPartition())
                    return false;

                assert !postReconciliationCounter.isDoneForPartition();

                // We need to try to query enough additional results to fulfill our query, but because we could still
                // get short reads on that additional query, just querying the number of results we miss may not be
                // enough. But we know that when this node answered n rows (counter.countedInCurrentPartition), only
                // x rows (postReconciliationCounter.countedInCurrentPartition()) made it in the final result.
                // So our ratio of live rows to requested rows is x/n, so since we miss n-x rows, we estimate that
                // we should request m rows so that m * x/n = n-x, that is m = (n^2/x) - n.
                // Also note that it's ok if we retrieve more results that necessary since our top level iterator is a
                // counting iterator.
                int n = postReconciliationCounter.countedInCurrentPartition();
                int x = counter.countedInCurrentPartition();
                int toQuery = x == 0
                              ? n * 2     // We didn't got any answer, so (somewhat randomly) ask for twice as much
                              : Math.max(((n * n) / x) - n, 1);

                DataLimits retryLimits = command.limits().forShortReadRetry(toQuery);
                ClusteringIndexFilter filter = command.clusteringIndexFilter(partitionKey());
                ClusteringIndexFilter retryFilter = lastClustering == null ? filter : filter.forPaging(metadata().comparator, lastClustering, false);
                SinglePartitionReadCommand<?> cmd = SinglePartitionReadCommand.create(command.metadata(),
                                                                                      command.nowInSec(),
                                                                                      command.columnFilter(),
                                                                                      command.rowFilter(),
                                                                                      retryLimits,
                                                                                      partitionKey(),
                                                                                      retryFilter);

                shortReadContinuation = doShortReadRetry(cmd);
                return shortReadContinuation.hasNext();
            }

            private UnfilteredRowIterator doShortReadRetry(SinglePartitionReadCommand<?> retryCommand)
            {
                DataResolver resolver = new DataResolver(keyspace, retryCommand, ConsistencyLevel.ONE, 1);
                ReadCallback handler = new ReadCallback(resolver, ConsistencyLevel.ONE, retryCommand, Collections.singletonList(source));
                if (StorageProxy.canDoLocalRequest(source))
                    StageManager.getStage(Stage.READ).maybeExecuteImmediately(new StorageProxy.LocalReadRunnable(retryCommand, handler));
                else
                    MessagingService.instance().sendRRWithFailure(retryCommand.createMessage(), source, handler);

                // We don't call handler.get() because we want to preserve tombstones since we're still in the middle of merging node results.
                handler.awaitResults();
                assert resolver.responses.size() == 1;
                return UnfilteredPartitionIterators.getOnlyElement(resolver.responses.get(0).payload.makeIterator(), retryCommand);
            }
        }
    }

    public boolean isDataPresent()
    {
        return !responses.isEmpty();
    }
}
