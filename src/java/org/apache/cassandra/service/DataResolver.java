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
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.DataLimits.Counter;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.MoreRows;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.net.*;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

public class DataResolver extends ResponseResolver
{
    private final List<AsyncOneResponse> repairResults = Collections.synchronizedList(new ArrayList<>());
    private final long queryStartNanoTime;

    public DataResolver(Keyspace keyspace, ReadCommand command, ConsistencyLevel consistency, int maxResponseCount, long queryStartNanoTime)
    {
        super(keyspace, command, consistency, maxResponseCount);
        this.queryStartNanoTime = queryStartNanoTime;
    }

    public PartitionIterator getData()
    {
        ReadResponse response = responses.iterator().next().payload;
        return UnfilteredPartitionIterators.filter(response.makeIterator(command), command.nowInSec());
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
            iters.add(msg.payload.makeIterator(command));
            sources[i] = msg.from;
        }

        // Even though every responses should honor the limit, we might have more than requested post reconciliation,
        // so ensure we're respecting the limit.
        DataLimits.Counter counter = command.limits().newCounter(command.nowInSec(), true);
        return counter.applyTo(mergeWithShortReadProtection(iters, sources, counter));
    }

    private PartitionIterator mergeWithShortReadProtection(List<UnfilteredPartitionIterator> results, InetAddress[] sources, DataLimits.Counter resultCounter)
    {
        // If we have only one results, there is no read repair to do and we can't get short reads
        if (results.size() == 1)
            return UnfilteredPartitionIterators.filter(results.get(0), command.nowInSec());

        UnfilteredPartitionIterators.MergeListener listener = new RepairMergeListener(sources);

        // So-called "short reads" stems from nodes returning only a subset of the results they have for a partition due to the limit,
        // but that subset not being enough post-reconciliation. So if we don't have limit, don't bother.
        if (!command.limits().isUnlimited())
        {
            for (int i = 0; i < results.size(); i++)
                results.set(i, Transformation.apply(results.get(i), new ShortReadProtection(sources[i], resultCounter, queryStartNanoTime)));
        }

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
            for (UnfilteredRowIterator iter : versions)
            {
                if (iter == null)
                    continue;

                // Everything will be in the same order
                return iter.isReverseOrder();
            }

            assert false : "Expected at least one iterator";
            return false;
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

            private final Row.Builder[] currentRows = new Row.Builder[sources.length];
            private final RowDiffListener diffListener;

            // The partition level deletion for the merge row.
            private DeletionTime partitionLevelDeletion;
            // When merged has a currently open marker, its time. null otherwise.
            private DeletionTime mergedDeletionTime;
            // For each source, the time of the current deletion as known by the source.
            private final DeletionTime[] sourceDeletionTime = new DeletionTime[sources.length];
            // For each source, record if there is an open range to send as repair, and from where.
            private final ClusteringBound[] markerToRepair = new ClusteringBound[sources.length];

            public MergeListener(DecoratedKey partitionKey, PartitionColumns columns, boolean isReversed)
            {
                this.partitionKey = partitionKey;
                this.columns = columns;
                this.isReversed = isReversed;

                this.diffListener = new RowDiffListener()
                {
                    public void onPrimaryKeyLivenessInfo(int i, Clustering clustering, LivenessInfo merged, LivenessInfo original)
                    {
                        if (merged != null && !merged.equals(original))
                            currentRow(i, clustering).addPrimaryKeyLivenessInfo(merged);
                    }

                    public void onDeletion(int i, Clustering clustering, Row.Deletion merged, Row.Deletion original)
                    {
                        if (merged != null && !merged.equals(original))
                            currentRow(i, clustering).addRowDeletion(merged);
                    }

                    public void onComplexDeletion(int i, Clustering clustering, ColumnDefinition column, DeletionTime merged, DeletionTime original)
                    {
                        if (merged != null && !merged.equals(original))
                            currentRow(i, clustering).addComplexDeletion(column, merged);
                    }

                    public void onCell(int i, Clustering clustering, Cell merged, Cell original)
                    {
                        if (merged != null && !merged.equals(original) && isQueried(merged))
                            currentRow(i, clustering).addCell(merged);
                    }

                    private boolean isQueried(Cell cell)
                    {
                        // When we read, we may have some cell that have been fetched but are not selected by the user. Those cells may
                        // have empty values as optimization (see CASSANDRA-10655) and hence they should not be included in the read-repair.
                        // This is fine since those columns are not actually requested by the user and are only present for the sake of CQL
                        // semantic (making sure we can always distinguish between a row that doesn't exist from one that do exist but has
                        /// no value for the column requested by the user) and so it won't be unexpected by the user that those columns are
                        // not repaired.
                        ColumnDefinition column = cell.column();
                        ColumnFilter filter = command.columnFilter();
                        return column.isComplex() ? filter.fetchedCellIsQueried(column, cell.path()) : filter.fetchedColumnIsQueried(column);
                    }
                };
            }

            private PartitionUpdate update(int i)
            {
                if (repairs[i] == null)
                    repairs[i] = new PartitionUpdate(command.metadata(), partitionKey, columns, 1);
                return repairs[i];
            }

            private Row.Builder currentRow(int i, Clustering clustering)
            {
                if (currentRows[i] == null)
                {
                    currentRows[i] = BTreeRow.sortedBuilder();
                    currentRows[i].newRow(clustering);
                }
                return currentRows[i];
            }

            public void onMergedPartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions)
            {
                this.partitionLevelDeletion = mergedDeletion;
                for (int i = 0; i < versions.length; i++)
                {
                    if (mergedDeletion.supersedes(versions[i]))
                        update(i).addPartitionDeletion(mergedDeletion);
                }
            }

            public void onMergedRows(Row merged, Row[] versions)
            {
                // If a row was shadowed post merged, it must be by a partition level or range tombstone, and we handle
                // those case directly in their respective methods (in other words, it would be inefficient to send a row
                // deletion as repair when we know we've already send a partition level or range tombstone that covers it).
                if (merged.isEmpty())
                    return;

                Rows.diff(diffListener, merged, versions);
                for (int i = 0; i < currentRows.length; i++)
                {
                    if (currentRows[i] != null)
                        update(i).add(currentRows[i].build());
                }
                Arrays.fill(currentRows, null);
            }

            private DeletionTime currentDeletion()
            {
                return mergedDeletionTime == null ? partitionLevelDeletion : mergedDeletionTime;
            }

            public void onMergedRangeTombstoneMarkers(RangeTombstoneMarker merged, RangeTombstoneMarker[] versions)
            {
                // The current deletion as of dealing with this marker.
                DeletionTime currentDeletion = currentDeletion();

                for (int i = 0; i < versions.length; i++)
                {
                    RangeTombstoneMarker marker = versions[i];

                    // Update what the source now thinks is the current deletion
                    if (marker != null)
                        sourceDeletionTime[i] = marker.isOpen(isReversed) ? marker.openDeletionTime(isReversed) : null;

                    // If merged == null, some of the source is opening or closing a marker
                    if (merged == null)
                    {
                        // but if it's not this source, move to the next one
                        if (marker == null)
                            continue;

                        // We have a close and/or open marker for a source, with nothing corresponding in merged.
                        // Because merged is a superset, this imply that we have a current deletion (being it due to an
                        // early opening in merged or a partition level deletion) and that this deletion will still be
                        // active after that point. Further whatever deletion was open or is open by this marker on the
                        // source, that deletion cannot supersedes the current one.
                        //
                        // What we want to know here is if the source deletion and merged deletion was or will be equal,
                        // because in that case we don't want to include any repair for the source, and otherwise we do.
                        //
                        // Note further that if the marker is a boundary, as both side of that boundary will have a
                        // different deletion time, only one side might be equal to the merged deletion. This means we
                        // can only be in one of 2 cases:
                        //   1) the source was up-to-date on deletion up to that point (markerToRepair[i] == null), and then
                        //      it won't be from that point on.
                        //   2) the source wasn't up-to-date on deletion up to that point (markerToRepair[i] != null), and
                        //      it may now be (if it isn't we just have nothing to do for that marker).
                        assert !currentDeletion.isLive();

                        if (markerToRepair[i] == null)
                        {
                            // Since there is an ongoing merged deletion, the only way we don't have an open repair for
                            // this source is that it had a range open with the same deletion as current and it's
                            // closing it. This imply we need to open a deletion for the source from that point.
                            assert marker.isClose(isReversed) && currentDeletion.equals(marker.closeDeletionTime(isReversed));
                            assert !marker.isOpen(isReversed) || currentDeletion.supersedes(marker.openDeletionTime(isReversed));
                            markerToRepair[i] = marker.closeBound(isReversed).invert();
                        }
                        // In case 2) above, we only have something to do if the source is up-to-date after that point
                        else if (marker.isOpen(isReversed) && currentDeletion.equals(marker.openDeletionTime(isReversed)))
                        {
                            closeOpenMarker(i, marker.openBound(isReversed).invert());
                        }
                    }
                    else
                    {
                        // We have a change of current deletion in merged (potentially to/from no deletion at all).

                        if (merged.isClose(isReversed))
                        {
                            // We're closing the merged range. If we're recorded that this should be repaird for the
                            // source, close and add said range to the repair to send.
                            if (markerToRepair[i] != null)
                                closeOpenMarker(i, merged.closeBound(isReversed));

                        }

                        if (merged.isOpen(isReversed))
                        {
                            // If we're opening a new merged range (or just switching deletion), then unless the source
                            // is up to date on that deletion (note that we've updated what the source deleteion is
                            // above), we'll have to sent the range to the source.
                            DeletionTime newDeletion = merged.openDeletionTime(isReversed);
                            DeletionTime sourceDeletion = sourceDeletionTime[i];
                            if (!newDeletion.equals(sourceDeletion))
                                markerToRepair[i] = merged.openBound(isReversed);
                        }
                    }
                }

                if (merged != null)
                    mergedDeletionTime = merged.isOpen(isReversed) ? merged.openDeletionTime(isReversed) : null;
            }

            private void closeOpenMarker(int i, ClusteringBound close)
            {
                ClusteringBound open = markerToRepair[i];
                update(i).add(new RangeTombstone(Slice.make(isReversed ? close : open, isReversed ? open : close), currentDeletion()));
                markerToRepair[i] = null;
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

    private class ShortReadProtection extends Transformation<UnfilteredRowIterator>
    {
        private final InetAddress source;
        private final DataLimits.Counter counter;
        private final DataLimits.Counter postReconciliationCounter;
        private final long queryStartNanoTime;

        private ShortReadProtection(InetAddress source, DataLimits.Counter postReconciliationCounter, long queryStartNanoTime)
        {
            this.source = source;
            this.counter = command.limits().newCounter(command.nowInSec(), false).onlyCount();
            this.postReconciliationCounter = postReconciliationCounter;
            this.queryStartNanoTime = queryStartNanoTime;
        }

        @Override
        public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            partition = Transformation.apply(partition, counter);
            // must apply and extend with same protection instance
            ShortReadRowProtection protection = new ShortReadRowProtection(partition.metadata(), partition.partitionKey());
            partition = MoreRows.extend(partition, protection);
            partition = Transformation.apply(partition, protection); // apply after, so it is retained when we extend (in case we need to reextend)
            return partition;
        }

        private class ShortReadRowProtection extends Transformation implements MoreRows<UnfilteredRowIterator>
        {
            final CFMetaData metadata;
            final DecoratedKey partitionKey;
            Clustering lastClustering;
            int lastCount = 0;

            private ShortReadRowProtection(CFMetaData metadata, DecoratedKey partitionKey)
            {
                this.metadata = metadata;
                this.partitionKey = partitionKey;
            }

            @Override
            public Row applyToRow(Row row)
            {
                lastClustering = row.clustering();
                return row;
            }

            @Override
            public UnfilteredRowIterator moreContents()
            {
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
                if (lastCount == counted(counter) || !counter.isDoneForPartition())
                    return null;
                lastCount = counted(counter);

                assert !postReconciliationCounter.isDoneForPartition();

                // We need to try to query enough additional results to fulfill our query, but because we could still
                // get short reads on that additional query, just querying the number of results we miss may not be
                // enough. But we know that when this node answered n rows (counter.countedInCurrentPartition), only
                // x rows (postReconciliationCounter.countedInCurrentPartition()) made it in the final result.
                // So our ratio of live rows to requested rows is x/n, so since we miss n-x rows, we estimate that
                // we should request m rows so that m * x/n = n-x, that is m = (n^2/x) - n.
                // Also note that it's ok if we retrieve more results that necessary since our top level iterator is a
                // counting iterator.
                int n = countedInCurrentPartition(postReconciliationCounter);
                int x = countedInCurrentPartition(counter);
                int toQuery = Math.max(((n * n) / x) - n, 1);

                DataLimits retryLimits = command.limits().forShortReadRetry(toQuery);
                ClusteringIndexFilter filter = command.clusteringIndexFilter(partitionKey);
                ClusteringIndexFilter retryFilter = lastClustering == null ? filter : filter.forPaging(metadata.comparator, lastClustering, false);
                SinglePartitionReadCommand cmd = SinglePartitionReadCommand.create(command.metadata(),
                                                                                   command.nowInSec(),
                                                                                   command.columnFilter(),
                                                                                   command.rowFilter(),
                                                                                   retryLimits,
                                                                                   partitionKey,
                                                                                   retryFilter);

                return doShortReadRetry(cmd);
            }

            /**
             * Returns the number of results counted by the counter.
             *
             * @param counter the counter.
             * @return the number of results counted by the counter
             */
            private int counted(Counter counter)
            {
                // We are interested by the number of rows but for GROUP BY queries 'counted' returns the number of
                // groups.
                if (command.limits().isGroupByLimit())
                    return counter.rowCounted();

                return counter.counted();
            }

            /**
             * Returns the number of results counted in the partition by the counter.
             *
             * @param counter the counter.
             * @return the number of results counted in the partition by the counter
             */
            private int countedInCurrentPartition(Counter counter)
            {
                // We are interested by the number of rows but for GROUP BY queries 'countedInCurrentPartition' returns
                // the number of groups in the current partition.
                if (command.limits().isGroupByLimit())
                    return counter.rowCountedInCurrentPartition();

                return counter.countedInCurrentPartition();
            }

            private UnfilteredRowIterator doShortReadRetry(SinglePartitionReadCommand retryCommand)
            {
                DataResolver resolver = new DataResolver(keyspace, retryCommand, ConsistencyLevel.ONE, 1, queryStartNanoTime);
                ReadCallback handler = new ReadCallback(resolver, ConsistencyLevel.ONE, retryCommand, Collections.singletonList(source), queryStartNanoTime);
                if (StorageProxy.canDoLocalRequest(source))
                      StageManager.getStage(Stage.READ).maybeExecuteImmediately(new StorageProxy.LocalReadRunnable(retryCommand, handler));
                else
                    MessagingService.instance().sendRRWithFailure(retryCommand.createMessage(MessagingService.current_version), source, handler);

                // We don't call handler.get() because we want to preserve tombstones since we're still in the middle of merging node results.
                handler.awaitResults();
                assert resolver.responses.size() == 1;
                return UnfilteredPartitionIterators.getOnlyElement(resolver.responses.get(0).payload.makeIterator(command), retryCommand);
            }
        }
    }

    public boolean isDataPresent()
    {
        return !responses.isEmpty();
    }
}
