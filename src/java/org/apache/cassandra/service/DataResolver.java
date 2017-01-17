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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
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
import org.apache.cassandra.utils.Pair;

public class DataResolver extends ResponseResolver
{
    private final Map<AsyncOneResponse, Pair<MessageOut, InetAddress>> repairResponseRequestMap = new HashMap<>();
    private final long queryStartNanoTime;
    private Optional<InetAddress> spareReadRepairNode;
    private int responseCntSnapshot;

    public DataResolver(Keyspace keyspace, ReadCommand command, ConsistencyLevel consistency, int maxResponseCount, long queryStartNanoTime)
    {
        super(keyspace, command, consistency, maxResponseCount);
        this.queryStartNanoTime = queryStartNanoTime;
        this.spareReadRepairNode = Optional.empty();
    }

    public DataResolver(Keyspace keyspace, ReadCommand command, ConsistencyLevel consistency, int maxResponseCount, long queryStartNanoTime, Optional<InetAddress> spareReadRepairNode)
    {
        this(keyspace, command, consistency, maxResponseCount, queryStartNanoTime);
        this.spareReadRepairNode = spareReadRepairNode;
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

        // we need to capture this count and save to this data resolver. All return results is based on this many
        // reponse count and below "responseCntSnapshot" is used to calculate whether read repair is ok or not
        // since response list can get more response later, but we only iterator "responseCntSnapshot" for any future
        // operations including read repair retry. So we have to save this count in current state.
        responseCntSnapshot = count;
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

    public void compareResponses()
    {
        // We need to fully consume the results to trigger read repairs if appropriate
        try (PartitionIterator iterator = resolve())
        {
            PartitionIterators.consume(iterator);
        }
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

        private RegularAndStaticColumns columns(List<UnfilteredRowIterator> versions)
        {
            Columns statics = Columns.NONE;
            Columns regulars = Columns.NONE;
            for (UnfilteredRowIterator iter : versions)
            {
                if (iter == null)
                    continue;

                RegularAndStaticColumns cols = iter.columns();
                statics = statics.mergeTo(cols.statics);
                regulars = regulars.mergeTo(cols.regulars);
            }
            return new RegularAndStaticColumns(statics, regulars);
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

        // it will be invoked when finishing all partition iterators
        public void close()
        {
            try
            {
                waitRepairToFinishWithPossibleRetry();
            }
            catch (TimeoutException ex)
            {
                // We got all responses, but timed out while repairing
                int blockFor = consistency.blockFor(keyspace);
                if (Tracing.isTracing())
                    Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", blockFor);
                logger.debug("Timeout while read-repairing after receiving all {} data and digest responses with exception {]", blockFor, ex);
                throw new ReadTimeoutException(consistency, blockFor-1, blockFor, true);
            }
        }

        /**
         * This method will wait for read repair response to come back. Based on consistency level requirement, if read
         * repair response takes more than sampleLatencyNanos, it will send the diff to a different node say retryNode no matter whether
         * this retryNode contains latest data or not. If we can get the response back from retryNode, we know we can gurantee consistency
         * level requirement for sure. Right now it only handles one read repair slowness(it can also handle more than one if speculative read retry
         * kicks in).
         * Below code also try to check read repair response again if the retryNode is slow also to hopefully previous slow read repair response get
         * back.
         * The reason why we have to block until we get enough read repair repsonse back is we need to garantee "monotonic quorum reads"
         * Detailed explanation can be seen at CASSANDRA-10726
         *
         * @throws TimeoutException
         */

        private void waitRepairToFinishWithPossibleRetry() throws TimeoutException
        {
            if (repairResponseRequestMap.isEmpty()) return;

            if (!consistency.satisfiesQuorumFor(keyspace))
            {
                //if not majority, there is no point to block since even we
                //block, there is also no guarantee for "monotonic read". So just return
                return;
            }

            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().id);
            long waitTimeNanos = cfs.sampleLatencyNanos;

            // no latency information, or we're overloaded
            if (waitTimeNanos > TimeUnit.MILLISECONDS.toNanos(command.getTimeout()) || waitTimeNanos == 0)
            {
                // try to choose a default value
                waitTimeNanos = TimeUnit.MILLISECONDS.toNanos((long)(DatabaseDescriptor.getReadRpcTimeout() / 4.0));
            }

            List<AsyncOneResponse> timeOuts = awaitRepairResponses(repairResponseRequestMap.keySet().stream().collect(Collectors.toList()), waitTimeNanos);

            int blockFor = consistency.blockFor(keyspace);
            long timeOutHostsCnt = distinctHostNum(timeOuts);
            if (responseCntSnapshot - timeOutHostsCnt >= blockFor)
            {
                //it's guaranteed to have at least blockFor replicas succeeded, then we don't need to worry about whether
                //read repair is done or not. "monotonic read" is already guaranteed
                return;
            }

            if (!spareReadRepairNode.isPresent())
            {
                throw new TimeoutException("read repair timeout and there is no retry");
            }

            // we only do extra read repair if we only need to do one more read repair to satisfy blockFor
            if (responseCntSnapshot - timeOutHostsCnt == blockFor - 1)
            {
                List<AsyncOneResponse> repairRetryResponses = new ArrayList<>();
                for(AsyncOneResponse response : timeOuts)
                {
                    Tracing.trace("retry read-repair-mutation to {}", spareReadRepairNode.get());
                    repairRetryResponses.add(MessagingService.instance().sendRR(repairResponseRequestMap.get(response).left,
                                                                                spareReadRepairNode.get()));
                }

                List<AsyncOneResponse> retryTimeOut = awaitRepairResponses(repairRetryResponses, waitTimeNanos);
                if (retryTimeOut.isEmpty()) return;

                //retry can not help, let's try previous repairResponse again to see whether there is one more done.
                if (timeOutHostsCnt - distinctHostNum(awaitRepairResponses(repairResponseRequestMap.keySet().stream().collect(Collectors.toList()), waitTimeNanos)) >= 1)
                {
                    return;
                }
                throw new TimeoutException("one more read repair can not help and check previous repair response again can not help either");
            } else {
                throw new TimeoutException("read repair timeout and will not retry read repair, diff count = " + (responseCntSnapshot - timeOutHostsCnt));
            }
        }

        /**
         * When doing the read repair, the mutation is per partition key, so it's possible we will repair multiple
         * partitions into different hosts. let's say RF = 5, we need to read partition p1, p2, p3, p4 from three nodes,
         * n1, n2, n3. If n1 contains latest data, n2 is missing p1 and p2, n2 is missing p3 and p4. So we need to run
         * repair for n2 by sending p1 and p2 partitions and run repair for n3 by sending p3 and p4 partitions. it's
         * possible p1 and p3 repair is slow, so beloew distinctHostNum will return 2. In this case, I will not retry
         * a new node for read repair since this read repair retry will only handle one slow host. If p3 and p4 is fast,
         * p1 and p2 repair is slow or just p1 repair is slow, below distinctHostNum will return 1, in this case, I will
         * retry 1 extra node and send p1, p2 to extra node or just p1 if only p1 read repair times out.
         * In same host, we can have multiple partition read repair and we can only handle one host slowness, so we should
         * get distinct host from read repair response future.
         * @param responses
         * @return
         */
        private long distinctHostNum(final List<AsyncOneResponse> responses)
        {
            return responses.stream().map(response -> repairResponseRequestMap.get(response).right).distinct().count();
        }

        /**
         *
         * @param responses
         * @param timeToWaitNanos
         * @return a list of response which have not responded in timeToWaitNanos window
         */
        private List<AsyncOneResponse> awaitRepairResponses(final List<AsyncOneResponse> responses, final long timeToWaitNanos)
        {
            List<AsyncOneResponse> ret = new ArrayList<>();
            long start = System.nanoTime();
            for(final AsyncOneResponse repairResponse : responses)
            {
                try
                {
                    long alreadyPassedNanos = System.nanoTime() - start ;
                    repairResponse.get(timeToWaitNanos - alreadyPassedNanos > 0 ? timeToWaitNanos - alreadyPassedNanos : 0,
                                       TimeUnit.NANOSECONDS);
                } catch (final TimeoutException e)
                {
                    ret.add(repairResponse);
                }
            }
            return ret;
        }

        private class MergeListener implements UnfilteredRowIterators.MergeListener
        {
            private final DecoratedKey partitionKey;
            private final RegularAndStaticColumns columns;
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

            public MergeListener(DecoratedKey partitionKey, RegularAndStaticColumns columns, boolean isReversed)
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

                    public void onComplexDeletion(int i, Clustering clustering, ColumnMetadata column, DeletionTime merged, DeletionTime original)
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
                        ColumnMetadata column = cell.column();
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
                        // But while the marker deletion (before and/or after this point) cannot supersed the current
                        // deletion, we want to know if it's equal to it (both before and after), because in that case
                        // the source is up to date and we don't want to include repair.
                        //
                        // So in practice we have 2 possible case:
                        //  1) the source was up-to-date on deletion up to that point (markerToRepair[i] == null). Then
                        //     it won't be from that point on unless it's a boundary and the new opened deletion time
                        //     is also equal to the current deletion (note that this implies the boundary has the same
                        //     closing and opening deletion time, which should generally not happen, but can due to legacy
                        //     reading code not avoiding this for a while, see CASSANDRA-13237).
                        //   2) the source wasn't up-to-date on deletion up to that point (markerToRepair[i] != null), and
                        //      it may now be (if it isn't we just have nothing to do for that marker).
                        assert !currentDeletion.isLive() : currentDeletion.toString();

                        if (markerToRepair[i] == null)
                        {
                            // Since there is an ongoing merged deletion, the only way we don't have an open repair for
                            // this source is that it had a range open with the same deletion as current and it's
                            // closing it.
                            assert marker.isClose(isReversed) && currentDeletion.equals(marker.closeDeletionTime(isReversed))
                                 : String.format("currentDeletion=%s, marker=%s", currentDeletion, marker.toString(command.metadata()));

                            // and so unless it's a boundary whose opening deletion time is still equal to the current
                            // deletion (see comment above for why this can actually happen), we have to repair the source
                            // from that point on.
                            if (!(marker.isOpen(isReversed) && currentDeletion.equals(marker.openDeletionTime(isReversed))))
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

            // it will be invoked after finish this row iterator
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
                    repairResponseRequestMap.put(MessagingService.instance().sendRR(msg, sources[i]), Pair.create(msg, sources[i]));
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
            final TableMetadata metadata;
            final DecoratedKey partitionKey;
            Clustering lastClustering;
            int lastCount = 0;

            private ShortReadRowProtection(TableMetadata metadata, DecoratedKey partitionKey)
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
                    MessagingService.instance().sendRRWithFailure(retryCommand.createMessage(), source, handler);

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

    public Map<AsyncOneResponse, Pair<MessageOut, InetAddress>> getRepairResponseRequestMap()
    {
        return new HashMap<>(repairResponseRequestMap);
    }
}
