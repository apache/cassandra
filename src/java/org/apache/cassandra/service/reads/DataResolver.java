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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.transform.EmptyPartitionsDiscarder;
import org.apache.cassandra.db.transform.Filter;
import org.apache.cassandra.db.transform.FilteredPartitions;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.service.reads.repair.RepairedDataTracker;
import org.apache.cassandra.service.reads.repair.RepairedDataVerifier;

import static com.google.common.collect.Iterables.*;
import static org.apache.cassandra.db.partitions.UnfilteredPartitionIterators.MergeListener;

public class DataResolver<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E>> extends ResponseResolver<E, P>
{
    private final boolean enforceStrictLiveness;
    private final ReadRepair<E, P> readRepair;

    public DataResolver(ReadCommand command, ReplicaPlan.Shared<E, P> replicaPlan, ReadRepair<E, P> readRepair, long queryStartNanoTime)
    {
        super(command, replicaPlan, queryStartNanoTime);
        this.enforceStrictLiveness = command.metadata().enforceStrictLiveness();
        this.readRepair = readRepair;
    }

    public PartitionIterator getData()
    {
        ReadResponse response = responses.get(0).payload;
        return UnfilteredPartitionIterators.filter(response.makeIterator(command), command.nowInSec());
    }

    public boolean isDataPresent()
    {
        return !responses.isEmpty();
    }

    @SuppressWarnings("resource")
    public PartitionIterator resolve()
    {
        // We could get more responses while this method runs, which is ok (we're happy to ignore any response not here
        // at the beginning of this method), so grab the response count once and use that through the method.
        Collection<MessageIn<ReadResponse>> messages = responses.snapshot();
        assert !any(messages, msg -> msg.payload.isDigestResponse());

        E replicas = replicaPlan().candidates().select(transform(messages, msg -> msg.from), false);
        List<UnfilteredPartitionIterator> iters = new ArrayList<>(
        Collections2.transform(messages, msg -> msg.payload.makeIterator(command)));
        assert replicas.size() == iters.size();

        // If requested, inspect each response for a digest of the replica's repaired data set
        RepairedDataTracker repairedDataTracker = command.isTrackingRepairedStatus()
                                                  ? new RepairedDataTracker(getRepairedDataVerifier(command))
                                                  : null;
        if (repairedDataTracker != null)
        {
            messages.forEach(msg -> {
                if (msg.payload.mayIncludeRepairedDigest() && replicas.byEndpoint().get(msg.from).isFull())
                {
                    repairedDataTracker.recordDigest(msg.from,
                                                     msg.payload.repairedDataDigest(),
                                                     msg.payload.isRepairedDigestConclusive());
                }
            });
        }

        /*
         * Even though every response, individually, will honor the limit, it is possible that we will, after the merge,
         * have more rows than the client requested. To make sure that we still conform to the original limit,
         * we apply a top-level post-reconciliation counter to the merged partition iterator.
         *
         * Short read protection logic (ShortReadRowsProtection.moreContents()) relies on this counter to be applied
         * to the current partition to work. For this reason we have to apply the counter transformation before
         * empty partition discard logic kicks in - for it will eagerly consume the iterator.
         *
         * That's why the order here is: 1) merge; 2) filter rows; 3) count; 4) discard empty partitions
         *
         * See CASSANDRA-13747 for more details.
         */
        DataLimits.Counter mergedResultCounter =
            command.limits().newCounter(command.nowInSec(), true, command.selectsFullPartition(), enforceStrictLiveness);

        UnfilteredPartitionIterator merged = mergeWithShortReadProtection(iters,
                                                                          replicaPlan.getWithContacts(replicas),
                                                                          mergedResultCounter,
                                                                          repairedDataTracker);
        FilteredPartitions filtered = FilteredPartitions.filter(merged, new Filter(command.nowInSec(), command.metadata().enforceStrictLiveness()));
        PartitionIterator counted = Transformation.apply(filtered, mergedResultCounter);
        return Transformation.apply(counted, new EmptyPartitionsDiscarder());
    }

    protected RepairedDataVerifier getRepairedDataVerifier(ReadCommand command)
    {
        return RepairedDataVerifier.simple(command);
    }

    private UnfilteredPartitionIterator mergeWithShortReadProtection(List<UnfilteredPartitionIterator> results,
                                                                     P sources,
                                                                     DataLimits.Counter mergedResultCounter,
                                                                     RepairedDataTracker repairedDataTracker)
    {
        // If we have only one results, there is no read repair to do, we can't get short
        // reads and we can't make a comparison between repaired data sets
        if (results.size() == 1)
            return results.get(0);

        /*
         * So-called short reads stems from nodes returning only a subset of the results they have due to the limit,
         * but that subset not being enough post-reconciliation. So if we don't have a limit, don't bother.
         */
        if (!command.limits().isUnlimited())
            for (int i = 0; i < results.size(); i++)
                results.set(i, ShortReadProtection.extend(sources.contacts().get(i), results.get(i), command, mergedResultCounter, queryStartNanoTime, enforceStrictLiveness));

        return UnfilteredPartitionIterators.merge(results, wrapMergeListener(readRepair.getMergeListener(sources), sources, repairedDataTracker));
    }

    private String makeResponsesDebugString(DecoratedKey partitionKey)
    {
        return Joiner.on(",\n").join(transform(getMessages().snapshot(), m -> m.from + " => " + m.payload.toDebugString(command, partitionKey)));
    }

    private UnfilteredPartitionIterators.MergeListener wrapMergeListener(UnfilteredPartitionIterators.MergeListener partitionListener,
                                                                         P sources,
                                                                         RepairedDataTracker repairedDataTracker)
    {
        // Avoid wrapping no-op listener as it doesn't throw, unless we're tracking repaired status
        // in which case we need to inject the tracker & verify on close
        if (partitionListener == UnfilteredPartitionIterators.MergeListener.NOOP)
        {
            if (repairedDataTracker == null)
                return partitionListener;

            return new UnfilteredPartitionIterators.MergeListener()
            {

                public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
                {
                    return UnfilteredRowIterators.MergeListener.NOOP;
                }

                public void close()
                {
                    repairedDataTracker.verify();
                }
            };
        }

        return new UnfilteredPartitionIterators.MergeListener()
        {
            public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
            {
                UnfilteredRowIterators.MergeListener rowListener = partitionListener.getRowMergeListener(partitionKey, versions);

                return new UnfilteredRowIterators.MergeListener()
                {
                    public void onMergedPartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions)
                    {
                        try
                        {
                            rowListener.onMergedPartitionLevelDeletion(mergedDeletion, versions);
                        }
                        catch (AssertionError e)
                        {
                            // The following can be pretty verbose, but it's really only triggered if a bug happen, so we'd
                            // rather get more info to debug than not.
                            TableMetadata table = command.metadata();
                            String details = String.format("Error merging partition level deletion on %s: merged=%s, versions=%s, sources={%s}, debug info:%n %s",
                                                           table,
                                                           mergedDeletion == null ? "null" : mergedDeletion.toString(),
                                                           '[' + Joiner.on(", ").join(transform(Arrays.asList(versions), rt -> rt == null ? "null" : rt.toString())) + ']',
                                                           sources.contacts(),
                                                           makeResponsesDebugString(partitionKey));
                            throw new AssertionError(details, e);
                        }
                    }

                    public void onMergedRows(Row merged, Row[] versions)
                    {
                        try
                        {
                            rowListener.onMergedRows(merged, versions);
                        }
                        catch (AssertionError e)
                        {
                            // The following can be pretty verbose, but it's really only triggered if a bug happen, so we'd
                            // rather get more info to debug than not.
                            TableMetadata table = command.metadata();
                            String details = String.format("Error merging rows on %s: merged=%s, versions=%s, sources={%s}, debug info:%n %s",
                                                           table,
                                                           merged == null ? "null" : merged.toString(table),
                                                           '[' + Joiner.on(", ").join(transform(Arrays.asList(versions), rt -> rt == null ? "null" : rt.toString(table))) + ']',
                                                           sources.contacts(),
                                                           makeResponsesDebugString(partitionKey));
                            throw new AssertionError(details, e);
                        }
                    }

                    public void onMergedRangeTombstoneMarkers(RangeTombstoneMarker merged, RangeTombstoneMarker[] versions)
                    {
                        try
                        {
                            // The code for merging range tombstones is a tad complex and we had the assertions there triggered
                            // unexpectedly in a few occasions (CASSANDRA-13237, CASSANDRA-13719). It's hard to get insights
                            // when that happen without more context that what the assertion errors give us however, hence the
                            // catch here that basically gather as much as context as reasonable.
                            rowListener.onMergedRangeTombstoneMarkers(merged, versions);
                        }
                        catch (AssertionError e)
                        {

                            // The following can be pretty verbose, but it's really only triggered if a bug happen, so we'd
                            // rather get more info to debug than not.
                            TableMetadata table = command.metadata();
                            String details = String.format("Error merging RTs on %s: merged=%s, versions=%s, sources={%s}, debug info:%n %s",
                                                           table,
                                                           merged == null ? "null" : merged.toString(table),
                                                           '[' + Joiner.on(", ").join(transform(Arrays.asList(versions), rt -> rt == null ? "null" : rt.toString(table))) + ']',
                                                           sources.contacts(),
                                                           makeResponsesDebugString(partitionKey));
                            throw new AssertionError(details, e);
                        }

                    }

                    public void close()
                    {
                        rowListener.close();
                    }
                };
            }

            public void close()
            {
                partitionListener.close();
                if (repairedDataTracker != null)
                    repairedDataTracker.verify();
            }
        };
    }
}
