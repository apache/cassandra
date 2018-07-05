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
import com.google.common.collect.Iterables;

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
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.locator.Replicas;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.repair.ReadRepair;

import static com.google.common.collect.Iterables.*;

public class DataResolver<E extends Endpoints<E>, L extends ReplicaLayout<E, L>> extends ResponseResolver<E, L>
{
    private final boolean enforceStrictLiveness;

    public DataResolver(ReadCommand command, L replicaLayout, ReadRepair<E, L> readRepair, long queryStartNanoTime)
    {
        super(command, replicaLayout, readRepair, queryStartNanoTime);
        this.enforceStrictLiveness = command.metadata().enforceStrictLiveness();
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

        E replicas = replicaLayout.all().keep(transform(messages, msg -> msg.from));
        List<UnfilteredPartitionIterator> iters = new ArrayList<>(
                Collections2.transform(messages, msg -> msg.payload.makeIterator(command)));
        assert replicas.size() == iters.size();

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
                                                                          replicaLayout.withSelected(replicas),
                                                                          mergedResultCounter);
        FilteredPartitions filtered = FilteredPartitions.filter(merged, new Filter(command.nowInSec(), command.metadata().enforceStrictLiveness()));
        PartitionIterator counted = Transformation.apply(filtered, mergedResultCounter);
        return Transformation.apply(counted, new EmptyPartitionsDiscarder());
    }

    private UnfilteredPartitionIterator mergeWithShortReadProtection(List<UnfilteredPartitionIterator> results,
                                                                     L sources,
                                                                     DataLimits.Counter mergedResultCounter)
    {
        // If we have only one results, there is no read repair to do and we can't get short reads
        if (results.size() == 1)
            return results.get(0);

        /*
         * So-called short reads stems from nodes returning only a subset of the results they have due to the limit,
         * but that subset not being enough post-reconciliation. So if we don't have a limit, don't bother.
         */
        if (!command.limits().isUnlimited())
            for (int i = 0; i < results.size(); i++)
                results.set(i, ShortReadProtection.extend(sources.selected().get(i), results.get(i), command, mergedResultCounter, queryStartNanoTime, enforceStrictLiveness));

        return UnfilteredPartitionIterators.merge(results, wrapMergeListener(readRepair.getMergeListener(sources), sources));
    }

    private String makeResponsesDebugString(DecoratedKey partitionKey)
    {
        return Joiner.on(",\n").join(transform(getMessages().snapshot(), m -> m.from + " => " + m.payload.toDebugString(command, partitionKey)));
    }

    private UnfilteredPartitionIterators.MergeListener wrapMergeListener(UnfilteredPartitionIterators.MergeListener partitionListener, L sources)
    {
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
                                                           sources.selected(),
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
                                                           sources.selected(),
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
                                                           sources.selected(),
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
            }
        };
    }
}
