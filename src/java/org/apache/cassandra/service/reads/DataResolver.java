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

import java.util.*;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import com.google.common.collect.Maps;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.transform.*;
import org.apache.cassandra.net.*;
import org.apache.cassandra.schema.TableMetadata;

public class DataResolver extends ResponseResolver
{
    private final long queryStartNanoTime;
    private final boolean enforceStrictLiveness;
    private final Map<InetAddressAndPort, Replica> replicaMap;

    public DataResolver(Keyspace keyspace, ReadCommand command, ConsistencyLevel consistency, ReplicaCollection replicas, int maxResponseCount, long queryStartNanoTime, ReadRepair readRepair)
    {
        super(keyspace, command, consistency, readRepair, maxResponseCount);
        this.queryStartNanoTime = queryStartNanoTime;
        this.enforceStrictLiveness = command.metadata().enforceStrictLiveness();

        replicaMap = Maps.newHashMapWithExpectedSize(replicas.size());
        for (Replica replica: replicas)
        {
            replicaMap.put(replica.getEndpoint(), replica);
        }
    }

    public PartitionIterator getData()
    {
        ReadResponse response = responses.iterator().next().payload;
        return UnfilteredPartitionIterators.filter(response.makeIterator(command), command.nowInSec());
    }

    public boolean isDataPresent()
    {
        return !responses.isEmpty();
    }

    public PartitionIterator resolve()
    {
        // We could get more responses while this method runs, which is ok (we're happy to ignore any response not here
        // at the beginning of this method), so grab the response count once and use that through the method.
        int count = responses.size();
        List<UnfilteredPartitionIterator> iters = new ArrayList<>(count);
        Replica[] sources = new Replica[count];
        for (int i = 0; i < count; i++)
        {
            MessageIn<ReadResponse> msg = responses.get(i);
            iters.add(msg.payload.makeIterator(command));

            Replica replica = replicaMap.get(msg.from);
            if (replica == null)
                replica = command.decorateEndpoint(msg.from);

            assert replica != null;
            sources[i] = replica;
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

        UnfilteredPartitionIterator merged = mergeWithShortReadProtection(iters, sources, mergedResultCounter);
        FilteredPartitions filtered = FilteredPartitions.filter(merged, new Filter(command.nowInSec(), command.metadata().enforceStrictLiveness()));
        PartitionIterator counted = Transformation.apply(filtered, mergedResultCounter);
        return Transformation.apply(counted, new EmptyPartitionsDiscarder());
    }

    private UnfilteredPartitionIterator mergeWithShortReadProtection(List<UnfilteredPartitionIterator> results,
                                                                     Replica[] sources,
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
                results.set(i, ShortReadProtection.extend(sources[i], results.get(i), command, mergedResultCounter, queryStartNanoTime, enforceStrictLiveness));

        return UnfilteredPartitionIterators.merge(results, command.nowInSec(), wrapMergeListener(readRepair.getMergeListener(sources), sources));
    }

    private String makeResponsesDebugString(DecoratedKey partitionKey)
    {
        return Joiner.on(",\n").join(Iterables.transform(getMessages(), m -> m.from + " => " + m.payload.toDebugString(command, partitionKey)));
    }

    private UnfilteredPartitionIterators.MergeListener wrapMergeListener(UnfilteredPartitionIterators.MergeListener partitionListener, Replica[] sources)
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
                                                           '[' + Joiner.on(", ").join(Iterables.transform(Arrays.asList(versions), rt -> rt == null ? "null" : rt.toString())) + ']',
                                                           Arrays.toString(sources),
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
                                                           '[' + Joiner.on(", ").join(Iterables.transform(Arrays.asList(versions), rt -> rt == null ? "null" : rt.toString(table))) + ']',
                                                           Arrays.toString(sources),
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
                                                           '[' + Joiner.on(", ").join(Iterables.transform(Arrays.asList(versions), rt -> rt == null ? "null" : rt.toString(table))) + ']',
                                                           Arrays.toString(sources),
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
