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

package org.apache.cassandra.service.reads.tracked;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.service.reads.tracked.PartialTrackedRangeRead.FollowUpReadInfo;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

import static org.apache.cassandra.service.reads.tracked.ExtendingCompletedRead.followUpReadRequired;
import static org.apache.cassandra.service.reads.tracked.ExtendingCompletedRead.toQuery;
import static org.apache.cassandra.service.reads.tracked.PartialTrackedRangeRead.makeFollowUpRead;

class FilteredFollowupRead extends AsyncPromise<TrackedDataResponse>
{
    private final TrackedDataResponse initialResponse;
    private final int toQuery;
    private final ConsistencyLevel consistencyLevel;
    private final Dispatcher.RequestTime requestTime;
    private final SortedMap<DecoratedKey, FollowUpReadInfo> followUpReadInfo;
    private final PartitionRangeReadCommand command;
    private final AbstractBounds<PartitionPosition> followUpBounds;
    private final DecoratedKey finalKey;

    public FilteredFollowupRead(TrackedDataResponse initialResponse,
                                int toQuery,
                                ConsistencyLevel consistencyLevel,
                                Dispatcher.RequestTime requestTime,
                                SortedMap<DecoratedKey, FollowUpReadInfo> followUpReadInfo,
                                PartitionRangeReadCommand command,
                                AbstractBounds<PartitionPosition> followUpBounds,
                                DecoratedKey finalKey)
    {
        this.initialResponse = initialResponse;
        this.toQuery = toQuery;
        this.consistencyLevel = consistencyLevel;
        this.requestTime = requestTime;
        this.followUpReadInfo = followUpReadInfo;
        this.command = command;
        this.followUpBounds = followUpBounds;
        this.finalKey = finalKey;
    }

    private boolean interleavesWithOriginal(DecoratedKey key)
    {
        if (finalKey == null)
            return false;
        return key.compareTo(finalKey) < 0;
    }

    public void start()
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        List<Future<TrackedDataResponse>> futures = new ArrayList<>();

        int remaining = toQuery;
        PeekingIterator<DecoratedKey> followUpKeys = Iterators.peekingIterator(followUpReadInfo.keySet().iterator());
        // query all keys that interleave with the range of keys from the original range read
        while (followUpKeys.hasNext() && (remaining > 0 || interleavesWithOriginal(followUpKeys.peek())))
        {
            DecoratedKey key = followUpKeys.next();
            FollowUpReadInfo info = followUpReadInfo.get(key);
            remaining -= info.potentialMatches;
            SinglePartitionReadCommand cmd = SinglePartitionReadCommand.fromRangeRead(key, command, command.limits().forShortReadRetry(toQuery));
            TrackedRead.Partition read = TrackedRead.Partition.create(metadata, cmd, consistencyLevel, requestTime);
            read.start(requestTime);
            futures.add(read.future());
        }

        SortedMap<DecoratedKey, FollowUpReadInfo> nextKeys = followUpKeys.hasNext() ? followUpReadInfo.tailMap(followUpKeys.next()) : Collections.emptySortedMap();

        AtomicReference<PartialTrackedRead> partialRead;
        if (remaining > 0)
        {
            partialRead = new AtomicReference<>();
            TrackedRead.Range rangeRead = makeFollowUpRead(command, followUpBounds, remaining, consistencyLevel, requestTime);
            rangeRead.startLocal(requestTime, partialRead::set, TrackedLocalReads.Completer.DEFAULT);
            futures.add(rangeRead.future());
        }
        else
        {
            partialRead = null;
        }

        FutureCombiner.allOf(futures).addCallback((responses, error) -> {
            if (error != null)
            {
                tryFailure(error);
                return;
            }

            try
            {
                List<TrackedDataResponse> allResponses = new ArrayList<>(responses);
                allResponses.add(initialResponse);
                TrackedDataResponse merged = TrackedDataResponse.merge(allResponses);
                DataLimits.Counter mergedResultCounter = command.limits().newCounter(command.nowInSec(),
                                                                                     true,
                                                                                     command.selectsFullPartition(),
                                                                                     command.metadata().enforceStrictLiveness());

                boolean partitionsFetched;
                boolean initialIteratorExhausted;
                TrackedDataResponse response;
                try (PartitionIterator iterator = merged.makeIteratorUnlimited(command))
                {
                    partitionsFetched = iterator.hasNext();
                    response = TrackedDataResponse.create(mergedResultCounter.applyTo(iterator), command.columnFilter());
                    initialIteratorExhausted = iterator.hasNext();
                }

                // although we check for interleaved keys in the initial read, we always query for them in the follow up, so
                // we just use normal short read protection checks here
                if (followUpReadRequired(command, mergedResultCounter, initialIteratorExhausted, partitionsFetched))
                {
                    AbstractBounds<PartitionPosition> nextBounds =  this.followUpBounds;
                    if (partialRead != null)
                    {
                        PartialTrackedRangeRead followUpRangeRead = (PartialTrackedRangeRead) partialRead.get();
                        nextBounds = followUpRangeRead.followUpBounds();
                    }
                    FilteredFollowupRead followUp = new FilteredFollowupRead(response, toQuery(command, mergedResultCounter), consistencyLevel, requestTime, nextKeys, command, nextBounds, null);
                    followUp.start();
                    followUp.addCallback((result, failure) -> {
                        if (failure != null)
                            tryFailure(failure);
                        else
                            trySuccess(result);
                    });
                }
                else
                {
                    trySuccess(response);
                }
            }
            catch (Throwable t)
            {
                tryFailure(t);
            }

        });
    }
}
