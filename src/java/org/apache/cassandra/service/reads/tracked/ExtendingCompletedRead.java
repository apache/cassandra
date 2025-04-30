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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.transform.EmptyPartitionsDiscarder;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

class ExtendingCompletedRead implements PartialTrackedRead.CompletedRead
{
    private static final Logger logger = LoggerFactory.getLogger(ExtendingCompletedRead.class);

    final PartitionRangeReadCommand command;
    final UnfilteredPartitionIterator iterator;
    // merged end-result counter
    final DataLimits.Counter mergedResultCounter;

    private final boolean partitionsFetched;
    private final boolean initialIteratorExhausted;
    protected final AbstractBounds<PartitionPosition> followUpBounds;

    public ExtendingCompletedRead(PartitionRangeReadCommand command,
                                  UnfilteredPartitionIterator iterator,
                                  boolean partitionsFetched,
                                  boolean initialIteratorExhausted,
                                  AbstractBounds<PartitionPosition> followUpBounds)
    {
        this.command = command;
        this.iterator = iterator;
        mergedResultCounter = command.limits().newCounter(command.nowInSec(),
                                                          true,
                                                          command.selectsFullPartition(),
                                                          command.metadata().enforceStrictLiveness());
        this.partitionsFetched = partitionsFetched;
        this.initialIteratorExhausted = initialIteratorExhausted;
        this.followUpBounds = followUpBounds;
    }

    @Override
    public TrackedDataResponse response()
    {
        PartitionIterator filtered = UnfilteredPartitionIterators.filter(iterator, command.nowInSec());
        PartitionIterator counted = Transformation.apply(filtered, mergedResultCounter);
        PartitionIterator result = Transformation.apply(counted, new EmptyPartitionsDiscarder());
        return TrackedDataResponse.create(result, command.columnFilter());
    }

    static boolean followUpReadRequired(ReadCommand command, DataLimits.Counter mergedResultCounter, boolean initialIteratorExhausted, boolean partitionsFetched)
    {
        // never try to request additional partitions from replicas if our reconciled partitions are already filled to the limit
        if (mergedResultCounter.isDone())
            return false;

        // we do not apply short read protection when we have no limits at all
        if (command.limits().isUnlimited())
            return false;

        /*
         * If this is a single partition read command or an (indexed) partition range read command with
         * a partition key specified, then we can't and shouldn't try fetch more partitions.
         */
        if (command.isLimitedToOnePartition())
            return false;

        /*
         * If the returned result doesn't have enough rows/partitions to satisfy even the original limit, don't ask for more.
         *
         * Can only take the short cut if there is no per partition limit set. Otherwise it's possible to hit false
         * positives due to some rows being uncounted for in certain scenarios (see CASSANDRA-13911).
         */
        if (initialIteratorExhausted && command.limits().perPartitionCount() == DataLimits.NO_LIMIT)
            return false;

        /*
         * Either we had an empty iterator as the initial response, or our moreContents() call got us an empty iterator.
         * There is no point to ask the replica for more rows - it has no more in the requested range.
         */
        if (!partitionsFetched)
            return false;

        return true;
    }

    protected boolean followUpRequired()
    {
        return followUpReadRequired(command, mergedResultCounter, initialIteratorExhausted, partitionsFetched);
    }

    static int toQuery(ReadCommand command, DataLimits.Counter mergedResultCounter)
    {
        /*
         * We are going to fetch one partition at a time for thrift and potentially more for CQL.
         * The row limit will either be set to the per partition limit - if the command has no total row limit set, or
         * the total # of rows remaining - if it has some. If we don't grab enough rows in some of the partitions,
         * then future ShortReadRowsProtection.moreContents() calls will fetch the missing ones.
         */
        return command.limits().count() != DataLimits.NO_LIMIT
               ? command.limits().count() - mergedResultCounter.rowsCounted()
               : command.limits().perPartitionCount();
    }

    @Override
    public Future<TrackedDataResponse> followupRead(TrackedDataResponse initialResponse, ConsistencyLevel consistencyLevel, long expiresAtNanos)
    {
        if (!followUpRequired())
            return null;


        /*
         * We are going to fetch one partition at a time for thrift and potentially more for CQL.
         * The row limit will either be set to the per partition limit - if the command has no total row limit set, or
         * the total # of rows remaining - if it has some. If we don't grab enough rows in some of the partitions,
         * then future ShortReadRowsProtection.moreContents() calls will fetch the missing ones.
         */
        int toQuery = toQuery(command, mergedResultCounter);

        ColumnFamilyStore.metricsFor(command.metadata().id).shortReadProtectionRequests.mark();
        Tracing.trace("Requesting {} extra rows from {} for short read protection", toQuery, FBUtilities.getBroadcastAddressAndPort());
        logger.info("Requesting {} extra rows from {} for short read protection", toQuery, FBUtilities.getBroadcastAddressAndPort());

        return makeFollowupRead(initialResponse, toQuery, consistencyLevel, expiresAtNanos);
    }

    protected Future<TrackedDataResponse> makeFollowupRead(TrackedDataResponse initialResponse, int toQuery, ConsistencyLevel consistencyLevel, long expiresAtNanos)
    {
        TrackedRead.Range followUpRead = PartialTrackedRangeRead.makeFollowUpRead(command, followUpBounds, toQuery, consistencyLevel, expiresAtNanos);
        followUpRead.start(expiresAtNanos);
        AsyncPromise<TrackedDataResponse> combinedRead = new AsyncPromise<>();
        followUpRead.future().addCallback((result, failure) -> {
            if (failure != null)
            {
                combinedRead.tryFailure(failure);
                return;
            }

            try
            {
                combinedRead.trySuccess(TrackedDataResponse.merge(initialResponse, result));
            }
            catch (Throwable t)
            {
                combinedRead.tryFailure(t);
            }
        });

        return combinedRead;
    }

    @Override
    public void close()
    {
        iterator.close();
    }
}
