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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RangeSplitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.CoordinationPlan;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.SatelliteReplicationStrategy;
import org.apache.cassandra.locator.satellites.KeyspaceFailoverState;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.compatibility.TokenRingUtils;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Pair;

class CoordinationPlanIterator extends AbstractIterator<CoordinationPlan.ForRangeRead>
{
    private final Keyspace keyspace;
    private final ConsistencyLevel consistency;
    private final TableId tableId;
    private final Index.QueryPlan indexQueryPlan;

    private final Deque<AbstractBounds<PartitionPosition>> ranges;

    CoordinationPlanIterator(AbstractBounds<PartitionPosition> keyRange,
                             @Nullable Index.QueryPlan indexQueryPlan,
                             Keyspace keyspace,
                             TableId tableId,
                             ConsistencyLevel consistency)
    {
        this.indexQueryPlan = indexQueryPlan;
        this.keyspace = keyspace;
        this.tableId = tableId;
        this.consistency = consistency;

        ReplicationParams replication = keyspace.getMetadata().params.replication;
        List<? extends AbstractBounds<PartitionPosition>> l = replication.isLocal() || replication.isMeta()
                                                              ? keyRange.unwrap()
                                                              : getRestrictedRanges(keyRange);
        this.ranges = new ArrayDeque<>(l);
    }

    /**
     * @return the number of {@link ReplicaPlan.ForRangeRead}s in this iterator
     */
    int size()
    {
        return ranges.size();
    }

    @Override
    protected CoordinationPlan.ForRangeRead computeNext()
    {
        ClusterMetadata metadata = ClusterMetadata.current();

        if (ranges.isEmpty())
            return endOfData();

        AbstractBounds<PartitionPosition> vnodeRange = ranges.poll();

        List<AbstractBounds<PartitionPosition>> subRanges = splitAtFailoverBoundaries(vnodeRange, keyspace, metadata);

        if (subRanges == null)
        {
            return CoordinationPlan.forRangeRead(metadata, keyspace, tableId, indexQueryPlan, consistency, vnodeRange, 1);
        }

        // if the range was split, return a plan for the first range, and put the other ranges at the head of the queue
        for (int i = subRanges.size() - 1; i >= 1; i--)
            ranges.addFirst(subRanges.get(i));

        return CoordinationPlan.forRangeRead(metadata, keyspace, tableId, indexQueryPlan, consistency, subRanges.get(0), 1);
    }

    /**
     * Compute all ranges we're going to query, in sorted order. Nodes can be replica destinations for many ranges,
     * so we need to restrict each scan to the specific range we want, or else we'd get duplicate results.
     */
    private static List<AbstractBounds<PartitionPosition>> getRestrictedRanges(final AbstractBounds<PartitionPosition> queryRange)
    {
        // special case for bounds containing exactly 1 (non-minimum) token
        if (queryRange instanceof Bounds && queryRange.left.equals(queryRange.right) && !queryRange.left.isMinimum())
        {
            return Collections.singletonList(queryRange);
        }

        ClusterMetadata metadata = ClusterMetadata.current();

        List<AbstractBounds<PartitionPosition>> ranges = new ArrayList<>();
        // divide the queryRange into pieces delimited by the ring and minimum tokens
        Iterator<Token> ringIter = TokenRingUtils.ringIterator(metadata.tokenMap.tokens(), queryRange.left.getToken(), true);
        AbstractBounds<PartitionPosition> remainder = queryRange;
        while (ringIter.hasNext())
        {
            /*
             * remainder is a range/bounds of partition positions and we want to split it with a token. We want to split
             * using the key returned by token.maxKeyBound. For instance, if remainder is [DK(10, 'foo'), DK(20, 'bar')],
             * and we have 3 nodes with tokens 0, 15, 30, we want to split remainder to A=[DK(10, 'foo'), 15] and
             * B=(15, DK(20, 'bar')]. But since we can't mix tokens and keys at the same time in a range, we use
             * 15.maxKeyBound() to have A include all keys having 15 as token and B include none of those (since that is
             * what our node owns).
             */
            Token upperBoundToken = ringIter.next();
            PartitionPosition upperBound = upperBoundToken.maxKeyBound();
            if (!remainder.left.equals(upperBound) && !remainder.contains(upperBound))
                // no more splits
                break;
            Pair<AbstractBounds<PartitionPosition>, AbstractBounds<PartitionPosition>> splits = remainder.split(upperBound);
            if (splits == null)
                continue;

            ranges.add(splits.left);
            remainder = splits.right;
        }
        ranges.add(remainder);

        return ranges;
    }

    /**
     * Split a single vnode range at failover state boundaries so that each sub-range is in a uniform
     * failover state. Returns null if no splitting is needed (non-SRS keyspace, no active transfer,
     * or range doesn't cross any state boundaries).
     *
     * <p>Uses the provided ClusterMetadata instance so the caller can use the same instance for
     * plan creation, ensuring consistency between split boundaries and coordination plans.
     *
     * <p>Follows the MigrationRouter.splitRangeByPendingRanges() pattern via shared {@link RangeSplitter}.
     */
    @VisibleForTesting
    static List<AbstractBounds<PartitionPosition>> splitAtFailoverBoundaries(AbstractBounds<PartitionPosition> range,
                                                                             Keyspace keyspace,
                                                                             ClusterMetadata metadata)
    {
        AbstractReplicationStrategy strategy = keyspace.getReplicationStrategy();
        if (!(strategy instanceof SatelliteReplicationStrategy))
            return null;

        KeyspaceFailoverState ksState = metadata.satelliteFailoverState.getKeyspaceState(keyspace.getName());
        if (ksState == null || ksState.isComplete())
            return null;

        // Collect all non-NORMAL ranges as state boundaries for splitting
        List<Range<Token>> stateBoundaries = new ArrayList<>();
        ksState.forEachRange((r, state) -> stateBoundaries.add(r));
        if (stateBoundaries.isEmpty())
            return null;

        List<AbstractBounds<PartitionPosition>> result = RangeSplitter.splitAtBoundaries(range, stateBoundaries);

        // No split occurred -- single sub-range is the original
        if (result.size() <= 1)
            return null;

        return result;
    }
}
