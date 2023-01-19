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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Pair;

class ReplicaPlanIterator extends AbstractIterator<ReplicaPlan.ForRangeRead>
{
    private final Keyspace keyspace;
    private final ConsistencyLevel consistency;
    private final Index.QueryPlan indexQueryPlan;
    @VisibleForTesting
    final Iterator<? extends AbstractBounds<PartitionPosition>> ranges;
    private final int rangeCount;

    ReplicaPlanIterator(AbstractBounds<PartitionPosition> keyRange,
                        @Nullable Index.QueryPlan indexQueryPlan,
                        Keyspace keyspace,
                        ConsistencyLevel consistency)
    {
        this.indexQueryPlan = indexQueryPlan;
        this.keyspace = keyspace;
        this.consistency = consistency;

        List<? extends AbstractBounds<PartitionPosition>> l = keyspace.getReplicationStrategy() instanceof LocalStrategy
                                                              ? keyRange.unwrap()
                                                              : getRestrictedRanges(keyRange);
        this.ranges = l.iterator();
        this.rangeCount = l.size();
    }

    /**
     * @return the number of {@link ReplicaPlan.ForRangeRead}s in this iterator
     */
    int size()
    {
        return rangeCount;
    }

    @Override
    protected ReplicaPlan.ForRangeRead computeNext()
    {
        if (!ranges.hasNext())
            return endOfData();

        return ReplicaPlans.forRangeRead(keyspace, indexQueryPlan, consistency, ranges.next(), 1);
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

        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();

        List<AbstractBounds<PartitionPosition>> ranges = new ArrayList<>();
        // divide the queryRange into pieces delimited by the ring and minimum tokens
        Iterator<Token> ringIter = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), queryRange.left.getToken(), true);
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
}
