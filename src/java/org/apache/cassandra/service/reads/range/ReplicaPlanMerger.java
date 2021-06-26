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

import java.util.Iterator;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.utils.AbstractIterator;

class ReplicaPlanMerger extends AbstractIterator<ReplicaPlan.ForRangeRead>
{
    private final Keyspace keyspace;
    private final ConsistencyLevel consistency;
    private final PeekingIterator<ReplicaPlan.ForRangeRead> ranges;

    ReplicaPlanMerger(Iterator<ReplicaPlan.ForRangeRead> iterator, Keyspace keyspace, ConsistencyLevel consistency)
    {
        this.keyspace = keyspace;
        this.consistency = consistency;
        this.ranges = Iterators.peekingIterator(iterator);
    }

    @Override
    protected ReplicaPlan.ForRangeRead computeNext()
    {
        if (!ranges.hasNext())
            return endOfData();

        ReplicaPlan.ForRangeRead current = ranges.next();

        // getRestrictedRange has broken the queried range into per-[vnode] token ranges, but this doesn't take
        // the replication factor into account. If the intersection of live endpoints for 2 consecutive ranges
        // still meets the CL requirements, then we can merge both ranges into the same RangeSliceCommand.
        while (ranges.hasNext())
        {
            // If the current range right is the min token, we should stop merging because CFS.getRangeSlice
            // don't know how to deal with a wrapping range.
            // Note: it would be slightly more efficient to have CFS.getRangeSlice on the destination nodes unwraps
            // the range if necessary and deal with it. However, we can't start sending wrapped range without breaking
            // wire compatibility, so it's likely easier not to bother;
            if (current.range().right.isMinimum())
                break;

            ReplicaPlan.ForRangeRead next = ranges.peek();
            ReplicaPlan.ForRangeRead merged = ReplicaPlans.maybeMerge(keyspace, consistency, current, next);
            if (merged == null)
                break;

            current = merged;
            ranges.next(); // consume the range we just merged since we've only peeked so far
        }
        return current;
    }
}
