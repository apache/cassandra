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

package org.apache.cassandra.index.sai.utils;

import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.plan.Expression;

/**
 * There are two steps in ordering:
 *
 * 1. Limit a single sstable's results to the correct keys. At this stage
 *    we put them back in primary key order to play nice with the rest of
 *    the query pipeline.
 * 2. Merge the results from multiple sstables. Now we leave them in the
 *    final, correct order.
 *
 * SegmentOrdering handles the first step.
 */
public interface MemtableOrdering
{
    /**
     * Filter the given RangeIterator results to the top `limit` results corresponding to the given expression,
     * Returns an iterator over the results that is put back in token order.
     *
     * This requires materializing the results into a BitSet or List, so any intersections we can perform
     * to minimize the input size should be performed before calling this.
     *
     * Assumes that the the given RangeIterator spans the same rows as the implementing index's segment.
     */
    default RangeIterator<PrimaryKey> limitToTopResults(QueryContext context, RangeIterator<PrimaryKey> iterator, Expression exp, int limit)
    {
        throw new UnsupportedOperationException();
    }
}
