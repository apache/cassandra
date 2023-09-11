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

package org.apache.cassandra.index.sai.memory;

import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;

/***
 * Analogue of SegmentOrdering, but for memtables.
 */
public interface MemtableOrdering
{
    /**
     * Filter the given RangeIterator results to the top `limit` results corresponding to the given expression,
     * Returns an iterator over the results that is put back in token order.
     * <p>
     * This requires materializing the results into a BitSet or List, so any intersections we can perform
     * to minimize the input size should be performed before calling this.
     * <p>
     * Assumes that the given RangeIterator spans the same rows as the implementing index's segment.
     */
    default KeyRangeIterator limitToTopResults(QueryContext context, KeyRangeIterator iterator, Expression exp)
    {
        throw new UnsupportedOperationException();
    }
}
