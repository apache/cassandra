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

package org.apache.cassandra.index.sai.disk.v1.segment;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

/**
 * A {@link SegmentOrdering} orders and limits a list of {@link PrimaryKey}s.
 * <p>
 * When using {@link SegmentOrdering} there are several steps to
 * build the list of Primary Keys to be ordered and limited:
 * <p>
 * 1. Find all primary keys that match each non-ordering query predicate.
 * 2. Union and intersect the results of step 1 to build a single {@link KeyRangeIterator}
 *    ordered by {@link PrimaryKey}.
 * 3. Filter out any shadowed primary keys.
 * 4. Fan the primary keys from step 3 out to each sstable segment to order and limit each
 *    list of primary keys.
 * <p>
 * SegmentOrdering handles the fourth step.
 * <p>
 * Note: a segment ordering is only used when a query has both ordering and non-ordering predicates.
 * Where a query has only ordering predicates, the ordering is handled by
 * {@link org.apache.cassandra.index.sai.disk.SSTableIndex#search(Expression, AbstractBounds, QueryContext)}.
 */
public interface SegmentOrdering
{
    /**
     * Reorder, limit, and put back into original order the results from a single sstable
     */
    default KeyRangeIterator limitToTopKResults(QueryContext queryContext, List<PrimaryKey> primaryKeys, Expression expression) throws IOException
    {
        throw new UnsupportedOperationException();
    }
}
