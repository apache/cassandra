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

import java.util.List;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.v1.vector.PrimaryKeyWithScore;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * Analogue of {@link org.apache.cassandra.index.sai.disk.v1.segment.SegmentOrdering}, but for memtables.
 */
public interface MemtableOrdering
{
    /**
     * Order the index based on the given orderer (expression).
     *
     * @param queryContext - the query context
     * @param orderer      - the expression to order by
     * @param keyRange     - the key range to search
     * @return an iterator over the results in score order.
     */
    CloseableIterator<PrimaryKeyWithScore> orderBy(QueryContext queryContext,
                                                   Expression orderer,
                                                   AbstractBounds<PartitionPosition> keyRange);

    /**
     * Order the given list of {@link PrimaryKey} results corresponding to the given orderer.
     * Returns an iterator over the results in score order.
     *
     * Assumes that the given  spans the same rows as the implementing index's segment.
     */
    CloseableIterator<PrimaryKeyWithScore> orderResultsBy(QueryContext context, List<PrimaryKey> results, Expression orderer);
}
