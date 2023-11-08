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

import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

/**
 * Analogue of {@link org.apache.cassandra.index.sai.disk.v1.segment.SegmentOrdering}, but for memtables.
 */
public interface MemtableOrdering
{
    /**
     * Filter the given list of {@code PrimaryKey} results to the top `limit` results corresponding to the given expression,
     * Returns an iterator over the results that is put back in token order.
     * <p>
     * Assumes that the given list spans the same rows as the implementing index's segment.
     */
    default KeyRangeIterator limitToTopResults(List<PrimaryKey> primaryKeys, Expression expression, int limit)
    {
        throw new UnsupportedOperationException();
    }
}
