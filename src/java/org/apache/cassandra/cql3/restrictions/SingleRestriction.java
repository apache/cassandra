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
package org.apache.cassandra.cql3.restrictions;

import java.util.List;

import com.google.common.collect.RangeSet;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.index.Index;

/**
 * A single restriction/clause on one or multiple column.
 */
public interface SingleRestriction extends Restriction
{
    /**
     * Checks if the restriction applies to the column level.
     * @return {@code true} if the applies to the column level, {@code false} otherwise.
     */
    boolean isColumnLevel();

    /**
     * Checks if this restriction use an {@code EQ} operator.
     * @return {@code true} if this restriction use an {@code EQ} operator, {@code false} otherwise.
     */
    boolean isEQ();

    /**
     * Checks if this restriction use an {@code IN} operator.
     * @return {@code true} if this restriction use an {@code IN} operator, {@code false} otherwise.
     */
    boolean isIN();

    /**
     * Checks if this restriction use an {@code ANN} operator.
     * @return {@code true} if this restriction use an {@code ANN} operator, {@code false} otherwise.
     */
    boolean isANN();

    /**
     * Checks if this restriction is selecting a range of values.
     * @return {@code true} if this restriction is selecting a range of values, {@code false} otherwise.
     */
    boolean isSlice();

    boolean isMultiColumn();

    /**
     * Check if this type of restriction is supported by the specified index.
     *
     * @param index the secondary index
     * @return <code>true</code> this type of restriction is supported by the specified index,
     * <code>false</code> otherwise.
     */
    boolean isSupportedBy(Index index);

    /**
     * Merges this restriction with the specified one.
     *
     * <p>Restriction are immutable. Therefore merging two restrictions result in a new one.
     * The reason behind this choice is that it allow a great flexibility in the way the merging can done while
     * preventing any side effect.</p>
     *
     * @param other the restriction to merge into this one
     * @return the restriction resulting of the merge
     */
    default SingleRestriction mergeWith(SingleRestriction other)
    {
        return new MergedRestriction(this, (SimpleRestriction) other);
    }

    /**
     * Returns the values selected by this restriction if the operator is an {@code EQ} or an {@code IN}.
     *
     * @param options the query options
     * @return the values selected by this restriction if the operator is an {@code EQ} or an {@code IN}.
     * @throws UnsupportedOperationException if the operator is not an {@code EQ} or an {@code IN}.
     */
    List<ClusteringElements> values(QueryOptions options);

    /**
     * Adds the ranges of values selected by this restriction to the specified {@code RangeSet} if the operator is an operator selecting ranges of data.
     *
     * @param rangeSet the range set to add to
     * @param options the query options
     * @throws UnsupportedOperationException if the operator is not an operator selecting ranges of data.
     */
    RangeSet<ClusteringElements> restrict(RangeSet<ClusteringElements> rangeSet, QueryOptions options);
}
