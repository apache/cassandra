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

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.MultiCBuilder;

/**
 * A single restriction/clause on one or multiple column.
 */
public interface SingleRestriction extends Restriction
{
    public default boolean isSlice()
    {
        return false;
    }

    public default boolean isEQ()
    {
        return false;
    }

    public default boolean isLIKE()
    {
        return false;
    }

    public default boolean isIN()
    {
        return false;
    }

    public default boolean isContains()
    {
        return false;
    }

    public default boolean isANN()
    {
        return false;
    }

    /**
     * @return <code>true</code> if this restriction is based on equality comparison rather than a range or negation
     */
    default boolean isEqualityBased()
    {
        return isEQ() || isIN();
    }

    public default boolean isNotNull()
    {
        return false;
    }

    public default boolean isMultiColumn()
    {
        return false;
    }

    /**
     * Checks if the specified bound is set or not.
     * @param b the bound type
     * @return <code>true</code> if the specified bound is set, <code>false</code> otherwise
     */
    public default boolean hasBound(Bound b)
    {
        return true;
    }

    /**
     * Checks if the specified bound is inclusive or not.
     * @param b the bound type
     * @return <code>true</code> if the specified bound is inclusive, <code>false</code> otherwise
     */
    public default boolean isInclusive(Bound b)
    {
        return true;
    }

    /**
     * Merges this restriction with the specified one.
     *
     * <p>Restriction are immutable. Therefore merging two restrictions result in a new one.
     * The reason behind this choice is that it allow a great flexibility in the way the merging can done while
     * preventing any side effect.</p>
     *
     * @param otherRestriction the restriction to merge into this one
     * @return the restriction resulting of the merge
     */
    public SingleRestriction mergeWith(SingleRestriction otherRestriction);

    /**
     * Appends the values of this <code>SingleRestriction</code> to the specified builder.
     *
     * @param builder the <code>MultiCBuilder</code> to append to.
     * @param options the query options
     * @return the <code>MultiCBuilder</code>
     */
    public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options);

    /**
     * Appends the values of the <code>SingleRestriction</code> for the specified bound to the specified builder.
     *
     * @param builder the <code>MultiCBuilder</code> to append to.
     * @param bound the bound
     * @param options the query options
     * @return the <code>MultiCBuilder</code>
     */
    public default MultiCBuilder appendBoundTo(MultiCBuilder builder, Bound bound, QueryOptions options)
    {
        return appendTo(builder, options);
    }
}
