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

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.MultiCBuilder;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.SecondaryIndexManager;

/**
 * A restriction/clause on a column.
 * The goal of this class being to group all conditions for a column in a SELECT.
 *
 * <p>Implementation of this class must be immutable. See {@link #mergeWith(Restriction)} for more explanation.</p>
 */
public interface Restriction
{
    public boolean isOnToken();
    public boolean isSlice();
    public boolean isEQ();
    public boolean isLIKE();
    public boolean isIN();
    public boolean isContains();
    public boolean isNotNull();
    public boolean isMultiColumn();

    /**
     * Returns the definition of the first column.
     * @return the definition of the first column.
     */
    public ColumnDefinition getFirstColumn();

    /**
     * Returns the definition of the last column.
     * @return the definition of the last column.
     */
    public ColumnDefinition getLastColumn();

    /**
     * Returns the column definitions in position order.
     * @return the column definitions in position order.
     */
    public List<ColumnDefinition> getColumnDefs();

    /**
     * Return an Iterable over all of the functions (both native and user-defined) used by any component
     * of the restriction
     * @return functions all functions found (may contain duplicates)
     */
    public Iterable<Function> getFunctions();

    /**
     * Checks if the specified bound is set or not.
     * @param b the bound type
     * @return <code>true</code> if the specified bound is set, <code>false</code> otherwise
     */
    public boolean hasBound(Bound b);

    /**
     * Checks if the specified bound is inclusive or not.
     * @param b the bound type
     * @return <code>true</code> if the specified bound is inclusive, <code>false</code> otherwise
     */
    public boolean isInclusive(Bound b);

    /**
     * Merges this restriction with the specified one.
     *
     * <p>Restriction are immutable. Therefore merging two restrictions result in a new one.
     * The reason behind this choice is that it allow a great flexibility in the way the merging can done while
     * preventing any side effect.</p>
     *
     * @param otherRestriction the restriction to merge into this one
     * @return the restriction resulting of the merge
     * @throws InvalidRequestException if the restrictions cannot be merged
     */
    public Restriction mergeWith(Restriction otherRestriction) throws InvalidRequestException;

    /**
     * Check if the restriction is on indexed columns.
     *
     * @param indexManager the index manager
     * @return <code>true</code> if the restriction is on indexed columns, <code>false</code>
     */
    public boolean hasSupportingIndex(SecondaryIndexManager indexManager);

    /**
     * Adds to the specified row filter the expressions corresponding to this <code>Restriction</code>.
     *
     * @param filter the row filter to add expressions to
     * @param indexManager the secondary index manager
     * @param options the query options
     * @throws InvalidRequestException if this <code>Restriction</code> cannot be converted into a row filter
     */
    public void addRowFilterTo(RowFilter filter,
                               SecondaryIndexManager indexManager,
                               QueryOptions options)
                               throws InvalidRequestException;

    /**
     * Appends the values of this <code>Restriction</code> to the specified builder.
     *
     * @param builder the <code>MultiCBuilder</code> to append to.
     * @param options the query options
     * @return the <code>MultiCBuilder</code>
     */
    public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options);

    /**
     * Appends the values of the <code>Restriction</code> for the specified bound to the specified builder.
     *
     * @param builder the <code>MultiCBuilder</code> to append to.
     * @param bound the bound
     * @param options the query options
     * @return the <code>MultiCBuilder</code>
     */
    public MultiCBuilder appendBoundTo(MultiCBuilder builder, Bound bound, QueryOptions options);
}
