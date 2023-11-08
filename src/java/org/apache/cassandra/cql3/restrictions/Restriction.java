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

import org.apache.cassandra.index.Index;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.IndexRegistry;

/**
 * <p>Implementation of this class must be immutable.</p>
 */
public interface Restriction
{
    public default boolean isOnToken()
    {
        return false;
    }

    /**
     * Returns the definition of the first column.
     * @return the definition of the first column.
     */
    public ColumnMetadata getFirstColumn();

    /**
     * Returns the definition of the last column.
     * @return the definition of the last column.
     */
    public ColumnMetadata getLastColumn();

    /**
     * Returns the column definitions in position order.
     * @return the column definitions in position order.
     */
    public List<ColumnMetadata> getColumnDefs();

    /**
     * Adds all functions (native and user-defined) used by any component of the restriction
     * to the specified list.
     * @param functions the list to add to
     */
    void addFunctionsTo(List<Function> functions);

    /**
     * Check if the restriction is on indexed columns.
     *
     * @param indexRegistry the index registry
     * @return <code>true</code> if the restriction is on indexed columns, <code>false</code>
     */
    boolean hasSupportingIndex(IndexRegistry indexRegistry);

    /**
     * Find first supporting index for current restriction
     *
     * @param indexRegistry the index registry
     * @return <code>index</code> if the restriction is on indexed columns, <code>null</code>
     */
    Index findSupportingIndex(IndexRegistry indexRegistry);

    /**
     * Find the first supporting index for the current restriction from an {@link Index.QueryPlan}.
     * @param indexQueryPlan the index query plan
     * @return <code>index</code> if the restriction is on indexed columns, <code>null</code>
     */
    Index findSupportingIndexFromQueryPlan(Index.QueryPlan indexQueryPlan);

    /**
     * Returns whether this restriction would need filtering if the specified index group were used.
     *
     * @param indexGroup an index group
     * @return {@code true} if this would need filtering if {@code indexGroup} were used, {@code false} otherwise
     */
    boolean needsFiltering(Index.Group indexGroup);

    /**
     * Adds to the specified row filter the expressions corresponding to this <code>Restriction</code>.
     *
     * @param filter the row filter to add expressions to
     * @param indexRegistry the index registry
     * @param options the query options
     */
    public void addToRowFilter(RowFilter filter,
                               IndexRegistry indexRegistry,
                               QueryOptions options);
}
