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
    /**
     * Checks if this restriction is a token restriction.
     * @return {@code true} if this restriction is a token restriction, {@code false} otherwise
     */
    default boolean isOnToken()
    {
        return false;
    }

    /**
     * Returns the column metadata in position order.
     * @return the column metadata in position order.
     */
    List<ColumnMetadata> columns();

    /**
     * Returns the metadata of the first column.
     * @return the metadata of the first column.
     */
    ColumnMetadata firstColumn();

    /**
     * Returns the metadata of the last column.
     * @return the metadata of the last column.
     */
    ColumnMetadata lastColumn();

    /**
     * Adds all functions (native and user-defined) used by any component of the restriction
     * to the specified list.
     * @param functions the list to add to
     */
    void addFunctionsTo(List<Function> functions);

    /**
     * Checks if this restriction requires filtering or indexing.
     * @return {@code true} if the restriction will require either filtering or indexing, {@code false} otherwise.
     */
    boolean needsFilteringOrIndexing();

    /**
     * Returns whether this restriction would need filtering if the specified index group were used.
     * Find the first index supporting this restriction.
     *
     * @param indexGroup an index group
     * @return {@code true} if this would need filtering if {@code indexGroup} were used, {@code false} otherwise
     */
    boolean needsFiltering(Index.Group indexGroup);

    /**
     * Check if the restriction is on indexed columns.
     *
     * @param indexes the available indexes
     * @return <code>true</code> if the restriction is on indexed columns, <code>false</code> otherwise
     */
    default boolean hasSupportingIndex(Iterable<Index> indexes)
    {
        return findSupportingIndex(indexes) != null;
    }

    /**
     * Find the first index supporting this restriction.
     *
     * @param indexes the available indexes
     * @return an {@code Index} if the restriction is on indexed columns, {@code null} otherwise.
     */
    Index findSupportingIndex(Iterable<Index> indexes);

    /**
     * Adds to the specified row filter the expressions corresponding to this <code>Restriction</code>.
     *
     * @param filter the row filter to add expressions to
     * @param indexRegistry the index registry
     * @param options the query options
     */
    void addToRowFilter(RowFilter filter,
                        IndexRegistry indexRegistry,
                        QueryOptions options);
}
