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
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.SecondaryIndexManager;

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
     * Adds all functions (native and user-defined) used by any component of the restriction
     * to the specified list.
     * @param functions the list to add to
     */
    void addFunctionsTo(List<Function> functions);

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
     */
    public void addRowFilterTo(RowFilter filter,
                               SecondaryIndexManager indexManager,
                               QueryOptions options);
}
