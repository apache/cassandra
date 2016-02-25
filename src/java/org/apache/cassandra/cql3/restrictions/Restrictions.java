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

import java.util.Collection;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.SecondaryIndexManager;

/**
 * Sets of restrictions
 */
public interface Restrictions
{
    /**
     * Returns the column definitions in position order.
     * @return the column definitions in position order.
     */
    public Collection<ColumnDefinition> getColumnDefs();

    /**
     * Return an Iterable over all of the functions (both native and user-defined) used by any component
     * of the restrictions
     * @return functions all functions found (may contain duplicates)
     */
    public Iterable<Function> getFunctions();

    /**
     * Check if the restriction is on indexed columns.
     *
     * @param indexManager the index manager
     * @return <code>true</code> if the restriction is on indexed columns, <code>false</code>
     */
    public boolean hasSupportingIndex(SecondaryIndexManager indexManager);

    /**
     * Adds to the specified row filter the expressions corresponding to this <code>Restrictions</code>.
     *
     * @param filter the row filter to add expressions to
     * @param indexManager the secondary index manager
     * @param options the query options
     * @throws InvalidRequestException if this <code>Restrictions</code> cannot be converted into a row filter
     */
    public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) throws InvalidRequestException;

    /**
     * Checks if this <code>PrimaryKeyRestrictionSet</code> is empty or not.
     *
     * @return <code>true</code> if this <code>PrimaryKeyRestrictionSet</code> is empty, <code>false</code> otherwise.
     */
    boolean isEmpty();

    /**
     * Returns the number of columns that have a restriction.
     *
     * @return the number of columns that have a restriction.
     */
    public int size();
}
