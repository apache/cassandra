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
package org.apache.cassandra.cql3;

import java.util.Collections;
import java.util.List;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public interface CQLStatement
{
    /**
     * Returns all bind variables for the statement
     */
    default List<ColumnSpecification> getBindVariables()
    {
        return Collections.emptyList();
    }

    /**
     * Returns an array with the same length as the number of partition key columns for the table corresponding
     * to table.  Each short in the array represents the bind index of the marker that holds the value for that
     * partition key column. If there are no bind markers for any of the partition key columns, null is returned.
     */
    default short[] getPartitionKeyBindVariableIndexes()
    {
        return null;
    }

    /**
     * Return an Iterable over all of the functions (both native and user-defined) used by any component of the statement
     *
     * @return functions all functions found (may contain duplicates)
     */
    default Iterable<Function> getFunctions()
    {
        return Collections.emptyList();
    }

    /**
     * Perform any access verification necessary for the statement.
     *
     * @param state the current client state
     */
    public void authorize(ClientState state);

    /**
     * Perform additional validation required by the statment. To be overriden by subclasses if needed.
     *
     * @param state the current client state
     */
    public void validate(ClientState state);

    /**
     * Execute the statement and return the resulting result or null if there is no result.
     *
     * @param state the current query state
     * @param options options for this query (consistency, variables, pageSize, ...)
     * @param queryStartNanoTime the timestamp returned by System.nanoTime() when this statement was received
     */
    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime);

    /**
     * Variant of execute used for internal query against the system tables, and thus only query the local node.
     *
     * @param state the current query state
     */
    public ResultMessage executeLocally(QueryState state, QueryOptions options);

    /**
     * Provides the context needed for audit logging statements.
     */
    AuditLogContext getAuditLogContext();

    /**
     * Whether or not this CQL Statement has LWT conditions
     */
    default public boolean hasConditions()
    {
        return false;
    }

    public static abstract class Raw
    {
        protected VariableSpecifications bindVariables;

        public void setBindVariables(List<ColumnIdentifier> variables)
        {
            bindVariables = new VariableSpecifications(variables);
        }

        public abstract CQLStatement prepare(ClientState state);
    }

    public static interface SingleKeyspaceCqlStatement extends CQLStatement
    {
        public String keyspace();
    }
}
