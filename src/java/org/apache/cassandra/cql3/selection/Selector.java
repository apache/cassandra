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
package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * A <code>Selector</code> is used to convert the data returned by the storage engine into the data requested by the
 * user. They correspond to the &lt;selector&gt; elements from the select clause.
 * <p>Since the introduction of aggregation, <code>Selector</code>s cannot be called anymore by multiple threads
 * as they have an internal state.</p>
 */
public abstract class Selector
{
    /**
     * A factory for <code>Selector</code> instances.
     */
    public static abstract class Factory
    {
        public void addFunctionsTo(List<Function> functions)
        {
        }

        /**
         * Returns the column specification corresponding to the output value of the selector instances created by
         * this factory.
         *
         * @param table the table meta data
         * @return a column specification
         */
        public ColumnSpecification getColumnSpecification(TableMetadata table)
        {
            return new ColumnSpecification(table.keyspace,
                                           table.name,
                                           new ColumnIdentifier(getColumnName(), true), // note that the name is not necessarily
                                                                                        // a true column name so we shouldn't intern it
                                           getReturnType());
        }

        /**
         * Creates a new <code>Selector</code> instance.
         *
         * @param options the options of the query for which the instance is created (some selector
         * depends on the bound values in particular).
         * @return a new <code>Selector</code> instance
         */
        public abstract Selector newInstance(QueryOptions options) throws InvalidRequestException;

        /**
         * Checks if this factory creates selectors instances that creates aggregates.
         *
         * @return <code>true</code> if this factory creates selectors instances that creates aggregates,
         * <code>false</code> otherwise
         */
        public boolean isAggregateSelectorFactory()
        {
            return false;
        }

        /**
         * Checks if this factory creates <code>writetime</code> selectors instances.
         *
         * @return <code>true</code> if this factory creates <code>writetime</code> selectors instances,
         * <code>false</code> otherwise
         */
        public boolean isWritetimeSelectorFactory()
        {
            return false;
        }

        /**
         * Checks if this factory creates <code>TTL</code> selectors instances.
         *
         * @return <code>true</code> if this factory creates <code>TTL</code> selectors instances,
         * <code>false</code> otherwise
         */
        public boolean isTTLSelectorFactory()
        {
            return false;
        }

        /**
         * Checks if this factory creates <code>Selector</code>s that simply return a column value.
         *
         * @param index the column index
         * @return <code>true</code> if this factory creates <code>Selector</code>s that simply return a column value,
         * <code>false</code> otherwise.
         */
        public boolean isSimpleSelectorFactory()
        {
            return false;
        }

        /**
         * Checks if this factory creates <code>Selector</code>s that simply return the specified column.
         *
         * @param index the column index
         * @return <code>true</code> if this factory creates <code>Selector</code>s that simply return
         * the specified column, <code>false</code> otherwise.
         */
        public boolean isSimpleSelectorFactoryFor(int index)
        {
            return false;
        }

        /**
         * Returns the name of the column corresponding to the output value of the selector instances created by
         * this factory.
         *
         * @return a column name
         */
        protected abstract String getColumnName();

        /**
         * Returns the type of the values returned by the selector instances created by this factory.
         *
         * @return the selector output type
         */
        protected abstract AbstractType<?> getReturnType();

        /**
         * Record a mapping between the ColumnDefinitions that are used by the selector
         * instances created by this factory and a column in the ResultSet.Metadata
         * returned with a query. In most cases, this is likely to be a 1:1 mapping,
         * but some selector instances may utilise multiple columns (or none at all)
         * to produce a value (i.e. functions).
         *
         * @param mapping the instance of the column mapping belonging to the current query's Selection
         * @param resultsColumn the column in the ResultSet.Metadata to which the ColumnDefinitions used
         *                      by the Selector are to be mapped
         */
        protected abstract void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn);

        /**
         * Checks if all the columns fetched by the selector created by this factory are known
         * @return {@code true} if all the columns fetched by the selector created by this factory are known,
         * {@code false} otherwise.
         */
        abstract boolean areAllFetchedColumnsKnown();

        /**
         * Adds the columns fetched by the selector created by this factory to the provided builder, assuming the
         * factory is terminal (i.e. that {@code isTerminal() == true}).
         *
         * @param builder the column builder to add fetched columns (and potential subselection) to.
         * @throws AssertionError if the method is called on a factory where {@code isTerminal()} returns {@code false}.
         */
        abstract void addFetchedColumns(ColumnFilter.Builder builder);
    }

    /**
     * Add to the provided builder the column (and potential subselections) to fetch for this
     * selection.
     *
     * @param builder the builder to add columns and subselections to.
     */
    public abstract void addFetchedColumns(ColumnFilter.Builder builder);

    /**
     * Add the current value from the specified <code>ResultSetBuilder</code>.
     *
     * @param protocolVersion protocol version used for serialization
     * @param rs the <code>ResultSetBuilder</code>
     * @throws InvalidRequestException if a problem occurs while add the input value
     */
    public abstract void addInput(ProtocolVersion protocolVersion, ResultSetBuilder rs) throws InvalidRequestException;

    /**
     * Returns the selector output.
     *
     * @param protocolVersion protocol version used for serialization
     * @return the selector output
     * @throws InvalidRequestException if a problem occurs while computing the output value
     */
    public abstract ByteBuffer getOutput(ProtocolVersion protocolVersion) throws InvalidRequestException;

    /**
     * Returns the <code>Selector</code> output type.
     *
     * @return the <code>Selector</code> output type.
     */
    public abstract AbstractType<?> getType();

    /**
     * Reset the internal state of this <code>Selector</code>.
     */
    public abstract void reset();
}
