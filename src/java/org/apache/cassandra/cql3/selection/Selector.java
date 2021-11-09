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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.schema.CQLTypeParser;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * A <code>Selector</code> is used to convert the data returned by the storage engine into the data requested by the
 * user. They correspond to the &lt;selector&gt; elements from the select clause.
 * <p>Since the introduction of aggregation, <code>Selector</code>s cannot be called anymore by multiple threads
 * as they have an internal state.</p>
 */
public abstract class Selector
{
    protected static abstract class SelectorDeserializer
    {
        protected abstract Selector deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException;

        protected final AbstractType<?> readType(TableMetadata metadata, DataInputPlus in) throws IOException
        {
            KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(metadata.keyspace);
            return readType(keyspace, in);
        }

        protected final AbstractType<?> readType(KeyspaceMetadata keyspace, DataInputPlus in) throws IOException
        {
            String cqlType = in.readUTF();
            return CQLTypeParser.parse(keyspace.name, cqlType, keyspace.types);
        }
    }

    /**
     * The <code>Selector</code> kinds.
     */
    public static enum Kind
    {
        SIMPLE_SELECTOR(SimpleSelector.deserializer),
        TERM_SELECTOR(TermSelector.deserializer),
        WRITETIME_OR_TTL_SELECTOR(WritetimeOrTTLSelector.deserializer),
        LIST_SELECTOR(ListSelector.deserializer),
        SET_SELECTOR(SetSelector.deserializer),
        MAP_SELECTOR(MapSelector.deserializer),
        TUPLE_SELECTOR(TupleSelector.deserializer),
        USER_TYPE_SELECTOR(UserTypeSelector.deserializer),
        FIELD_SELECTOR(FieldSelector.deserializer),
        SCALAR_FUNCTION_SELECTOR(ScalarFunctionSelector.deserializer),
        AGGREGATE_FUNCTION_SELECTOR(AggregateFunctionSelector.deserializer),
        ELEMENT_SELECTOR(ElementsSelector.ElementSelector.deserializer),
        SLICE_SELECTOR(ElementsSelector.SliceSelector.deserializer);

        private final SelectorDeserializer deserializer;

        Kind(SelectorDeserializer deserializer)
        {
            this.deserializer = deserializer;
        }
    }

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

    public static class Serializer
    {
        public void serialize(Selector selector, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(selector.kind().ordinal());
            selector.serialize(out, version);
        }

        public Selector deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            Kind kind = Kind.values()[in.readUnsignedByte()];
            return kind.deserializer.deserialize(in, version, metadata);
        }

        public int serializedSize(Selector selector, int version)
        {
            return TypeSizes.sizeof((byte) selector.kind().ordinal()) + selector.serializedSize(version);
        }
    }

    /**
     * The {@code Selector} serializer.
     */
    public static final Serializer serializer = new Serializer();

    /**
     * The {@code Selector} kind.
     */
    private final Kind kind;

    /**
     * Returns the {@code Selector} kind.
     * @return the {@code Selector} kind
     */
    public final Kind kind()
    {
        return kind;
    }

    protected Selector(Kind kind)
    {
        this.kind = kind;
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

    /**
     * A selector is terminal if it doesn't require any input for it's output to be computed, i.e. if {@link #getOutput}
     * result doesn't depend of {@link #addInput}. This is typically the case of a constant value or functions on constant
     * values.
     */
    public boolean isTerminal()
    {
        return false;
    }

    protected abstract int serializedSize(int version);

    protected abstract void serialize(DataOutputPlus out, int version) throws IOException;

    protected static void writeType(DataOutputPlus out, AbstractType<?> type) throws IOException
    {
        out.writeUTF(type.asCQL3Type().toString());
    }

    protected static int sizeOf(AbstractType<?> type)
    {
        return TypeSizes.sizeof(type.asCQL3Type().toString());
    }
}
