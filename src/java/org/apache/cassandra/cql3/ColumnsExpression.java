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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkContainsNoDuplicates;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkContainsOnly;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * An expression including one or several columns.
 *
 * <p>This class can be modified to add support for more column expressions like UDT fields, List elements,
 * functions on columns... </p>
 */
public final class ColumnsExpression
{
    /**
     * Represent the expression kind
     */
    public enum Kind
    {
        /**
         * Single column expression (e.g. {@code columnA})
         */
        SINGLE_COLUMN
        {
            @Override
            void validateColumns(TableMetadata table, List<ColumnMetadata> columns)
            {
            }

            @Override
            AbstractType<?> type(TableMetadata table, List<ColumnMetadata> columns, ElementExpression element)
            {
                return columns.get(0).type;
            }

            @Override
            String toCQLString(List<String> columns, String element)
            {
                return columns.get(0);
            }

            @Override
            public String toString()
            {
                return "single column";
            }
        },
        /**
         * Multi-column expression (e.g. {@code (columnA, columnB)})
         */
        MULTI_COLUMN
        {
            @Override
            protected void validateColumns(TableMetadata table, List<ColumnMetadata> columns)
            {
                int previousPosition = -1;
                for (int i = 0, m = columns.size(); i < m; i++)
                {
                    ColumnMetadata column = columns.get(i);
                    checkTrue(column.isClusteringColumn(), "Multi-column relations can only be applied to clustering columns but was applied to: %s", column.name);
                    checkFalse(columns.lastIndexOf(column) != i, "Column \"%s\" appeared twice in a relation: %s", column.name, this);

                    // check that no clustering columns were skipped
                    checkFalse(previousPosition != -1 && column.position() != previousPosition + 1,
                               "Clustering columns must appear in the PRIMARY KEY order in multi-column relations: %s", toCQLString(columns, null));

                    previousPosition = column.position();
                }
            }

            @Override
            AbstractType<?> type(TableMetadata table, List<ColumnMetadata> columns, ElementExpression element)
            {
                return new TupleType(ColumnMetadata.typesOf(columns));
            }

            @Override
            String toCQLString(List<String> columns, String element)
            {
                StringBuilder builder = new StringBuilder().append('(');
                Joiner.on(", ").appendTo(builder, columns);
                return builder.append(')').toString();
            }

            @Override
            public String toString()
            {
                return "multi-column";
            }
        },
        /**
         * Token expression (e.g. {@code token(columnA, columnB)})
         */
        TOKEN
        {
            @Override
            protected void validateColumns(TableMetadata table, List<ColumnMetadata> columns)
            {
                if (columns.equals(table.partitionKeyColumns()))
                    return;

                // If the columns do not match the partition key columns, let's try to narrow down the problem
                checkTrue(new HashSet<>(columns).containsAll(table.partitionKeyColumns()),
                        "The token() function must be applied to all partition key components or none of them");

                checkContainsNoDuplicates(columns, "The token() function contains duplicate partition key components");

                checkContainsOnly(columns, table.partitionKeyColumns(), "The token() function must contains only partition key components");

                throw invalidRequest("The token function arguments must be in the partition key order: %s",
                                     Joiner.on(", ").join(ColumnMetadata.toIdentifiers(table.partitionKeyColumns())));
            }

            @Override
            AbstractType<?> type(TableMetadata table, List<ColumnMetadata> columns, ElementExpression element)
            {
                return table.partitioner.getTokenValidator();
            }

            @Override
            String toCQLString(List<String> columns, String element)
            {
                StringBuilder builder = new StringBuilder();
                builder.append("token(");
                Joiner.on(", ").appendTo(builder, columns);
                return builder.append(')').toString();
            }

            @Override
            public String toString()
            {
                return "token";
            }
        },
        /**
         * Element expression (e.g. {@code columnA[?]}). This is used for collection elements and UDT fields. For more
         * information see {@link ElementExpression}.
         */
        ELEMENT
        {
            @Override
            void validateColumns(TableMetadata table, List<ColumnMetadata> columns)
            {
            }

            @Override
            AbstractType<?> type(TableMetadata table, List<ColumnMetadata> columns, ElementExpression element)
            {
                return element.type();
            }

            @Override
            String toCQLString(List<String> columns, String element)
            {
                return columns.get(0) + element;
            }
        };

        /**
         * Validates that the specified columns are valid for this kind of expression.
         * @param table the table metadata
         * @param columns the expression column
         */
        abstract void validateColumns(TableMetadata table, List<ColumnMetadata> columns);

        /**
         * Returns the expression type.
         *
         * @param table             the table metadata
         * @param columns           the expression columns
         * @param element           the element expression in case of ELEMENT columns expression
         * @return the expression type
         */
        abstract AbstractType<?> type(TableMetadata table, List<ColumnMetadata> columns, ElementExpression element);

        /**
         * Returns CQL representation of the expression.
         *
         * @param columns           the expression's columns
         * @param element           the element in case of ELEMENT columns expression
         * @return the CQL representation of the expression.
         */
        abstract String toCQLString(List<String> columns, String element);

        String toCQLString(List<ColumnMetadata> columns, ElementExpression elementExpression)
        {
            return toCQLString(ColumnMetadata.cqlNames(columns), elementExpression != null ? elementExpression.toCQLString() : "");
        }

        String toCQLString(List<ColumnIdentifier> identifiers, ElementExpression.Raw rawElement)
        {
            String element = rawElement == null ? "" : rawElement.toCQLString();
            return toCQLString(ColumnIdentifier.toCqlStrings(identifiers), element);
        }
    }

    private final Kind kind;

    /**
     * The type represented by this expression.
     * <li>
     *  <ul>for a single column the type of the expression will be the one of the column</ul>
     *  <ul>for a multi-column expression the type will be a tuple type</ul>
     *  <ul>for a token expression the type will be the token type</ul>
     *  <ul>for an element expression, the type will be one of the elements of interest(UDT field or collection element)</ul>
     * </li>
     */
    private final AbstractType<?> type;

    /**
     * The expression columns
     */
    private final List<ColumnMetadata> columns;

    /**
     * The element if this is an ELEMENT expression, {@code null} otherwise.
     * Like UDT field or collection element.
     */
    private final ElementExpression element; //Only relevant for ELEMENT kind

    ColumnsExpression(Kind kind, AbstractType<?> type, List<ColumnMetadata> columns,  ElementExpression element)
    {
        assert kind != Kind.ELEMENT || element != null: "Element expression must have an element";
        this.kind = kind;
        this.type = type;
        this.columns = columns;
        this.element = element; // This could be null for kinds that don't use it
    }

    /**
     * Returns the expression type.
     * @return the expression type.
     */
    public AbstractType<?> type()
    {
        return type;
    }

    /**
     * Creates an expression for a single column (e.g. {@code columnA}).
     * @param column the column
     * @return an expression for a single column.
     */
    public static ColumnsExpression singleColumn(ColumnMetadata column)
    {
        return new ColumnsExpression(Kind.SINGLE_COLUMN, column.type, ImmutableList.of(column), null);
    }

    /**
     * Creates an expression for multi-columns (e.g. {@code (columnA, columnB)}).
     * @param columns the columns
     * @return an expression for multi-columns.
     */
    @VisibleForTesting
    public static ColumnsExpression multiColumns(List<ColumnMetadata> columns)
    {
        AbstractType<?> type = new TupleType(ColumnMetadata.types(columns));
        return new ColumnsExpression(Kind.MULTI_COLUMN, type, ImmutableList.copyOf(columns),null);
    }

    /**
     * Returns the first column metadata.
     * @return the first column metadata.
     */
    public ColumnMetadata firstColumn()
    {
        return columns().get(0);
    }

    /**
     * Returns the last column metadata.
     * @return the last column metadata.
     */
    public ColumnMetadata lastColumn()
    {
        return columns.get(columns.size() - 1);
    }

    /**
     * Returns the columns metadata in position order.
     * @return the columns metadata in position order.
     */
    public List<ColumnMetadata> columns()
    {
        return columns;
    }

    /**
     * Returns the column kind (partition key, clustering, static or regular).
     * @return the column kind.
     */
    public ColumnMetadata.Kind columnsKind()
    {
        // All columns must have the same kind.
        return firstColumn().kind;
    }

    /**
     * Returns the expression kind.
     * @return the expression kind.
     */
    public Kind kind()
    {
        return kind;
    }

    /**
     * Returns the key, index or fieldname specifying the selected element.
     * @return the key, index or fieldname specifying the selected element.
     */
    public ByteBuffer element(QueryOptions options)
    {
        return element.bindAndGet(options);
    }

    /**
     * Checks if this instance is a collection element expression.
     * @return {@code true} if this instance is a collection element expression, {@code false} otherwise.
     */
    public boolean isCollectionElementExpression()
    {
        return kind == Kind.ELEMENT && element != null && element.kind() == ElementExpression.Kind.COLLECTION_ELEMENT;
    }

    /**
     * Checks if this instance is a map element expression.
     * @return {@code true} if this instance is a map element expression, {@code false} otherwise.
     */
    public boolean isMapElementExpression()
    {
        return kind == Kind.ELEMENT && element != null && element.kind() == ElementExpression.Kind.COLLECTION_ELEMENT && firstColumn().type.unwrap() instanceof MapType;
    }

    /**
     * Collects the column specifications for the bind variables.
     * This is obviously a no-op if the expression is not a {@code ELEMENET_EXPRESSION} expression.
     *
     * @param boundNames the variables specification where to collect the
     * bind variables of the map key/collection element in.
     */
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        if (element != null)
            element.collectMarkerSpecification(boundNames);
    }

    /**
     * Checks if this instance is a column level expression (single or multi-column expression).
     * @return {@code true} if this instance is a column level expression, {@code false} otherwise.
     */
    public boolean isColumnLevelExpression()
    {
        return kind == Kind.SINGLE_COLUMN || kind == Kind.MULTI_COLUMN;
    }

    /**
     * Adds all functions (native and user-defined) used by any component of the restriction
     * to the specified list.
     * @param functions the list to add to
     */
    public void addFunctionsTo(List<Function> functions)
    {
        if (element != null)
            element.addFunctionsTo(functions);
    }

    /**
     * Returns CQL representation of this expression.
     * @return the CQL representation of this expression.
     */
    public String toCQLString()
    {
        return kind.toCQLString(columns, element);
    }

    @Override
    public String toString()
    {
        String prefix = kind == Kind.SINGLE_COLUMN ? "column "
                                                   : kind == Kind.MULTI_COLUMN ? "tuple " : "";

        return prefix + toCQLString();
    }

    /**
     * Returns the column specification corresponding to this expression.
     * @return the column specification corresponding to this expression.
     */
    public ColumnSpecification columnSpecification()
    {
        ColumnMetadata column = firstColumn();
        return kind == Kind.SINGLE_COLUMN ? column
                                          : new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier(toCQLString(), true), type) ;
    }

    /**
     * The parsed version of the {@link ColumnsExpression} as outputed by the CQL parser.
     * {@code Raw.prepare} will be called upon schema binding to create the {@link ColumnsExpression}.
     */
    public static final class Raw
    {
        private final Kind kind;

        /**
         * The columns identifiers
         */
        private final List<ColumnIdentifier> identifiers;

        private final ElementExpression.Raw rawElement;

        private Raw(Kind kind, List<ColumnIdentifier> identifiers, ElementExpression.Raw rawElement)
        {
            this.kind = kind;
            this.identifiers = identifiers;
            this.rawElement = rawElement;
        }

        /**
         * Returns the expression kind.
         * @return the expression kind.
         */
        public Kind kind()
        {
            return kind;
        }

        /**
         * Creates a raw expression for a single column (e.g. {@code columnA}).
         * @param identifier the column identifier
         * @return a raw expression for a single column.
         */
        public static Raw singleColumn(ColumnIdentifier identifier)
        {
            return new Raw(Kind.SINGLE_COLUMN, ImmutableList.of(identifier), null);
        }

        /**
         * Creates a raw expression for multi-column (e.g. {@code (columnA, columnB)}).
         *
         * @param identifiers the columns identifier
         * @return a raw expression for multi-column.
         */
        public static Raw multiColumn(List<ColumnIdentifier> identifiers)
        {
            return new Raw(Kind.MULTI_COLUMN, identifiers, null);
        }

        /**
         * Creates a raw expression for token restrictions (e.g. {@code token(columnA, columnB)}).
         *
         * @param identifiers the columns identifiers
         * @return a raw token expression.
         */
        public static Raw token(List<ColumnIdentifier> identifiers)
        {
            return new Raw(Kind.TOKEN, identifiers, null);
        }

        /**
         * Creates a raw expression for collection element conditions (e.g. {@code columnA[?]}).
         *
         * @param identifier the collection element column identifier
         * @param rawCollectionElement the raw collection element
         * @return a raw element expression.
         */
        public static Raw collectionElement(ColumnIdentifier identifier, Term.Raw rawCollectionElement)
        {
            return new Raw(Kind.ELEMENT, ImmutableList.of(identifier), new ElementExpression.Raw(rawCollectionElement, null, ElementExpression.Kind.COLLECTION_ELEMENT));
        }

        /**
         * Creates a raw expression for a UDT field conditions.
         *
         * @param identifier the UDT field column identifier
         * @param rawUdtField the raw UDT field
         * @return a raw element expression.
         */
        public static Raw udtField(ColumnIdentifier identifier, FieldIdentifier rawUdtField)
        {
            return new Raw(Kind.ELEMENT, ImmutableList.of(identifier), new ElementExpression.Raw(null, rawUdtField, ElementExpression.Kind.UDT_FIELD));
        }

        /**
         * Renames an identifier in this expression, if applicable.
         *
         * @param from the old identifier
         * @param to the new identifier
         * @return this object, if the old identifier is not in the set of identifiers that this expression covers; otherwise
         *         a new Raw expression with "from" replaced by "to" is returned.
         */
        public Raw renameIdentifier(ColumnIdentifier from, ColumnIdentifier to)
        {
            if (!identifiers.contains(from))
                return this;

            List<ColumnIdentifier> newIdentifiers = identifiers.stream()
                                                               .map(e -> e.equals(from) ? to : e)
                                                               .collect(Collectors.toList());
            return new Raw(kind, newIdentifiers, rawElement);
        }

        /**
         * Checks if this raw expression contains bind markers.
         * @return {@code true} if this raw expression contains bind markers, {@code false} otherwise.
         */
        public boolean containsBindMarkers()
        {
            return rawElement != null && rawElement.containsBindMarkers();
        }

        /**
         * Bind this {@link Raw} instance to the schema and return the resulting {@link ColumnsExpression}.
         *
         * @param table the table schema
         * @return the {@link ColumnsExpression} resulting from the schema binding
         */
        public ColumnsExpression prepare(TableMetadata table)
        {
            List<ColumnMetadata> columns = getColumnsMetadata(table, identifiers);
            kind.validateColumns(table, columns);

            ElementExpression elementExpression = null;
            if (kind == Kind.ELEMENT)
                elementExpression = rawElement.prepare(columns.get(0));

            AbstractType<?> type = kind.type(table, columns, elementExpression);

            return new ColumnsExpression(kind, type, columns, elementExpression);
        }

        /**
         * Returns the columns corresponding to the identifiers.
         *
         * @param table the table metadata
         * @param identifiers the columns identifiers
         * @return the definition of the columns to which apply the token restriction.
         * @throws InvalidRequestException if the entity cannot be resolved
         */
        private static List<ColumnMetadata> getColumnsMetadata(TableMetadata table, List<ColumnIdentifier> identifiers)
        {
            List<ColumnMetadata> columns = new ArrayList<>(identifiers.size());
            for (ColumnIdentifier id : identifiers)
                columns.add(table.getExistingColumn(id));
            return columns;
        }

        /**
         * Returns the columns' identifiers.
         *
         * @return identifiers.
         */
        public List<ColumnIdentifier> identifiers()
        {
            return identifiers;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(kind, identifiers, rawElement);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof Raw))
                return false;

            Raw r = (Raw) o;
            return kind == r.kind && Objects.equals(identifiers, r.identifiers) && Objects.equals(rawElement, r.rawElement);
        }

        /**
         * Returns CQL representation of this raw expression.
         *
         * @return the CQL representation of this raw expression.
         */
        public String toCQLString()
        {
            return kind.toCQLString(identifiers, rawElement);
        }
    }
}
