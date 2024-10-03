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
import java.util.List;
import java.util.Objects;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.terms.Constants;
import org.apache.cassandra.cql3.terms.Lists;
import org.apache.cassandra.cql3.terms.Maps;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.ColumnMetadata;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * An element expression representation in case of element column expression.
 *
 * <p>This class can be modified to add support for more element expressions like range of elements, for example. </p>
 */

public final class ElementExpression
{
    /**
     * Represent the expression kind
     */
    public enum Kind
    {
        /**
         * UDT field expression (e.g. {@code columnA.fieldA})
         */
        UDT_FIELD
        {
            @Override
            public String toCQLString(String element)
            {
                return '.' + element;
            }

            @Override
            public String toString()
            {
                return "UDT field";
            }
        },
        /**
         * Collection element expression (e.g. {@code columnA[?]})
         */
        COLLECTION_ELEMENT
        {
            @Override
            public String toCQLString(String element)
            {
                return '[' + element + ']';
            }

            @Override
            public String toString()
            {
                return "collection element";
            }
        };

        public abstract String toCQLString(String element);
    }

    /**
     * The kind of element expression - udt field or collection element.
     */
    private final ElementExpression.Kind kind;

    /**
     * The type of the key, index or udt field.
     */
    private final AbstractType<?> keyOrIndexType;

    /**
     * The term representing the key, index or udt field.
     */
    private final Term keyOrIndex;

    /**
     * The element type.
     */
    private final AbstractType<?> type;

    private ElementExpression(ElementExpression.Kind kind, AbstractType<?> type, AbstractType<?> keyOrIndexType, Term keyOrIndex)
    {
        this.kind = kind;
        this.type = type;
        this.keyOrIndexType = keyOrIndexType;
        this.keyOrIndex = keyOrIndex;
    }

    /**
     * Returns the expression kind.
     * @return the expression kind.
     */
    public ElementExpression.Kind kind()
    {
        return kind;
    }

    /**
     * Returns the element type.
     * @return the element type.
     */
    public AbstractType<?> type()
    {
        return type;
    }

    /**
     * Collects the column specifications for the bind variables.
     *
     * @param boundNames the variables specification where to collect the
     * bind variables of the map key/collection element in.
     */
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        keyOrIndex.collectMarkerSpecification(boundNames);
    }

    /**
     * Adds all functions (native and user-defined) used by any component of the restriction
     * to the specified list.
     * @param functions the list to add to
     */
    public void addFunctionsTo(List<Function> functions)
    {
        keyOrIndex.addFunctionsTo(functions);
    }

    /**
     * Returns the ByteBuffer representation of the key or index.
     *
     * @param options the query options
     * @return the ByteBuffer representation of the key or index.
     */
    public ByteBuffer bindAndGet(QueryOptions options)
    {
        return keyOrIndex.bindAndGet(options);
    }

    public String toCQLString()
    {
        // If a Term is not terminal it can be a row marker or a function.
        // We ignore the fact that it could be a function for now.

        String value = keyOrIndex.isTerminal() ? keyOrIndexType.asCQL3Type().toCQLLiteral(((Term.Terminal) keyOrIndex).get()) : "?";
        return kind.toCQLString(value);
    }

    @Override
    public String toString()
    {
        return this.kind.toString();
    }

    public static final class Raw
    {
        private final Kind kind;
        private final Term.Raw rawCollectionElement;

        private final FieldIdentifier udtField;

        Raw(Term.Raw collectionElement, FieldIdentifier udtField, Kind kind)
        {
            this.rawCollectionElement = collectionElement;
            this.udtField = udtField;
            this.kind = kind;
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
         * Bind this {@link Raw} instance to the schema and return the resulting {@link ElementExpression}.
         *
         * @param column     the column
         * @return the {@link ElementExpression} resulting from the schema binding
         */
        ElementExpression prepare(ColumnMetadata column)
        {
            if (kind == Kind.COLLECTION_ELEMENT)
            {
                AbstractType<?> baseType = column.type.unwrap();

                if (!(baseType.isCollection()))
                    throw invalidRequest("Invalid element access syntax for non-collection column %s", column.name);

                Term term = prepareCollectionElement(column);
                CollectionType<?> collectionType = (CollectionType<?>) baseType;
                AbstractType<?> elementType = collectionType.valueComparator();
                AbstractType<?> keyOrIndexType = collectionType.isMap() ? ((MapType<?, ?>) collectionType).getKeysType() : Int32Type.instance;
                return new ElementExpression(kind, elementType, keyOrIndexType, term);
            }

            UserType userType = (UserType) column.type;
            int fieldPosition = userType.fieldPosition(udtField);
            if (fieldPosition == -1)
                throw invalidRequest("Unknown field %s for column %s", udtField, column.name);

            return new ElementExpression(kind,
                                         userType.type(fieldPosition),
                                         UTF8Type.instance,
                                         new Constants.Value(udtField.bytes));
        }

        private Term prepareCollectionElement(ColumnMetadata receiver)
        {
            ColumnSpecification elementSpec;
            switch ((((CollectionType<?>) receiver.type.unwrap()).kind))
            {
                case LIST:
                    elementSpec = Lists.indexSpecOf(receiver);
                    break;
                case MAP:
                    elementSpec = Maps.keySpecOf(receiver);
                    break;
                case SET:
                    throw invalidRequest("Invalid element access syntax for set column %s", receiver.name);
                default:
                    throw new AssertionError();
            }

            return rawCollectionElement.prepare(receiver.ksName, elementSpec);
        }


        /**
         * Checks if this raw expression contains bind markers.
         * @return {@code true} if this raw expression contains bind markers, {@code false} otherwise.
         */
        public boolean containsBindMarkers()
        {
            return rawCollectionElement != null && rawCollectionElement.containsBindMarker();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(kind, rawCollectionElement, udtField);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof ElementExpression.Raw))
                return false;

            ElementExpression.Raw r = (ElementExpression.Raw) o;
            return kind == r.kind && Objects.equals(rawCollectionElement, r.rawCollectionElement) && Objects.equals(udtField, r.udtField);
        }

        public String toCQLString()
        {
            String element = rawCollectionElement == null ? udtField.toString() : rawCollectionElement.getText();
            return kind.toCQLString(element);
        }

        @Override
        public String toString()
        {
            return this.kind.toString();
        }
    }
}
