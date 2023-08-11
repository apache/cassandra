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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public class Vectors
{
    private Vectors() {}

    private static AbstractType<?> elementsType(AbstractType<?> type)
    {
        return ((VectorType<?>) type.unwrap()).getElementsType();
    }

    private static ColumnSpecification valueSpecOf(ColumnSpecification column)
    {
        return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("value(" + column.name + ')', true), elementsType(column.type));
    }

    /**
     * Tests that the vector with the specified elements can be assigned to the specified column.
     *
     * @param receiver the receiving column
     * @param elements the vector elements
     */
    public static AssignmentTestable.TestResult testVectorAssignment(ColumnSpecification receiver,
                                                                     List<? extends AssignmentTestable> elements)
    {
        if (!receiver.type.isVector())
            return AssignmentTestable.TestResult.NOT_ASSIGNABLE;

        // If there is no elements, we can't say it's an exact match (an empty vector if fundamentally polymorphic).
        if (elements.isEmpty())
            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;

        ColumnSpecification valueSpec = valueSpecOf(receiver);
        return AssignmentTestable.TestResult.testAll(receiver.ksName, valueSpec, elements);
    }

    /**
     * Returns the exact VectorType from the items if it can be known.
     *
     * @param items the items mapped to the vector elements
     * @param mapper the mapper used to retrieve the element types from the items
     * @return the exact VectorType from the items if it can be known or <code>null</code>
     */
    public static <T> VectorType<?> getExactVectorTypeIfKnown(List<T> items,
                                                              java.util.function.Function<T, AbstractType<?>> mapper)
    {
        // TODO - this doesn't feel right... if you are dealing with a literal then the value is `null`, so we will ignore
        // if there are multiple times, we randomly select the first?  This logic matches Lists.getExactListTypeIfKnown but feels flawed
        Optional<AbstractType<?>> type = items.stream().map(mapper).filter(Objects::nonNull).findFirst();
        return type.isPresent() ? VectorType.getInstance(type.get(), items.size()) : null;
    }

    public static <T> VectorType<?> getPreferredCompatibleType(List<T> items,
                                                               java.util.function.Function<T, AbstractType<?>> mapper)
    {
        Set<AbstractType<?>> types = items.stream().map(mapper).filter(Objects::nonNull).collect(Collectors.toSet());
        AbstractType<?> type = AssignmentTestable.getCompatibleTypeIfKnown(types);
        return type == null ? null : VectorType.getInstance(type, items.size());
    }

    public static class Literal extends Term.Raw
    {
        private final List<Term.Raw> elements;

        public Literal(List<Term.Raw> elements)
        {
            this.elements = elements;
        }

        @Override
        public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            if (!receiver.type.isVector())
                return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
            VectorType<?> type = (VectorType<?>) receiver.type;
            if (elements.size() != type.dimension)
                return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
            ColumnSpecification valueSpec = valueSpecOf(receiver);
            return AssignmentTestable.TestResult.testAll(receiver.ksName, valueSpec, elements);
        }

        @Override
        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            if (!receiver.type.isVector())
                throw new InvalidRequestException(String.format("Invalid vector literal for %s of type %s", receiver.name, receiver.type.asCQL3Type()));
            VectorType<?> type = (VectorType<?>) receiver.type;
            if (elements.size() != type.dimension)
                throw new InvalidRequestException(String.format("Invalid vector literal for %s of type %s; expected %d elements, but given %d", receiver.name, receiver.type.asCQL3Type(), type.dimension, elements.size()));

            ColumnSpecification valueSpec = valueSpecOf(receiver);
            List<Term> values = new ArrayList<>(elements.size());
            boolean allTerminal = true;
            for (Term.Raw rt : elements)
            {
                if (!rt.testAssignment(keyspace, valueSpec).isAssignable())
                    throw new InvalidRequestException(String.format("Invalid vector literal for %s: value %s is not of type %s", receiver.name, rt, valueSpec.type.asCQL3Type()));

                Term t = rt.prepare(keyspace, valueSpec);

                if (t instanceof Term.NonTerminal)
                    allTerminal = false;

                values.add(t);
            }
            DelayedValue<?> value = new DelayedValue<>(type, values);
            return allTerminal ? value.bind(QueryOptions.DEFAULT) : value;
        }

        @Override
        public String getText()
        {
            return Lists.listToString(elements, Term.Raw::getText);
        }

        @Override
        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return getExactVectorTypeIfKnown(elements, e -> e.getExactTypeIfKnown(keyspace));
        }

        @Override
        public AbstractType<?> getCompatibleTypeIfKnown(String keyspace)
        {
            return getPreferredCompatibleType(elements, e -> e.getCompatibleTypeIfKnown(keyspace));
        }
    }

    public static class Value<T> extends Term.MultiItemTerminal
    {
        public final VectorType<T> type;
        public final List<ByteBuffer> elements;

        public Value(VectorType<T> type, List<ByteBuffer> elements)
        {
            this.type = type;
            this.elements = elements;
        }

        public ByteBuffer get(ProtocolVersion version)
        {
            return type.decomposeRaw(elements);
        }

        public List<ByteBuffer> getElements()
        {
            return elements;
        }
    }

    /**
     * Basically similar to a Value, but with some non-pure function (that need
     * to be evaluated at execution time) in it.
     */
    public static class DelayedValue<T> extends Term.NonTerminal
    {
        private final VectorType<T> type;
        private final List<Term> elements;

        public DelayedValue(VectorType<T> type, List<Term> elements)
        {
            this.type = type;
            this.elements = elements;
        }

        public boolean containsBindMarker()
        {
            return elements.stream().anyMatch(Term::containsBindMarker);
        }

        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
            elements.forEach(t -> t.collectMarkerSpecification(boundNames));
        }

        public Terminal bind(QueryOptions options) throws InvalidRequestException
        {
            List<ByteBuffer> buffers = new ArrayList<>(elements.size());
            for (Term t : elements)
            {
                ByteBuffer bytes = t.bindAndGet(options);

                if (bytes == null || bytes == ByteBufferUtil.UNSET_BYTE_BUFFER || type.elementType.isNull(bytes))
                    throw new InvalidRequestException("null is not supported inside vectors");

                buffers.add(bytes);
            }
            return new Value<>(type, buffers);
        }

        public void addFunctionsTo(List<Function> functions)
        {
            Terms.addFunctions(elements, functions);
        }
    }
}
