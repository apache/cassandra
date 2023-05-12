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

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public class Vectors
{
    private static AbstractType<?> unwrap(AbstractType<?> type)
    {
        return type.isReversed() ? unwrap(((ReversedType<?>) type).baseType) : type;
    }

    private static AbstractType<?> elementsType(AbstractType<?> type)
    {
        return ((VectorType<?>) unwrap(type)).getElementsType();
    }

    public static ColumnSpecification valueSpecOf(ColumnSpecification column)
    {
        return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("value(" + column.name + ")", true), elementsType(column.type));
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
            if (!(receiver.type instanceof VectorType))
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
            if (!(receiver.type instanceof VectorType))
                throw new InvalidRequestException(String.format("Invalid vector literal for %s of type %s", receiver.name, receiver.type.asCQL3Type()));
            VectorType<?> type = (VectorType<?>) receiver.type;
            if (elements.size() != type.dimension)
                throw new InvalidRequestException(String.format("Invalid vector literal for %s of type %s; expected %d elements, but given %d", receiver.name, receiver.type.asCQL3Type(), type.dimension, elements.size()));

            ColumnSpecification valueSpec = valueSpecOf(receiver);
            for (Term.Raw rt : elements)
            {
                if (!rt.testAssignment(keyspace, valueSpec).isAssignable())
                    throw new InvalidRequestException(String.format("Invalid vector literal for %s: value %s is not of type %s", receiver.name, rt, valueSpec.type.asCQL3Type()));
            }

            List<Term> values = new ArrayList<>(elements.size());
            boolean allTerminal = true;
            for (Term.Raw rt : elements)
            {
                Term t = rt.prepare(keyspace, valueSpec);

                if (t.containsBindMarker())
                    throw new InvalidRequestException(String.format("Invalid vector literal for %s: bind variables are not supported inside vector literals", receiver.name));

                if (t instanceof Term.NonTerminal)
                    allTerminal = false;

                values.add(t);
            }
            DelayedValue value = new DelayedValue(type, values);
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
            // not enough information to know dimension
            return null;
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

        public static <T> Value fromSerialized(ByteBuffer value, VectorType<T> type) throws InvalidRequestException
        {
            try
            {
                return new Value(type, type.split(value));
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException(e.getMessage());
            }
        }

        public ByteBuffer get(ProtocolVersion version)
        {
            return type.decomposeRaw(elements);
        }

//        public boolean equals(VectorType<?> vt, Value v)
//        {
//            if (elements.size() != v.elements.size())
//                return false;
//
//            for (int i = 0; i < elements.size(); i++)
//                if (vt.getElementsType().compare(elements.get(i), v.elements.get(i)) != 0)
//                    return false;
//
//            return true;
//        }

        public List<ByteBuffer> getElements()
        {
            return elements;
        }
    }

    /**
     * Basically similar to a Value, but with some non-pure function (that need
     * to be evaluated at execution time) in it.
     *
     * Note: this would also work for a list with bind markers, but we don't support
     * that because 1) it's not excessively useful and 2) we wouldn't have a good
     * column name to return in the ColumnSpecification for those markers (not a
     * blocker per-se but we don't bother due to 1)).
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
            // False since we don't support them in vector
            return false;
        }

        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
        }

        public Terminal bind(QueryOptions options) throws InvalidRequestException
        {
            List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(elements.size());
            for (Term t : elements)
            {
                ByteBuffer bytes = t.bindAndGet(options);

                if (bytes == null || bytes == ByteBufferUtil.UNSET_BYTE_BUFFER || ByteBufferAccessor.instance.isEmpty(bytes))
                    throw new InvalidRequestException("null is not supported inside vectors");

                buffers.add(bytes);
            }
            return new Value(type, buffers);
        }

        public void addFunctionsTo(List<Function> functions)
        {
            Terms.addFunctions(elements, functions);
        }
    }
}
