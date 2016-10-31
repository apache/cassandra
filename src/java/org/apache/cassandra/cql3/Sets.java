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

import static org.apache.cassandra.cql3.Constants.UNSET_VALUE;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Static helper methods and classes for sets.
 */
public abstract class Sets
{
    private Sets() {}

    public static ColumnSpecification valueSpecOf(ColumnSpecification column)
    {
        return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("value(" + column.name + ")", true), ((SetType)column.type).getElementsType());
    }

    public static class Literal extends Term.Raw
    {
        private final List<Term.Raw> elements;

        public Literal(List<Term.Raw> elements)
        {
            this.elements = elements;
        }

        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            validateAssignableTo(keyspace, receiver);

            // We've parsed empty maps as a set literal to break the ambiguity so
            // handle that case now
            if (receiver.type instanceof MapType && elements.isEmpty())
                return new Maps.Value(Collections.<ByteBuffer, ByteBuffer>emptyMap());

            ColumnSpecification valueSpec = Sets.valueSpecOf(receiver);
            Set<Term> values = new HashSet<>(elements.size());
            boolean allTerminal = true;
            for (Term.Raw rt : elements)
            {
                Term t = rt.prepare(keyspace, valueSpec);

                if (t.containsBindMarker())
                    throw new InvalidRequestException(String.format("Invalid set literal for %s: bind variables are not supported inside collection literals", receiver.name));

                if (t instanceof Term.NonTerminal)
                    allTerminal = false;

                values.add(t);
            }
            DelayedValue value = new DelayedValue(((SetType)receiver.type).getElementsType(), values);
            return allTerminal ? value.bind(QueryOptions.DEFAULT) : value;
        }

        private void validateAssignableTo(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            if (!(receiver.type instanceof SetType))
            {
                // We've parsed empty maps as a set literal to break the ambiguity so
                // handle that case now
                if ((receiver.type instanceof MapType) && elements.isEmpty())
                    return;

                throw new InvalidRequestException(String.format("Invalid set literal for %s of type %s", receiver.name, receiver.type.asCQL3Type()));
            }

            ColumnSpecification valueSpec = Sets.valueSpecOf(receiver);
            for (Term.Raw rt : elements)
            {
                if (!rt.testAssignment(keyspace, valueSpec).isAssignable())
                    throw new InvalidRequestException(String.format("Invalid set literal for %s: value %s is not of type %s", receiver.name, rt, valueSpec.type.asCQL3Type()));
            }
        }

        public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            if (!(receiver.type instanceof SetType))
            {
                // We've parsed empty maps as a set literal to break the ambiguity so handle that case now
                if (receiver.type instanceof MapType && elements.isEmpty())
                    return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;

                return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
            }

            // If there is no elements, we can't say it's an exact match (an empty set if fundamentally polymorphic).
            if (elements.isEmpty())
                return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;

            ColumnSpecification valueSpec = Sets.valueSpecOf(receiver);
            return AssignmentTestable.TestResult.testAll(keyspace, valueSpec, elements);
        }

        @Override
        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            for (Term.Raw term : elements)
            {
                AbstractType<?> type = term.getExactTypeIfKnown(keyspace);
                if (type != null)
                    return SetType.getInstance(type, false);
            }
            return null;
        }

        public String getText()
        {
            return elements.stream().map(Term.Raw::getText).collect(Collectors.joining(", ", "{", "}"));
        }
    }

    public static class Value extends Term.Terminal
    {
        public final SortedSet<ByteBuffer> elements;

        public Value(SortedSet<ByteBuffer> elements)
        {
            this.elements = elements;
        }

        public static Value fromSerialized(ByteBuffer value, SetType type, ProtocolVersion version) throws InvalidRequestException
        {
            try
            {
                // Collections have this small hack that validate cannot be called on a serialized object,
                // but compose does the validation (so we're fine).
                Set<?> s = type.getSerializer().deserializeForNativeProtocol(value, version);
                SortedSet<ByteBuffer> elements = new TreeSet<>(type.getElementsType());
                for (Object element : s)
                    elements.add(type.getElementsType().decompose(element));
                return new Value(elements);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException(e.getMessage());
            }
        }

        public ByteBuffer get(ProtocolVersion protocolVersion)
        {
            return CollectionSerializer.pack(elements, elements.size(), protocolVersion);
        }

        public boolean equals(SetType st, Value v)
        {
            if (elements.size() != v.elements.size())
                return false;

            Iterator<ByteBuffer> thisIter = elements.iterator();
            Iterator<ByteBuffer> thatIter = v.elements.iterator();
            AbstractType elementsType = st.getElementsType();
            while (thisIter.hasNext())
                if (elementsType.compare(thisIter.next(), thatIter.next()) != 0)
                    return false;

            return true;
        }
    }

    // See Lists.DelayedValue
    public static class DelayedValue extends Term.NonTerminal
    {
        private final Comparator<ByteBuffer> comparator;
        private final Set<Term> elements;

        public DelayedValue(Comparator<ByteBuffer> comparator, Set<Term> elements)
        {
            this.comparator = comparator;
            this.elements = elements;
        }

        public boolean containsBindMarker()
        {
            // False since we don't support them in collection
            return false;
        }

        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
        }

        public Terminal bind(QueryOptions options) throws InvalidRequestException
        {
            SortedSet<ByteBuffer> buffers = new TreeSet<>(comparator);
            for (Term t : elements)
            {
                ByteBuffer bytes = t.bindAndGet(options);

                if (bytes == null)
                    throw new InvalidRequestException("null is not supported inside collections");
                if (bytes == ByteBufferUtil.UNSET_BYTE_BUFFER)
                    return UNSET_VALUE;

                buffers.add(bytes);
            }
            return new Value(buffers);
        }

        public void addFunctionsTo(List<Function> functions)
        {
            Terms.addFunctions(elements, functions);
        }
    }

    public static class Marker extends AbstractMarker
    {
        protected Marker(int bindIndex, ColumnSpecification receiver)
        {
            super(bindIndex, receiver);
            assert receiver.type instanceof SetType;
        }

        public Terminal bind(QueryOptions options) throws InvalidRequestException
        {
            ByteBuffer value = options.getValues().get(bindIndex);
            if (value == null)
                return null;
            if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
                return UNSET_VALUE;
            return Value.fromSerialized(value, (SetType)receiver.type, options.getProtocolVersion());
        }
    }

    public static class Setter extends Operation
    {
        public Setter(ColumnDefinition column, Term t)
        {
            super(column, t);
        }

        public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException
        {
            Term.Terminal value = t.bind(params.options);
            if (value == UNSET_VALUE)
                return;

            // delete + add
            if (column.type.isMultiCell())
                params.setComplexDeletionTimeForOverwrite(column);
            Adder.doAdd(value, column, params);
        }
    }

    public static class Adder extends Operation
    {
        public Adder(ColumnDefinition column, Term t)
        {
            super(column, t);
        }

        public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to add items to a frozen set";
            Term.Terminal value = t.bind(params.options);
            if (value != UNSET_VALUE)
                doAdd(value, column, params);
        }

        static void doAdd(Term.Terminal value, ColumnDefinition column, UpdateParameters params) throws InvalidRequestException
        {
            if (column.type.isMultiCell())
            {
                if (value == null)
                    return;

                for (ByteBuffer bb : ((Value) value).elements)
                {
                    if (bb == ByteBufferUtil.UNSET_BYTE_BUFFER)
                        continue;

                    params.addCell(column, CellPath.create(bb), ByteBufferUtil.EMPTY_BYTE_BUFFER);
                }
            }
            else
            {
                // for frozen sets, we're overwriting the whole cell
                if (value == null)
                    params.addTombstone(column);
                else
                    params.addCell(column, value.get(ProtocolVersion.CURRENT));
            }
        }
    }

    // Note that this is reused for Map subtraction too (we subtract a set from a map)
    public static class Discarder extends Operation
    {
        public Discarder(ColumnDefinition column, Term t)
        {
            super(column, t);
        }

        public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to remove items from a frozen set";

            Term.Terminal value = t.bind(params.options);
            if (value == null || value == UNSET_VALUE)
                return;

            // This can be either a set or a single element
            Set<ByteBuffer> toDiscard = value instanceof Sets.Value
                                      ? ((Sets.Value)value).elements
                                      : Collections.singleton(value.get(params.options.getProtocolVersion()));

            for (ByteBuffer bb : toDiscard)
                params.addTombstone(column, CellPath.create(bb));
        }
    }

    public static class ElementDiscarder extends Operation
    {
        public ElementDiscarder(ColumnDefinition column, Term k)
        {
            super(column, k);
        }

        public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to delete a single element in a frozen set";
            Term.Terminal elt = t.bind(params.options);
            if (elt == null)
                throw new InvalidRequestException("Invalid null set element");

            params.addTombstone(column, CellPath.create(elt.get(params.options.getProtocolVersion())));
        }
    }
}
