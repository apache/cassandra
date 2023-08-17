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
package org.apache.cassandra.cql3.terms;

import static org.apache.cassandra.cql3.terms.Constants.UNSET_VALUE;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Operation;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.MultiElementType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Static helper methods and classes for sets.
 */
public final class Sets
{
    private Sets() {}

    public static ColumnSpecification valueSpecOf(ColumnSpecification column)
    {
        return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("value(" + column.name + ')', true), elementsType(column.type));
    }

    private static AbstractType<?> elementsType(AbstractType<?> type)
    {
        return ((SetType<?>) type.unwrap()).getElementsType();
    }

    /**
     * Tests that the set with the specified elements can be assigned to the specified column.
     *
     * @param receiver the receiving column
     * @param elements the set elements
     */
    public static AssignmentTestable.TestResult testSetAssignment(ColumnSpecification receiver,
                                                                  List<? extends AssignmentTestable> elements)
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

        ColumnSpecification valueSpec = valueSpecOf(receiver);
        return AssignmentTestable.TestResult.testAll(receiver.ksName, valueSpec, elements);
    }

    /**
     * Create a <code>String</code> representation of the set containing the specified elements.
     *
     * @param elements the set elements
     * @return a <code>String</code> representation of the set
     */
    public static String setToString(List<?> elements)
    {
        return setToString(elements, Object::toString);
    }

    /**
     * Create a <code>String</code> representation of the set from the specified items associated to
     * the set elements.
     *
     * @param items items associated to the set elements
     * @param mapper the mapper used to map the items to the <code>String</code> representation of the set elements
     * @return a <code>String</code> representation of the set
     */
    public static <T> String setToString(Iterable<T> items, java.util.function.Function<T, String> mapper)
    {
        return StreamSupport.stream(items.spliterator(), false)
                            .map(mapper)
                            .collect(Collectors.joining(", ", "{", "}"));
    }

    /**
     * Returns the exact SetType from the items if it can be known.
     *
     * @param items the items mapped to the set elements
     * @param mapper the mapper used to retrieve the element types from the items
     * @return the exact SetType from the items if it can be known or <code>null</code>
     */
    public static <T> SetType<?> getExactSetTypeIfKnown(List<T> items,
                                                        java.util.function.Function<T, AbstractType<?>> mapper)
    {
        Optional<AbstractType<?>> type = items.stream().map(mapper).filter(Objects::nonNull).findFirst();
        return type.isPresent() ? SetType.getInstance(type.get(), false) : null;
    }

    public static <T> SetType<?> getPreferredCompatibleType(List<T> items,
                                                            java.util.function.Function<T, AbstractType<?>> mapper)
    {
        Set<AbstractType<?>> types = items.stream().map(mapper).filter(Objects::nonNull).collect(Collectors.toSet());
        AbstractType<?> type = AssignmentTestable.getCompatibleTypeIfKnown(types);
        return type == null ? null : SetType.getInstance(type, false);
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
                return new MultiElements.Value(((MapType<?, ?>) receiver.type), Collections.emptyList());

            ColumnSpecification valueSpec = Sets.valueSpecOf(receiver);
            List<Term> values = new ArrayList<>(elements.size());
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
            MultiElements.DelayedValue value = new MultiElements.DelayedValue((MultiElementType<?>) receiver.type.unwrap(), values);
            return allTerminal ? value.bind(QueryOptions.DEFAULT) : value;
        }

        private void validateAssignableTo(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            AbstractType<?> type = receiver.type.unwrap();

            if (!(type instanceof SetType))
            {
                // We've parsed empty maps as a set literal to break the ambiguity so
                // handle that case now
                if ((type instanceof MapType) && elements.isEmpty())
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
            return testSetAssignment(receiver, elements);
        }

        @Override
        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return getExactSetTypeIfKnown(elements, p -> p.getExactTypeIfKnown(keyspace));
        }

        @Override
        public AbstractType<?> getCompatibleTypeIfKnown(String keyspace)
        {
            return Sets.getPreferredCompatibleType(elements, p -> p.getCompatibleTypeIfKnown(keyspace));
        }

        public String getText()
        {
            return setToString(elements, Term.Raw::getText);
        }
    }

    public static class Setter extends Operation
    {
        public Setter(ColumnMetadata column, Term t)
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
        public Adder(ColumnMetadata column, Term t)
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

        static void doAdd(Term.Terminal value, ColumnMetadata column, UpdateParameters params) throws InvalidRequestException
        {
            SetType<?> type = (SetType<?>) column.type;

            if (value == null)
            {
                // for frozen sets, we're overwriting the whole cell
                if (!type.isMultiCell())
                    params.addTombstone(column);

                return;
            }

            List<ByteBuffer> elements = value.getElements();

            if (type.isMultiCell())
            {
                if (elements.isEmpty())
                    return;

                // Guardrails about collection size are only checked for the added elements without considering
                // already existent elements. This is done so to avoid read-before-write, having additional checks
                // during SSTable write.
                Guardrails.itemsPerCollection.guard(type.collectionSize(elements), column.name.toString(), false, params.clientState);

                int dataSize = 0;
                for (ByteBuffer bb : elements)
                {
                    if (bb == ByteBufferUtil.UNSET_BYTE_BUFFER)
                        continue;

                    Cell<?> cell = params.addCell(column, CellPath.create(bb), ByteBufferUtil.EMPTY_BYTE_BUFFER);
                    dataSize += cell.dataSize();
                }
                Guardrails.collectionSize.guard(dataSize, column.name.toString(), false, params.clientState);
            }
            else
            {
                Guardrails.itemsPerCollection.guard(type.collectionSize(elements), column.name.toString(), false, params.clientState);
                Cell<?> cell = params.addCell(column, value.get());
                Guardrails.collectionSize.guard(cell.dataSize(), column.name.toString(), false, params.clientState);
            }
        }
    }

    // Note that this is reused for Map subtraction too (we subtract a set from a map)
    public static class Discarder extends Operation
    {
        public Discarder(ColumnMetadata column, Term t)
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
            List<ByteBuffer> toDiscard = value.getElements();

            for (ByteBuffer bb : toDiscard)
                params.addTombstone(column, CellPath.create(bb));
        }
    }

    public static class ElementDiscarder extends Operation
    {
        public ElementDiscarder(ColumnMetadata column, Term k)
        {
            super(column, k);
        }

        public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to delete a single element in a frozen set";
            Term.Terminal elt = t.bind(params.options);
            if (elt == null)
                throw new InvalidRequestException("Invalid null set element");

            params.addTombstone(column, CellPath.create(elt.get()));
        }
    }
}
