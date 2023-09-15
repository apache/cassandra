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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * Static helper methods and classes for tuples.
 */
public final class Tuples
{
    private Tuples() {}

    public static ColumnSpecification componentSpecOf(ColumnSpecification column, int component)
    {
        return new ColumnSpecification(column.ksName,
                                       column.cfName,
                                       new ColumnIdentifier(String.format("%s[%d]", column.name, component), true),
                                       (getTupleType(column.type)).type(component));
    }

    /**
     * A raw, literal tuple.  When prepared, this will become a Tuples.Value or Tuples.DelayedValue, depending
     * on whether the tuple holds NonTerminals.
     */
    public static class Literal extends Term.Raw
    {
        private final List<Term.Raw> elements;

        public Literal(List<Term.Raw> elements)
        {
            this.elements = elements;
        }

        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            // The parser cannot differentiate between a tuple with one element and a term between parenthesis.
            // By consequence, we need to wait until we know the target type to determine which one it is.
            if (elements.size() == 1 && !checkIfTupleType(receiver.type))
                return elements.get(0).prepare(keyspace, receiver);

            TupleType tupleType = getTupleType(receiver.type);

            if (elements.size() != tupleType.size())
                throw invalidRequest("Expected %d elements in value for tuple %s, but got %d: %s",
                                     tupleType.size(), receiver.name, elements.size(), this);

            validateTupleAssignableTo(receiver, elements);

            List<Term> values = new ArrayList<>(elements.size());
            boolean allTerminal = true;
            for (int i = 0; i < elements.size(); i++)
            {
                Term value = elements.get(i).prepare(keyspace, componentSpecOf(receiver, i));
                if (value instanceof Term.NonTerminal)
                    allTerminal = false;

                values.add(value);
            }

            MultiElements.DelayedValue value = new MultiElements.DelayedValue(tupleType, values);
            return allTerminal ? value.bind(QueryOptions.DEFAULT) : value;
        }

        public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            // The parser cannot differentiate between a tuple with one element and a term between parenthesis.
            // By consequence, we need to wait until we know the target type to determine which one it is.
            if (elements.size() == 1 && !checkIfTupleType(receiver.type))
                return elements.get(0).testAssignment(keyspace, receiver);

            return testTupleAssignment(receiver, elements);
        }

        @Override
        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            List<AbstractType<?>> types = new ArrayList<>(elements.size());
            for (Term.Raw term : elements)
            {
                AbstractType<?> type = term.getExactTypeIfKnown(keyspace);
                if (type == null)
                    return null;
                types.add(type);
            }
            return new TupleType(types);
        }

        public String getText()
        {
            return tupleToString(elements, Term.Raw::getText);
        }
    }

    /**
     * Create a <code>String</code> representation of the tuple containing the specified elements.
     *
     * @param elements the tuple elements
     * @return a <code>String</code> representation of the tuple
     */
    public static String tupleToString(List<?> elements)
    {
        return tupleToString(elements, Object::toString);
    }

    /**
     * Create a <code>String</code> representation of the tuple from the specified items associated to
     * the tuples elements.
     *
     * @param items items associated to the tuple elements
     * @param mapper the mapper used to map the items to the <code>String</code> representation of the tuple elements
     * @return a <code>String</code> representation of the tuple
     */
    public static <T> String tupleToString(Iterable<T> items, java.util.function.Function<T, String> mapper)
    {
        return StreamSupport.stream(items.spliterator(), false)
                            .map(mapper)
                            .collect(Collectors.joining(", ", "(", ")"));
    }

    /**
     * Returns the exact TupleType from the items if it can be known.
     *
     * @param items the items mapped to the tuple elements
     * @param mapper the mapper used to retrieve the element types from the  items
     * @return the exact TupleType from the items if it can be known or <code>null</code>
     */
    public static <T> TupleType getExactTupleTypeIfKnown(List<T> items,
                                                         java.util.function.Function<T, AbstractType<?>> mapper)
    {
        List<AbstractType<?>> types = new ArrayList<>(items.size());
        for (T item : items)
        {
            AbstractType<?> type = mapper.apply(item);
            if (type == null)
                return null;
            types.add(type);
        }
        return new TupleType(types);
    }

    /**
     * Checks if the tuple with the specified elements can be assigned to the specified column.
     *
     * @param receiver the receiving column
     * @param elements the tuple elements
     * @throws InvalidRequestException if the tuple cannot be assigned to the specified column.
     */
    public static void validateTupleAssignableTo(ColumnSpecification receiver,
                                                 List<? extends AssignmentTestable> elements)
    {
        if (!checkIfTupleType(receiver.type))
            throw invalidRequest("Invalid tuple type literal for %s of type %s", receiver.name, receiver.type.asCQL3Type());

        TupleType tt = getTupleType(receiver.type);
        for (int i = 0; i < elements.size(); i++)
        {
            if (i >= tt.size())
            {
                throw invalidRequest("Invalid tuple literal for %s: too many elements. Type %s expects %d but got %d",
                                     receiver.name, tt.asCQL3Type(), tt.size(), elements.size());
            }

            AssignmentTestable value = elements.get(i);
            ColumnSpecification spec = componentSpecOf(receiver, i);
            if (!value.testAssignment(receiver.ksName, spec).isAssignable())
                throw invalidRequest("Invalid tuple literal for %s: component %d is not of type %s",
                                     receiver.name, i, spec.type.asCQL3Type());
        }
    }

    /**
     * Tests that the tuple with the specified elements can be assigned to the specified column.
     *
     * @param receiver the receiving column
     * @param elements the tuple elements
     */
    public static AssignmentTestable.TestResult testTupleAssignment(ColumnSpecification receiver,
                                                                    List<? extends AssignmentTestable> elements)
    {
        try
        {
            validateTupleAssignableTo(receiver, elements);
            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
        }
        catch (InvalidRequestException e)
        {
            return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
        }
    }

    public static boolean checkIfTupleType(AbstractType<?> tuple)
    {
        return (tuple instanceof TupleType) ||
               (tuple instanceof ReversedType && ((ReversedType<?>) tuple).baseType instanceof TupleType);

    }

    public static TupleType getTupleType(AbstractType<?> tuple)
    {
        return (tuple instanceof ReversedType ? ((TupleType) ((ReversedType<?>) tuple).baseType) : (TupleType)tuple);
    }
}
