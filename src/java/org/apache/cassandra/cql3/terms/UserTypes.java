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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.Operation;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.terms.Constants.UNSET_VALUE;

/**
 * Static helper methods and classes for user types.
 */
public final class UserTypes
{
    private UserTypes() {}

    public static ColumnSpecification fieldSpecOf(ColumnSpecification column, int field)
    {
        UserType ut = (UserType)column.type.unwrap();
        return new ColumnSpecification(column.ksName,
                                       column.cfName,
                                       new ColumnIdentifier(column.name + "." + ut.fieldName(field), true),
                                       ut.fieldType(field));
    }

    public static <T extends AssignmentTestable> void validateUserTypeAssignableTo(ColumnSpecification receiver,
                                                                                   Map<FieldIdentifier, T> entries)
    {
        if (!receiver.type.isUDT())
            throw new InvalidRequestException(String.format("Invalid user type literal for %s of type %s", receiver, receiver.type.asCQL3Type()));

        UserType ut = (UserType) receiver.type;
        for (int i = 0; i < ut.size(); i++)
        {
            FieldIdentifier field = ut.fieldName(i);
            T value = entries.get(field);
            if (value == null)
                continue;

            ColumnSpecification fieldSpec = fieldSpecOf(receiver, i);
            if (!value.testAssignment(receiver.ksName, fieldSpec).isAssignable())
            {
                throw new InvalidRequestException(String.format("Invalid user type literal for %s: field %s is not of type %s",
                        receiver, field, fieldSpec.type.asCQL3Type()));
            }
        }
    }

    /**
     * Tests that the map with the specified entries can be assigned to the specified column.
     *
     * @param receiver the receiving column
     * @param entries the map entries
     */
    public static <T extends AssignmentTestable> AssignmentTestable.TestResult testUserTypeAssignment(ColumnSpecification receiver,
                                                                                                      Map<FieldIdentifier, T> entries)
    {
        try
        {
            validateUserTypeAssignableTo(receiver, entries);
            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
        }
        catch (InvalidRequestException e)
        {
            return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
        }
    }

    /**
     * Create a {@code String} representation of the user type from the specified items associated to
     * the user type entries.
     *
     * @param items items associated to the user type entries
     * @return a {@code String} representation of the user type
     */
    public static <T> String userTypeToString(Map<FieldIdentifier, T> items)
    {
        return userTypeToString(items, Object::toString);
    }

    /**
     * Create a {@code String} representation of the user type from the specified items associated to
     * the user type entries.
     *
     * @param items items associated to the user type entries
     * @return a {@code String} representation of the user type
     */
    public static <T> String userTypeToString(Map<FieldIdentifier, T> items,
                                              java.util.function.Function<T, String> mapper)
    {
        return items.entrySet()
                    .stream()
                    .map(p -> String.format("%s: %s", p.getKey(), mapper.apply(p.getValue())))
                    .collect(Collectors.joining(", ", "{", "}"));
    }

    public static class Literal extends Term.Raw
    {
        public final Map<FieldIdentifier, Term.Raw> entries;

        public Literal(Map<FieldIdentifier, Term.Raw> entries)
        {
            this.entries = entries;
        }

        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            validateAssignableTo(keyspace, receiver);

            UserType ut = (UserType)receiver.type.unwrap();
            boolean allTerminal = true;
            List<Term> values = new ArrayList<>(entries.size());
            int foundValues = 0;
            for (int i = 0; i < ut.size(); i++)
            {
                FieldIdentifier field = ut.fieldName(i);
                Term.Raw raw = entries.get(field);
                if (raw == null)
                    raw = Constants.NULL_LITERAL;
                else
                    ++foundValues;
                Term value = raw.prepare(keyspace, fieldSpecOf(receiver, i));

                if (value instanceof Term.NonTerminal)
                    allTerminal = false;

                values.add(value);
            }
            if (foundValues != entries.size())
            {
                // We had some field that are not part of the type
                for (FieldIdentifier id : entries.keySet())
                {
                    if (!ut.fieldNames().contains(id))
                        throw new InvalidRequestException(String.format("Unknown field '%s' in value of user defined type %s", id, ut.getNameAsString()));
                }
            }

            MultiElements.DelayedValue value = new MultiElements.DelayedValue(((UserType)receiver.type.unwrap()), values);
            return allTerminal ? value.bind(QueryOptions.DEFAULT) : value;
        }

        private void validateAssignableTo(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            AbstractType<?> unwrapped = receiver.type.unwrap();

            if (!unwrapped.isUDT())
                throw new InvalidRequestException(String.format("Invalid user type literal for %s of type %s", receiver.name, receiver.type.asCQL3Type()));

            UserType ut = (UserType)unwrapped;
            for (int i = 0; i < ut.size(); i++)
            {
                FieldIdentifier field = ut.fieldName(i);
                Term.Raw value = entries.get(field);
                if (value == null)
                    continue;

                ColumnSpecification fieldSpec = fieldSpecOf(receiver, i);
                if (!value.testAssignment(keyspace, fieldSpec).isAssignable())
                {
                    throw new InvalidRequestException(String.format("Invalid user type literal for %s: field %s is not of type %s",
                            receiver.name, field, fieldSpec.type.asCQL3Type()));
                }
            }
        }

        public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            return testUserTypeAssignment(receiver, entries);
        }

        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return null;
        }

        public String getText()
        {
            return userTypeToString(entries, Term.Raw::getText);
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

            UserType type = (UserType) column.type;
            if (type.isMultiCell())
            {
                // setting a whole UDT at once means we overwrite all cells, so delete existing cells
                params.setComplexDeletionTimeForOverwrite(column);
                if (value == null)
                    return;

                Iterator<FieldIdentifier> fieldNameIter = type.fieldNames().iterator();
                for (ByteBuffer buffer : value.getElements())
                {
                    assert fieldNameIter.hasNext();
                    FieldIdentifier fieldName = fieldNameIter.next();
                    if (buffer == null)
                        continue;

                    CellPath fieldPath = type.cellPathForField(fieldName);
                    params.addCell(column, fieldPath, buffer);
                }
            }
            else
            {
                // for frozen UDTs, we're overwriting the whole cell value
                if (value == null)
                    params.addTombstone(column);
                else
                    params.addCell(column, value.get());
            }
        }
    }

    public static class SetterByField extends Operation
    {
        private final FieldIdentifier field;

        public SetterByField(ColumnMetadata column, FieldIdentifier field, Term t)
        {
            super(column, t);
            this.field = field;
        }

        public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException
        {
            // we should not get here for frozen UDTs
            assert column.type.isMultiCell() : "Attempted to set an individual field on a frozen UDT";

            Term.Terminal value = t.bind(params.options);
            if (value == UNSET_VALUE)
                return;

            CellPath fieldPath = ((UserType) column.type).cellPathForField(field);
            if (value == null)
                params.addTombstone(column, fieldPath);
            else
                params.addCell(column, fieldPath, value.get());
        }
    }

    public static class DeleterByField extends Operation
    {
        private final FieldIdentifier field;

        public DeleterByField(ColumnMetadata column, FieldIdentifier field)
        {
            super(column, null);
            this.field = field;
        }

        public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException
        {
            // we should not get here for frozen UDTs
            assert column.type.isMultiCell() : "Attempted to delete a single field from a frozen UDT";

            CellPath fieldPath = ((UserType) column.type).cellPathForField(field);
            params.addTombstone(column, fieldPath);
        }
    }
}
