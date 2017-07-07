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
import java.util.*;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql3.Constants.UNSET_VALUE;

/**
 * Static helper methods and classes for user types.
 */
public abstract class UserTypes
{
    private UserTypes() {}

    public static ColumnSpecification fieldSpecOf(ColumnSpecification column, int field)
    {
        UserType ut = (UserType)column.type;
        return new ColumnSpecification(column.ksName,
                                       column.cfName,
                                       new ColumnIdentifier(column.name + "." + ut.fieldName(field), true),
                                       ut.fieldType(field));
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

            UserType ut = (UserType)receiver.type;
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

            DelayedValue value = new DelayedValue(((UserType)receiver.type), values);
            return allTerminal ? value.bind(QueryOptions.DEFAULT) : value;
        }

        private void validateAssignableTo(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            if (!receiver.type.isUDT())
                throw new InvalidRequestException(String.format("Invalid user type literal for %s of type %s", receiver.name, receiver.type.asCQL3Type()));

            UserType ut = (UserType)receiver.type;
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
            try
            {
                validateAssignableTo(keyspace, receiver);
                return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
            }
            catch (InvalidRequestException e)
            {
                return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
            }
        }

        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return null;
        }

        public String getText()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            Iterator<Map.Entry<FieldIdentifier, Term.Raw>> iter = entries.entrySet().iterator();
            while (iter.hasNext())
            {
                Map.Entry<FieldIdentifier, Term.Raw> entry = iter.next();
                sb.append(entry.getKey()).append(": ").append(entry.getValue().getText());
                if (iter.hasNext())
                    sb.append(", ");
            }
            sb.append("}");
            return sb.toString();
        }
    }

    public static class Value extends Term.MultiItemTerminal
    {
        private final UserType type;
        public final ByteBuffer[] elements;

        public Value(UserType type, ByteBuffer[] elements)
        {
            this.type = type;
            this.elements = elements;
        }

        public static Value fromSerialized(ByteBuffer bytes, UserType type)
        {
            type.validate(bytes);
            return new Value(type, type.split(bytes));
        }

        public ByteBuffer get(ProtocolVersion protocolVersion)
        {
            return TupleType.buildValue(elements);
        }

        public boolean equals(UserType userType, Value v)
        {
            if (elements.length != v.elements.length)
                return false;

            for (int i = 0; i < elements.length; i++)
                if (userType.fieldType(i).compare(elements[i], v.elements[i]) != 0)
                    return false;

            return true;
        }

        public List<ByteBuffer> getElements()
        {
            return Arrays.asList(elements);
        }
    }

    public static class DelayedValue extends Term.NonTerminal
    {
        private final UserType type;
        private final List<Term> values;

        public DelayedValue(UserType type, List<Term> values)
        {
            this.type = type;
            this.values = values;
        }

        public void addFunctionsTo(List<Function> functions)
        {
            Terms.addFunctions(values, functions);
        }

        public boolean containsBindMarker()
        {
            for (Term t : values)
                if (t.containsBindMarker())
                    return true;
            return false;
        }

        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
            for (int i = 0; i < type.size(); i++)
                values.get(i).collectMarkerSpecification(boundNames);
        }

        private ByteBuffer[] bindInternal(QueryOptions options) throws InvalidRequestException
        {
            if (values.size() > type.size())
            {
                throw new InvalidRequestException(String.format(
                        "UDT value contained too many fields (expected %s, got %s)", type.size(), values.size()));
            }

            ByteBuffer[] buffers = new ByteBuffer[values.size()];
            for (int i = 0; i < type.size(); i++)
            {
                buffers[i] = values.get(i).bindAndGet(options);
                // Since a frozen UDT value is always written in its entirety Cassandra can't preserve a pre-existing
                // value by 'not setting' the new value. Reject the query.
                if (!type.isMultiCell() && buffers[i] == ByteBufferUtil.UNSET_BYTE_BUFFER)
                    throw new InvalidRequestException(String.format("Invalid unset value for field '%s' of user defined type %s", type.fieldNameAsString(i), type.getNameAsString()));
            }
            return buffers;
        }

        public Value bind(QueryOptions options) throws InvalidRequestException
        {
            return new Value(type, bindInternal(options));
        }

        @Override
        public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException
        {
            return UserType.buildValue(bindInternal(options));
        }
    }

    public static class Marker extends AbstractMarker
    {
        protected Marker(int bindIndex, ColumnSpecification receiver)
        {
            super(bindIndex, receiver);
            assert receiver.type.isUDT();
        }

        public Terminal bind(QueryOptions options) throws InvalidRequestException
        {
            ByteBuffer value = options.getValues().get(bindIndex);
            if (value == null)
                return null;
            if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
                return UNSET_VALUE;
            return Value.fromSerialized(value, (UserType) receiver.type);
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

            Value userTypeValue = (Value) value;
            if (column.type.isMultiCell())
            {
                // setting a whole UDT at once means we overwrite all cells, so delete existing cells
                params.setComplexDeletionTimeForOverwrite(column);
                if (value == null)
                    return;

                Iterator<FieldIdentifier> fieldNameIter = userTypeValue.type.fieldNames().iterator();
                for (ByteBuffer buffer : userTypeValue.elements)
                {
                    assert fieldNameIter.hasNext();
                    FieldIdentifier fieldName = fieldNameIter.next();
                    if (buffer == null)
                        continue;

                    CellPath fieldPath = userTypeValue.type.cellPathForField(fieldName);
                    params.addCell(column, fieldPath, buffer);
                }
            }
            else
            {
                // for frozen UDTs, we're overwriting the whole cell value
                if (value == null)
                    params.addTombstone(column);
                else
                    params.addCell(column, value.get(params.options.getProtocolVersion()));
            }
        }
    }

    public static class SetterByField extends Operation
    {
        private final FieldIdentifier field;

        public SetterByField(ColumnDefinition column, FieldIdentifier field, Term t)
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
                params.addCell(column, fieldPath, value.get(params.options.getProtocolVersion()));
        }
    }

    public static class DeleterByField extends Operation
    {
        private final FieldIdentifier field;

        public DeleterByField(ColumnDefinition column, FieldIdentifier field)
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
