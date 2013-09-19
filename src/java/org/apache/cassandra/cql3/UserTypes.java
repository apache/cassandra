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

import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Static helper methods and classes for user types.
 */
public abstract class UserTypes
{
    private UserTypes() {}

    public static ColumnSpecification fieldSpecOf(ColumnSpecification column, int field)
    {
        return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier(column.name + "." + field, true), ((UserType)column.type).types.get(field));
    }

    public static class Literal implements Term.Raw
    {
        public final Map<ColumnIdentifier, Term.Raw> entries;

        public Literal(Map<ColumnIdentifier, Term.Raw> entries)
        {
            this.entries = entries;
        }

        public Term prepare(ColumnSpecification receiver) throws InvalidRequestException
        {
            validateAssignableTo(receiver);

            UserType ut = (UserType)receiver.type;
            boolean allTerminal = true;
            List<Term> values = new ArrayList<>(entries.size());
            for (int i = 0; i < ut.types.size(); i++)
            {
                ColumnIdentifier field = new ColumnIdentifier(ut.columnNames.get(i), UTF8Type.instance);
                Term value = entries.get(field).prepare(fieldSpecOf(receiver, i));

                if (value instanceof Term.NonTerminal)
                    allTerminal = false;

                values.add(value);
            }
            DelayedValue value = new DelayedValue(((UserType)receiver.type), values);
            return allTerminal ? value.bind(Collections.<ByteBuffer>emptyList()) : value;
        }

        private void validateAssignableTo(ColumnSpecification receiver) throws InvalidRequestException
        {
            if (!(receiver.type instanceof UserType))
                throw new InvalidRequestException(String.format("Invalid user type literal for %s of type %s", receiver, receiver.type.asCQL3Type()));

            UserType ut = (UserType)receiver.type;
            for (int i = 0; i < ut.types.size(); i++)
            {
                ColumnIdentifier field = new ColumnIdentifier(ut.columnNames.get(i), UTF8Type.instance);
                Term.Raw value = entries.get(field);
                if (value == null)
                    throw new InvalidRequestException(String.format("Invalid user type literal for %s: missing field %s", receiver, field));

                ColumnSpecification fieldSpec = fieldSpecOf(receiver, i);
                if (!value.isAssignableTo(fieldSpec))
                    throw new InvalidRequestException(String.format("Invalid user type literal for %s: field %s is not of type %s", receiver, field, fieldSpec.type.asCQL3Type()));
            }
        }

        public boolean isAssignableTo(ColumnSpecification receiver)
        {
            try
            {
                validateAssignableTo(receiver);
                return true;
            }
            catch (InvalidRequestException e)
            {
                return false;
            }
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            Iterator<Map.Entry<ColumnIdentifier, Term.Raw>> iter = entries.entrySet().iterator();
            while (iter.hasNext())
            {
                Map.Entry<ColumnIdentifier, Term.Raw> entry = iter.next();
                sb.append(entry.getKey()).append(":").append(entry.getValue());
                if (iter.hasNext())
                    sb.append(", ");
            }
            sb.append("}");
            return sb.toString();
        }
    }

    // Same purpose than Lists.DelayedValue, except we do handle bind marker in that case
    public static class DelayedValue extends Term.NonTerminal
    {
        private final UserType type;
        private final List<Term> values;

        public DelayedValue(UserType type, List<Term> values)
        {
            this.type = type;
            this.values = values;
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
            for (int i = 0; i < type.types.size(); i++)
                values.get(i).collectMarkerSpecification(boundNames);
        }

        private ByteBuffer[] bindInternal(List<ByteBuffer> variables) throws InvalidRequestException
        {
            ByteBuffer[] buffers = new ByteBuffer[values.size()];
            for (int i = 0; i < type.types.size(); i++)
            {
                ByteBuffer buffer = values.get(i).bindAndGet(variables);
                if (buffer == null)
                    throw new InvalidRequestException("null is not supported inside user type literals");
                if (buffer.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
                    throw new InvalidRequestException(String.format("Value for field %s is too long. User type fields are limited to %d bytes but %d bytes provided",
                                                                    UTF8Type.instance.getString(type.columnNames.get(i)),
                                                                    FBUtilities.MAX_UNSIGNED_SHORT,
                                                                    buffer.remaining()));

                buffers[i] = buffer;
            }
            return buffers;
        }

        public Constants.Value bind(List<ByteBuffer> variables) throws InvalidRequestException
        {
            return new Constants.Value(bindAndGet(variables));
        }

        @Override
        public ByteBuffer bindAndGet(List<ByteBuffer> variables) throws InvalidRequestException
        {
            return CompositeType.build(bindInternal(variables));
        }
    }
}
