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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.base.Joiner;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Static helper methods and classes for sets.
 */
public abstract class Sets
{
    private Sets() {}

    public static ColumnSpecification valueSpecOf(ColumnSpecification column)
    {
        return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("value(" + column.name + ")", true), ((SetType)column.type).elements);
    }

    public static class Literal implements Term.Raw
    {
        private final List<Term.Raw> elements;

        public Literal(List<Term.Raw> elements)
        {
            this.elements = elements;
        }

        public Term.Terminal prepare(ColumnSpecification receiver) throws InvalidRequestException
        {
            validateAssignableTo(receiver);

            // We've parsed empty maps as a set literal to break the ambiguity so
            // handle that case now
            if (receiver.type instanceof MapType && elements.isEmpty())
                return new Maps.Value(Collections.<ByteBuffer, ByteBuffer>emptyMap());

            ColumnSpecification valueSpec = Sets.valueSpecOf(receiver);
            Set<ByteBuffer> values = new TreeSet<ByteBuffer>(((SetType)receiver.type).elements);
            for (Term.Raw rt : elements)
            {
                Term t = rt.prepare(valueSpec);

                if (t instanceof Term.NonTerminal)
                    throw new InvalidRequestException(String.format("Invalid set literal for %s: bind variables are not supported inside collection literals", receiver));

                // We don't allow prepared marker in collections, nor nested collections (for the later, prepare will throw an exception)
                assert t instanceof Constants.Value;
                ByteBuffer bytes = ((Constants.Value)t).bytes;
                if (bytes == null)
                    throw new InvalidRequestException("null is not supported inside collections");

                // We don't support value > 64K because the serialization format encode the length as an unsigned short.
                if (bytes.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
                    throw new InvalidRequestException(String.format("Set value is too long. Set values are limited to %d bytes but %d bytes value provided",
                                                                    FBUtilities.MAX_UNSIGNED_SHORT,
                                                                    bytes.remaining()));

                if (!values.add(bytes))
                    throw new InvalidRequestException(String.format("Invalid set literal: duplicate value %s", rt));
            }
            return new Value(values);
        }

        private void validateAssignableTo(ColumnSpecification receiver) throws InvalidRequestException
        {
            if (!(receiver.type instanceof SetType))
            {
                // We've parsed empty maps as a set literal to break the ambiguity so
                // handle that case now
                if (receiver.type instanceof MapType && elements.isEmpty())
                    return;

                throw new InvalidRequestException(String.format("Invalid set literal for %s of type %s", receiver, receiver.type.asCQL3Type()));
            }

            ColumnSpecification valueSpec = Sets.valueSpecOf(receiver);
            for (Term.Raw rt : elements)
            {
                if (!rt.isAssignableTo(valueSpec))
                    throw new InvalidRequestException(String.format("Invalid set literal for %s: value %s is not of type %s", receiver, rt, valueSpec.type.asCQL3Type()));
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
            return "{" + Joiner.on(", ").join(elements) + "}";
        }
    }

    public static class Value extends Term.Terminal
    {
        public final Set<ByteBuffer> elements;

        public Value(Set<ByteBuffer> elements)
        {
            this.elements = elements;
        }

        public static Value fromSerialized(ByteBuffer value, SetType type) throws InvalidRequestException
        {
            try
            {
                // Collections have this small hack that validate cannot be called on a serialized object,
                // but compose does the validation (so we're fine).
                Set<?> s = type.compose(value);
                Set<ByteBuffer> elements = new LinkedHashSet<ByteBuffer>(s.size());
                for (Object element : s)
                    elements.add(type.elements.decompose(element));
                return new Value(elements);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException(e.getMessage());
            }
        }

        public ByteBuffer get()
        {
            return CollectionType.pack(new ArrayList<ByteBuffer>(elements), elements.size());
        }
    }

    public static class Marker extends AbstractMarker
    {
        protected Marker(int bindIndex, ColumnSpecification receiver)
        {
            super(bindIndex, receiver);
            assert receiver.type instanceof SetType;
        }

        public Value bind(List<ByteBuffer> values) throws InvalidRequestException
        {
            ByteBuffer value = values.get(bindIndex);
            return value == null ? null : Value.fromSerialized(value, (SetType)receiver.type);
        }
    }

    public static class Setter extends Operation
    {
        public Setter(ColumnIdentifier column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, ColumnNameBuilder prefix, UpdateParameters params) throws InvalidRequestException
        {
            // delete + add
            ColumnNameBuilder column = prefix.add(columnName.key);
            cf.addAtom(params.makeTombstoneForOverwrite(column.build(), column.buildAsEndOfRange()));
            Adder.doAdd(t, cf, column, params);
        }
    }

    public static class Adder extends Operation
    {
        public Adder(ColumnIdentifier column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, ColumnNameBuilder prefix, UpdateParameters params) throws InvalidRequestException
        {
            doAdd(t, cf, prefix.add(columnName.key), params);
        }

        static void doAdd(Term t, ColumnFamily cf, ColumnNameBuilder columnName, UpdateParameters params) throws InvalidRequestException
        {
            Term.Terminal value = t.bind(params.variables);
            if (value == null)
                return;

            assert value instanceof Sets.Value : value;

            Set<ByteBuffer> toAdd = ((Sets.Value)value).elements;
            for (ByteBuffer bb : toAdd)
            {
                ByteBuffer cellName = columnName.copy().add(bb).build();
                cf.addColumn(params.makeColumn(cellName, ByteBufferUtil.EMPTY_BYTE_BUFFER));
            }
        }
    }

    public static class Discarder extends Operation
    {
        public Discarder(ColumnIdentifier column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, ColumnNameBuilder prefix, UpdateParameters params) throws InvalidRequestException
        {
            Term.Terminal value = t.bind(params.variables);
            if (value == null)
                return;

            // This can be either a set or a single element
            Set<ByteBuffer> toDiscard = value instanceof Constants.Value
                                      ? Collections.singleton(((Constants.Value)value).bytes)
                                      : ((Sets.Value)value).elements;

            ColumnNameBuilder column = prefix.add(columnName.key);
            for (ByteBuffer bb : toDiscard)
            {
                ByteBuffer cellName = column.copy().add(bb).build();
                cf.addColumn(params.makeTombstone(cellName));
            }
        }
    }
}
