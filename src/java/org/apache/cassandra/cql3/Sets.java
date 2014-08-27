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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.base.Joiner;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
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

        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            validateAssignableTo(keyspace, receiver);

            // We've parsed empty maps as a set literal to break the ambiguity so
            // handle that case now
            if (receiver.type instanceof MapType && elements.isEmpty())
                return new Maps.Value(Collections.<ByteBuffer, ByteBuffer>emptyMap());

            ColumnSpecification valueSpec = Sets.valueSpecOf(receiver);
            Set<Term> values = new HashSet<Term>(elements.size());
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
            DelayedValue value = new DelayedValue(((SetType)receiver.type).elements, values);
            return allTerminal ? value.bind(QueryOptions.DEFAULT) : value;
        }

        private void validateAssignableTo(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            if (!(receiver.type instanceof SetType))
            {
                // We've parsed empty maps as a set literal to break the ambiguity so
                // handle that case now
                if (receiver.type instanceof MapType && elements.isEmpty())
                    return;

                throw new InvalidRequestException(String.format("Invalid set literal for %s of type %s", receiver.name, receiver.type.asCQL3Type()));
            }

            ColumnSpecification valueSpec = Sets.valueSpecOf(receiver);
            for (Term.Raw rt : elements)
            {
                if (!rt.isAssignableTo(keyspace, valueSpec))
                    throw new InvalidRequestException(String.format("Invalid set literal for %s: value %s is not of type %s", receiver.name, rt, valueSpec.type.asCQL3Type()));
            }
        }

        public boolean isAssignableTo(String keyspace, ColumnSpecification receiver)
        {
            try
            {
                validateAssignableTo(keyspace, receiver);
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

        public static Value fromSerialized(ByteBuffer value, SetType type, int version) throws InvalidRequestException
        {
            try
            {
                // Collections have this small hack that validate cannot be called on a serialized object,
                // but compose does the validation (so we're fine).
                Set<?> s = (Set<?>)type.getSerializer().deserializeForNativeProtocol(value, version);
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

        public ByteBuffer get(QueryOptions options)
        {
            return CollectionSerializer.pack(new ArrayList<ByteBuffer>(elements), elements.size(), options.getProtocolVersion());
        }

        public boolean equals(SetType st, Value v)
        {
            if (elements.size() != v.elements.size())
                return false;

            Iterator<ByteBuffer> thisIter = elements.iterator();
            Iterator<ByteBuffer> thatIter = v.elements.iterator();
            while (thisIter.hasNext())
                if (st.elements.compare(thisIter.next(), thatIter.next()) != 0)
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

        public Value bind(QueryOptions options) throws InvalidRequestException
        {
            Set<ByteBuffer> buffers = new TreeSet<ByteBuffer>(comparator);
            for (Term t : elements)
            {
                ByteBuffer bytes = t.bindAndGet(options);

                if (bytes == null)
                    throw new InvalidRequestException("null is not supported inside collections");

                // We don't support value > 64K because the serialization format encode the length as an unsigned short.
                if (bytes.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
                    throw new InvalidRequestException(String.format("Set value is too long. Set values are limited to %d bytes but %d bytes value provided",
                                                                    FBUtilities.MAX_UNSIGNED_SHORT,
                                                                    bytes.remaining()));

                buffers.add(bytes);
            }
            return new Value(buffers);
        }
    }

    public static class Marker extends AbstractMarker
    {
        protected Marker(int bindIndex, ColumnSpecification receiver)
        {
            super(bindIndex, receiver);
            assert receiver.type instanceof SetType;
        }

        public Value bind(QueryOptions options) throws InvalidRequestException
        {
            ByteBuffer value = options.getValues().get(bindIndex);
            return value == null ? null : Value.fromSerialized(value, (SetType)receiver.type, options.getProtocolVersion());
        }
    }

    public static class Setter extends Operation
    {
        public Setter(ColumnDefinition column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, Composite prefix, UpdateParameters params) throws InvalidRequestException
        {
            // delete + add
            CellName name = cf.getComparator().create(prefix, column);
            cf.addAtom(params.makeTombstoneForOverwrite(name.slice()));
            Adder.doAdd(t, cf, prefix, column, params);
        }
    }

    public static class Adder extends Operation
    {
        public Adder(ColumnDefinition column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, Composite prefix, UpdateParameters params) throws InvalidRequestException
        {
            doAdd(t, cf, prefix, column, params);
        }

        static void doAdd(Term t, ColumnFamily cf, Composite prefix, ColumnDefinition column, UpdateParameters params) throws InvalidRequestException
        {
            Term.Terminal value = t.bind(params.options);
            if (value == null)
                return;

            assert value instanceof Sets.Value : value;

            Set<ByteBuffer> toAdd = ((Sets.Value)value).elements;
            for (ByteBuffer bb : toAdd)
            {
                CellName cellName = cf.getComparator().create(prefix, column, bb);
                cf.addColumn(params.makeColumn(cellName, ByteBufferUtil.EMPTY_BYTE_BUFFER));
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

        public void execute(ByteBuffer rowKey, ColumnFamily cf, Composite prefix, UpdateParameters params) throws InvalidRequestException
        {
            Term.Terminal value = t.bind(params.options);
            if (value == null)
                return;

            // This can be either a set or a single element
            Set<ByteBuffer> toDiscard = value instanceof Constants.Value
                                      ? Collections.singleton(((Constants.Value)value).bytes)
                                      : ((Sets.Value)value).elements;

            for (ByteBuffer bb : toDiscard)
            {
                cf.addColumn(params.makeTombstone(cf.getComparator().create(prefix, column, bb)));
            }
        }
    }
}
