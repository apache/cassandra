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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * Static helper methods and classes for maps.
 */
public abstract class Maps
{
    private Maps() {}

    public static ColumnSpecification keySpecOf(ColumnSpecification column)
    {
        return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("key(" + column.name + ")", true), ((MapType)column.type).keys);
    }

    public static ColumnSpecification valueSpecOf(ColumnSpecification column)
    {
        return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("value(" + column.name + ")", true), ((MapType)column.type).values);
    }

    public static class Literal implements Term.Raw
    {
        public final List<Pair<Term.Raw, Term.Raw>> entries;

        public Literal(List<Pair<Term.Raw, Term.Raw>> entries)
        {
            this.entries = entries;
        }

        public Value prepare(ColumnSpecification receiver) throws InvalidRequestException
        {
            validateAssignableTo(receiver);

            ColumnSpecification keySpec = Maps.keySpecOf(receiver);
            ColumnSpecification valueSpec = Maps.valueSpecOf(receiver);
            Map<ByteBuffer, ByteBuffer> values = new TreeMap<ByteBuffer, ByteBuffer>(((MapType)receiver.type).keys);
            for (Pair<Term.Raw, Term.Raw> entry : entries)
            {
                Term k = entry.left.prepare(keySpec);
                Term v = entry.right.prepare(valueSpec);

                if (k instanceof Term.NonTerminal || v instanceof Term.NonTerminal)
                    throw new InvalidRequestException(String.format("Invalid map literal for %s: bind variables are not supported inside collection literals", receiver));

                // We don't support values > 64K because the serialization format encode the length as an unsigned short.
                ByteBuffer keyBytes = ((Constants.Value)k).bytes;
                if (keyBytes == null)
                    throw new InvalidRequestException("null is not supported inside collections");
                if (keyBytes.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
                    throw new InvalidRequestException(String.format("Map key is too long. Map keys are limited to %d bytes but %d bytes keys provided",
                                                                    FBUtilities.MAX_UNSIGNED_SHORT,
                                                                    keyBytes.remaining()));

                ByteBuffer valueBytes = ((Constants.Value)v).bytes;
                if (valueBytes == null)
                    throw new InvalidRequestException("null is not supported inside collections");
                if (valueBytes.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
                    throw new InvalidRequestException(String.format("Map value is too long. Map values are limited to %d bytes but %d bytes value provided",
                                                                    FBUtilities.MAX_UNSIGNED_SHORT,
                                                                    valueBytes.remaining()));

                if (values.put(keyBytes, valueBytes) != null)
                    throw new InvalidRequestException(String.format("Invalid map literal: duplicate entry for key %s", entry.left));
            }
            return new Value(values);
        }

        private void validateAssignableTo(ColumnSpecification receiver) throws InvalidRequestException
        {
            if (!(receiver.type instanceof MapType))
                throw new InvalidRequestException(String.format("Invalid map literal for %s of type %s", receiver, receiver.type.asCQL3Type()));

            ColumnSpecification keySpec = Maps.keySpecOf(receiver);
            ColumnSpecification valueSpec = Maps.valueSpecOf(receiver);
            for (Pair<Term.Raw, Term.Raw> entry : entries)
            {
                if (!entry.left.isAssignableTo(keySpec))
                    throw new InvalidRequestException(String.format("Invalid map literal for %s: key %s is not of type %s", receiver, entry.left, keySpec.type.asCQL3Type()));
                if (!entry.right.isAssignableTo(valueSpec))
                    throw new InvalidRequestException(String.format("Invalid map literal for %s: value %s is not of type %s", receiver, entry.right, valueSpec.type.asCQL3Type()));
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
            for (int i = 0; i < entries.size(); i++)
            {
                if (i > 0) sb.append(", ");
                sb.append(entries.get(i).left).append(":").append(entries.get(i).right);
            }
            sb.append("}");
            return sb.toString();
        }
    }

    public static class Value extends Term.Terminal
    {
        public final Map<ByteBuffer, ByteBuffer> map;

        public Value(Map<ByteBuffer, ByteBuffer> map)
        {
            this.map = map;
        }

        public static Value fromSerialized(ByteBuffer value, MapType type) throws InvalidRequestException
        {
            try
            {
                // Collections have this small hack that validate cannot be called on a serialized object,
                // but compose does the validation (so we're fine).
                Map<?, ?> m = type.compose(value);
                Map<ByteBuffer, ByteBuffer> map = new LinkedHashMap<ByteBuffer, ByteBuffer>(m.size());
                for (Map.Entry<?, ?> entry : m.entrySet())
                    map.put(type.keys.decompose(entry.getKey()), type.values.decompose(entry.getValue()));
                return new Value(map);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException(e.getMessage());
            }
        }

        public ByteBuffer get()
        {
            List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(2 * map.size());
            for (Map.Entry<ByteBuffer, ByteBuffer> entry : map.entrySet())
            {
                buffers.add(entry.getKey());
                buffers.add(entry.getValue());
            }
            return CollectionType.pack(buffers, map.size());
        }
    }

    public static class Marker extends AbstractMarker
    {
        protected Marker(int bindIndex, ColumnSpecification receiver)
        {
            super(bindIndex, receiver);
            assert receiver.type instanceof MapType;
        }

        public Value bind(List<ByteBuffer> values) throws InvalidRequestException
        {
            ByteBuffer value = values.get(bindIndex);
            return value == null ? null : Value.fromSerialized(value, (MapType)receiver.type);
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
            // delete + put
            ColumnNameBuilder column = prefix.add(columnName.key);
            cf.addAtom(params.makeTombstoneForOverwrite(column.build(), column.buildAsEndOfRange()));
            Putter.doPut(t, cf, column, params);
        }
    }

    public static class SetterByKey extends Operation
    {
        private final Term k;

        public SetterByKey(ColumnIdentifier column, Term k, Term t)
        {
            super(column, t);
            this.k = k;
        }

        @Override
        public void collectMarkerSpecification(ColumnSpecification[] boundNames)
        {
            super.collectMarkerSpecification(boundNames);
            k.collectMarkerSpecification(boundNames);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, ColumnNameBuilder prefix, UpdateParameters params) throws InvalidRequestException
        {
            ByteBuffer key = k.bindAndGet(params.variables);
            ByteBuffer value = t.bindAndGet(params.variables);
            if (key == null)
                throw new InvalidRequestException("Invalid null map key");

            ByteBuffer cellName = prefix.add(columnName.key).add(key).build();

            if (value == null)
            {
                cf.addColumn(params.makeTombstone(cellName));
            }
            else
            {
                // We don't support value > 64K because the serialization format encode the length as an unsigned short.
                if (value.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
                    throw new InvalidRequestException(String.format("Map value is too long. Map values are limited to %d bytes but %d bytes value provided",
                                                                    FBUtilities.MAX_UNSIGNED_SHORT,
                                                                    value.remaining()));

                cf.addColumn(params.makeColumn(cellName, value));
            }
        }
    }

    public static class Putter extends Operation
    {
        public Putter(ColumnIdentifier column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, ColumnNameBuilder prefix, UpdateParameters params) throws InvalidRequestException
        {
            doPut(t, cf, prefix.add(columnName.key), params);
        }

        static void doPut(Term t, ColumnFamily cf, ColumnNameBuilder columnName, UpdateParameters params) throws InvalidRequestException
        {
            Term.Terminal value = t.bind(params.variables);
            if (value == null)
                return;
            assert value instanceof Maps.Value;

            Map<ByteBuffer, ByteBuffer> toAdd = ((Maps.Value)value).map;
            for (Map.Entry<ByteBuffer, ByteBuffer> entry : toAdd.entrySet())
            {
                ByteBuffer cellName = columnName.copy().add(entry.getKey()).build();
                cf.addColumn(params.makeColumn(cellName, entry.getValue()));
            }
        }
    }

    public static class DiscarderByKey extends Operation
    {
        public DiscarderByKey(ColumnIdentifier column, Term k)
        {
            super(column, k);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, ColumnNameBuilder prefix, UpdateParameters params) throws InvalidRequestException
        {
            Term.Terminal key = t.bind(params.variables);
            if (key == null)
                throw new InvalidRequestException("Invalid null map key");
            assert key instanceof Constants.Value;

            ByteBuffer cellName = prefix.add(columnName.key).add(((Constants.Value)key).bytes).build();
            cf.addColumn(params.makeTombstone(cellName));
        }
    }
}
