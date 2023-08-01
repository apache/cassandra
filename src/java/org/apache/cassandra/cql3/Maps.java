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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.cql3.Constants.UNSET_VALUE;

/**
 * Static helper methods and classes for maps.
 */
public abstract class Maps
{
    private Maps() {}

    public static ColumnSpecification keySpecOf(ColumnSpecification column)
    {
        return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("key(" + column.name + ")", true), keysType(column.type));
    }

    public static ColumnSpecification valueSpecOf(ColumnSpecification column)
    {
        return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("value(" + column.name + ")", true), valuesType(column.type));
    }

    private static AbstractType<?> unwrap(AbstractType<?> type)
    {
        return type.isReversed() ? unwrap(((ReversedType<?>) type).baseType) : type;
    }

    private static AbstractType<?> keysType(AbstractType<?> type)
    {
        return ((MapType<?, ?>) unwrap(type)).getKeysType();
    }

    private static AbstractType<?> valuesType(AbstractType<?> type)
    {
        return ((MapType<?, ?>) unwrap(type)).getValuesType();
    }

    /**
     * Tests that the map with the specified entries can be assigned to the specified column.
     *
     * @param receiver the receiving column
     * @param entries the map entries
     */
    public static <T extends AssignmentTestable> AssignmentTestable.TestResult testMapAssignment(ColumnSpecification receiver,
                                                                                                 List<Pair<T, T>> entries)
    {
        ColumnSpecification keySpec = keySpecOf(receiver);
        ColumnSpecification valueSpec = valueSpecOf(receiver);

        // It's an exact match if all are exact match, but is not assignable as soon as any is non assignable.
        AssignmentTestable.TestResult res = AssignmentTestable.TestResult.EXACT_MATCH;
        for (Pair<T, T> entry : entries)
        {
            AssignmentTestable.TestResult t1 = entry.left.testAssignment(receiver.ksName, keySpec);
            AssignmentTestable.TestResult t2 = entry.right.testAssignment(receiver.ksName, valueSpec);
            if (t1 == AssignmentTestable.TestResult.NOT_ASSIGNABLE || t2 == AssignmentTestable.TestResult.NOT_ASSIGNABLE)
                return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
            if (t1 != AssignmentTestable.TestResult.EXACT_MATCH || t2 != AssignmentTestable.TestResult.EXACT_MATCH)
                res = AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
        }
        return res;
    }

    /**
     * Create a <code>String</code> representation of the list containing the specified elements.
     *
     * @param entries the list elements
     * @return a <code>String</code> representation of the list
     */
    public static <T> String mapToString(List<Pair<T, T>> entries)
    {
        return mapToString(entries, Object::toString);
    }

    /**
     * Create a <code>String</code> representation of the map from the specified items associated to
     * the map entries.
     *
     * @param items items associated to the map entries
     * @param mapper the mapper used to map the items to the <code>String</code> representation of the map entries
     * @return a <code>String</code> representation of the map
     */
    public static <T> String mapToString(List<Pair<T, T>> items,
                                         java.util.function.Function<T, String> mapper)
    {
        return items.stream()
                .map(p -> String.format("%s: %s", mapper.apply(p.left), mapper.apply(p.right)))
                .collect(Collectors.joining(", ", "{", "}"));
    }

    /**
     * Returns the exact MapType from the entries if it can be known.
     *
     * @param entries the entries
     * @param mapper the mapper used to retrieve the key and value types from the entries
     * @return the exact MapType from the entries if it can be known or <code>null</code>
     */
    public static <T> MapType<?, ?> getExactMapTypeIfKnown(List<Pair<T, T>> entries,
                                                           java.util.function.Function<T, AbstractType<?>> mapper)
    {
        AbstractType<?> keyType = null;
        AbstractType<?> valueType = null;
        for (Pair<T, T> entry : entries)
        {
            if (keyType == null)
                keyType = mapper.apply(entry.left);
            if (valueType == null)
                valueType = mapper.apply(entry.right);
            if (keyType != null && valueType != null)
                return MapType.getInstance(keyType, valueType, false);
        }
        return null;
    }

    public static <T> MapType<?, ?> getPreferredCompatibleType(List<Pair<T, T>> entries,
                                                               java.util.function.Function<T, AbstractType<?>> mapper)
    {
        Set<AbstractType<?>> keyTypes = entries.stream().map(Pair::left).map(mapper).filter(Objects::nonNull).collect(Collectors.toSet());
        AbstractType<?> keyType = AssignmentTestable.getCompatibleTypeIfKnown(keyTypes);
        if (keyType == null)
            return null;

        Set<AbstractType<?>> valueTypes = entries.stream().map(Pair::right).map(mapper).filter(Objects::nonNull).collect(Collectors.toSet());
        AbstractType<?> valueType = AssignmentTestable.getCompatibleTypeIfKnown(valueTypes);
        if (valueType == null)
            return null;

        return  MapType.getInstance(keyType, valueType, false);
    }

    public static class Literal extends Term.Raw
    {
        public final List<Pair<Term.Raw, Term.Raw>> entries;

        public Literal(List<Pair<Term.Raw, Term.Raw>> entries)
        {
            this.entries = entries;
        }

        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            validateAssignableTo(keyspace, receiver);

            ColumnSpecification keySpec = Maps.keySpecOf(receiver);
            ColumnSpecification valueSpec = Maps.valueSpecOf(receiver);
            Map<Term, Term> values = new HashMap<>(entries.size());
            boolean allTerminal = true;
            for (Pair<Term.Raw, Term.Raw> entry : entries)
            {
                Term k = entry.left.prepare(keyspace, keySpec);
                Term v = entry.right.prepare(keyspace, valueSpec);

                if (k.containsBindMarker() || v.containsBindMarker())
                    throw new InvalidRequestException(String.format("Invalid map literal for %s: bind variables are not supported inside collection literals", receiver.name));

                if (k instanceof Term.NonTerminal || v instanceof Term.NonTerminal)
                    allTerminal = false;

                values.put(k, v);
            }
            DelayedValue value = new DelayedValue(keysType(receiver.type), values);
            return allTerminal ? value.bind(QueryOptions.DEFAULT) : value;
        }

        private void validateAssignableTo(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            AbstractType<?> type = unwrap(receiver.type);

            if (!(type instanceof MapType))
                throw new InvalidRequestException(String.format("Invalid map literal for %s of type %s", receiver.name, receiver.type.asCQL3Type()));

            ColumnSpecification keySpec = Maps.keySpecOf(receiver);
            ColumnSpecification valueSpec = Maps.valueSpecOf(receiver);
            for (Pair<Term.Raw, Term.Raw> entry : entries)
            {
                if (!entry.left.testAssignment(keyspace, keySpec).isAssignable())
                    throw new InvalidRequestException(String.format("Invalid map literal for %s: key %s is not of type %s", receiver.name, entry.left, keySpec.type.asCQL3Type()));
                if (!entry.right.testAssignment(keyspace, valueSpec).isAssignable())
                    throw new InvalidRequestException(String.format("Invalid map literal for %s: value %s is not of type %s", receiver.name, entry.right, valueSpec.type.asCQL3Type()));
            }
        }

        public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            return testMapAssignment(receiver, entries);
        }

        @Override
        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            return getExactMapTypeIfKnown(entries, p -> p.getExactTypeIfKnown(keyspace));
        }

        @Override
        public AbstractType<?> getCompatibleTypeIfKnown(String keyspace)
        {
            return Maps.getPreferredCompatibleType(entries, p -> p.getCompatibleTypeIfKnown(keyspace));
        }

        public String getText()
        {
            return mapToString(entries, Term.Raw::getText);
        }
    }

    public static class Value extends Term.Terminal
    {
        public final SortedMap<ByteBuffer, ByteBuffer> map;

        public Value(SortedMap<ByteBuffer, ByteBuffer> map)
        {
            this.map = map;
        }

        public static <K, V> Value fromSerialized(ByteBuffer value, MapType<K, V> type) throws InvalidRequestException
        {
            try
            {
                // Collections have this small hack that validate cannot be called on a serialized object,
                // but compose does the validation (so we're fine).
                Map<K, V> m = type.getSerializer().deserialize(value, ByteBufferAccessor.instance);
                // We depend on Maps to be properly sorted by their keys, so use a sorted map implementation here.
                SortedMap<ByteBuffer, ByteBuffer> map = new TreeMap<>(type.getKeysType());
                for (Map.Entry<K, V> entry : m.entrySet())
                    map.put(type.getKeysType().decompose(entry.getKey()), type.getValuesType().decompose(entry.getValue()));
                return new Value(map);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException(e.getMessage());
            }
        }

        @Override
        public ByteBuffer get(ProtocolVersion version)
        {
            List<ByteBuffer> buffers = new ArrayList<>(2 * map.size());
            for (Map.Entry<ByteBuffer, ByteBuffer> entry : map.entrySet())
            {
                buffers.add(entry.getKey());
                buffers.add(entry.getValue());
            }
            return CollectionSerializer.pack(buffers, map.size());
        }

        public boolean equals(MapType<?, ?> mt, Value v)
        {
            if (map.size() != v.map.size())
                return false;

            // We use the fact that we know the maps iteration will both be in comparator order
            Iterator<Map.Entry<ByteBuffer, ByteBuffer>> thisIter = map.entrySet().iterator();
            Iterator<Map.Entry<ByteBuffer, ByteBuffer>> thatIter = v.map.entrySet().iterator();
            while (thisIter.hasNext())
            {
                Map.Entry<ByteBuffer, ByteBuffer> thisEntry = thisIter.next();
                Map.Entry<ByteBuffer, ByteBuffer> thatEntry = thatIter.next();
                if (mt.getKeysType().compare(thisEntry.getKey(), thatEntry.getKey()) != 0 || mt.getValuesType().compare(thisEntry.getValue(), thatEntry.getValue()) != 0)
                    return false;
            }

            return true;
        }
    }

    // See Lists.DelayedValue
    public static class DelayedValue extends Term.NonTerminal
    {
        private final Comparator<ByteBuffer> comparator;
        private final Map<Term, Term> elements;

        public DelayedValue(Comparator<ByteBuffer> comparator, Map<Term, Term> elements)
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
            SortedMap<ByteBuffer, ByteBuffer> buffers = new TreeMap<>(comparator);
            for (Map.Entry<Term, Term> entry : elements.entrySet())
            {
                // We don't support values > 64K because the serialization format encode the length as an unsigned short.
                ByteBuffer keyBytes = entry.getKey().bindAndGet(options);

                if (keyBytes == null)
                    throw new InvalidRequestException("null is not supported inside collections");
                if (keyBytes == ByteBufferUtil.UNSET_BYTE_BUFFER)
                    throw new InvalidRequestException("unset value is not supported for map keys");

                ByteBuffer valueBytes = entry.getValue().bindAndGet(options);
                if (valueBytes == null)
                    throw new InvalidRequestException("null is not supported inside collections");
                if (valueBytes == ByteBufferUtil.UNSET_BYTE_BUFFER)
                    return UNSET_VALUE;

                buffers.put(keyBytes, valueBytes);
            }
            return new Value(buffers);
        }

        public void addFunctionsTo(List<Function> functions)
        {
            Terms.addFunctions(elements.keySet(), functions);
            Terms.addFunctions(elements.values(), functions);
        }
    }

    public static class Marker extends AbstractMarker
    {
        protected Marker(int bindIndex, ColumnSpecification receiver)
        {
            super(bindIndex, receiver);
            assert receiver.type instanceof MapType;
        }

        public Terminal bind(QueryOptions options) throws InvalidRequestException
        {
            ByteBuffer value = options.getValues().get(bindIndex);
            if (value == null)
                return null;
            if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
                return UNSET_VALUE;
            return Value.fromSerialized(value, (MapType<?, ?>) receiver.type);
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

            // delete + put
            if (column.type.isMultiCell())
                params.setComplexDeletionTimeForOverwrite(column);
            Putter.doPut(value, column, params);
        }
    }

    public static class SetterByKey extends Operation
    {
        private final Term k;

        public SetterByKey(ColumnMetadata column, Term k, Term t)
        {
            super(column, t);
            this.k = k;
        }

        @Override
        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
            super.collectMarkerSpecification(boundNames);
            k.collectMarkerSpecification(boundNames);
        }

        public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to set a value for a single key on a frozen map";
            ByteBuffer key = k.bindAndGet(params.options);
            ByteBuffer value = t.bindAndGet(params.options);
            if (key == null)
                throw new InvalidRequestException("Invalid null map key");
            if (key == ByteBufferUtil.UNSET_BYTE_BUFFER)
                throw new InvalidRequestException("Invalid unset map key");

            CellPath path = CellPath.create(key);

            if (value == null)
            {
                params.addTombstone(column, path);
            }
            else if (value != ByteBufferUtil.UNSET_BYTE_BUFFER)
            {
                params.addCell(column, path, value);
            }
        }
    }

    public static class Putter extends Operation
    {
        public Putter(ColumnMetadata column, Term t)
        {
            super(column, t);
        }

        public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to add items to a frozen map";
            Term.Terminal value = t.bind(params.options);
            if (value != UNSET_VALUE)
                doPut(value, column, params);
        }

        static void doPut(Term.Terminal value, ColumnMetadata column, UpdateParameters params) throws InvalidRequestException
        {
            if (value == null)
            {
                // for frozen maps, we're overwriting the whole cell
                if (!column.type.isMultiCell())
                    params.addTombstone(column);

                return;
            }

            SortedMap<ByteBuffer, ByteBuffer> elements = ((Value) value).map;

            if (column.type.isMultiCell())
            {
                if (elements.size() == 0)
                    return;

                // Guardrails about collection size are only checked for the added elements without considering
                // already existent elements. This is done so to avoid read-before-write, having additional checks
                // during SSTable write.
                Guardrails.itemsPerCollection.guard(elements.size(), column.name.toString(), false, params.clientState);

                int dataSize = 0;
                for (Map.Entry<ByteBuffer, ByteBuffer> entry : elements.entrySet())
                {
                    Cell<?> cell = params.addCell(column, CellPath.create(entry.getKey()), entry.getValue());
                    dataSize += cell.dataSize();
                }
                Guardrails.collectionSize.guard(dataSize, column.name.toString(), false, params.clientState);
            }
            else
            {
                Guardrails.itemsPerCollection.guard(elements.size(), column.name.toString(), false, params.clientState);
                Cell<?> cell = params.addCell(column, value.get(ProtocolVersion.CURRENT));
                Guardrails.collectionSize.guard(cell.dataSize(), column.name.toString(), false, params.clientState);
            }
        }
    }

    public static class DiscarderByKey extends Operation
    {
        public DiscarderByKey(ColumnMetadata column, Term k)
        {
            super(column, k);
        }

        public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to delete a single key in a frozen map";
            Term.Terminal key = t.bind(params.options);
            if (key == null)
                throw new InvalidRequestException("Invalid null map key");
            if (key == Constants.UNSET_VALUE)
                throw new InvalidRequestException("Invalid unset map key");

            params.addTombstone(column, CellPath.create(key.get(params.options.getProtocolVersion())));
        }
    }
}
