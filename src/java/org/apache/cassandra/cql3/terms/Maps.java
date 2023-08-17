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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Operation;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.MultiElementType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.cql3.terms.Constants.UNSET_VALUE;

/**
 * Static helper methods and classes for maps.
 */
public final class Maps
{
    private Maps() {}

    public static ColumnSpecification keySpecOf(ColumnSpecification column)
    {
        return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("key(" + column.name + ')', true), keysType(column.type));
    }

    public static ColumnSpecification valueSpecOf(ColumnSpecification column)
    {
        return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("value(" + column.name + ')', true), valuesType(column.type));
    }

    private static AbstractType<?> keysType(AbstractType<?> type)
    {
        return ((MapType<?, ?>) type.unwrap()).getKeysType();
    }

    private static AbstractType<?> valuesType(AbstractType<?> type)
    {
        return ((MapType<?, ?>) type.unwrap()).getValuesType();
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
            // In CQL maps are represented as a list of key value pairs (e.g. {k1 : v1, k2 : v2, ...}).
            // Whereas, internally maps are serialized as a lists where each key is followed by its value (e.g. [k1, v1, k2, v2, ...])
            // Therefore, we must go from one format to another.
            List<Term> values = new ArrayList<>(entries.size() << 1);
            boolean allTerminal = true;
            for (Pair<Term.Raw, Term.Raw> entry : entries)
            {
                Term k = entry.left.prepare(keyspace, keySpec);
                Term v = entry.right.prepare(keyspace, valueSpec);

                if (k.containsBindMarker() || v.containsBindMarker())
                    throw new InvalidRequestException(String.format("Invalid map literal for %s: bind variables are not supported inside collection literals", receiver.name));

                if (k instanceof Term.NonTerminal || v instanceof Term.NonTerminal)
                    allTerminal = false;

                values.add(k);
                values.add(v);
            }
            MultiElements.DelayedValue value = new MultiElements.DelayedValue((MultiElementType<?>) receiver.type.unwrap(), values);
            return allTerminal ? value.bind(QueryOptions.DEFAULT) : value;
        }

        private void validateAssignableTo(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            AbstractType<?> type = receiver.type.unwrap();

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
            MapType<?, ?> type = (MapType<?, ?>) column.type;

            if (value == null)
            {
                // for frozen maps, we're overwriting the whole cell
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
                Iterator<ByteBuffer> iter = elements.iterator();
                while(iter.hasNext())
                {
                    Cell<?> cell = params.addCell(column, CellPath.create(iter.next()), iter.next());
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

            params.addTombstone(column, CellPath.create(key.get()));
        }
    }
}
