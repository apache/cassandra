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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * A CQL3 condition.
 */
public class ColumnCondition
{
    public final CFDefinition.Name column;
    private final Term value;

    private ColumnCondition(CFDefinition.Name column, Term value)
    {
        this.column = column;
        this.value = value;
    }

    // The only ones we support so far
    public static ColumnCondition equal(CFDefinition.Name column, Term value)
    {
        return new ColumnCondition(column, value);
    }

    /**
     * Collects the column specification for the bind variables of this operation.
     *
     * @param boundNames the list of column specification where to collect the
     * bind variables of this term in.
     */
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        value.collectMarkerSpecification(boundNames);
    }

    public ColumnCondition.WithVariables with(List<ByteBuffer> variables)
    {
        return new WithVariables(variables);
    }

    public class WithVariables
    {
        private final List<ByteBuffer> variables;

        private WithVariables(List<ByteBuffer> variables)
        {
            this.variables = variables;
        }

        // Not overriding equals() because we need the variables to have been attached when this is
        // called and so having a non standard method name might help avoid mistakes
        public boolean equalsTo(WithVariables other) throws InvalidRequestException
        {
            return column.equals(other.column())
                && value.bindAndGet(variables).equals(other.value().bindAndGet(other.variables));
        }

        private CFDefinition.Name column()
        {
            return column;
        }

        private Term value()
        {
            return value;
        }

        private ColumnNameBuilder copyOrUpdatePrefix(CFMetaData cfm, ColumnNameBuilder rowPrefix)
        {
            return column.kind == CFDefinition.Name.Kind.STATIC ? cfm.getStaticColumnNameBuilder() : rowPrefix.copy();
        }

        /**
         * Validates whether this condition applies to {@code current}.
         */
        public boolean appliesTo(ColumnNameBuilder rowPrefix, ColumnFamily current, long now) throws InvalidRequestException
        {
            if (column.type instanceof CollectionType)
                return collectionAppliesTo((CollectionType)column.type, rowPrefix, current, now);

            ColumnNameBuilder prefix = copyOrUpdatePrefix(current.metadata(), rowPrefix);
            ByteBuffer columnName = column.kind == CFDefinition.Name.Kind.VALUE_ALIAS
                                  ? prefix.build()
                                  : prefix.add(column.name.key).build();

            Column c = current.getColumn(columnName);
            ByteBuffer v = value.bindAndGet(variables);
            return v == null
                 ? c == null || !c.isLive(now)
                 : c != null && c.isLive(now) && column.type.compare(c.value(), v) == 0;
        }

        private boolean collectionAppliesTo(CollectionType type, ColumnNameBuilder rowPrefix, ColumnFamily current, final long now) throws InvalidRequestException
        {
            ColumnNameBuilder collectionPrefix = copyOrUpdatePrefix(current.metadata(), rowPrefix).add(column.name.key);
            // We are testing for collection equality, so we need to have the expected values *and* only those.
            ColumnSlice[] collectionSlice = new ColumnSlice[]{ new ColumnSlice(collectionPrefix.build(), collectionPrefix.buildAsEndOfRange()) };
            // Filter live columns, this makes things simpler afterwards
            Iterator<Column> iter = Iterators.filter(current.iterator(collectionSlice), new Predicate<Column>()
            {
                public boolean apply(Column c)
                {
                    // we only care about live columns
                    return c.isLive(now);
                }
            });

            Term.Terminal v = value.bind(variables);
            if (v == null)
                return !iter.hasNext();

            switch (type.kind)
            {
                case LIST: return listAppliesTo((ListType)type, current.metadata(), iter, ((Lists.Value)v).elements);
                case SET: return setAppliesTo((SetType)type, current.metadata(), iter, ((Sets.Value)v).elements);
                case MAP: return mapAppliesTo((MapType)type, current.metadata(), iter, ((Maps.Value)v).map);
            }
            throw new AssertionError();
        }

        private ByteBuffer collectionKey(CFMetaData cfm, Column c)
        {
            ByteBuffer[] bbs = ((CompositeType)cfm.comparator).split(c.name());
            return bbs[bbs.length - 1];
        }

        private boolean listAppliesTo(ListType type, CFMetaData cfm, Iterator<Column> iter, List<ByteBuffer> elements)
        {
            for (ByteBuffer e : elements)
                if (!iter.hasNext() || type.elements.compare(iter.next().value(), e) != 0)
                    return false;
            // We must not have more elements than expected
            return !iter.hasNext();
        }

        private boolean setAppliesTo(SetType type, CFMetaData cfm, Iterator<Column> iter, Set<ByteBuffer> elements)
        {
            Set<ByteBuffer> remaining = new TreeSet<>(type.elements);
            remaining.addAll(elements);
            while (iter.hasNext())
            {
                if (remaining.isEmpty())
                    return false;

                if (!remaining.remove(collectionKey(cfm, iter.next())))
                    return false;
            }
            return remaining.isEmpty();
        }

        private boolean mapAppliesTo(MapType type, CFMetaData cfm, Iterator<Column> iter, Map<ByteBuffer, ByteBuffer> elements)
        {
            Map<ByteBuffer, ByteBuffer> remaining = new TreeMap<>(type.keys);
            remaining.putAll(elements);
            while (iter.hasNext())
            {
                if (remaining.isEmpty())
                    return false;

                Column c = iter.next();
                ByteBuffer previous = remaining.remove(collectionKey(cfm, c));
                if (previous == null || type.values.compare(previous, c.value()) != 0)
                    return false;
            }
            return remaining.isEmpty();
        }
    }

    public static class Raw
    {
        private final Term.Raw value;

        public Raw(Term.Raw value)
        {
            this.value = value;
        }

        public ColumnCondition prepare(CFDefinition.Name receiver) throws InvalidRequestException
        {
            if (receiver.type instanceof CounterColumnType)
                throw new InvalidRequestException("Condtions on counters are not supported");

            return ColumnCondition.equal(receiver, value.prepare(receiver));
        }
    }
}
