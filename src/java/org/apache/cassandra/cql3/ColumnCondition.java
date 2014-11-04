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

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import static com.google.common.collect.Lists.newArrayList;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A CQL3 condition.
 */
public class ColumnCondition
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnCondition.class);

    public final ColumnDefinition column;

    // For collection, when testing the equality of a specific element, null otherwise.
    private final Term collectionElement;

    private final Term value;  // a single value or a marker for a list of IN values
    private final List<Term> inValues;

    public final Operator operator;

    private ColumnCondition(ColumnDefinition column, Term collectionElement, Term value, List<Term> inValues, Operator op)
    {
        this.column = column;
        this.collectionElement = collectionElement;
        this.value = value;
        this.inValues = inValues;
        this.operator = op;

        if (!operator.equals(Operator.IN))
            assert this.inValues == null;
    }

    public static ColumnCondition condition(ColumnDefinition column, Term value, Operator op)
    {
        return new ColumnCondition(column, null, value, null, op);
    }

    public static ColumnCondition condition(ColumnDefinition column, Term collectionElement, Term value, Operator op)
    {
        return new ColumnCondition(column, collectionElement, value, null, op);
    }

    public static ColumnCondition inCondition(ColumnDefinition column, List<Term> inValues)
    {
        return new ColumnCondition(column, null, null, inValues, Operator.IN);
    }

    public static ColumnCondition inCondition(ColumnDefinition column, Term collectionElement, List<Term> inValues)
    {
        return new ColumnCondition(column, collectionElement, null, inValues, Operator.IN);
    }

    public static ColumnCondition inCondition(ColumnDefinition column, Term inMarker)
    {
        return new ColumnCondition(column, null, inMarker, null, Operator.IN);
    }

    public static ColumnCondition inCondition(ColumnDefinition column, Term collectionElement, Term inMarker)
    {
        return new ColumnCondition(column, collectionElement, inMarker, null, Operator.IN);
    }

    /**
     * Collects the column specification for the bind variables of this operation.
     *
     * @param boundNames the list of column specification where to collect the
     * bind variables of this term in.
     */
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        if (collectionElement != null)
            collectionElement.collectMarkerSpecification(boundNames);

        if (operator.equals(Operator.IN) && inValues != null)
        {
            for (Term value : inValues)
                value.collectMarkerSpecification(boundNames);
        }
        else
        {
            value.collectMarkerSpecification(boundNames);
        }
    }

    public ColumnCondition.Bound bind(QueryOptions options) throws InvalidRequestException
    {
        boolean isInCondition = operator.equals(Operator.IN);
        if (column.type instanceof CollectionType)
        {
            if (collectionElement == null)
                return isInCondition ? new CollectionInBound(this, options) : new CollectionBound(this, options);
            else
                return isInCondition ? new ElementAccessInBound(this, options) : new ElementAccessBound(this, options);
        }
        return isInCondition ? new SimpleInBound(this, options) : new SimpleBound(this, options);
    }

    public static abstract class Bound
    {
        public final ColumnDefinition column;
        public final Operator operator;

        protected Bound(ColumnDefinition column, Operator operator)
        {
            this.column = column;
            this.operator = operator;
        }

        /**
         * Validates whether this condition applies to {@code current}.
         */
        public abstract boolean appliesTo(Composite rowPrefix, ColumnFamily current, long now) throws InvalidRequestException;

        public ByteBuffer getCollectionElementValue()
        {
            return null;
        }

        protected boolean isSatisfiedByValue(ByteBuffer value, Cell c, AbstractType<?> type, Operator operator, long now) throws InvalidRequestException
        {
            ByteBuffer columnValue = (c == null || !c.isLive(now)) ? null : c.value();
            return compareWithOperator(operator, type, value, columnValue);
        }

        /** Returns true if the operator is satisfied (i.e. "value operator otherValue == true"), false otherwise. */
        protected boolean compareWithOperator(Operator operator, AbstractType<?> type, ByteBuffer value, ByteBuffer otherValue) throws InvalidRequestException
        {
            if (value == null)
            {
                switch (operator)
                {
                    case EQ:
                        return otherValue == null;
                    case NEQ:
                        return otherValue != null;
                    default:
                        throw new InvalidRequestException(String.format("Invalid comparison with null for operator \"%s\"", operator));
                }
            }
            else if (otherValue == null)
            {
                // the condition value is not null, so only NEQ can return true
                return operator.equals(Operator.NEQ);
            }
            int comparison = type.compare(otherValue, value);
            switch (operator)
            {
                case EQ:
                    return comparison == 0;
                case LT:
                    return comparison < 0;
                case LTE:
                    return comparison <= 0;
                case GT:
                    return comparison > 0;
                case GTE:
                    return comparison >= 0;
                case NEQ:
                    return comparison != 0;
                default:
                    // we shouldn't get IN, CONTAINS, or CONTAINS KEY here
                    throw new AssertionError();
            }
        }

        protected Iterator<Cell> collectionColumns(CellName collection, ColumnFamily cf, final long now)
        {
            // We are testing for collection equality, so we need to have the expected values *and* only those.
            ColumnSlice[] collectionSlice = new ColumnSlice[]{ collection.slice() };
            // Filter live columns, this makes things simpler afterwards
            return Iterators.filter(cf.iterator(collectionSlice), new Predicate<Cell>()
            {
                public boolean apply(Cell c)
                {
                    // we only care about live columns
                    return c.isLive(now);
                }
            });
        }
    }

    /**
     * A condition on a single non-collection column. This does not support IN operators (see SimpleInBound).
     */
    static class SimpleBound extends Bound
    {
        public final ByteBuffer value;

        private SimpleBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert !(column.type instanceof CollectionType) && condition.collectionElement == null;
            assert !condition.operator.equals(Operator.IN);
            this.value = condition.value.bindAndGet(options);
        }

        public boolean appliesTo(Composite rowPrefix, ColumnFamily current, long now) throws InvalidRequestException
        {
            CellName name = current.metadata().comparator.create(rowPrefix, column);
            return isSatisfiedByValue(value, current.getColumn(name), column.type, operator, now);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column, value, operator);
        }
    }

    /**
     * An IN condition on a single non-collection column.
     */
    static class SimpleInBound extends Bound
    {
        public final List<ByteBuffer> inValues;

        private SimpleInBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert !(column.type instanceof CollectionType) && condition.collectionElement == null;
            assert condition.operator.equals(Operator.IN);
            if (condition.inValues == null)
                this.inValues = ((Lists.Marker) condition.value).bind(options).getElements();
            else
            {
                this.inValues = new ArrayList<>(condition.inValues.size());
                for (Term value : condition.inValues)
                    this.inValues.add(value.bindAndGet(options));
            }
        }

        public boolean appliesTo(Composite rowPrefix, ColumnFamily current, long now) throws InvalidRequestException
        {
            CellName name = current.metadata().comparator.create(rowPrefix, column);
            for (ByteBuffer value : inValues)
            {
                if (isSatisfiedByValue(value, current.getColumn(name), column.type, Operator.EQ, now))
                    return true;
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column, inValues, operator);
        }
    }

    /** A condition on an element of a collection column. IN operators are not supported here, see ElementAccessInBound. */
    static class ElementAccessBound extends Bound
    {
        public final ByteBuffer collectionElement;
        public final ByteBuffer value;

        private ElementAccessBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert column.type instanceof CollectionType && condition.collectionElement != null;
            assert !condition.operator.equals(Operator.IN);
            this.collectionElement = condition.collectionElement.bindAndGet(options);
            this.value = condition.value.bindAndGet(options);
        }

        public boolean appliesTo(Composite rowPrefix, ColumnFamily current, final long now) throws InvalidRequestException
        {
            if (collectionElement == null)
                throw new InvalidRequestException("Invalid null value for " + (column.type instanceof MapType ? "map" : "list") + " element access");

            if (column.type instanceof MapType)
            {
                Cell cell = current.getColumn(current.metadata().comparator.create(rowPrefix, column, collectionElement));
                return isSatisfiedByValue(value, cell, ((MapType) column.type).values, operator, now);
            }

            // sets don't have element access, so it's a list
            assert column.type instanceof ListType;
            ByteBuffer columnValue = getListItem(
                    collectionColumns(current.metadata().comparator.create(rowPrefix, column), current, now),
                    getListIndex(collectionElement));
            return compareWithOperator(operator, ((ListType)column.type).elements, value, columnValue);
        }

        static int getListIndex(ByteBuffer collectionElement) throws InvalidRequestException
        {
            int idx = ByteBufferUtil.toInt(collectionElement);
            if (idx < 0)
                throw new InvalidRequestException(String.format("Invalid negative list index %d", idx));
            return idx;
        }

        static ByteBuffer getListItem(Iterator<Cell> iter, int index) throws InvalidRequestException
        {
            int adv = Iterators.advance(iter, index);
            if (adv == index && iter.hasNext())
                return iter.next().value();
            else
                return null;
        }

        public ByteBuffer getCollectionElementValue()
        {
            return collectionElement;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column, collectionElement, value, operator);
        }
    }

    static class ElementAccessInBound extends Bound
    {
        public final ByteBuffer collectionElement;
        public final List<ByteBuffer> inValues;

        private ElementAccessInBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert column.type instanceof CollectionType && condition.collectionElement != null;
            this.collectionElement = condition.collectionElement.bindAndGet(options);

            if (condition.inValues == null)
                this.inValues = ((Lists.Marker) condition.value).bind(options).getElements();
            else
            {
                this.inValues = new ArrayList<>(condition.inValues.size());
                for (Term value : condition.inValues)
                    this.inValues.add(value.bindAndGet(options));
            }
        }

        public boolean appliesTo(Composite rowPrefix, ColumnFamily current, final long now) throws InvalidRequestException
        {
            if (collectionElement == null)
                throw new InvalidRequestException("Invalid null value for " + (column.type instanceof MapType ? "map" : "list") + " element access");

            CellNameType nameType = current.metadata().comparator;
            if (column.type instanceof MapType)
            {
                CellName name = nameType.create(rowPrefix, column, collectionElement);
                Cell item = current.getColumn(name);
                AbstractType<?> valueType = ((MapType) column.type).values;
                for (ByteBuffer value : inValues)
                {
                    if (isSatisfiedByValue(value, item, valueType, Operator.EQ, now))
                        return true;
                }
                return false;
            }

            assert column.type instanceof ListType;
            ByteBuffer columnValue = ElementAccessBound.getListItem(
                    collectionColumns(nameType.create(rowPrefix, column), current, now),
                    ElementAccessBound.getListIndex(collectionElement));

            AbstractType<?> valueType = ((ListType) column.type).elements;
            for (ByteBuffer value : inValues)
            {
                if (compareWithOperator(Operator.EQ, valueType, value, columnValue))
                    return true;
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column, collectionElement, inValues, operator);
        }
    }

    /** A condition on an entire collection column. IN operators are not supported here, see CollectionInBound. */
    static class CollectionBound extends Bound
    {
        public final Term.Terminal value;

        private CollectionBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert column.type instanceof CollectionType && condition.collectionElement == null;
            assert !condition.operator.equals(Operator.IN);
            this.value = condition.value.bind(options);
        }

        public boolean appliesTo(Composite rowPrefix, ColumnFamily current, final long now) throws InvalidRequestException
        {
            CollectionType type = (CollectionType)column.type;

            Iterator<Cell> iter = collectionColumns(current.metadata().comparator.create(rowPrefix, column), current, now);
            if (value == null)
            {
                if (operator.equals(Operator.EQ))
                    return !iter.hasNext();
                else if (operator.equals(Operator.NEQ))
                    return iter.hasNext();
                else
                    throw new InvalidRequestException(String.format("Invalid comparison with null for operator \"%s\"", operator));
            }

            return valueAppliesTo(type, iter, value, operator);
        }

        static boolean valueAppliesTo(CollectionType type, Iterator<Cell> iter, Term.Terminal value, Operator operator)
        {
            if (value == null)
                return !iter.hasNext();

            switch (type.kind)
            {
                case LIST: return listAppliesTo((ListType)type, iter, ((Lists.Value)value).elements, operator);
                case SET: return setAppliesTo((SetType)type, iter, ((Sets.Value)value).elements, operator);
                case MAP: return mapAppliesTo((MapType)type, iter, ((Maps.Value)value).map, operator);
            }
            throw new AssertionError();
        }

        private static boolean setOrListAppliesTo(AbstractType<?> type, Iterator<Cell> iter, Iterator<ByteBuffer> conditionIter, Operator operator, boolean isSet)
        {
            while(iter.hasNext())
            {
                if (!conditionIter.hasNext())
                    return operator.equals(Operator.GT) || operator.equals(Operator.GTE) || operator.equals(Operator.NEQ);

                // for lists we use the cell value; for sets we use the cell name
                ByteBuffer cellValue = isSet? iter.next().name().collectionElement() : iter.next().value();
                int comparison = type.compare(cellValue, conditionIter.next());
                if (comparison != 0)
                    return evaluateComparisonWithOperator(comparison, operator);
            }

            if (conditionIter.hasNext())
                return operator.equals(Operator.LT) || operator.equals(Operator.LTE) || operator.equals(Operator.NEQ);

            // they're equal
            return operator == Operator.EQ || operator == Operator.LTE || operator == Operator.GTE;
        }

        private static boolean evaluateComparisonWithOperator(int comparison, Operator operator)
        {
            // called when comparison != 0
            switch (operator)
            {
                case EQ:
                    return false;
                case LT:
                case LTE:
                    return comparison < 0;
                case GT:
                case GTE:
                    return comparison > 0;
                case NEQ:
                    return true;
                default:
                    throw new AssertionError();
            }
        }

        static boolean listAppliesTo(ListType type, Iterator<Cell> iter, List<ByteBuffer> elements, Operator operator)
        {
            return setOrListAppliesTo(type.elements, iter, elements.iterator(), operator, false);
        }

        static boolean setAppliesTo(SetType type, Iterator<Cell> iter, Set<ByteBuffer> elements, Operator operator)
        {
            ArrayList<ByteBuffer> sortedElements = new ArrayList<>(elements.size());
            sortedElements.addAll(elements);
            Collections.sort(sortedElements, type.elements);
            return setOrListAppliesTo(type.elements, iter, sortedElements.iterator(), operator, true);
        }

        static boolean mapAppliesTo(MapType type, Iterator<Cell> iter, Map<ByteBuffer, ByteBuffer> elements, Operator operator)
        {
            Iterator<Map.Entry<ByteBuffer, ByteBuffer>> conditionIter = elements.entrySet().iterator();
            while(iter.hasNext())
            {
                if (!conditionIter.hasNext())
                    return operator.equals(Operator.GT) || operator.equals(Operator.GTE) || operator.equals(Operator.NEQ);

                Map.Entry<ByteBuffer, ByteBuffer> conditionEntry = conditionIter.next();
                Cell c = iter.next();

                // compare the keys
                int comparison = type.keys.compare(c.name().collectionElement(), conditionEntry.getKey());
                if (comparison != 0)
                    return evaluateComparisonWithOperator(comparison, operator);

                // compare the values
                comparison = type.values.compare(c.value(), conditionEntry.getValue());
                if (comparison != 0)
                    return evaluateComparisonWithOperator(comparison, operator);
            }

            if (conditionIter.hasNext())
                return operator.equals(Operator.LT) || operator.equals(Operator.LTE) || operator.equals(Operator.NEQ);

            // they're equal
            return operator == Operator.EQ || operator == Operator.LTE || operator == Operator.GTE;
        }

        @Override
        public int hashCode()
        {
            Object val = null;
            if (value != null)
            {
                switch (((CollectionType)column.type).kind)
                {
                    case LIST:
                        val = ((Lists.Value)value).elements.hashCode();
                        break;
                    case SET:
                        val = ((Sets.Value)value).elements.hashCode();
                        break;
                    case MAP:
                        val = ((Maps.Value)value).map.hashCode();
                        break;
                }
            }
            return Objects.hashCode(column, val);
        }
    }

    public static class CollectionInBound extends Bound
    {
        public final List<Term.Terminal> inValues;

        private CollectionInBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert column.type instanceof CollectionType && condition.collectionElement == null;
            assert condition.operator.equals(Operator.IN);
            inValues = new ArrayList<>();
            if (condition.inValues == null)
            {
                // We have a list of serialized collections that need to be deserialized for later comparisons
                CollectionType collectionType = (CollectionType) column.type;
                Lists.Marker inValuesMarker = (Lists.Marker) condition.value;
                if (column.type instanceof ListType)
                {
                    ListType deserializer = ListType.getInstance(collectionType.valueComparator());
                    for (ByteBuffer buffer : inValuesMarker.bind(options).elements)
                    {
                        if (buffer == null)
                            this.inValues.add(null);
                        else
                            this.inValues.add(Lists.Value.fromSerialized(buffer, deserializer, options.getProtocolVersion()));
                    }
                }
                else if (column.type instanceof MapType)
                {
                    MapType deserializer = MapType.getInstance(collectionType.nameComparator(), collectionType.valueComparator());
                    for (ByteBuffer buffer : inValuesMarker.bind(options).elements)
                    {
                        if (buffer == null)
                            this.inValues.add(null);
                        else
                            this.inValues.add(Maps.Value.fromSerialized(buffer, deserializer, options.getProtocolVersion()));
                    }
                }
                else if (column.type instanceof SetType)
                {
                    SetType deserializer = SetType.getInstance(collectionType.valueComparator());
                    for (ByteBuffer buffer : inValuesMarker.bind(options).elements)
                    {
                        if (buffer == null)
                            this.inValues.add(null);
                        else
                            this.inValues.add(Sets.Value.fromSerialized(buffer, deserializer, options.getProtocolVersion()));
                    }
                }
            }
            else
            {
                for (Term value : condition.inValues)
                    this.inValues.add(value.bind(options));
            }
        }

        public boolean appliesTo(Composite rowPrefix, ColumnFamily current, final long now) throws InvalidRequestException
        {
            CollectionType type = (CollectionType)column.type;
            CellName name = current.metadata().comparator.create(rowPrefix, column);

            // copy iterator contents so that we can properly reuse them for each comparison with an IN value
            List<Cell> cells = newArrayList(collectionColumns(name, current, now));
            for (Term.Terminal value : inValues)
            {
                if (CollectionBound.valueAppliesTo(type, cells.iterator(), value, Operator.EQ))
                    return true;
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            List<Collection<ByteBuffer>> inValueBuffers = new ArrayList<>(inValues.size());
            switch (((CollectionType)column.type).kind)
            {
                case LIST:
                    for (Term.Terminal term : inValues)
                        inValueBuffers.add(term == null ? null : ((Lists.Value)term).elements);
                    break;
                case SET:
                    for (Term.Terminal term : inValues)
                        inValueBuffers.add(term == null ? null : ((Sets.Value)term).elements);
                    break;
                case MAP:
                    for (Term.Terminal term : inValues)
                    {
                        if (term != null)
                        {
                            inValueBuffers.add(((Maps.Value)term).map.keySet());
                            inValueBuffers.add(((Maps.Value)term).map.values());
                        }
                        else
                            inValueBuffers.add(null);
                    }
                    break;
            }
            return Objects.hashCode(column, inValueBuffers, operator);
        }
    }

    public static class Raw
    {
        private final Term.Raw value;
        private final List<Term.Raw> inValues;
        private final AbstractMarker.INRaw inMarker;

        // Can be null, only used with the syntax "IF m[e] = ..." (in which case it's 'e')
        private final Term.Raw collectionElement;

        private final Operator operator;

        private Raw(Term.Raw value, List<Term.Raw> inValues, AbstractMarker.INRaw inMarker, Term.Raw collectionElement, Operator op)
        {
            this.value = value;
            this.inValues = inValues;
            this.inMarker = inMarker;
            this.collectionElement = collectionElement;
            this.operator = op;
        }

        /** A condition on a column. For example: "IF col = 'foo'" */
        public static Raw simpleCondition(Term.Raw value, Operator op)
        {
            return new Raw(value, null, null, null, op);
        }

        /** An IN condition on a column. For example: "IF col IN ('foo', 'bar', ...)" */
        public static Raw simpleInCondition(List<Term.Raw> inValues)
        {
            return new Raw(null, inValues, null, null, Operator.IN);
        }

        /** An IN condition on a column with a single marker. For example: "IF col IN ?" */
        public static Raw simpleInCondition(AbstractMarker.INRaw inMarker)
        {
            return new Raw(null, null, inMarker, null, Operator.IN);
        }

        /** A condition on a collection element. For example: "IF col['key'] = 'foo'" */
        public static Raw collectionCondition(Term.Raw value, Term.Raw collectionElement, Operator op)
        {
            return new Raw(value, null, null, collectionElement, op);
        }

        /** An IN condition on a collection element. For example: "IF col['key'] IN ('foo', 'bar', ...)" */
        public static Raw collectionInCondition(Term.Raw collectionElement, List<Term.Raw> inValues)
        {
            return new Raw(null, inValues, null, collectionElement, Operator.IN);
        }

        /** An IN condition on a collection element with a single marker. For example: "IF col['key'] IN ?" */
        public static Raw collectionInCondition(Term.Raw collectionElement, AbstractMarker.INRaw inMarker)
        {
            return new Raw(null, null, inMarker, collectionElement, Operator.IN);
        }

        public ColumnCondition prepare(String keyspace, ColumnDefinition receiver) throws InvalidRequestException
        {
            if (receiver.type instanceof CounterColumnType)
                throw new InvalidRequestException("Conditions on counters are not supported");

            if (collectionElement == null)
            {
                if (operator.equals(Operator.IN))
                {
                    if (inValues == null)
                        return ColumnCondition.inCondition(receiver, inMarker.prepare(keyspace, receiver));
                    List<Term> terms = new ArrayList<>(inValues.size());
                    for (Term.Raw value : inValues)
                        terms.add(value.prepare(keyspace, receiver));
                    return ColumnCondition.inCondition(receiver, terms);
                }
                else
                {
                    return ColumnCondition.condition(receiver, value.prepare(keyspace, receiver), operator);
                }
            }

            if (!(receiver.type.isCollection()))
                throw new InvalidRequestException(String.format("Invalid element access syntax for non-collection column %s", receiver.name));

            ColumnSpecification elementSpec, valueSpec;
            switch ((((CollectionType)receiver.type).kind))
            {
                case LIST:
                    elementSpec = Lists.indexSpecOf(receiver);
                    valueSpec = Lists.valueSpecOf(receiver);
                    break;
                case MAP:
                    elementSpec = Maps.keySpecOf(receiver);
                    valueSpec = Maps.valueSpecOf(receiver);
                    break;
                case SET:
                    throw new InvalidRequestException(String.format("Invalid element access syntax for set column %s", receiver.name));
                default:
                    throw new AssertionError();
            }
            if (operator.equals(Operator.IN))
            {
                if (inValues == null)
                    return ColumnCondition.inCondition(receiver, collectionElement.prepare(keyspace, elementSpec), inMarker.prepare(keyspace, valueSpec));
                List<Term> terms = new ArrayList<>(inValues.size());
                for (Term.Raw value : inValues)
                    terms.add(value.prepare(keyspace, valueSpec));
                return ColumnCondition.inCondition(receiver, collectionElement.prepare(keyspace, elementSpec), terms);
            }
            else
            {
                return ColumnCondition.condition(receiver, collectionElement.prepare(keyspace, elementSpec), value.prepare(keyspace, valueSpec), operator);
            }
        }
    }
}
