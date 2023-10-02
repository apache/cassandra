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

import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.Term.Raw;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * Relations encapsulate the relationship between an entity of some kind, and
 * a value (term). For example, {@code <key> > "start" or "colname1" = "somevalue"}.
 *
 */
public final class SingleColumnRelation extends Relation
{
    private final ColumnIdentifier entity;
    private final Term.Raw mapKey;
    private final Term.Raw value;
    private final List<Term.Raw> inValues;

    private SingleColumnRelation(ColumnIdentifier entity, Term.Raw mapKey, Operator type, Term.Raw value, List<Term.Raw> inValues)
    {
        this.entity = entity;
        this.mapKey = mapKey;
        this.relationType = type;
        this.value = value;
        this.inValues = inValues;

        if (type == Operator.IS_NOT)
            assert value == Constants.NULL_LITERAL;
    }

    /**
     * Creates a new relation.
     *
     * @param entity the kind of relation this is; what the term is being compared to.
     * @param mapKey the key into the entity identifying the value the term is being compared to.
     * @param type the type that describes how this entity relates to the value.
     * @param value the value being compared.
     */
    public SingleColumnRelation(ColumnIdentifier entity, Term.Raw mapKey, Operator type, Term.Raw value)
    {
        this(entity, mapKey, type, value, null);
    }

    /**
     * Creates a new relation.
     *
     * @param entity the kind of relation this is; what the term is being compared to.
     * @param type the type that describes how this entity relates to the value.
     * @param value the value being compared.
     */
    public SingleColumnRelation(ColumnIdentifier entity, Operator type, Term.Raw value)
    {
        this(entity, null, type, value);
    }

    public Term.Raw getValue()
    {
        return value;
    }

    public List<? extends Term.Raw> getInValues()
    {
        return inValues;
    }

    public static SingleColumnRelation createInRelation(ColumnIdentifier entity, List<Term.Raw> inValues)
    {
        return new SingleColumnRelation(entity, null, Operator.IN, null, inValues);
    }

    public ColumnIdentifier getEntity()
    {
        return entity;
    }

    public Term.Raw getMapKey()
    {
        return mapKey;
    }

    @Override
    protected Term toTerm(List<? extends ColumnSpecification> receivers,
                          Raw raw,
                          String keyspace,
                          VariableSpecifications boundNames)
                          throws InvalidRequestException
    {
        assert receivers.size() == 1;

        Term term = raw.prepare(keyspace, receivers.get(0));
        term.collectMarkerSpecification(boundNames);
        return term;
    }

    public SingleColumnRelation withNonStrictOperator()
    {
        switch (relationType)
        {
            case GT: return new SingleColumnRelation(entity, Operator.GTE, value);
            case LT: return new SingleColumnRelation(entity, Operator.LTE, value);
            default: return this;
        }
    }

    public Relation renameIdentifier(ColumnIdentifier from, ColumnIdentifier to)
    {
        return entity.equals(from)
               ? new SingleColumnRelation(to, mapKey, operator(), value, inValues)
               : this;
    }

    @Override
    public String toCQLString()
    {
        String entityAsString = entity.toCQLString();
        if (mapKey != null)
            entityAsString = String.format("%s[%s]", entityAsString, mapKey);

        if (isIN())
            return String.format("%s IN %s", entityAsString, Tuples.tupleToString(inValues));

        return String.format("%s %s %s", entityAsString, relationType, value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(relationType, entity, mapKey, value, inValues);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof SingleColumnRelation))
            return false;

        SingleColumnRelation scr = (SingleColumnRelation) o;
        return Objects.equals(entity, scr.entity)
            && Objects.equals(relationType, scr.relationType)
            && Objects.equals(mapKey, scr.mapKey)
            && Objects.equals(value, scr.value)
            && Objects.equals(inValues, scr.inValues);
    }

    @Override
    protected Restriction newEQRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        ColumnMetadata columnDef = table.getExistingColumn(entity);
        if (mapKey == null)
        {
            Term term = toTerm(toReceivers(columnDef), value, table.keyspace, boundNames);
            return new SingleColumnRestriction.EQRestriction(columnDef, term);
        }
        List<? extends ColumnSpecification> receivers = toReceivers(columnDef);
        Term entryKey = toTerm(Collections.singletonList(receivers.get(0)), mapKey, table.keyspace, boundNames);
        Term entryValue = toTerm(Collections.singletonList(receivers.get(1)), value, table.keyspace, boundNames);
        return new SingleColumnRestriction.ContainsRestriction(columnDef, entryKey, entryValue);
    }

    @Override
    protected Restriction newINRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        ColumnMetadata columnDef = table.getExistingColumn(entity);
        List<? extends ColumnSpecification> receivers = toReceivers(columnDef);
        List<Term> terms = toTerms(receivers, inValues, table.keyspace, boundNames);
        if (terms == null)
        {
            Term term = toTerm(receivers, value, table.keyspace, boundNames);
            return new SingleColumnRestriction.InRestrictionWithMarker(columnDef, (Lists.Marker) term);
        }

        // An IN restrictions with only one element is the same than an EQ restriction
        if (terms.size() == 1)
            return new SingleColumnRestriction.EQRestriction(columnDef, terms.get(0));

        return new SingleColumnRestriction.InRestrictionWithValues(columnDef, terms);
    }

    @Override
    protected Restriction newSliceRestriction(TableMetadata table,
                                              VariableSpecifications boundNames,
                                              Bound bound,
                                              boolean inclusive)
    {
        ColumnMetadata columnDef = table.getExistingColumn(entity);

        if (columnDef.type.referencesDuration())
        {
            checkFalse(columnDef.type.isCollection(), "Slice restrictions are not supported on collections containing durations");
            checkFalse(columnDef.type.isTuple(), "Slice restrictions are not supported on tuples containing durations");
            checkFalse(columnDef.type.isUDT(), "Slice restrictions are not supported on UDTs containing durations");
            throw invalidRequest("Slice restrictions are not supported on duration columns");
        }

        Term term = toTerm(toReceivers(columnDef), value, table.keyspace, boundNames);
        return new SingleColumnRestriction.SliceRestriction(columnDef, bound, inclusive, term);
    }

    @Override
    protected Restriction newContainsRestriction(TableMetadata table,
                                                 VariableSpecifications boundNames,
                                                 boolean isKey) throws InvalidRequestException
    {
        ColumnMetadata columnDef = table.getExistingColumn(entity);
        Term term = toTerm(toReceivers(columnDef), value, table.keyspace, boundNames);
        return new SingleColumnRestriction.ContainsRestriction(columnDef, term, isKey);
    }

    @Override
    protected Restriction newIsNotRestriction(TableMetadata table,
                                              VariableSpecifications boundNames) throws InvalidRequestException
    {
        ColumnMetadata columnDef = table.getExistingColumn(entity);
        // currently enforced by the grammar
        assert value == Constants.NULL_LITERAL : "Expected null literal for IS NOT relation: " + this.toString();
        return new SingleColumnRestriction.IsNotNullRestriction(columnDef);
    }

    @Override
    protected Restriction newLikeRestriction(TableMetadata table, VariableSpecifications boundNames, Operator operator)
    {
        if (mapKey != null)
            throw invalidRequest("%s can't be used with collections.", operator());

        ColumnMetadata columnDef = table.getExistingColumn(entity);
        Term term = toTerm(toReceivers(columnDef), value, table.keyspace, boundNames);

        return new SingleColumnRestriction.LikeRestriction(columnDef, operator, term);
    }

    /**
     * Returns the receivers for this relation.
     * @param columnDef the column definition
     * @return the receivers for the specified relation.
     * @throws InvalidRequestException if the relation is invalid
     */
    private List<? extends ColumnSpecification> toReceivers(ColumnMetadata columnDef) throws InvalidRequestException
    {
        ColumnSpecification receiver = columnDef;

        checkFalse(isContainsKey() && !(receiver.type instanceof MapType), "Cannot use CONTAINS KEY on non-map column %s", receiver.name);
        checkFalse(isContains() && !(receiver.type.isCollection()), "Cannot use CONTAINS on non-collection column %s", receiver.name);

        if (mapKey != null)
        {
            checkFalse(receiver.type instanceof ListType, "Indexes on list entries (%s[index] = value) are not currently supported.", receiver.name);
            checkTrue(receiver.type instanceof MapType, "Column %s cannot be used as a map", receiver.name);
            checkTrue(receiver.type.isMultiCell(), "Map-entry equality predicates on frozen map column %s are not supported", receiver.name);
            checkTrue(isEQ(), "Only EQ relations are supported on map entries");
        }

        // Non-frozen UDTs don't support any operator
        checkFalse(receiver.type.isUDT() && receiver.type.isMultiCell(),
                   "Non-frozen UDT column '%s' (%s) cannot be restricted by any relation",
                   receiver.name,
                   receiver.type.asCQL3Type());

        if (receiver.type.isCollection())
        {
            // We don't support relations against entire collections (unless they're frozen), like "numbers = {1, 2, 3}"
            checkFalse(receiver.type.isMultiCell() && !isLegalRelationForNonFrozenCollection(),
                       "Collection column '%s' (%s) cannot be restricted by a '%s' relation",
                       receiver.name,
                       receiver.type.asCQL3Type(),
                       operator());

            if (isContainsKey() || isContains())
            {
                receiver = makeCollectionReceiver(receiver, isContainsKey());
            }
            else if (receiver.type.isMultiCell() && mapKey != null && isEQ())
            {
                List<ColumnSpecification> receivers = new ArrayList<>(2);
                receivers.add(makeCollectionReceiver(receiver, true));
                receivers.add(makeCollectionReceiver(receiver, false));
                return receivers;
            }
        }

        return Collections.singletonList(receiver);
    }

    private static ColumnSpecification makeCollectionReceiver(ColumnSpecification receiver, boolean forKey)
    {
        return ((CollectionType<?>) receiver.type).makeCollectionReceiver(receiver, forKey);
    }

    private boolean isLegalRelationForNonFrozenCollection()
    {
        return isContainsKey() || isContains() || isMapEntryEquality();
    }

    private boolean isMapEntryEquality()
    {
        return mapKey != null && isEQ();
    }

    private boolean canHaveOnlyOneValue()
    {
        return isEQ() || isLIKE() || (isIN() && inValues != null && inValues.size() == 1);
    }
}
