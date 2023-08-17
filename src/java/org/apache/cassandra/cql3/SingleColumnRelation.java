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

import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.cassandra.cql3.terms.Constants;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.cql3.terms.Terms;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
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
 * A filter on a column values.
 */
public final class SingleColumnRelation extends Relation
{
    private final ColumnIdentifier column;
    private final Term.Raw mapKey;
    private final Term.Raw value;
    private final Terms.Raw inValues;

    private SingleColumnRelation(ColumnIdentifier column,
                                 @Nullable Term.Raw mapKey,
                                 Operator operator,
                                 @Nullable Term.Raw value,
                                 @Nullable Terms.Raw inValues)
    {
        // If mapKey is not null the Operator should be EQ
        assert mapKey == null || operator == Operator.EQ;

        this.column = column;
        this.mapKey = mapKey;
        this.operator = operator;
        this.value = value;
        this.inValues = inValues;

        assert operator != Operator.IS_NOT || value == Constants.NULL_LITERAL;
    }

    /**
     * Creates a new relation.
     *
     * @param column the column that need to be filtered
     * @param mapKey the map key used for filtering map column using key value
     * @param operator the operator used to perform the filtering
     * @param value the value to which the column values must be compared
     */
    public SingleColumnRelation(ColumnIdentifier column, Term.Raw mapKey, Operator operator, Term.Raw value)
    {
        this(column, mapKey, operator, value, null);
    }

    /**
     * Creates a new relation.
     *
     * @param column the column that need to be filtered
     * @param operator the operator used to perform the filtering
     * @param value the value to which the column values must be compared
     */
    public SingleColumnRelation(ColumnIdentifier column, Operator operator, Term.Raw value)
    {
        this(column, null, operator, value);
    }

     /**
     * Creates a new relation.
     *
     * @param column the column that need to be filtered
     * @param operator the operator used to perform the filtering
     * @param inValues the IN value being compared.
     */
    public SingleColumnRelation(ColumnIdentifier column, Operator operator, Terms.Raw inValues)
    {
        this(column, null, operator, null, inValues);
    }

    public Term.Raw getValue()
    {
        return value;
    }

    public Terms.Raw getInValues()
    {
        return inValues;
    }

    public static SingleColumnRelation createInRelation(ColumnIdentifier column, Terms.Raw inValues)
    {
        return new SingleColumnRelation(column, null, Operator.IN, null, inValues);
    }

    public Relation renameIdentifier(ColumnIdentifier from, ColumnIdentifier to)
    {
        return column.equals(from)
               ? new SingleColumnRelation(to, mapKey, operator(), value, inValues)
               : this;
    }

    @Override
    public String toCQLString()
    {
        String columnAsString = column.toCQLString();
        if (mapKey != null)
            columnAsString = String.format("%s[%s]", columnAsString, mapKey);

        if (isIN())
            return String.format("%s IN %s", columnAsString, inValues);

        return String.format("%s %s %s", columnAsString, operator, value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operator, column, mapKey, value, inValues);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof SingleColumnRelation))
            return false;

        SingleColumnRelation scr = (SingleColumnRelation) o;
        return Objects.equals(column, scr.column)
               && operator == scr.operator
               && Objects.equals(mapKey, scr.mapKey)
               && Objects.equals(value, scr.value)
               && Objects.equals(inValues, scr.inValues);
    }

    @Override
    protected Restriction newEQRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        ColumnMetadata column = table.getExistingColumn(this.column);
        validateReceiver(column);
        if (mapKey == null)
        {
            Term term = toTerm(column, value, table.keyspace, boundNames);
            return new SingleColumnRestriction.EQRestriction(column, term);
        }

        checkFalse(column.type instanceof ListType, "Indexes on list entries (%s[index] = value) are not supported.", column.name);
        checkTrue(column.type instanceof MapType, "Column %s cannot be used as a map", column.name);
        checkTrue(column.type.isMultiCell(), "Map-entry equality predicates on frozen map column %s are not supported", column.name);

        Term entryKey = toTerm(makeCollectionReceiver(column, true), mapKey, table.keyspace, boundNames);
        Term entryValue = toTerm(makeCollectionReceiver(column, false), value, table.keyspace, boundNames);
        return new SingleColumnRestriction.ContainsRestriction(column, entryKey, entryValue);
    }

    @Override
    protected Restriction newINRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        ColumnMetadata column = table.getExistingColumn(this.column);
        validateReceiver(column);
        Terms terms = toTerms(column, inValues, table.keyspace, boundNames);

        // An IN restriction with only one element is the same as an EQ restriction
        if (terms.containsSingleTerm())
            return new SingleColumnRestriction.EQRestriction(column, terms.asSingleTerm());

        return new SingleColumnRestriction.INRestriction(column, terms);
    }

    @Override
    protected Restriction newSliceRestriction(TableMetadata table,
                                              VariableSpecifications boundNames,
                                              Bound bound,
                                              boolean inclusive)
    {
        ColumnMetadata columnDef = table.getExistingColumn(column);

        if (columnDef.type.referencesDuration())
        {
            checkFalse(columnDef.type.isCollection(), "Slice restrictions are not supported on collections containing durations");
            checkFalse(columnDef.type.isTuple(), "Slice restrictions are not supported on tuples containing durations");
            checkFalse(columnDef.type.isUDT(), "Slice restrictions are not supported on UDTs containing durations");
            throw invalidRequest("Slice restrictions are not supported on duration columns");
        }
        validateReceiver(columnDef);

        Term term = toTerm(columnDef, value, table.keyspace, boundNames);
        return new SingleColumnRestriction.SliceRestriction(columnDef, bound, inclusive, term);
    }

    @Override
    protected Restriction newContainsRestriction(TableMetadata table,
                                                 VariableSpecifications boundNames,
                                                 boolean isKey) throws InvalidRequestException
    {
        ColumnMetadata column = table.getExistingColumn(this.column);

        checkFalse(isContainsKey() && !(column.type instanceof MapType), "Cannot use CONTAINS KEY on non-map column %s", column.name);
        checkFalse(isContains() && !(column.type.isCollection()), "Cannot use CONTAINS on non-collection column %s", column.name);
        validateReceiver(column);

        ColumnSpecification receiver = makeCollectionReceiver(column, isKey);

        Term term = toTerm(receiver, value, table.keyspace, boundNames);
        return new SingleColumnRestriction.ContainsRestriction(column, term, isKey);
    }

    @Override
    protected Restriction newIsNotRestriction(TableMetadata table,
                                              VariableSpecifications boundNames) throws InvalidRequestException
    {
        ColumnMetadata columnDef = table.getExistingColumn(column);
        // currently enforced by the grammar
        assert value == Constants.NULL_LITERAL : "Expected null literal for IS NOT relation: " + this.toString();
        return new SingleColumnRestriction.IsNotNullRestriction(columnDef);
    }

    @Override
    protected Restriction newLikeRestriction(TableMetadata table, VariableSpecifications boundNames, Operator operator)
    {
        ColumnMetadata column = table.getExistingColumn(this.column);
        validateReceiver(column);
        Term term = toTerm(column, value, table.keyspace, boundNames);

        return new SingleColumnRestriction.LikeRestriction(column, operator, term);
    }

    /**
     * Validate the receiving column for this relation.
     * @param receiver the column definition
     * @throws InvalidRequestException if the relation is invalid
     */
    private void validateReceiver(ColumnMetadata receiver) throws InvalidRequestException
    {
        if (receiver.type.isMultiCell())
        {
            // Non-frozen UDTs don't support any operator
            checkFalse(receiver.type.isUDT(),
                       "Non-frozen UDT column '%s' (%s) cannot be restricted by any relation",
                       receiver.name,
                       receiver.type.asCQL3Type());

            // We don't support relations against entire collections (unless they're frozen), like "numbers = {1, 2, 3}"
            checkFalse(receiver.type.isCollection() && !isLegalRelationForNonFrozenCollection(),
                       "Collection column '%s' (%s) cannot be restricted by a '%s' relation",
                       receiver.name,
                       receiver.type.asCQL3Type(),
                       operator());
        }
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
}
