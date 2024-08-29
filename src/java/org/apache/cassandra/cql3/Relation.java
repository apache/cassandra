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

import java.util.List;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cql3.restrictions.SimpleRestriction;
import org.apache.cassandra.cql3.restrictions.SingleRestriction;
import org.apache.cassandra.cql3.terms.*;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.statements.RequestValidations.*;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

/**
 * The parsed version of a {@code SimpleRestriction} as outputed by the CQL parser.
 * {@code Relation.prepare} will be called upon schema binding to create a {@code SimpleRestriction}.
 */
public final class Relation
{
    /**
     * The raw columns'expression.
     */
    private final ColumnsExpression.Raw rawExpressions;

    /**
     * The relation operator
     */
    private final Operator operator;

    /**
     * The raw terms.
     */
    private final Terms.Raw rawTerms;

    private Relation(ColumnsExpression.Raw rawExpressions, Operator operator, Terms.Raw rawTerms)
    {
        this.rawExpressions = rawExpressions;
        this.operator = operator;
        this.rawTerms = rawTerms;
    }

    public Operator operator()
    {
        return operator;
    }

    /**
     * Creates a relation for a single column (e.g. {@code columnA = ?} ).
     *
     * @param identifier the column identifier to which the relation applies
     * @param operator the relation operator
     * @param rawTerm the term to which the column values must be compared
     * @return a relation for a single column.
     */
    public static Relation singleColumn(ColumnIdentifier identifier, Operator operator, Term.Raw rawTerm)
    {
        assert operator.kind() == Operator.Kind.BINARY;
        return new Relation(ColumnsExpression.Raw.singleColumn(identifier), operator, Terms.Raw.of(rawTerm));
    }

    /**
     * Creates a relation for a single column (e.g. {@code columnA IN ?} ).
     *
     * @param identifier the column identifier to which the relation applies
     * @param operator the relation operator
     * @param rawTerms the terms to which the column values must be compared
     * @return a relation for a single column.
     */
    public static Relation singleColumn(ColumnIdentifier identifier, Operator operator, Terms.Raw rawTerms)
    {
        assert operator.kind() != Operator.Kind.BINARY;
        return new Relation(ColumnsExpression.Raw.singleColumn(identifier), operator, rawTerms);
    }

    /**
     * Creates a relation for a map element (e.g. {@code columnA[?] = ?}).
     *
     * @param identifier the map column identifier
     * @param rawKey the map element key (we do not support list elements in relations yet)
     * @param operator the relation operator
     * @param rawTerm the term to which the map element must be compared
     * @return a relation for a map element.
     */
    @VisibleForTesting
    static Relation mapElement(ColumnIdentifier identifier, Term.Raw rawKey, Operator operator, Term.Raw rawTerm)
    {
        assert operator.kind() == Operator.Kind.BINARY;
        return new Relation(ColumnsExpression.Raw.collectionElement(identifier, rawKey), operator, Terms.Raw.of(rawTerm));
    }

    /**
     * Creates a relation for multiple columns (e.g. {@code (columnA, columnB) = (?, ?)}).
     *
     * @param identifiers the columns identifiers
     * @param operator the relation operator
     * @param rawTerm the term (tuple) to which the multiple columns must be compared
     * @return a relation for multiple columns.
     */
    public static Relation multiColumn(List<ColumnIdentifier> identifiers, Operator operator, Term.Raw rawTerm)
    {
        assert operator.kind() == Operator.Kind.BINARY;
        return new Relation(ColumnsExpression.Raw.multiColumn(identifiers), operator, Terms.Raw.of(rawTerm));
    }

    /**
     * Creates a relation for multiple columns (e.g. {@code (columnA, columnB) = (?, ?)}).
     *
     * @param identifiers the columns identifiers
     * @param operator the relation operator
     * @param rawTerms the terms (tuples) to which the multiple columns must be compared
     * @return a relation for multiple columns.
     */
    public static Relation multiColumn(List<ColumnIdentifier> identifiers, Operator operator, Terms.Raw rawTerms)
    {
        assert operator.kind() != Operator.Kind.BINARY;
        return new Relation(ColumnsExpression.Raw.multiColumn(identifiers), operator, rawTerms);
    }

    /**
     * Creates a relation for token expression (e.g. {@code token(columnA, columnB) = ?} ).
     *
     * @param identifiers the column identifiers for the partition columns
     * @param operator the relation operator
     * @param rawTerm the terms to which the token value must be compared
     * @return a relation for a token expression.
     */
    public static Relation token(List<ColumnIdentifier> identifiers, Operator operator, Term.Raw rawTerm)
    {
        assert operator.kind() == Operator.Kind.BINARY;
        return new Relation(ColumnsExpression.Raw.token(identifiers), operator, Terms.Raw.of(rawTerm));
    }

    /**
     * Creates a relation for token expression (e.g. {@code token(columnA, columnB) = ?} ).
     *
     * @param identifiers the column identifiers for the partition columns
     * @param operator the relation operator
     * @param rawTerms the terms to which the token value must be compared
     * @return a relation for a token expression.
     */
    public static Relation token(List<ColumnIdentifier> identifiers, Operator operator, Terms.Raw rawTerms)
    {
        assert operator.kind() == Operator.Kind.TERNARY;
        return new Relation(ColumnsExpression.Raw.token(identifiers), operator, rawTerms);
    }

    /**
     * Checks if this relation is a token relation (e.g. <pre>token(a) = token(1)</pre>).
     *
     * @return <code>true</code> if this relation is a token relation, <code>false</code> otherwise.
     */
    public boolean onToken()
    {
        return rawExpressions.kind() == ColumnsExpression.Kind.TOKEN;
    }

    /**
     * Converts this <code>Relation</code> into a <code>Restriction</code>.
     *
     * @param table the table metadata
     * @param boundNames the variables specification where to collect the bind variables
     * @return the <code>Restriction</code> corresponding to this <code>Relation</code>
     * @throws InvalidRequestException if this <code>Relation</code> is not valid
     */
    public SingleRestriction toRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        ColumnsExpression columnsExpression = rawExpressions.prepare(table);

        // TODO support restrictions on list elements as we do in conditions, then we can probably move below validations
        //  to ElementExpression prepare/validateColumns
        if (columnsExpression.isMapElementExpression())
        {
            ColumnMetadata column = columnsExpression.firstColumn();
            checkFalse(column.type instanceof ListType, "Indexes on list entries (%s[index] = value) are not supported.", column.name);
            checkTrue(column.type instanceof MapType, "Column %s cannot be used as a map", column.name);
            checkTrue(column.type.isMultiCell(), "Map-entry predicates on frozen map column %s are not supported", column.name);
            columnsExpression.collectMarkerSpecification(boundNames);
        }

        operator.validateFor(columnsExpression);

        ColumnSpecification receiver = columnsExpression.columnSpecification();
        if (!operator.appliesToColumnValues())
            receiver = ((CollectionType<?>) receiver.type).makeCollectionReceiver(receiver, operator.appliesToMapKeys());

        Terms terms = rawTerms.prepare(table.keyspace, receiver);
        terms.collectMarkerSpecification(boundNames);

        // An IN restriction with only one element is the same as an EQ restriction
        if (operator.isIN() && terms.containsSingleTerm())
            return new SimpleRestriction(columnsExpression, Operator.EQ, terms);

        return new SimpleRestriction(columnsExpression, operator, terms);
    }

    public ColumnIdentifier column()
    {
        return rawExpressions.identifiers().get(0);
    }

    /**
     * Renames an identifier in this Relation, if applicable.
     * @param from the old identifier
     * @param to the new identifier
     * @return this object, if the old identifier is not in the set of entities that this relation covers; otherwise
     *         a new Relation with "from" replaced by "to" is returned.
     */
    public Relation renameIdentifier(ColumnIdentifier from, ColumnIdentifier to)
    {
        return new Relation(rawExpressions.renameIdentifier(from, to), operator, rawTerms);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        Relation relation = (Relation) o;
        return Objects.equals(rawExpressions, relation.rawExpressions)
            && operator == relation.operator
            && Objects.equals(rawTerms, relation.rawTerms);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rawExpressions, operator, rawTerms);
    }

    /**
     * Returns a CQL representation of this relation.
     *
     * @return a CQL representation of this relation
     */
    public String toCQLString()
    {
        return operator.buildCQLString(rawExpressions, rawTerms);
    }

    @Override
    public String toString()
    {
        return toCQLString();
    }
}
