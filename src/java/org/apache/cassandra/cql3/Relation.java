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

import org.apache.cassandra.cql3.restrictions.SimpleRestriction;
import org.apache.cassandra.cql3.restrictions.SingleRestriction;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.cql3.terms.Terms;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

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

    public static Relation singleColumn(ColumnIdentifier identifier, Operator operator, Term.Raw rawTerm)
    {
        return new Relation(ColumnsExpression.Raw.singleColumn(identifier), operator, Terms.Raw.of(rawTerm));
    }

    public static Relation singleColumn(ColumnIdentifier identifier, Operator operator, Terms.Raw rawTerms)
    {
        return new Relation(ColumnsExpression.Raw.singleColumn(identifier), operator, rawTerms);
    }

    public static Relation mapElement(ColumnIdentifier identifier, Term.Raw rawKey, Operator operator, Term.Raw rawTerm)
    {
        return new Relation(ColumnsExpression.Raw.mapElement(identifier, rawKey), operator, Terms.Raw.of(rawTerm));
    }

    public static Relation multiColumns(List<ColumnIdentifier> identifiers, Operator operator, Term.Raw rawTerm)
    {
        return new Relation(ColumnsExpression.Raw.multiColumns(identifiers), operator, Terms.Raw.of(rawTerm));
    }

    public static Relation multiColumns(List<ColumnIdentifier> identifiers, Operator operator, Terms.Raw rawTerms)
    {
        return new Relation(ColumnsExpression.Raw.multiColumns(identifiers), operator, rawTerms);
    }

    public static Relation token(List<ColumnIdentifier> identifiers, Operator operator, Term.Raw rawTerm)
    {
        return new Relation(ColumnsExpression.Raw.token(identifiers), operator, Terms.Raw.of(rawTerm));
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
     * @param table the Column Family meta data
     * @param boundNames the variables specification where to collect the bind variables
     * @return the <code>Restriction</code> corresponding to this <code>Relation</code>
     * @throws InvalidRequestException if this <code>Relation</code> is not valid
     */
    public SingleRestriction toRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        if (operator == Operator.NEQ)
            throw invalidRequest("Unsupported '!=' relation: %s", this);

        ColumnsExpression expression = rawExpressions.prepare(table);
        expression.collectMarkerSpecification(boundNames);

        operator.validateFor(expression);

        ColumnSpecification receiver = expression.columnSpecification(table);
        if (!operator.appliesToColumnValues())
            receiver = ((CollectionType<?>) receiver.type).makeCollectionReceiver(receiver, operator.appliesToMapKeys());

        Terms terms = rawTerms.prepare(table.keyspace, receiver);
        terms.collectMarkerSpecification(boundNames);

        // An IN restriction with only one element is the same as an EQ restriction
        if (operator.isIN() && terms.containsSingleTerm())
            return new SimpleRestriction(expression, Operator.EQ, terms);

        return new SimpleRestriction(expression, operator, terms);
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

    /**
     * Returns a CQL representation of this relation.
     *
     * @return a CQL representation of this relation
     */
    public String toCQLString()
    {
        return String.format("%s %s %s", rawExpressions.toCQLString(), operator, rawTerms.getText());
    }

    @Override
    public String toString()
    {
        return toCQLString();
    }
}
