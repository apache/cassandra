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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.cql3.terms.Terms;
import org.apache.cassandra.cql3.terms.Tuples;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.restrictions.MultiColumnRestriction;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * A relation using the tuple notation, which typically affects multiple columns.
 * Examples:
 * {@code
 *  - SELECT ... WHERE (a, b, c) > (1, 'a', 10)
 *  - SELECT ... WHERE (a, b, c) IN ((1, 2, 3), (4, 5, 6))
 *  - SELECT ... WHERE (a, b) < ?
 *  - SELECT ... WHERE (a, b) IN ?
 * }
 */
public class MultiColumnRelation extends Relation
{
    private final List<ColumnIdentifier> entities;

    /** A literal or raw marker */
    private final Term.Raw value;

    /** A list of literals and/or raw markers or an IN raw marker */
    private final Terms.Raw inValues;

    private MultiColumnRelation(List<ColumnIdentifier> entities, Operator relationType, Term.Raw value, Terms.Raw inValues)
    {
        this.entities = entities;
        this.operator = relationType;
        this.value = value;

        this.inValues = inValues;
    }

    /**
     * Creates a multi-column EQ, LT, LTE, GT, or GTE relation.
     * {@code
     * For example: "SELECT ... WHERE (a, b) > (0, 1)"
     * }
     * @param entities the columns on the LHS of the relation
     * @param relationType the relation operator
     * @param value a literal or raw marker
     * @return a new <code>MultiColumnRelation</code> instance
     */
    public static MultiColumnRelation createNonInRelation(List<ColumnIdentifier> entities, Operator relationType, Term.Raw value)
    {
        assert relationType != Operator.IN;
        return new MultiColumnRelation(entities, relationType, value, null);
    }

    /**
     * Creates a multi-column IN relation with either a marker for the IN values or a list of IN values or markers.
     * For example: "SELECT ... WHERE (a, b) IN ((0, 1), (2, 3))"
     * @param entities the columns on the LHS of the relation
     * @param inValues the IN values as a {@code Terms.Raw}
     * @return a new <code>MultiColumnRelation</code> instance
     */
    public static MultiColumnRelation createInRelation(List<ColumnIdentifier> entities, Terms.Raw inValues)
    {
        return new MultiColumnRelation(entities, Operator.IN, null, inValues);
    }

    public List<ColumnIdentifier> getEntities()
    {
        return entities;
    }

    /**
     * For non-IN relations, returns a literal or a raw marker for a single tuple.
     * @return a literal or a raw marker for non-IN relations.
     */
    public Term.Raw getValue()
    {
        return value;
    }

    public Terms.Raw getInValues()
    {
        assert operator == Operator.IN;
        return inValues;
    }

    @Override
    protected Restriction newEQRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        List<ColumnMetadata> receivers = receivers(table);
        Term term = toTerm(Tuples.makeReceiver(receivers), getValue(), table.keyspace, boundNames);
        return new MultiColumnRestriction.EQRestriction(receivers, term);
    }

    @Override
    protected Restriction newINRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        List<ColumnMetadata> receivers = receivers(table);
        Terms terms = toTerms(Tuples.makeReceiver(receivers), inValues, table.keyspace, boundNames);

        if (terms.containsSingleTerm())
            return new MultiColumnRestriction.EQRestriction(receivers, terms.asSingleTerm());

        return new MultiColumnRestriction.INRestriction(receivers, terms);
    }

    @Override
    protected Restriction newSliceRestriction(TableMetadata table, VariableSpecifications boundNames, Bound bound, boolean inclusive)
    {
        List<ColumnMetadata> receivers = receivers(table);
        Term term = toTerm(Tuples.makeReceiver(receivers), getValue(), table.keyspace, boundNames);
        return new MultiColumnRestriction.SliceRestriction(receivers, bound, inclusive, term);
    }

    @Override
    protected Restriction newContainsRestriction(TableMetadata table, VariableSpecifications boundNames, boolean isKey)
    {
        throw invalidRequest("%s cannot be used for multi-column relations", operator());
    }

    @Override
    protected Restriction newIsNotRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        // this is currently disallowed by the grammar
        throw new AssertionError(String.format("%s cannot be used for multi-column relations", operator()));
    }

    @Override
    protected Restriction newLikeRestriction(TableMetadata table, VariableSpecifications boundNames, Operator operator)
    {
        throw invalidRequest("%s cannot be used for multi-column relations", operator());
    }

    protected List<ColumnMetadata> receivers(TableMetadata table) throws InvalidRequestException
    {
        List<ColumnMetadata> names = new ArrayList<>(getEntities().size());
        int previousPosition = -1;
        for (ColumnIdentifier id : getEntities())
        {
            ColumnMetadata def = table.getExistingColumn(id);
            checkTrue(def.isClusteringColumn(), "Multi-column relations can only be applied to clustering columns but was applied to: %s", def.name);
            checkFalse(names.contains(def), "Column \"%s\" appeared twice in a relation: %s", def.name, this);

            // check that no clustering columns were skipped
            checkFalse(previousPosition != -1 && def.position() != previousPosition + 1,
                       "Clustering columns must appear in the PRIMARY KEY order in multi-column relations: %s", this);

            names.add(def);
            previousPosition = def.position();
        }
        return names;
    }

    @Override
    public Relation renameIdentifier(ColumnIdentifier from, ColumnIdentifier to)
    {
        if (!entities.contains(from))
            return this;

        List<ColumnIdentifier> newEntities = entities.stream().map(e -> e.equals(from) ? to : e).collect(Collectors.toList());
        return new MultiColumnRelation(newEntities, operator(), value, inValues);
    }

    @Override
    public String toCQLString()
    {
        StringBuilder builder = new StringBuilder(Tuples.tupleToString(entities, ColumnIdentifier::toCQLString));
        if (isIN())
        {
            return builder.append(" IN ")
                          .append(inValues.getText())
                          .toString();
        }
        return builder.append(' ')
                      .append(operator)
                      .append(' ')
                      .append(value)
                      .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operator, entities, value, inValues);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof MultiColumnRelation))
            return false;

        MultiColumnRelation mcr = (MultiColumnRelation) o;
        return Objects.equals(entities, mcr.entities)
               && operator == mcr.operator
               && Objects.equals(value, mcr.value)
               && Objects.equals(inValues, mcr.inValues);
    }
}
