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

/**
 * A relation using the tuple notation, which typically affects multiple columns.
 * Examples:
 *  - SELECT ... WHERE (a, b, c) > (1, 'a', 10)
 *  - SELECT ... WHERE (a, b, c) IN ((1, 2, 3), (4, 5, 6))
 *  - SELECT ... WHERE (a, b) < ?
 *  - SELECT ... WHERE (a, b) IN ?
 */
public class MultiColumnRelation extends Relation
{
    private final List<ColumnIdentifier.Raw> entities;

    /** A Tuples.Literal or Tuples.Raw marker */
    private final Term.MultiColumnRaw valuesOrMarker;

    /** A list of Tuples.Literal or Tuples.Raw markers */
    private final List<? extends Term.MultiColumnRaw> inValues;

    private final Tuples.INRaw inMarker;

    private MultiColumnRelation(List<ColumnIdentifier.Raw> entities, Operator relationType, Term.MultiColumnRaw valuesOrMarker, List<? extends Term.MultiColumnRaw> inValues, Tuples.INRaw inMarker)
    {
        this.entities = entities;
        this.relationType = relationType;
        this.valuesOrMarker = valuesOrMarker;

        this.inValues = inValues;
        this.inMarker = inMarker;
    }

    /**
     * Creates a multi-column EQ, LT, LTE, GT, or GTE relation.
     * For example: "SELECT ... WHERE (a, b) > (0, 1)"
     * @param entities the columns on the LHS of the relation
     * @param relationType the relation operator
     * @param valuesOrMarker a Tuples.Literal instance or a Tuples.Raw marker
     */
    public static MultiColumnRelation createNonInRelation(List<ColumnIdentifier.Raw> entities, Operator relationType, Term.MultiColumnRaw valuesOrMarker)
    {
        assert relationType != Operator.IN;
        return new MultiColumnRelation(entities, relationType, valuesOrMarker, null, null);
    }

    /**
     * Creates a multi-column IN relation with a list of IN values or markers.
     * For example: "SELECT ... WHERE (a, b) IN ((0, 1), (2, 3))"
     * @param entities the columns on the LHS of the relation
     * @param inValues a list of Tuples.Literal instances or a Tuples.Raw markers
     */
    public static MultiColumnRelation createInRelation(List<ColumnIdentifier.Raw> entities, List<? extends Term.MultiColumnRaw> inValues)
    {
        return new MultiColumnRelation(entities, Operator.IN, null, inValues, null);
    }

    /**
     * Creates a multi-column IN relation with a marker for the IN values.
     * For example: "SELECT ... WHERE (a, b) IN ?"
     * @param entities the columns on the LHS of the relation
     * @param inMarker a single IN marker
     */
    public static MultiColumnRelation createSingleMarkerInRelation(List<ColumnIdentifier.Raw> entities, Tuples.INRaw inMarker)
    {
        return new MultiColumnRelation(entities, Operator.IN, null, null, inMarker);
    }

    public List<ColumnIdentifier.Raw> getEntities()
    {
        return entities;
    }

    /**
     * For non-IN relations, returns the Tuples.Literal or Tuples.Raw marker for a single tuple.
     */
    public Term.MultiColumnRaw getValue()
    {
        assert relationType != Operator.IN;
        return valuesOrMarker;
    }

    /**
     * For IN relations, returns the list of Tuples.Literal instances or Tuples.Raw markers.
     * If a single IN marker was used, this will return null;
     */
    public List<? extends Term.MultiColumnRaw> getInValues()
    {

        return inValues;
    }

    /**
     * For IN relations, returns the single marker for the IN values if there is one, otherwise null.
     */
    public Tuples.INRaw getInMarker()
    {
        return inMarker;
    }

    public boolean isMultiColumn()
    {
        return true;
    }

    @Override
    public String toString()
    {
        if (relationType == Operator.IN)
        {
            StringBuilder sb = new StringBuilder(Tuples.tupleToString(entities));
            sb.append(" IN ");
            sb.append(inMarker != null ? '?' : Tuples.tupleToString(inValues));
            return sb.toString();
        }
        else
        {
            StringBuilder sb = new StringBuilder(Tuples.tupleToString(entities));
            sb.append(" ");
            sb.append(relationType);
            sb.append(" ");
            sb.append(valuesOrMarker);
            return sb.toString();
        }
    }
}