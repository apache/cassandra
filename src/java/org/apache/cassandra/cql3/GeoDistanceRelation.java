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
import java.util.List;
import java.util.Objects;

import com.google.common.base.Joiner;

import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * A relation using a GEO_DISTANCE function to get vectors of kind lat, lon within a given distance.
 * Examples:
 * <ul>
 * <li>SELECT ... WHERE GEO_DISTANCE(geopoint, :querypoint) &lt; :distance</li>
 * </ul>
 */
public class GeoDistanceRelation extends Relation
{
    private final ColumnIdentifier entity;
    private final Term.Raw point;
    private final Term.Raw distance;

    public GeoDistanceRelation(ColumnIdentifier entity, Term.Raw point, Operator type, Term.Raw distance)
    {
        this.entity = entity;
        this.point = point;
        this.relationType = type;
        this.distance = distance;
    }

    public Term.Raw getValue()
    {
        throw invalidRequest("%s cannot be used with the GEO_DISTANCE function", operator());
    }

    public List<? extends Term.Raw> getInValues()
    {
        return null;
    }

    @Override
    protected Restriction newSliceRestriction(TableMetadata table,
                                              VariableSpecifications boundNames,
                                              Bound bound,
                                              boolean inclusive)
    {
        ColumnMetadata columnDef = table.getExistingColumn(entity);
        if (bound == Bound.START)
            throw invalidRequest("%s cannot be used with the GEO_DISTANCE function", operator());
        if (!(columnDef.type instanceof VectorType) || ((VectorType<?>) columnDef.type).dimension != 2)
            throw invalidRequest("GEO_DISTANCE is only supported against vector<float, 2> columns");

        Term pointTerm = toTerm(Collections.singletonList(columnDef), point, table.keyspace, boundNames);

        var vectorRadius = new ColumnSpecification(columnDef.ksName, columnDef.cfName,
                                                   new ColumnIdentifier("radius", true), FloatType.instance);
        Term distanceTerm = toTerm(Collections.singletonList(vectorRadius), distance, table.keyspace, boundNames);
        return new SingleColumnRestriction.BoundedAnnRestriction(columnDef, pointTerm, distanceTerm, inclusive);
    }

    @Override
    protected Restriction newEQRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        throw invalidRequest("%s cannot be used with the GEO_DISTANCE function", operator());

    }

    @Override
    protected Restriction newNEQRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        throw invalidRequest("%s cannot be used with the GEO_DISTANCE function", operator());
    }

    @Override
    protected Restriction newContainsRestriction(TableMetadata table, VariableSpecifications boundNames, boolean isKey)
    {
        throw invalidRequest("%s cannot be used with the GEO_DISTANCE function", operator());
    }

    @Override
    protected Restriction newNotContainsRestriction(TableMetadata table, VariableSpecifications boundNames, boolean isKey)
    {
        throw invalidRequest("%s cannot be used with the GEO_DISTANCE function", operator());
    }

    @Override
    protected Restriction newINRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        throw invalidRequest("%s cannot be used with the GEO_DISTANCE function", operator());
    }

    @Override
    protected Restriction newNotINRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        throw invalidRequest("%s cannot be used with the GEO_DISTANCE function", operator());
    }

    @Override
    protected Restriction newIsNotRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        throw invalidRequest("%s cannot be used with the GEO_DISTANCE function", operator());
    }

    @Override
    protected Restriction newLikeRestriction(TableMetadata table, VariableSpecifications boundNames, Operator operator)
    {
        throw invalidRequest("%s cannot be used with the GEO_DISTANCE function", operator());
    }

    @Override
    protected Restriction newAnnRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        throw invalidRequest("%s cannot be used with the GEO_DISTANCE function", operator());
    }

    @Override
    protected Restriction newAnalyzerMatchesRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        throw invalidRequest("%s cannot be used with the GEO_DISTANCE function", operator());
    }

    @Override
    protected Term toTerm(List<? extends ColumnSpecification> receivers,
                          Term.Raw raw,
                          String keyspace,
                          VariableSpecifications boundNames) throws InvalidRequestException
    {
        Term term = raw.prepare(keyspace, receivers.get(0));
        term.collectMarkerSpecification(boundNames);
        return term;
    }

    @Override
    public Relation renameIdentifier(ColumnIdentifier from, ColumnIdentifier to)
    {
        if (!entity.equals(from))
            return this;

        return new GeoDistanceRelation(to, point, operator(), distance);
    }

    @Override
    public String toCQLString()
    {
        return String.format("GEO_DISTANCE(%s, %s) %s %s", entity.toCQLString(), point, relationType, distance);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(relationType, entity, point, distance);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof GeoDistanceRelation))
            return false;

        GeoDistanceRelation geoDistanceRelation = (GeoDistanceRelation) o;
        return relationType == geoDistanceRelation.relationType
               && entity.equals(geoDistanceRelation.entity)
               && point.equals(geoDistanceRelation.point)
               && distance.equals(geoDistanceRelation.distance);
    }
}
