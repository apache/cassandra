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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.Term.Raw;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.restrictions.TokenRestriction;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkContainsNoDuplicates;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkContainsOnly;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * A relation using the token function.
 * Examples:
 * <ul>
 * <li>SELECT ... WHERE token(a) &gt; token(1)</li>
 * <li>SELECT ... WHERE token(a, b) &gt; token(1, 3)</li>
 * </ul>
 */
public final class TokenRelation extends Relation
{
    private final List<ColumnIdentifier> entities;

    private final Term.Raw value;

    public TokenRelation(List<ColumnIdentifier> entities, Operator type, Term.Raw value)
    {
        this.entities = entities;
        this.relationType = type;
        this.value = value;
    }

    @Override
    public boolean onToken()
    {
        return true;
    }

    public Term.Raw getValue()
    {
        return value;
    }

    public List<? extends Term.Raw> getInValues()
    {
        return null;
    }

    @Override
    protected Restriction newEQRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        List<ColumnMetadata> columnDefs = getColumnDefinitions(table);
        Term term = toTerm(toReceivers(table, columnDefs), value, table.keyspace, boundNames);
        return new TokenRestriction.EQRestriction(table, columnDefs, term);
    }

    @Override
    protected Restriction newINRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        throw invalidRequest("%s cannot be used with the token function", operator());
    }

    @Override
    protected Restriction newSliceRestriction(TableMetadata table,
                                              VariableSpecifications boundNames,
                                              Bound bound,
                                              boolean inclusive)
    {
        List<ColumnMetadata> columnDefs = getColumnDefinitions(table);
        Term term = toTerm(toReceivers(table, columnDefs), value, table.keyspace, boundNames);
        return new TokenRestriction.SliceRestriction(table, columnDefs, bound, inclusive, term);
    }

    @Override
    protected Restriction newContainsRestriction(TableMetadata table, VariableSpecifications boundNames, boolean isKey)
    {
        throw invalidRequest("%s cannot be used with the token function", operator());
    }

    @Override
    protected Restriction newIsNotRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        throw invalidRequest("%s cannot be used with the token function", operator());
    }

    @Override
    protected Restriction newLikeRestriction(TableMetadata table, VariableSpecifications boundNames, Operator operator)
    {
        throw invalidRequest("%s cannot be used with the token function", operator);
    }

    @Override
    protected Term toTerm(List<? extends ColumnSpecification> receivers,
                          Raw raw,
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
        if (!entities.contains(from))
            return this;

        List<ColumnIdentifier> newEntities = entities.stream().map(e -> e.equals(from) ? to : e).collect(Collectors.toList());
        return new TokenRelation(newEntities, operator(), value);
    }

    @Override
    public String toCQLString()
    {
        return String.format("token%s %s %s", Tuples.tupleToString(entities, ColumnIdentifier::toCQLString), relationType, value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(relationType, entities, value);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof TokenRelation))
            return false;

        TokenRelation tr = (TokenRelation) o;
        return relationType.equals(tr.relationType) && entities.equals(tr.entities) && value.equals(tr.value);
    }

    /**
     * Returns the definition of the columns to which apply the token restriction.
     *
     * @param table the table metadata
     * @return the definition of the columns to which apply the token restriction.
     * @throws InvalidRequestException if the entity cannot be resolved
     */
    private List<ColumnMetadata> getColumnDefinitions(TableMetadata table)
    {
        List<ColumnMetadata> columnDefs = new ArrayList<>(entities.size());
        for (ColumnIdentifier id : entities)
            columnDefs.add(table.getExistingColumn(id));
        return columnDefs;
    }

    /**
     * Returns the receivers for this relation.
     *
     * @param table the table meta data
     * @param columnDefs the column definitions
     * @return the receivers for the specified relation.
     * @throws InvalidRequestException if the relation is invalid
     */
    private static List<? extends ColumnSpecification> toReceivers(TableMetadata table,
                                                                   List<ColumnMetadata> columnDefs)
                                                                   throws InvalidRequestException
    {

        if (!columnDefs.equals(table.partitionKeyColumns()))
        {
            checkTrue(columnDefs.containsAll(table.partitionKeyColumns()),
                      "The token() function must be applied to all partition key components or none of them");

            checkContainsNoDuplicates(columnDefs, "The token() function contains duplicate partition key components");

            checkContainsOnly(columnDefs, table.partitionKeyColumns(), "The token() function must contains only partition key components");

            throw invalidRequest("The token function arguments must be in the partition key order: %s",
                                 Joiner.on(", ").join(ColumnMetadata.toIdentifiers(table.partitionKeyColumns())));
        }

        ColumnMetadata firstColumn = columnDefs.get(0);
        return Collections.singletonList(new ColumnSpecification(firstColumn.ksName,
                                                                 firstColumn.cfName,
                                                                 new ColumnIdentifier("partition key token", true),
                                                                 table.partitioner.getTokenValidator()));
    }
}
