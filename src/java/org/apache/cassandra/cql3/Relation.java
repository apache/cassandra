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

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public abstract class Relation
{
    protected Operator relationType;

    public Operator operator()
    {
        return relationType;
    }

    /**
     * Returns the raw value for this relation, or null if this is an IN relation.
     */
    public abstract Term.Raw getValue();

    /**
     * Returns the list of raw IN values for this relation, or null if this is not an IN relation.
     */
    public abstract List<? extends Term.Raw> getInValues();

    /**
     * Checks if this relation apply to multiple columns.
     *
     * @return <code>true</code> if this relation apply to multiple columns, <code>false</code> otherwise.
     */
    public boolean isMultiColumn()
    {
        return false;
    }

    /**
     * Checks if this relation is a token relation (e.g. <pre>token(a) = token(1)</pre>).
     *
     * @return <code>true</code> if this relation is a token relation, <code>false</code> otherwise.
     */
    public boolean onToken()
    {
        return false;
    }

    /**
     * Checks if the operator of this relation is a <code>CONTAINS</code>.
     * @return <code>true</code>  if the operator of this relation is a <code>CONTAINS</code>, <code>false</code>
     * otherwise.
     */
    public final boolean isContains()
    {
        return relationType == Operator.CONTAINS;
    }

    /**
     * Checks if the operator of this relation is a <code>CONTAINS_KEY</code>.
     * @return <code>true</code>  if the operator of this relation is a <code>CONTAINS_KEY</code>, <code>false</code>
     * otherwise.
     */
    public final boolean isContainsKey()
    {
        return relationType == Operator.CONTAINS_KEY;
    }

    /**
     * Checks if the operator of this relation is a <code>IN</code>.
     * @return <code>true</code>  if the operator of this relation is a <code>IN</code>, <code>false</code>
     * otherwise.
     */
    public final boolean isIN()
    {
        return relationType == Operator.IN;
    }

    /**
     * Checks if the operator of this relation is a <code>EQ</code>.
     * @return <code>true</code>  if the operator of this relation is a <code>EQ</code>, <code>false</code>
     * otherwise.
     */
    public final boolean isEQ()
    {
        return relationType == Operator.EQ;
    }

    public final boolean isLIKE()
    {
        return relationType == Operator.LIKE_PREFIX
                || relationType == Operator.LIKE_SUFFIX
                || relationType == Operator.LIKE_CONTAINS
                || relationType == Operator.LIKE_MATCHES
                || relationType == Operator.LIKE;
    }

    /**
     * Checks if the operator of this relation is a <code>Slice</code> (GT, GTE, LTE, LT).
     *
     * @return <code>true</code> if the operator of this relation is a <code>Slice</code>, <code>false</code> otherwise.
     */
    public final boolean isSlice()
    {
        return relationType == Operator.GT
                || relationType == Operator.GTE
                || relationType == Operator.LTE
                || relationType == Operator.LT;
    }

    /**
     * Converts this <code>Relation</code> into a <code>Restriction</code>.
     *
     * @param table the Column Family meta data
     * @param boundNames the variables specification where to collect the bind variables
     * @return the <code>Restriction</code> corresponding to this <code>Relation</code>
     * @throws InvalidRequestException if this <code>Relation</code> is not valid
     */
    public final Restriction toRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        switch (relationType)
        {
            case EQ: return newEQRestriction(table, boundNames);
            case LT: return newSliceRestriction(table, boundNames, Bound.END, false);
            case LTE: return newSliceRestriction(table, boundNames, Bound.END, true);
            case GTE: return newSliceRestriction(table, boundNames, Bound.START, true);
            case GT: return newSliceRestriction(table, boundNames, Bound.START, false);
            case IN: return newINRestriction(table, boundNames);
            case CONTAINS: return newContainsRestriction(table, boundNames, false);
            case CONTAINS_KEY: return newContainsRestriction(table, boundNames, true);
            case IS_NOT: return newIsNotRestriction(table, boundNames);
            case LIKE_PREFIX:
            case LIKE_SUFFIX:
            case LIKE_CONTAINS:
            case LIKE_MATCHES:
            case LIKE:
                return newLikeRestriction(table, boundNames, relationType);
            case ANN:
                throw invalidRequest("ANN is only supported in ORDER BY");
            default: throw invalidRequest("Unsupported \"!=\" relation: %s", this);
        }
    }

    /**
     * Creates a new EQ restriction instance.
     *
     * @param table the table meta data
     * @param boundNames the variables specification where to collect the bind variables
     * @return a new EQ restriction instance.
     * @throws InvalidRequestException if the relation cannot be converted into an EQ restriction.
     */
    protected abstract Restriction newEQRestriction(TableMetadata table, VariableSpecifications boundNames);

    /**
     * Creates a new IN restriction instance.
     *
     * @param table the table meta data
     * @param boundNames the variables specification where to collect the bind variables
     * @return a new IN restriction instance
     * @throws InvalidRequestException if the relation cannot be converted into an IN restriction.
     */
    protected abstract Restriction newINRestriction(TableMetadata table, VariableSpecifications boundNames);

    /**
     * Creates a new Slice restriction instance.
     *
     * @param table the table meta data
     * @param boundNames the variables specification where to collect the bind variables
     * @param bound the slice bound
     * @param inclusive <code>true</code> if the bound is included.
     * @return a new slice restriction instance
     * @throws InvalidRequestException if the <code>Relation</code> is not valid
     */
    protected abstract Restriction newSliceRestriction(TableMetadata table,
                                                       VariableSpecifications boundNames,
                                                       Bound bound,
                                                       boolean inclusive);

    /**
     * Creates a new Contains restriction instance.
     *
     * @param table the table meta data
     * @param boundNames the variables specification where to collect the bind variables
     * @param isKey <code>true</code> if the restriction to create is a CONTAINS KEY
     * @return a new Contains <code>Restriction</code> instance
     * @throws InvalidRequestException if the <code>Relation</code> is not valid
     */
    protected abstract Restriction newContainsRestriction(TableMetadata table, VariableSpecifications boundNames, boolean isKey);

    protected abstract Restriction newIsNotRestriction(TableMetadata table, VariableSpecifications boundNames);

    protected abstract Restriction newLikeRestriction(TableMetadata table, VariableSpecifications boundNames, Operator operator);

    /**
     * Converts the specified <code>Raw</code> into a <code>Term</code>.
     * @param receivers the columns to which the values must be associated at
     * @param raw the raw term to convert
     * @param keyspace the keyspace name
     * @param boundNames the variables specification where to collect the bind variables
     *
     * @return the <code>Term</code> corresponding to the specified <code>Raw</code>
     * @throws InvalidRequestException if the <code>Raw</code> term is not valid
     */
    protected abstract Term toTerm(List<? extends ColumnSpecification> receivers,
                                   Term.Raw raw,
                                   String keyspace,
                                   VariableSpecifications boundNames);

    /**
     * Converts the specified <code>Raw</code> terms into a <code>Term</code>s.
     * @param receivers the columns to which the values must be associated at
     * @param raws the raw terms to convert
     * @param keyspace the keyspace name
     * @param boundNames the variables specification where to collect the bind variables
     *
     * @return the <code>Term</code>s corresponding to the specified <code>Raw</code> terms
     * @throws InvalidRequestException if the <code>Raw</code> terms are not valid
     */
    protected final List<Term> toTerms(List<? extends ColumnSpecification> receivers,
                                       List<? extends Term.Raw> raws,
                                       String keyspace,
                                       VariableSpecifications boundNames)
    {
        if (raws == null)
            return null;

        List<Term> terms = new ArrayList<>(raws.size());
        for (int i = 0, m = raws.size(); i < m; i++)
            terms.add(toTerm(receivers, raws.get(i), keyspace, boundNames));

        return terms;
    }

    /**
     * Renames an identifier in this Relation, if applicable.
     * @param from the old identifier
     * @param to the new identifier
     * @return this object, if the old identifier is not in the set of entities that this relation covers; otherwise
     *         a new Relation with "from" replaced by "to" is returned.
     */
    public abstract Relation renameIdentifier(ColumnIdentifier from, ColumnIdentifier to);

    /**
     * Returns a CQL representation of this relation.
     *
     * @return a CQL representation of this relation
     */
    public abstract String toCQLString();

    @Override
    public String toString()
    {
        return toCQLString();
    }
}
