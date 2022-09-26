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

import com.google.common.collect.ImmutableList;

import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.cql3.restrictions.CustomIndexExpression;

import static java.lang.String.join;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;

public final class WhereClause
{
    private static final WhereClause EMPTY = new WhereClause(new Builder());

    public final List<Relation> relations;
    public final List<CustomIndexExpression> expressions;

    private WhereClause(Builder builder)
    {
        relations = builder.relations.build();
        expressions = builder.expressions.build();
    }

    public static WhereClause empty()
    {
        return EMPTY;
    }

    public boolean containsCustomExpressions()
    {
        return !expressions.isEmpty();
    }

    /**
     * Renames identifiers in all relations
     * @param from the old identifier
     * @param to the new identifier
     * @return a new WhereClause with with "from" replaced by "to" in all relations
     */
    public WhereClause renameIdentifier(ColumnIdentifier from, ColumnIdentifier to)
    {
        WhereClause.Builder builder = new WhereClause.Builder();

        relations.stream()
                 .map(r -> r.renameIdentifier(from, to))
                 .forEach(builder::add);

        expressions.forEach(builder::add);

        return builder.build();
    }

    public static WhereClause parse(String cql) throws RecognitionException
    {
        return CQLFragmentParser.parseAnyUnhandled(CqlParser::whereClause, cql).build();
    }

    @Override
    public String toString()
    {
        return toCQLString();
    }

    /**
     * Returns a CQL representation of this WHERE clause.
     *
     * @return a CQL representation of this WHERE clause
     */
    public String toCQLString()
    {
        return join(" AND ",
                    concat(transform(relations, Relation::toCQLString),
                           transform(expressions, CustomIndexExpression::toCQLString)));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof WhereClause))
            return false;

        WhereClause wc = (WhereClause) o;
        return relations.equals(wc.relations) && expressions.equals(wc.expressions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(relations, expressions);
    }

    /**
     * Checks if the where clause contains some token relations.
     *
     * @return {@code true} if it is the case, {@code false} otherwise.
     */
    public boolean containsTokenRelations()
    {
        for (Relation rel : relations)
        {
            if (rel.onToken())
                return true;
        }
        return false;
    }

    public static final class Builder
    {
        ImmutableList.Builder<Relation> relations = new ImmutableList.Builder<>();
        ImmutableList.Builder<CustomIndexExpression> expressions = new ImmutableList.Builder<>();

        public Builder add(Relation relation)
        {
            relations.add(relation);
            return this;
        }

        public Builder add(CustomIndexExpression expression)
        {
            expressions.add(expression);
            return this;
        }

        public WhereClause build()
        {
            return new WhereClause(this);
        }
    }
}
