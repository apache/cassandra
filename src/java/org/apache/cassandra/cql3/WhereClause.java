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

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.restrictions.CustomIndexExpression;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public final class WhereClause
{

    private static final WhereClause EMPTY = new WhereClause(new Builder());

    public final List<Relation> relations;
    public final List<CustomIndexExpression> expressions;

    private WhereClause(Builder builder)
    {
        this.relations = builder.relations.build();
        this.expressions = builder.expressions.build();

    }

    public static WhereClause empty()
    {
        return EMPTY;
    }

    public boolean containsCustomExpressions()
    {
        return !expressions.isEmpty();
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
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
