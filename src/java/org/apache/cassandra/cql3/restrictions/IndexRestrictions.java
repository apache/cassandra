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

package org.apache.cassandra.cql3.restrictions;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class IndexRestrictions
{
    public static final String INDEX_NOT_FOUND = "Invalid index expression, index %s not found for %s";
    public static final String INVALID_INDEX = "Target index %s cannot be used to query %s";
    public static final String CUSTOM_EXPRESSION_NOT_SUPPORTED = "Index %s does not support custom expressions";
    public static final String NON_CUSTOM_INDEX_IN_EXPRESSION = "Only CUSTOM indexes may be used in custom index expressions, %s is not valid";
    public static final String MULTIPLE_EXPRESSIONS = "Multiple custom index expressions in a single query are not supported";

    private final List<Restrictions> regularRestrictions = new ArrayList<>();
    private final List<CustomIndexExpression> customExpressions = new ArrayList<>();

    public void add(Restrictions restrictions)
    {
        regularRestrictions.add(restrictions);
    }

    public void add(CustomIndexExpression expression)
    {
        customExpressions.add(expression);
    }

    public boolean isEmpty()
    {
        return regularRestrictions.isEmpty() && customExpressions.isEmpty();
    }

    public List<Restrictions> getRestrictions()
    {
        return regularRestrictions;
    }

    public List<CustomIndexExpression> getCustomIndexExpressions()
    {
        return customExpressions;
    }

    static InvalidRequestException invalidIndex(QualifiedName indexName, TableMetadata table)
    {
        return new InvalidRequestException(String.format(INVALID_INDEX, indexName.getName(), table));
    }

    static InvalidRequestException indexNotFound(QualifiedName indexName, TableMetadata table)
    {
        return new InvalidRequestException(String.format(INDEX_NOT_FOUND, indexName.getName(), table));
    }

    static InvalidRequestException nonCustomIndexInExpression(QualifiedName indexName)
    {
        return new InvalidRequestException(String.format(NON_CUSTOM_INDEX_IN_EXPRESSION, indexName.getName()));
    }

    static InvalidRequestException customExpressionNotSupported(QualifiedName indexName)
    {
        return new InvalidRequestException(String.format(CUSTOM_EXPRESSION_NOT_SUPPORTED, indexName.getName()));
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
