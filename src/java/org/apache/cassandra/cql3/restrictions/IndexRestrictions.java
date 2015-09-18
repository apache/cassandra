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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.IndexName;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class IndexRestrictions
{
    public static final String INDEX_NOT_FOUND = "Invalid index expression, index %s not found for %s.%s";
    public static final String INVALID_INDEX = "Target index %s cannot be used to query %s.%s";
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

    static InvalidRequestException invalidIndex(IndexName indexName, CFMetaData cfm)
    {
        return new InvalidRequestException(String.format(INVALID_INDEX, indexName.getIdx(), cfm.ksName, cfm.cfName));
    }

    static InvalidRequestException indexNotFound(IndexName indexName, CFMetaData cfm)
    {
        return new InvalidRequestException(String.format(INDEX_NOT_FOUND,indexName.getIdx(), cfm.ksName, cfm.cfName));
    }

    static InvalidRequestException nonCustomIndexInExpression(IndexName indexName)
    {
        return new InvalidRequestException(String.format(NON_CUSTOM_INDEX_IN_EXPRESSION, indexName.getIdx()));
    }

    static InvalidRequestException customExpressionNotSupported(IndexName indexName)
    {
        return new InvalidRequestException(String.format(CUSTOM_EXPRESSION_NOT_SUPPORTED, indexName.getIdx()));
    }
}
