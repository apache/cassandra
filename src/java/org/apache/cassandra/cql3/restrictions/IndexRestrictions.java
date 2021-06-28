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
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public final class IndexRestrictions
{
    /**
     * The empty {@code IndexRestrictions}.
     */
    private static final IndexRestrictions EMPTY_RESTRICTIONS = new IndexRestrictions(Collections.EMPTY_LIST, Collections.EMPTY_LIST);

    public static final String INDEX_NOT_FOUND = "Invalid index expression, index %s not found for %s";
    public static final String INVALID_INDEX = "Target index %s cannot be used to query %s";
    public static final String CUSTOM_EXPRESSION_NOT_SUPPORTED = "Index %s does not support custom expressions";
    public static final String NON_CUSTOM_INDEX_IN_EXPRESSION = "Only CUSTOM indexes may be used in custom index expressions, %s is not valid";
    public static final String MULTIPLE_EXPRESSIONS = "Multiple custom index expressions in a single query are not supported";

    private final List<Restrictions> regularRestrictions;
    private final List<CustomIndexExpression> externalRestrictions;

    private IndexRestrictions(List<Restrictions> regularRestrictions, List<CustomIndexExpression> externalExpressions)
    {
        this.regularRestrictions = regularRestrictions;
        this.externalRestrictions = externalExpressions;
    }

    /**
     * Returns an empty {@code IndexRestrictions}.
     * @return an empty {@code IndexRestrictions}
     */
    public static IndexRestrictions of()
    {
        return EMPTY_RESTRICTIONS;
    }

    /**
     * Creates a new {@code IndexRestrictions.Builder} instance.
     * @return a new {@code IndexRestrictions.Builder} instance.
     */
    public static Builder builder()
    {
        return new IndexRestrictions.Builder();
    }

    public boolean isEmpty()
    {
        return regularRestrictions.isEmpty() && externalRestrictions.isEmpty();
    }

    /**
     * Returns the regular restrictions.
     * @return the regular restrictions
     */
    public List<Restrictions> getRestrictions()
    {
        return regularRestrictions;
    }

    /**
     * Returns the external restrictions.
     * @return the external restrictions
     */
    public List<CustomIndexExpression> getExternalExpressions()
    {
        return externalRestrictions;
    }

    /**
     * Returns the number of restrictions in external expression and regular restrictions.
     * @return Returns the number of restrictions in external expression and regular restrictions.
     */
    private int numOfSupportedRestrictions()
    {
        int numberOfRestrictions = getExternalExpressions().size();
        for (Restrictions restrictions : getRestrictions())
            numberOfRestrictions += restrictions.size();

        return numberOfRestrictions;
    }

    /**
     * Returns whether these restrictions would need filtering if the specified index registry were used.
     *
     * @param indexRegistry an index registry
     * @param hasClusteringColumnRestrictions {@code true} if there are restricted clustering columns
     * @param hasMultipleContains {@code true} if there are multiple "contains" restrictions
     * @return {@code true} if this would need filtering if {@code indexRegistry} were used, {@code false} otherwise
     */
    public boolean needFiltering(IndexRegistry indexRegistry, boolean hasClusteringColumnRestrictions, boolean hasMultipleContains)
    {
        // We need filtering if any clustering columns have restrictions that are not supported
        // by their indexes.
        if (numOfSupportedRestrictions() == 0)
            return hasClusteringColumnRestrictions;

        for (Index.Group group : indexRegistry.listIndexGroups())
            if (!needFiltering(group, hasMultipleContains))
                return false;

        return true;
    }

    /**
     * Returns whether these restrictions would need filtering if the specified index group were used.
     *
     * @param indexGroup an index group
     * @param hasMultipleContains {@code true} if there are multiple "contains" restrictions
     * @return {@code true} if this would need filtering if {@code indexGroup} were used, {@code false} otherwise
     */
    private boolean needFiltering(Index.Group indexGroup, boolean hasMultipleContains)
    {
        if (hasMultipleContains && !indexGroup.supportsMultipleContains())
            return true;

        for (Restrictions restrictions : regularRestrictions)
            if (restrictions.needsFiltering(indexGroup))
                return true;

        for (CustomIndexExpression restriction : externalRestrictions)
            if (restriction.needsFiltering(indexGroup))
                return true;

        return false;
    }

    public boolean indexBeingUsed(Index.Group indexGroup)
    {
        for (Restrictions restrictions : regularRestrictions)
            if (!restrictions.needsFiltering(indexGroup))
                return true;

        for (CustomIndexExpression restriction : externalRestrictions)
            if (!restriction.needsFiltering(indexGroup))
                return true;

        return false;
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
        return invalidRequest(NON_CUSTOM_INDEX_IN_EXPRESSION, indexName.getName());
    }

    static InvalidRequestException customExpressionNotSupported(QualifiedName indexName)
    {
        return invalidRequest(CUSTOM_EXPRESSION_NOT_SUPPORTED, indexName.getName());
    }

    /**
     * Builder for IndexRestrictions.
     */
    public static final class Builder
    {
        /**
         * Builder for the regular restrictions.
         */
        private List<Restrictions> regularRestrictions = new ArrayList<>();

        /**
         * Builder for the custom expressions.
         */
        private List<CustomIndexExpression> externalRestrictions = new ArrayList<>();

        private Builder() {}

        /**
         * Adds the specified restrictions.
         *
         * @param restrictions the restrictions to add
         * @return this {@code Builder}
         */
        public Builder add(Restrictions restrictions)
        {
            regularRestrictions.add(restrictions);
            return this;
        }

        /**
         * Adds the restrictions and custom expressions from the specified {@code IndexRestrictions}.
         *
         * @param restrictions the restrictions and custom expressions to add
         * @return this {@code Builder}
         */
        public Builder add(IndexRestrictions restrictions)
        {
            regularRestrictions.addAll(restrictions.regularRestrictions);
            externalRestrictions.addAll(restrictions.externalRestrictions);
            return this;
        }

        /**
         * Adds the specified index expression.
         *
         * @param restriction the index expression to add
         * @return this {@code Builder}
         */
        public Builder add(CustomIndexExpression restriction)
        {
            externalRestrictions.add(restriction);
            return this;
        }

        /**
         * Builds a new {@code IndexRestrictions} instance
         * @return a new {@code IndexRestrictions} instance
         */
        public IndexRestrictions build()
        {
            return new IndexRestrictions(Collections.unmodifiableList(regularRestrictions), Collections.unmodifiableList(externalRestrictions));
        }
    }
}
