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
package org.apache.cassandra.cql3.conditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.CQL3CasRequest;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * A set of <code>ColumnCondition</code>s.
 *
 */
public final class ColumnConditions extends AbstractConditions
{
    /**
     * The conditions on regular columns.
     */
    private final List<ColumnCondition> columnConditions;

    /**
     * The conditions on static columns
     */
    private final List<ColumnCondition> staticConditions;

    /**
     * Creates a new <code>ColumnConditions</code> instance for the specified builder.
     */
    private ColumnConditions(Builder builder)
    {
        this.columnConditions = builder.columnConditions;
        this.staticConditions = builder.staticConditions;
    }

    @Override
    public boolean appliesToStaticColumns()
    {
        return !staticConditions.isEmpty();
    }

    @Override
    public boolean appliesToRegularColumns()
    {
        return !columnConditions.isEmpty();
    }

    @Override
    public Collection<ColumnMetadata> getColumns()
    {
        return Stream.concat(columnConditions.stream(), staticConditions.stream())
                     .map(e -> e.column)
                     .collect(Collectors.toList());
    }

    @Override
    public boolean isEmpty()
    {
        return columnConditions.isEmpty() && staticConditions.isEmpty();
    }

    /**
     * Adds the conditions to the specified CAS request.
     *
     * @param request the request
     * @param clustering the clustering prefix
     * @param options the query options
     */
    public void addConditionsTo(CQL3CasRequest request,
                                Clustering<?> clustering,
                                QueryOptions options)
    {
        if (!columnConditions.isEmpty())
            request.addConditions(clustering, columnConditions, options);
        if (!staticConditions.isEmpty())
            request.addConditions(Clustering.STATIC_CLUSTERING, staticConditions, options);
    }

    @Override
    public void addFunctionsTo(List<Function> functions)
    {
        columnConditions.forEach(p -> p.addFunctionsTo(functions));
        staticConditions.forEach(p -> p.addFunctionsTo(functions));
    }

    /**
     * Creates a new <code>Builder</code> for <code>ColumnConditions</code>.
     * @return a new <code>Builder</code> for <code>ColumnConditions</code>
     */
    public static Builder newBuilder()
    {
        return new Builder();
    }

    /**
     * A <code>Builder</code> for <code>ColumnConditions</code>.
     *
     */
    public static final class Builder
    {
        /**
         * The conditions on regular columns.
         */
        private List<ColumnCondition> columnConditions = Collections.emptyList();

        /**
         * The conditions on static columns
         */
        private List<ColumnCondition> staticConditions = Collections.emptyList();

        /**
         * Adds the specified <code>ColumnCondition</code> to this set of conditions.
         * @param condition the condition to add
         */
        public Builder add(ColumnCondition condition)
        {
            List<ColumnCondition> conds;
            if (condition.column.isStatic())
            {
                if (staticConditions.isEmpty())
                    staticConditions = new ArrayList<>();
                conds = staticConditions;
            }
            else
            {
                if (columnConditions.isEmpty())
                    columnConditions = new ArrayList<>();
                conds = columnConditions;
            }
            conds.add(condition);
            return this;
        }

        public ColumnConditions build()
        {
            return new ColumnConditions(this);
        }

        private Builder()
        {
        }
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
