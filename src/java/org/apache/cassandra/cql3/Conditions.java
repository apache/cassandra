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

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.CQL3CasRequest;
import org.apache.cassandra.db.Clustering;

/**
 * Conditions that can be applied to a mutation statement.
 *
 */
public interface Conditions
{
    /**
     * An EMPTY condition
     */
    static final Conditions EMPTY_CONDITION = ColumnConditions.newBuilder().build();

    /**
     * IF EXISTS condition
     */
    static final Conditions IF_EXISTS_CONDITION = new IfExistsCondition();

    /**
     * IF NOT EXISTS condition
     */
    static final Conditions IF_NOT_EXISTS_CONDITION = new IfNotExistsCondition();

    /**
     * Returns the functions used by the conditions.
     * @return the functions used by the conditions
     */
    Iterable<Function> getFunctions();

    /**
     * Returns the column definitions to which apply the conditions.
     * @return the column definitions to which apply the conditions.
     */
    Iterable<ColumnDefinition> getColumns();

    /**
     * Checks if this <code>Conditions</code> is empty.
     * @return <code>true</code> if this <code>Conditions</code> is empty, <code>false</code> otherwise.
     */
    boolean isEmpty();

    /**
     * Checks if this is a IF EXIST condition.
     * @return <code>true</code> if this is a IF EXIST condition, <code>false</code> otherwise.
     */
    boolean isIfExists();

    /**
     * Checks if this is a IF NOT EXIST condition.
     * @return <code>true</code> if this is a IF NOT EXIST condition, <code>false</code> otherwise.
     */
    boolean isIfNotExists();

    /**
     * Checks if some of the conditions apply to static columns.
     *
     * @return <code>true</code> if some of the conditions apply to static columns, <code>false</code> otherwise.
     */
    boolean appliesToStaticColumns();

    /**
     * Checks if some of the conditions apply to regular columns.
     *
     * @return <code>true</code> if some of the conditions apply to regular columns, <code>false</code> otherwise.
     */
    boolean appliesToRegularColumns();

    /**
     * Adds the conditions to the specified CAS request.
     *
     * @param request the request
     * @param clustering the clustering prefix
     * @param options the query options
     */
    public void addConditionsTo(CQL3CasRequest request,
                                Clustering clustering,
                                QueryOptions options);
}
