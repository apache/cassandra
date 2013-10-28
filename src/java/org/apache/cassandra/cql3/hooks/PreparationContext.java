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
package org.apache.cassandra.cql3.hooks;

import java.util.List;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.service.ClientState;

/**
 * Contextual information about the preparation of a CQLStatement.
 * Used by {@link org.apache.cassandra.cql3.hooks.PostPreparationHook}
 */
public class PreparationContext
{
    public final ClientState clientState;
    public final String queryString;
    public final List<ColumnSpecification> boundNames;

    public PreparationContext(ClientState clientState, String queryString, List<ColumnSpecification> boundNames)
    {
        this.clientState = clientState;
        this.queryString = queryString;
        this.boundNames = boundNames;
    }
}
