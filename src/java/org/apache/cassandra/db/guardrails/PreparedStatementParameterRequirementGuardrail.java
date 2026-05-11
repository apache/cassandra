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

package org.apache.cassandra.db.guardrails;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.service.ClientState;

public class PreparedStatementParameterRequirementGuardrail extends Guardrail
{
    @VisibleForTesting
    public static final String MISPREPARED_STATEMENT_MESSAGE = "The query contains only literal values and no bind markers. Using one or more '?' placeholder values (bind markers) allows a prepared statement to be reused.";

    PreparedStatementParameterRequirementGuardrail()
    {
        super("prepared_statements_require_parameters", null);
    }

    public void guard(CQLStatement statement, StatementRestrictions restrictions, @Nullable ClientState state, String keyspace, String table)
    {
        if (restrictions == null
            || !statement.eligibleAsPreparedStatement()
            || !statement.getBindVariables().isEmpty()
            || (!restrictions.hasPartitionKeyRestrictions()
                && !restrictions.hasClusteringColumnsRestrictions()
                && !restrictions.hasNonPrimaryKeyRestrictions()))
            return;

        if (!enabled(state))
            return;

        final GuardrailsConfig config = Guardrails.CONFIG_PROVIDER.getOrCreate(state);

        boolean failOn = config.getPreparedStatementsRequireParametersEnabled();
        if (!failOn && !config.getPreparedStatementsRequireParametersWarned())
            return;

        final String message = MISPREPARED_STATEMENT_MESSAGE + " Query executed on keyspace '" + keyspace + "', table '" + table + "'.";

        if (failOn)
            fail(message, state);
        else
            warn(message);
    }
}

