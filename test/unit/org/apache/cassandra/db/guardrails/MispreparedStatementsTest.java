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

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.QueryProcessor;

import static org.apache.cassandra.db.guardrails.PreparedStatementParameterRequirementGuardrail.MISPREPARED_STATEMENT_MESSAGE;

public class MispreparedStatementsTest extends GuardrailTester
{
    private boolean preparedStatementsRequireParametersWarned;
    private boolean preparedStatementsRequireParametersEnabled;
    private String[] mispreparedQueries;
    private String[] preparableQueries;

    @Before
    public void setup()
    {
        preparedStatementsRequireParametersWarned = guardrails().getPreparedStatementsRequireParametersWarned();
        preparedStatementsRequireParametersEnabled = guardrails().getPreparedStatementsRequireParametersEnabled();
        createTable("create table %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        mispreparedQueries = new String[]{
        String.format("SELECT * FROM %s WHERE pk = 1", currentTable()),
        String.format("SELECT * FROM %s WHERE pk = 1 AND ck = 1", currentTable()),
        String.format("SELECT * FROM %s WHERE ck = 1 ALLOW FILTERING", currentTable()),
        String.format("SELECT * FROM %s WHERE v = 'a' ALLOW FILTERING", currentTable()),
        String.format("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 'a')", currentTable()),
        String.format("UPDATE %s SET v = 'b' WHERE pk = 1 AND ck = 1", currentTable()),
        String.format("DELETE FROM %s WHERE pk = 1 AND ck = 1", currentTable()),
        String.format("BEGIN BATCH " +
                      "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 'a');" +
                      "UPDATE %s SET v = 'b' WHERE pk = 2 AND ck = 2;" +
                      "APPLY BATCH;", currentTable(), currentTable()) };
        preparableQueries = new String[]{
        String.format("SELECT * FROM %s WHERE pk = ?", currentTable()),
        String.format("INSERT INTO %s (pk, ck, v) VALUES (?, 1, 'a')", currentTable()),
        String.format("UPDATE %s SET v = ? WHERE pk = 1 AND ck = 1", currentTable()),
        String.format("SELECT * FROM %s WHERE pk = 1 AND ck = ?", currentTable()),
        String.format("TRUNCATE %s", currentTable())
        };
        userClientState.setKeyspace(KEYSPACE);
    }

    @After
    public void tear()
    {
        guardrails().setPreparedStatementsRequireParametersEnabled(preparedStatementsRequireParametersEnabled);
        guardrails().setPreparedStatementsRequireParametersWarned(preparedStatementsRequireParametersWarned);
    }

    @Test
    public void testPreparedStatementsRequireParametersEnabledGuardrailEnabled() throws Throwable
    {
        guardrails().setPreparedStatementsRequireParametersEnabled(true);

        for (String query : mispreparedQueries)
            assertGuardrailViolated(query);

        for (String query : preparableQueries)
            assertGuardrailAllowed(query);
    }

    @Test
    public void testPreparedStatementsRequireParametersWarnEnabled() throws Throwable
    {
        guardrails().setPreparedStatementsRequireParametersEnabled(false);
        for (String query : mispreparedQueries)
        {
            // Skip the batch query in this loop because it generates multiple warnings
            if (query.contains("BEGIN BATCH")) continue;

            assertWarns(() -> QueryProcessor.getStatement(query, userClientState).validatePrepare(userClientState),
                        MISPREPARED_STATEMENT_MESSAGE + " Query executed on keyspace '" + KEYSPACE + "', table '" + currentTable() + "'.");
        }
    }

    @Test
    public void testDoesNotWarnTwiceForSameQuery() throws Throwable
    {
        guardrails().setPreparedStatementsRequireParametersEnabled(false);
        // We use a fully qualified name (KEYSPACE.table) here and ensure ClientState keyspace is null
        // to suppress the "USE <keyspace> anti-pattern" warning, which otherwise conflicts with
        // assertWarns by adding an unexpected second warning to the result.
        String query = String.format("SELECT * FROM %s.%s WHERE pk = 1", KEYSPACE, currentTable());
        assertWarns(() -> QueryProcessor.instance.prepare(query, userClientState),
                    MISPREPARED_STATEMENT_MESSAGE + " Query executed on keyspace '" + KEYSPACE + "', table '" + currentTable() + "'.");
        assertValid(() -> QueryProcessor.instance.prepare(query, userClientState));
    }

    @Test
    public void testBatchPreparedStatementsWarnEnabled() throws Throwable
    {
        guardrails().setPreparedStatementsRequireParametersEnabled(false);
        String batchQuery = mispreparedQueries[7];

        String message = MISPREPARED_STATEMENT_MESSAGE + " Query executed on keyspace '" + KEYSPACE + "', table '" + currentTable() + "'.";

        assertWarns(() -> QueryProcessor.getStatement(batchQuery, userClientState).validatePrepare(userClientState),
                    List.of(message, message));
    }

    private void assertGuardrailViolated(String query) throws Throwable
    {
        assertFails(() -> QueryProcessor.getStatement(query, userClientState).validatePrepare(userClientState),
                    MISPREPARED_STATEMENT_MESSAGE + " Query executed on keyspace '" + KEYSPACE + "', table '" + currentTable() + "'.");
    }

    private void assertGuardrailAllowed(String query) throws Throwable
    {
        assertValid(() -> QueryProcessor.getStatement(query, userClientState).validatePrepare(userClientState));
    }
}
