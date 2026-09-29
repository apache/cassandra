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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the guardrail for disabling user access to the ALTER TABLE statement, {@link Guardrails#ddlEnabled}.
 * <p>
 * NOTE: This test class depends on {@link #currentTable()} method for setup, cleanup, and execution of tests. You'll
 * need to refactor this if you add tests that make changes to the current table as the test teardown will no longer match
 * setup.
 * <p>
 * This test is a leftover from CASSANDRA-17495 which was accommodated for the purposes of CASSANDRA-19556.
 */
public class GuardrailAlterTableTest extends GuardrailTester
{
    private static final String DDL_ERROR_MSG = "DDL statement is not allowed";

    public GuardrailAlterTableTest()
    {
        super(Guardrails.ddlEnabled);
    }

    @Before
    public void setupTest() throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (k INT, c INT, v TEXT, PRIMARY KEY(k, c))");
        super.beforeGuardrailTest();
    }

    @After
    public void afterTest() throws Throwable
    {
        dropTable("DROP TABLE %s");
        setGuardrail(true);
    }

    private void setGuardrail(boolean alterTableEnabled)
    {
        guardrails().setDDLEnabled(alterTableEnabled);
    }

    /**
     * Confirm that ALTER TABLE queries either work (guardrail enabled) or fail (guardrail disabled) appropriately
     */
    @Test
    public void testGuardrailEnabledAndDisabled()
    {
        setGuardrail(false);
        assertFailQuery("ALTER TABLE %s ADD test_one text");

        setGuardrail(true);
        executeNet("ALTER TABLE %s ADD test_two text;");

        setGuardrail(false);
        assertFailQuery("ALTER TABLE %s ADD test_three text");
    }

    /**
     * Confirm the guardrail appropriately catches the ALTER DROP case on a column
     */
    @Test
    public void testAppliesToAlterDropColumn()
    {
        setGuardrail(true);
        executeNet("ALTER TABLE %s ADD test_one text;");

        setGuardrail(false);
        assertFailQuery("ALTER TABLE %s DROP test_one");

        setGuardrail(true);
        executeNet("ALTER TABLE %s DROP test_one");
    }

    /**
     * Confirm the guardrail appropriately catches the ALTER RENAME case on a column
     */
    @Test
    public void testAppliesToAlterRenameColumn()
    {
        setGuardrail(false);

        setGuardrail(false);
        assertFailQuery("ALTER TABLE %s RENAME c TO renamed_c");

        setGuardrail(true);
        executeNet("ALTER TABLE %s RENAME c TO renamed_c");
    }

    private void assertFailQuery(String query)
    {
        assertThatThrownBy(() -> executeNet(query))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageContaining(DDL_ERROR_MSG);
    }
}
