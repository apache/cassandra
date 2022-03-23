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


/**
 * Tests the guardrail for disabling user access to the ALTER TABLE statement, {@link Guardrails#alterTableEnabled}.
 *
 * NOTE: This test class depends on {@link #currentTable()} method for setup, cleanup, and execution of tests. You'll
 * need to refactor this if you add tests that make changes to the current table as the test teardown will no longer match
 * setup.
 */
public class GuardrailAlterTableTest extends GuardrailTester
{
    public GuardrailAlterTableTest()
    {
        super(Guardrails.alterTableEnabled);
    }

    @Before
    public void setupTest() throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (k INT, c INT, v TEXT, PRIMARY KEY(k, c))");
    }

    @After
    public void afterTest() throws Throwable
    {
        dropTable("DROP TABLE %s");
        setGuardrail(true);
    }

    private void setGuardrail(boolean alterTableEnabled)
    {
        guardrails().setAlterTableEnabled(alterTableEnabled);
    }

    /**
     * Confirm that ALTER TABLE queries either work (guardrail enabled) or fail (guardrail disabled) appropriately
     * @throws Throwable
     */
    @Test
    public void testGuardrailEnabledAndDisabled() throws Throwable
    {
        setGuardrail(false);
        assertFails("ALTER TABLE %s ADD test_one text;", "changing columns");

        setGuardrail(true);
        assertValid("ALTER TABLE %s ADD test_two text;");

        setGuardrail(false);
        assertFails("ALTER TABLE %s ADD test_three text;", "changing columns");
    }

    /**
     * Confirm the guardrail appropriately catches the ALTER DROP case on a column
     * @throws Throwable
     */
    @Test
    public void testAppliesToAlterDropColumn() throws Throwable
    {
        setGuardrail(true);
        assertValid("ALTER TABLE %s ADD test_one text;");

        setGuardrail(false);
        assertFails("ALTER TABLE %s DROP test_one", "changing columns");

        setGuardrail(true);
        assertValid("ALTER TABLE %s DROP test_one");
    }

    /**
     * Confirm the guardrail appropriately catches the ALTER RENAME case on a column
     * @throws Throwable
     */
    @Test
    public void testAppliesToAlterRenameColumn() throws Throwable
    {
        setGuardrail(false);
        assertFails("ALTER TABLE %s RENAME c TO renamed_c", "changing columns");

        setGuardrail(true);
        assertValid("ALTER TABLE %s RENAME c TO renamed_c");
    }

    /**
     * Confirm we can always alter properties via the options map regardless of guardrail state
     * @throws Throwable
     */
    @Test
    public void testAlterViaMapAlwaysWorks() throws Throwable
    {
        setGuardrail(false);
        assertValid("ALTER TABLE %s WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32 };");

        setGuardrail(true);
        assertValid("ALTER TABLE %s WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32 };");
    }

    /**
     * Confirm the other form of ALTER TABLE property map changing always works regardless of guardrail state
     * @throws Throwable
     */
    @Test
    public void testAlterOptionsAlwaysWorks() throws Throwable
    {
        setGuardrail(true);
        assertValid("ALTER TABLE %s WITH GC_GRACE_SECONDS = 456; ");

        setGuardrail(false);
        assertValid("ALTER TABLE %s WITH GC_GRACE_SECONDS = 123; ");
    }
}
