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

import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;

public class GuardrailAllowFilteringTest extends GuardrailTester
{
    private boolean enableState;

    @Before
    public void setupTest()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)");
        enableState = getGuardrial();
    }

    @After
    public void teardownTest()
    {
        setGuardrail(enableState);
    }

    private void setGuardrail(boolean allowFilteringEnabled)
    {
        guardrails().setAllowFilteringEnabled(allowFilteringEnabled);
    }

    private boolean getGuardrial()
    {
        return guardrails().getAllowFilteringEnabled();
    }

    @Test
    public void testAllowFilteringDisabled() throws Throwable
    {
        setGuardrail(false);
        assertFails("SELECT * FROM %s WHERE a = 5 ALLOW FILTERING", "Querying with ALLOW FILTERING is not allowed");
    }

    @Test
    public void testAllowFilteringDisabedNotUsed() throws Throwable
    {
        setGuardrail(false);
        execute("INSERT INTO %s (k, a, b) VALUES (1, 1, 1)");
        assertValid("SELECT * FROM %s");
    }

    @Test
    public void testAllowFilteringEnabled() throws Throwable
    {
        setGuardrail(true);
        execute("INSERT INTO %s (k, a, b) VALUES (1, 1, 1)");
        assertValid("SELECT * FROM %s WHERE a = 5 ALLOW FILTERING");
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        setGuardrail(false);
        testExcludedUsers(() -> "SELECT * FROM %s WHERE a = 5 ALLOW FILTERING");
    }

    @Test
    public void testSystemTable() throws Throwable
    {
        setGuardrail(false);
        assertValid(String.format("SELECT * FROM %s.%s WHERE table_name = '%s' ALLOW FILTERING",
                                  SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                  SchemaKeyspaceTables.TABLES,
                                  currentTable()));
    }
}
