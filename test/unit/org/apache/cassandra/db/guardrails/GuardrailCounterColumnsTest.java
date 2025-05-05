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

public class GuardrailCounterColumnsTest extends GuardrailTester
{
    public static String ERROR_MSG = "COUNTER type columns is not allowed";
    private static final String KEYSPACE = "counter_columns_test_keyspace";

    public GuardrailCounterColumnsTest()
    {
        super(Guardrails.counterColumnsEnabled);
    }

    @Before
    public void setup() throws Throwable
    {
        execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    }

    private void setGuardrail(boolean enabled)
    {
        guardrails().setCounterColumnsEnabled(enabled);
    }

    @After
    public void afterTest() throws Throwable
    {
        setGuardrail(true);
        execute("DROP KEYSPACE IF EXISTS " + KEYSPACE);
    }

    @Test
    public void testCanCreateWithGuardrailEnabled() throws Throwable
    {
        assertValid("CREATE TABLE " + KEYSPACE + ".test_counter (id int PRIMARY KEY, count counter);");
    }

    @Test
    public void testGuardrailBlocksCreate() throws Throwable
    {
        setGuardrail(false);
        assertFails("CREATE TABLE " + KEYSPACE + ".test_counter (id int PRIMARY KEY, count counter);", ERROR_MSG);
    }

    @Test
    public void testCanAlterWithGuardrailEnabled() throws Throwable
    {
        // Create a counter table with one counter column
        execute("CREATE TABLE " + KEYSPACE + ".test_counter_add (id int PRIMARY KEY, count counter);");

        // Can add another counter column with guardrail enabled
        assertValid("ALTER TABLE " + KEYSPACE + ".test_counter_add ADD newcount counter;");
    }

    @Test
    public void testGuardrailBlocksAlter() throws Throwable
    {
        // First create a counter table when guardrail is enabled
        execute("CREATE TABLE " + KEYSPACE + ".test_counter_add (id int PRIMARY KEY, count counter);");

        // Now disable the guardrail
        setGuardrail(false);

        // Should fail trying to add another counter column
        assertFails("ALTER TABLE " + KEYSPACE + ".test_counter_add ADD newcount counter;", ERROR_MSG);
    }

    @Test
    public void testToggle() throws Throwable
    {
        setGuardrail(false);
        assertFails("CREATE TABLE " + KEYSPACE + ".test_counter (id int PRIMARY KEY, count counter);", ERROR_MSG);

        setGuardrail(true);
        assertValid("CREATE TABLE " + KEYSPACE + ".test_counter (id int PRIMARY KEY, count counter);");

        // Create a counter table for alter testing
        execute("CREATE TABLE " + KEYSPACE + ".test_counter_add (id int PRIMARY KEY, count counter);");

        setGuardrail(false);
        assertFails("ALTER TABLE " + KEYSPACE + ".test_counter_add ADD newcount counter;", ERROR_MSG);

        setGuardrail(true);
        assertValid("ALTER TABLE " + KEYSPACE + ".test_counter_add ADD newcount counter;");
    }
}
