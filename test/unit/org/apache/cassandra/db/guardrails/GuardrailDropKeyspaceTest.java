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
import org.junit.Test;

public class GuardrailDropKeyspaceTest extends GuardrailTester
{
    private String keyspaceQuery = "CREATE KEYSPACE dkdt WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";

    private void setGuardrail(boolean enabled)
    {
        Guardrails.instance.setDropKeyspaceEnabled(enabled);
    }

    public GuardrailDropKeyspaceTest()
    {
        super(Guardrails.dropKeyspaceEnabled);
    }

    @After
    public void afterTest() throws Throwable
    {
        setGuardrail(true);
        execute("DROP KEYSPACE IF EXISTS dkdt");
    }

    @Test
    public void testCanDropWhileFeatureEnabled() throws Throwable
    {
        setGuardrail(true);
        createKeyspace(keyspaceQuery);
        execute("DROP KEYSPACE dkdt");
    }

    @Test
    public void testCannotDropWhileFeatureDisabled() throws Throwable
    {
        setGuardrail(false);
        createKeyspace(keyspaceQuery);
        assertFails("DROP KEYSPACE dkdt", "DROP KEYSPACE functionality is not allowed");
    }

    @Test
    public void testIfExistsDoesNotBypassCheck() throws Throwable
    {
        setGuardrail(false);
        createKeyspace(keyspaceQuery);
        assertFails("DROP KEYSPACE IF EXISTS dkdt", "DROP KEYSPACE functionality is not allowed");
    }

    @Test
    public void testToggle() throws Throwable
    {
        setGuardrail(false);
        createKeyspace(keyspaceQuery);
        assertFails("DROP KEYSPACE IF EXISTS dkdt", "DROP KEYSPACE functionality is not allowed");

        setGuardrail(true);
        execute("DROP KEYSPACE dkdt");
    }
}
