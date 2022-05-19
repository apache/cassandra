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

public class GuardrailSimpleStrategyTest extends GuardrailTester
{
    public static String ERROR_MSG = "SimpleStrategy is not allowed";

    public GuardrailSimpleStrategyTest()
    {
        super(Guardrails.simpleStrategyEnabled);
    }

    private void setGuardrail(boolean enabled)
    {
        guardrails().setSimpleStrategyEnabled(enabled);
    }

    @After
    public void afterTest() throws Throwable
    {
        setGuardrail(true);
        execute("DROP KEYSPACE IF EXISTS test_ss;");
    }

    @Test
    public void testCanCreateWithGuardrailEnabled() throws Throwable
    {
        assertValid("CREATE KEYSPACE test_ss WITH replication = {'class': 'SimpleStrategy'};");
    }

    @Test
    public void testCanAlterWithGuardrailEnabled() throws Throwable
    {
        execute("CREATE KEYSPACE test_ss WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1':2, 'datacenter2':0};");
        assertValid("ALTER KEYSPACE test_ss WITH replication = {'class': 'SimpleStrategy'};");
    }

    @Test
    public void testGuardrailBlocksCreate() throws Throwable
    {
        setGuardrail(false);
        assertFails("CREATE KEYSPACE test_ss WITH replication = {'class': 'SimpleStrategy'};", ERROR_MSG);
    }

    @Test
    public void testGuardrailBlocksAlter() throws Throwable
    {
        setGuardrail(false);
        execute("CREATE KEYSPACE test_ss WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1':2, 'datacenter2':0};");
        assertFails("ALTER KEYSPACE test_ss WITH replication = {'class': 'SimpleStrategy'};", ERROR_MSG);
    }

    @Test
    public void testToggle() throws Throwable
    {
        setGuardrail(false);
        assertFails("CREATE KEYSPACE test_ss WITH replication = {'class': 'SimpleStrategy'};", ERROR_MSG);

        setGuardrail(true);
        assertValid("CREATE KEYSPACE test_ss WITH replication = {'class': 'SimpleStrategy'};");
        execute("ALTER KEYSPACE test_ss WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1':2, 'datacenter2':0};");

        setGuardrail(false);
        assertFails("ALTER KEYSPACE test_ss WITH replication = {'class': 'SimpleStrategy'};", ERROR_MSG);

        setGuardrail(true);
        assertValid("ALTER KEYSPACE test_ss WITH replication = {'class': 'SimpleStrategy'};");
    }
}
