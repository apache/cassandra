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

import static java.lang.String.format;

public class GuardrailDropTruncateTableTest extends GuardrailTester
{
    private String tableQuery = "CREATE TABLE %s(pk int, ck int, v int, PRIMARY KEY(pk, ck))";

    private void setGuardrail(boolean enabled)
    {
        Guardrails.instance.setDropTruncateTableEnabled(enabled);
    }

    @After
    public void afterTest()
    {
        setGuardrail(true);
    }

    @Test
    public void testCanDropWhileFeatureEnabled() throws Throwable
    {
        setGuardrail(true);
        createTable(tableQuery);
        assertValid(String.format("DROP TABLE %s", currentTable()));
    }

    @Test
    public void testCannotDropWhileFeatureDisabled() throws Throwable
    {
        setGuardrail(false);
        createTable(tableQuery);
        assertFails("DROP TABLE %s", "DROP and TRUNCATE TABLE functionality is not allowed");
    }

    @Test
    public void testIfExistsDoesNotBreakGuardrail() throws Throwable
    {
        setGuardrail(false);
        createTable(tableQuery);
        assertFails("DROP TABLE IF EXISTS %s", "DROP and TRUNCATE TABLE functionality is not allowed");
    }

    @Test
    public void testCanTruncateWhileFeatureEnabled() throws Throwable
    {
        setGuardrail(true);
        createTable(tableQuery);
        assertValid(String.format("TRUNCATE TABLE %s", currentTable()));
    }

    @Test
    public void testCannotTruncateWhileFeatureDisabled() throws Throwable
    {
        setGuardrail(false);
        createTable(tableQuery);
        assertFails("TRUNCATE TABLE %s", "DROP and TRUNCATE TABLE functionality is not allowed");
    }

    @Test
    public void testExcludedUsersCanAlwaysDropAndTruncate() throws Throwable
    {
        String table = keyspace() + '.' + createTableName();
        setGuardrail(false);
        testExcludedUsers(() -> format("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int, v4 int)", table),
                          () -> format("TRUNCATE TABLE %s", table),
                          () -> format("DROP TABLE %s", table));

    }
}
