/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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

package org.apache.cassandra.guardrails;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.config.DatabaseDescriptor;

public class GuardrailTruncateTableTest extends GuardrailTester
{
    private static boolean truncateTableEnabled;

    @BeforeClass
    public static void setup()
    {
        truncateTableEnabled = DatabaseDescriptor.getGuardrailsConfig().truncate_table_enabled;
    }

    @AfterClass
    public static void tearDown()
    {
        setGuardrails(truncateTableEnabled);
    }

    private static void setGuardrails(boolean truncate_table_enabled)
    {
        DatabaseDescriptor.getGuardrailsConfig().truncate_table_enabled = truncate_table_enabled;
    }

    private void testTruncate(boolean truncateTableEnabled) throws Throwable
    {
        setGuardrails(truncateTableEnabled);
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY(a, b))");

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 0, 2);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 3);

        assertRows(execute("SELECT * FROM %s"), row(1, 0, 2), row(1, 1, 3), row(0, 0, 0), row(0, 1, 1));

        assertValid("TRUNCATE %s");

        assertEmpty(execute("SELECT * FROM %s"));
    }

    @Test
    public void testEnabledTruncateTable() throws Throwable
    {
        testTruncate(true);
    }

    @Test(expected = InvalidQueryException.class)
    public void testDisabledTruncateTable() throws Throwable
    {
        testTruncate(false);
    }
}