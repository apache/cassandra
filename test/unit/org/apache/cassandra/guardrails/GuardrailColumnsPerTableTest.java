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


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static java.lang.String.format;

/**
 * Tests the guardrail for the max number of columns in a table.
 */
public class GuardrailColumnsPerTableTest extends GuardrailTester
{
    private static final long TABLES_PER_COLUMN_THRESHOLD = 3;
    // Name use when testing CREATE TABLE that should fail (we need to provide it as assertFails, which
    // is use to assert the failure, does not know that it is a CREATE TABLE and would thus reuse the name of the
    // previously created table, which is not what we want).
    private static final String FAIL_TABLE = "failure_table_creation_test";

    private long defaultColumnsPerTableThreshold;

    @Before
    public void before()
    {
        defaultColumnsPerTableThreshold = config().columns_per_table_failure_threshold;
        config().columns_per_table_failure_threshold = TABLES_PER_COLUMN_THRESHOLD;
    }

    @After
    public void after()
    {
        config().columns_per_table_failure_threshold = defaultColumnsPerTableThreshold;
    }

    @Test
    public void testConfigValidation()
    {
        testValidationOfStrictlyPositiveProperty((c, v) -> c.columns_per_table_failure_threshold = v,
                                                 "columns_per_table_failure_threshold");
    }

    @Test
    public void testCreateTable() throws Throwable
    {
        // partition key on skinny table
        assertValid(format("CREATE TABLE %s (k1 int, v int, PRIMARY KEY((k1)))", createTableName()));
        assertValid(format("CREATE TABLE %s (k1 int, k2 int, v int, PRIMARY KEY((k1, k2)))", createTableName()));
        assertFails(4, "CREATE TABLE %s (k1 int, k2 int, k3 int, v int, PRIMARY KEY((k1, k2, k3)))", FAIL_TABLE);
        assertFails(5, "CREATE TABLE %s (k1 int, k2 int, k3 int, k4 int, v int, PRIMARY KEY((k1, k2, k3, k4)))", FAIL_TABLE);

        // partition key on wide table
        assertValid(format("CREATE TABLE %s (k1 int, c int, v int, PRIMARY KEY(k1, c))", createTableName()));
        assertFails(4, "CREATE TABLE %s (k1 int, k2 int, c int, v int, PRIMARY KEY((k1, k2), c))", FAIL_TABLE);
        assertFails(5, "CREATE TABLE %s (k1 int, k2 int, k3 int, c int, v int, PRIMARY KEY((k1, k2, k3), c))", FAIL_TABLE);

        // clustering key
        assertValid(format("CREATE TABLE %s (k int, c1 int, v int, PRIMARY KEY(k, c1))", createTableName()));
        assertFails(4, "CREATE TABLE %s (k int, c1 int, c2 int, v int, PRIMARY KEY(k, c1, c2))", FAIL_TABLE);
        assertFails(5, "CREATE TABLE %s (k int, c1 int, c2 int, c3 int, v int, PRIMARY KEY(k, c1, c2, c3))", FAIL_TABLE);

        // static column
        assertValid(format("CREATE TABLE %s (k int, c int, s1 int STATIC, PRIMARY KEY(k, c))", createTableName()));
        assertFails(4, "CREATE TABLE %s (k int, c int, s1 int STATIC, s2 int STATIC, PRIMARY KEY(k, c))", FAIL_TABLE);
        assertFails(5, "CREATE TABLE %s (k int, c int, s1 int STATIC, s2 int STATIC, s3 int STATIC, PRIMARY KEY(k, c))", FAIL_TABLE);

        // regular column on skinny table
        assertValid(format("CREATE TABLE %s (k int PRIMARY KEY, v1 int)", createTableName()));
        assertValid(format("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int)", createTableName()));
        assertFails(4, "CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)", FAIL_TABLE);
        assertFails(5, "CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int, v4 int)", FAIL_TABLE);

        // regular column on wide table
        assertValid(format("CREATE TABLE %s (k int, c int, v1 int, PRIMARY KEY(k, c))", createTableName()));
        assertFails(4, "CREATE TABLE %s (k int, c int, v1 int, v2 int, PRIMARY KEY(k, c))", FAIL_TABLE);
        assertFails(5, "CREATE TABLE %s (k int, c int, v1 int, v2 int, v3 int, PRIMARY KEY(k, c))", FAIL_TABLE);

        // udt
        String udt = createType("CREATE TYPE %s (a int, b int, c int, d int)");
        assertValid(format("CREATE TABLE %s (k int PRIMARY KEY, v %s)", createTableName(), udt));
    }

    @Test
    public void testAlterTableAddColumn() throws Throwable
    {
        // skinny table under threshold
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int)");
        assertValid("ALTER TABLE %s ADD v2 int");
        assertFails(4, "ALTER TABLE %s ADD v3 int");

        // skinny table at threshold
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int)");
        assertFails(4, "ALTER TABLE %s ADD v3 int");

        // wide table
        createTable("CREATE TABLE %s (k int, c int, v1 int, PRIMARY KEY(k, c))");
        assertFails(4, "ALTER TABLE %s ADD v2 int");
        assertFails(4, "ALTER TABLE %s ADD s int STATIC");

        // udt
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int)");
        String udt = createType("CREATE TYPE %s (a int, b int, c int, d int)");
        assertValid("ALTER TABLE %s ADD v2 " + udt);
    }

    /**
     * Verifies that its possible to drop columns from a table that has more columns than the current threshold.
     */
    @Test
    public void testAlterTableDropColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int)");
        DatabaseDescriptor.getGuardrailsConfig().columns_per_table_failure_threshold = 2L;
        assertValid("ALTER TABLE %s DROP v2");
        assertFails(3, "ALTER TABLE %s ADD v2 int");
    }

    private void assertFails(int columns, String query) throws Throwable
    {
        assertFails(columns, query, currentTable());
    }

    private void assertFails(int columns, String query, String tableName) throws Throwable
    {
        String errorMessage = format("Tables cannot have more than %s columns, but %s provided for table %s",
                                     DatabaseDescriptor.getGuardrailsConfig().columns_per_table_failure_threshold,
                                     columns,
                                     tableName);
        assertFails(errorMessage, format(query, keyspace() + '.' + tableName));
    }
}
