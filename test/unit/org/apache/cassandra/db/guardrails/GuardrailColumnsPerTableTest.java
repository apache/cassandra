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

import org.junit.Test;

import static java.lang.String.format;

/**
 * Tests the guardrail for the number of columns in a table, {@link Guardrails#columnsPerTable}.
 */
public class GuardrailColumnsPerTableTest extends ThresholdTester
{
    private static final int COLUMNS_PER_TABLE_WARN_THRESHOLD = 2;
    private static final int COLUMNS_PER_TABLE_FAIL_THRESHOLD = 4;

    public GuardrailColumnsPerTableTest()
    {
        super(COLUMNS_PER_TABLE_WARN_THRESHOLD,
              COLUMNS_PER_TABLE_FAIL_THRESHOLD,
              Guardrails.columnsPerTable,
              Guardrails::setColumnsPerTableThreshold,
              Guardrails::getColumnsPerTableWarnThreshold,
              Guardrails::getColumnsPerTableFailThreshold);
    }

    @Override
    protected long currentValue()
    {
        return getCurrentColumnFamilyStore().metadata().columns().size();
    }

    @Test
    public void testCreateTable() throws Throwable
    {
        // partition key on skinny table
        assertCreateTableValid("CREATE TABLE %s (k1 int, v int, PRIMARY KEY((k1)))");
        assertCreateTableWarns(3, "CREATE TABLE %s (k1 int, k2 int, v int, PRIMARY KEY((k1, k2)))");
        assertCreateTableWarns(4, "CREATE TABLE %s (k1 int, k2 int, k3 int, v int, PRIMARY KEY((k1, k2, k3)))");
        assertCreateTableFails(5, "CREATE TABLE %s (k1 int, k2 int, k3 int, k4 int, v int, PRIMARY KEY((k1, k2, k3, k4)))");
        assertCreateTableFails(6, "CREATE TABLE %s (k1 int, k2 int, k3 int, k4 int, k5 int, v int, PRIMARY KEY((k1, k2, k3, k4, k5)))");

        // partition key on wide table
        assertCreateTableWarns(3, "CREATE TABLE %s (k1 int, c int, v int, PRIMARY KEY(k1, c))");
        assertCreateTableWarns(4, "CREATE TABLE %s (k1 int, k2 int, c int, v int, PRIMARY KEY((k1, k2), c))");
        assertCreateTableFails(5, "CREATE TABLE %s (k1 int, k2 int, k3 int, c int, v int, PRIMARY KEY((k1, k2, k3), c))");
        assertCreateTableFails(6, "CREATE TABLE %s (k1 int, k2 int, k3 int, k4 int, c int, v int, PRIMARY KEY((k1, k2, k3, k4), c))");

        // clustering key
        assertCreateTableWarns(3, "CREATE TABLE %s (k int, c1 int, v int, PRIMARY KEY(k, c1))");
        assertCreateTableWarns(4, "CREATE TABLE %s (k int, c1 int, c2 int, v int, PRIMARY KEY(k, c1, c2))");
        assertCreateTableFails(5, "CREATE TABLE %s (k int, c1 int, c2 int, c3 int, v int, PRIMARY KEY(k, c1, c2, c3))");
        assertCreateTableFails(6, "CREATE TABLE %s (k int, c1 int, c2 int, c3 int, c4 int, v int, PRIMARY KEY(k, c1, c2, c3, c4))");

        // static column
        assertCreateTableWarns(3, "CREATE TABLE %s (k int, c int, s1 int STATIC, PRIMARY KEY(k, c))");
        assertCreateTableWarns(4, "CREATE TABLE %s (k int, c int, s1 int STATIC, s2 int STATIC, PRIMARY KEY(k, c))");
        assertCreateTableFails(5, "CREATE TABLE %s (k int, c int, s1 int STATIC, s2 int STATIC, s3 int STATIC, PRIMARY KEY(k, c))");
        assertCreateTableFails(6, "CREATE TABLE %s (k int, c int, s1 int STATIC, s2 int STATIC, s3 int STATIC, s4 int STATIC, PRIMARY KEY(k, c))");

        // regular column on skinny table
        assertCreateTableValid("CREATE TABLE %s (k int PRIMARY KEY, v1 int)");
        assertCreateTableWarns(3, "CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int)");
        assertCreateTableWarns(4, "CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        assertCreateTableFails(5, "CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int, v4 int)");
        assertCreateTableFails(6, "CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int, v4 int, v5 int)");

        // regular column on wide table
        assertCreateTableWarns(3, "CREATE TABLE %s (k int, c int, v1 int, PRIMARY KEY(k, c))");
        assertCreateTableWarns(4, "CREATE TABLE %s (k int, c int, v1 int, v2 int, PRIMARY KEY(k, c))");
        assertCreateTableFails(5, "CREATE TABLE %s (k int, c int, v1 int, v2 int, v3 int, PRIMARY KEY(k, c))");
        assertCreateTableFails(6, "CREATE TABLE %s (k int, c int, v1 int, v2 int, v3 int, v4 int, PRIMARY KEY(k, c))");

        // udt
        String udt = createType("CREATE TYPE %s (a int, b int, c int, d int)");
        assertValid(format("CREATE TABLE %s (k int PRIMARY KEY, v %s)", createTableName(), udt));
    }

    @Test
    public void testAlterTableAddColumn() throws Throwable
    {
        // skinny table under threshold
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int)");
        assertAddColumnWarns("ALTER TABLE %s ADD v2 int");
        assertAddColumnWarns("ALTER TABLE %s ADD v3 int");
        assertAddColumnFails("ALTER TABLE %s ADD v4 int");

        // skinny table at threshold
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        assertAddColumnFails("ALTER TABLE %s ADD v4 int");

        // wide table
        createTable("CREATE TABLE %s (k int, c int, v1 int, v2 int, PRIMARY KEY(k, c))");
        assertAddColumnFails("ALTER TABLE %s ADD v3 int");
        assertAddColumnFails("ALTER TABLE %s ADD s int STATIC");

        // udt
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int)");
        String udt = createType("CREATE TYPE %s (a int, b int, c int, d int)");
        assertAddColumnWarns("ALTER TABLE %s ADD v2 " + udt);
    }

    /**
     * Verifies that its possible to drop columns from a table that has more columns than the current threshold.
     */
    @Test
    public void testAlterTableDropColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        guardrails().setColumnsPerTableThreshold(2, 2);
        assertDropColumnValid("ALTER TABLE %s DROP v2");
        assertDropColumnValid("ALTER TABLE %s DROP v3");
        assertAddColumnFails("ALTER TABLE %s ADD v2 int");
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        String table = keyspace() + '.' + createTableName();
        testExcludedUsers(() -> format("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int, v4 int)", table),
                          () -> format("ALTER TABLE %s ADD v5 int", table),
                          () -> format("DROP TABLE %s", table));
    }

    private void assertCreateTableValid(String query) throws Throwable
    {
        assertMaxThresholdValid(format(query, keyspace() + '.' + createTableName()));
    }

    private void assertDropColumnValid(String query) throws Throwable
    {
        assertValid(format(query, keyspace() + '.' + currentTable()));
    }

    private void assertCreateTableWarns(int numColumns, String query) throws Throwable
    {
        assertWarns(numColumns, query, createTableName());
    }

    private void assertAddColumnWarns(String query) throws Throwable
    {
        assertWarns(currentValue() + 1, query, currentTable());
    }

    private void assertWarns(long numColumns, String query, String tableName) throws Throwable
    {
        assertThresholdWarns(format(query, keyspace() + '.' + tableName),
                             format("The table %s has %s columns, this exceeds the warning threshold of %s.",
                                    tableName,
                                    numColumns,
                                    guardrails().getColumnsPerTableWarnThreshold())
        );
    }

    private void assertAddColumnFails(String query) throws Throwable
    {
        assertFails(currentValue() + 1, query, currentTable());
    }

    private void assertCreateTableFails(long numColumns, String query) throws Throwable
    {
        assertFails(numColumns, query, FAIL_TABLE);
    }

    private void assertFails(long numColumns, String query, String tableName) throws Throwable
    {
        assertThresholdFails(format(query, keyspace() + '.' + tableName),
                             format("Tables cannot have more than %s columns, but %s provided for table %s",
                                    guardrails().getColumnsPerTableFailThreshold(),
                                    numColumns,
                                    tableName)
        );
    }
}
