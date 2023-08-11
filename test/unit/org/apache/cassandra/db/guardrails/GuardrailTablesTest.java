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

import org.apache.cassandra.config.Converters;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.Schema;

import static java.lang.String.format;

/**
 * Tests the guardrail for the max number of user tables, {@link Guardrails#tables}.
 */
public class GuardrailTablesTest extends ThresholdTester
{
    private static final int TABLES_LIMIT_WARN_THRESHOLD = 1;
    private static final int TABLES_LIMIT_FAIL_THRESHOLD = 2;

    public GuardrailTablesTest()
    {
        super(TABLES_LIMIT_WARN_THRESHOLD,
              TABLES_LIMIT_FAIL_THRESHOLD,
              Guardrails.tables,
              Guardrails::setTablesThreshold,
              Guardrails::getTablesWarnThreshold,
              Guardrails::getTablesFailThreshold);
    }

    @Override
    protected long currentValue()
    {
        return Keyspace.open(KEYSPACE_PER_TEST).getColumnFamilyStores().size();
    }

    @Test
    public void testCreateTableWithGuardrailThreshold() throws Throwable
    {
        assertCreateTable();
    }
    
    @Test
    public void testCreateTableWithDeprecatedTableCountThreshold() throws Throwable
    {
        // Convert and set a deprecated threshold value based on the total number of tables, not just user tables
        int convertedValue = (int) Converters.TABLE_COUNT_THRESHOLD_TO_GUARDRAIL.convert(Schema.instance.getNumberOfTables());
        Guardrails.instance.setTablesThreshold(convertedValue + 1, TABLES_LIMIT_FAIL_THRESHOLD);

        assertCreateTable();
    }

    private void assertCreateTable() throws Throwable
    {
        // create tables until hitting the two warn/fail thresholds
        String t1 = assertCreateTableValid();
        String t2 = assertCreateTableWarns();
        assertCreateTableFails();

        // drop a table and hit the warn/fail threshold again
        dropTable(t2);
        String t3 = assertCreateTableWarns();
        assertCreateTableFails();

        // drop two tables and hit the warn/fail threshold again
        dropTable(t1);
        dropTable(t3);
        assertCreateTableValid();
        assertCreateTableWarns();
        assertCreateTableFails();

        // test excluded users
        testExcludedUsers(this::createTableQuery,
                          this::createTableQuery,
                          this::createTableQuery);
    }

    @Override
    protected void dropTable(String tableName)
    {
        super.dropTable(format("DROP TABLE %s.%s", KEYSPACE_PER_TEST, tableName));
    }

    private String assertCreateTableValid() throws Throwable
    {
        String tableName = createTableName();
        assertMaxThresholdValid(createTableQuery(tableName));
        return tableName;
    }

    private String assertCreateTableWarns() throws Throwable
    {
        String tableName = createTableName();
        assertThresholdWarns(createTableQuery(tableName),
                             format("Creating table %s, current number of tables 2 exceeds warning threshold of 1", tableName)
        );
        return tableName;
    }

    private void assertCreateTableFails() throws Throwable
    {
        String tableName = createTableName();
        assertThresholdFails(createTableQuery(tableName),
                             format("Cannot have more than 2 tables, aborting the creation of table %s", tableName)
        );
    }

    private String createTableQuery()
    {
        return createTableQuery(createTableName());
    }

    private String createTableQuery(String tableName)
    {
        return format("CREATE TABLE %s.%s (k1 int, v int, PRIMARY KEY((k1)))", KEYSPACE_PER_TEST, tableName);
    }
}
