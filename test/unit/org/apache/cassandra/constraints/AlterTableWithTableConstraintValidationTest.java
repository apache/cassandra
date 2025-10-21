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

package org.apache.cassandra.constraints;

import org.junit.Test;

import org.apache.cassandra.exceptions.InvalidRequestException;

public class AlterTableWithTableConstraintValidationTest extends CqlConstraintValidationTester
{
    @Test
    public void testCreateTableWithColumnNamedConstraintDescribeTableNonFunction() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (pk int, ck1 int CHECK ck1 < 100, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        execute("ALTER TABLE %s ALTER ck1 DROP CHECK");

        String tableCreateStatement = "CREATE TABLE " + KEYSPACE + "." + table + " (\n" +
                                      "    pk int,\n" +
                                      "    ck1 int,\n" +
                                      "    ck2 int,\n" +
                                      "    v int,\n" +
                                      "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                      ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                      "    AND " + tableParametersCql();

        assertRowsNet(executeDescribeNet(KEYSPACE, "DESCRIBE TABLE " + KEYSPACE + "." + table),
                      row(KEYSPACE,
                          "table",
                          table,
                          tableCreateStatement));
    }

    @Test
    public void testCreateTableAddConstraint() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        execute("ALTER TABLE %s ALTER ck1 CHECK ck1 < 100 AND ck1 > 10");

        String tableCreateStatement = "CREATE TABLE " + KEYSPACE + "." + table + " (\n" +
                                      "    pk int,\n" +
                                      "    ck1 int CHECK ck1 < 100 AND ck1 > 10,\n" +
                                      "    ck2 int,\n" +
                                      "    v int,\n" +
                                      "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                      ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                      "    AND " + tableParametersCql();

        assertRowsNet(executeDescribeNet(KEYSPACE, "DESCRIBE TABLE " + KEYSPACE + "." + table),
                      row(KEYSPACE,
                          "table",
                          table,
                          tableCreateStatement));
    }

    @Test
    public void testCreateTableAddMultipleConstraints() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        execute("ALTER TABLE %s ALTER ck1 CHECK ck1 < 100");
        execute("ALTER TABLE %s ALTER ck2 CHECK ck2 > 10");

        String tableCreateStatement = "CREATE TABLE " + KEYSPACE + "." + table + " (\n" +
                                      "    pk int,\n" +
                                      "    ck1 int CHECK ck1 < 100,\n" +
                                      "    ck2 int CHECK ck2 > 10,\n" +
                                      "    v int,\n" +
                                      "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                      ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                      "    AND " + tableParametersCql();

        assertRowsNet(executeDescribeNet(KEYSPACE, "DESCRIBE TABLE " + KEYSPACE + "." + table),
                      row(KEYSPACE,
                          "table",
                          table,
                          tableCreateStatement));
    }

    @Test
    public void testCreateTableAddMultipleMixedConstraints() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (pk int, ck1 int, ck2 text, v int, PRIMARY KEY ((pk), ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        execute("ALTER TABLE %s ALTER ck1 CHECK ck1 < 100");

        String tableCreateStatement = "CREATE TABLE " + KEYSPACE + "." + table + " (\n" +
                                      "    pk int,\n" +
                                      "    ck1 int CHECK ck1 < 100,\n" +
                                      "    ck2 text,\n" +
                                      "    v int,\n" +
                                      "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                      ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                      "    AND " + tableParametersCql();

        assertRowsNet(executeDescribeNet(KEYSPACE, "DESCRIBE TABLE " + KEYSPACE + "." + table),
                      row(KEYSPACE,
                          "table",
                          table,
                          tableCreateStatement));

        execute("ALTER TABLE %s ALTER ck2 CHECK LENGTH() = 4");

        tableCreateStatement = "CREATE TABLE " + KEYSPACE + "." + table + " (\n" +
                               "    pk int,\n" +
                               "    ck1 int CHECK ck1 < 100,\n" +
                               "    ck2 text CHECK LENGTH() = 4,\n" +
                               "    v int,\n" +
                               "    PRIMARY KEY (pk, ck1, ck2)\n" +
                               ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                               "    AND " + tableParametersCql();

        assertRowsNet(executeDescribeNet(KEYSPACE, "DESCRIBE TABLE " + KEYSPACE + "." + table),
                      row(KEYSPACE,
                          "table",
                          table,
                          tableCreateStatement));

        execute("ALTER TABLE %s ALTER v CHECK NOT NULL");

        tableCreateStatement = "CREATE TABLE " + KEYSPACE + "." + table + " (\n" +
                               "    pk int,\n" +
                               "    ck1 int CHECK ck1 < 100,\n" +
                               "    ck2 text CHECK LENGTH() = 4,\n" +
                               "    v int CHECK NOT NULL,\n" +
                               "    PRIMARY KEY (pk, ck1, ck2)\n" +
                               ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                               "    AND " + tableParametersCql();

        assertRowsNet(executeDescribeNet(KEYSPACE, "DESCRIBE TABLE " + KEYSPACE + "." + table),
                      row(KEYSPACE,
                          "table",
                          table,
                          tableCreateStatement));
    }

    @Test
    public void testCreateTableAddAndRemoveConstraint() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (pk int, ck1 int, ck2 text, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        execute("ALTER TABLE %s ALTER ck1 CHECK ck1 < 100");

        String tableCreateStatement = "CREATE TABLE " + KEYSPACE + "." + table + " (\n" +
                                      "    pk int,\n" +
                                      "    ck1 int CHECK ck1 < 100,\n" +
                                      "    ck2 text,\n" +
                                      "    v int,\n" +
                                      "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                      ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                      "    AND " + tableParametersCql();

        assertRowsNet(executeDescribeNet(KEYSPACE, "DESCRIBE TABLE " + KEYSPACE + "." + table),
                      row(KEYSPACE,
                          "table",
                          table,
                          tableCreateStatement));

        execute("ALTER TABLE %s ALTER ck1 DROP CHECK");

        String tableCreateStatement2 = "CREATE TABLE " + KEYSPACE + "." + table + " (\n" +
                                      "    pk int,\n" +
                                      "    ck1 int,\n" +
                                      "    ck2 text,\n" +
                                      "    v int,\n" +
                                      "    PRIMARY KEY (pk, ck1, ck2)\n" +
                                      ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                      "    AND " + tableParametersCql();

        assertRowsNet(executeDescribeNet(KEYSPACE, "DESCRIBE TABLE " + KEYSPACE + "." + table),
                      row(KEYSPACE,
                          "table",
                          table,
                          tableCreateStatement2));
    }

    @Test
    public void testAlterWithConstraintsAndCdcEnabled() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, ck1 int, ck2 int, PRIMARY KEY ((pk),ck1, ck2)) WITH cdc = true;");
        // It works
        execute("ALTER TABLE %s ALTER ck1 CHECK ck1 < 100");
    }

    @Test
    public void testAlterWithCdcAndPKConstraintsEnabled() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text CHECK length() = 100, ck1 int, ck2 int, PRIMARY KEY ((pk), ck1, ck2));");
        // It works
        execute("ALTER TABLE %s WITH cdc = true");
    }

    @Test
    public void testAlterWithCdcAndRegularConstraintsEnabled() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, ck1 int CHECK ck1 < 100, ck2 int, PRIMARY KEY (pk));");
        // It works
        execute("ALTER TABLE %s WITH cdc = true");
    }

    @Test
    public void testAlterWithCdcAndClusteringConstraintsEnabled() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, ck1 int CHECK ck1 < 100, ck2 int, PRIMARY KEY ((pk), ck1, ck2));");
        // It works
        execute("ALTER TABLE %s WITH cdc = true");
    }

    @Test
    public void testCreateTableAddConstraintWithIfExists() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");
        execute("ALTER TABLE %s ALTER IF EXISTS foo CHECK foo < 100");
    }

    @Test
    public void testCreateTableAddConstraintWithNonExistingColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");
        String expectedErrorMessage = "Column 'foo' doesn't exist";
        assertInvalidThrowMessage(expectedErrorMessage, InvalidRequestException.class, "ALTER TABLE %s ALTER foo CHECK foo < 100");
    }

    @Test
    public void testAlterTableAlterExistingColumnWithCheckOnNonExistingColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 text, ck2 text, v int, PRIMARY KEY ((pk),ck1, ck2));");
        assertInvalidThrowMessage("Constraint ck3 < 100 was not specified on a column it operates on: ck1 but on: ck3",
                                  InvalidRequestException.class,
                                  "ALTER TABLE %s ALTER ck1 CHECK ck3 < 100");
    }

    @Test
    public void testAlterTableAddNewColumnWithCheckOnNonExistingColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 text, ck2 text, v int, PRIMARY KEY ((pk),ck1, ck2));");

        assertInvalidThrowMessage("Constraint v3 < 100 was not specified on a column it operates on: v2 but on: v3",
                                  InvalidRequestException.class,
                                  "ALTER TABLE %s ADD v2 int CHECK v3 < 100");
    }

    @Test
    public void testAlterTableAddColumnWithCheck()
    {
        createTable("CREATE TABLE %s (pk text, col1 int, primary key (pk));");
        execute("ALTER TABLE %s ADD col2 int CHECK col2 > 0");
    }

    @Test
    public void testNotNullSyntax() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, col1 int NOT NULL, primary key (pk));");
        createTable("CREATE TABLE %s (pk text, col1 int CHECK NOT NULL, primary key (pk));");
        createTable("CREATE TABLE %s (pk text, col1 int NOT NULL CHECK col1 > 0, primary key (pk));");
        execute("ALTER TABLE %s ALTER col1 CHECK col1 > 100");
        execute("ALTER TABLE %s ALTER col1 CHECK NOT NULL AND col1 > 100");

        assertInvalidThrowMessage("Duplicate definition of NOT NULL constraint",
                                  InvalidRequestException.class,
                                  "CREATE TABLE %s (pk text, col1 int NOT NULL CHECK NOT NULL, primary key (pk));");
    }
}
