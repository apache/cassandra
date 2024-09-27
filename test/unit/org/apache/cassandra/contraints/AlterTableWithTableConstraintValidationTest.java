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

package org.apache.cassandra.contraints;

import org.junit.Test;
import org.apache.cassandra.cql3.ConstraintViolationException;


public class AlterTableWithTableConstraintValidationTest extends CqlConstraintValidationTester
{

    @Test
    public void testCreateTableWithColumnNamedConstraintDescribeTableNonFunction() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2), CONSTRAINT cons1 CHECK ck1 < 100) WITH CLUSTERING ORDER BY (ck1 ASC);");

        execute("ALTER TABLE %s DROP CONSTRAINT cons1");

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
    public void testCreateTableWithColumnDropNonExistingConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");
        assertInvalidThrow(ConstraintViolationException.class, "ALTER TABLE %s DROP CONSTRAINT cons1");
    }

    @Test
    public void testCreateTableWithColumnDropNonExistingConstraintWithAlternativeConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2), CONSTRAINT cons1 CHECK ck1 < 100) WITH CLUSTERING ORDER BY (ck1 ASC);");
        assertInvalidThrow(ConstraintViolationException.class, "ALTER TABLE %s DROP CONSTRAINT cons2");
    }

    @Test
    public void testCreateTableAddConstraint() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        execute("ALTER TABLE %s ADD CONSTRAINT cons1 CHECK ck1 < 100");

        String tableCreateStatement = "CREATE TABLE " + KEYSPACE + "." + table + " (\n" +
                                      "    pk int,\n" +
                                      "    ck1 int,\n" +
                                      "    ck2 int,\n" +
                                      "    v int,\n" +
                                      "    PRIMARY KEY (pk, ck1, ck2),\n" +
                                      "    CONSTRAINT cons1 CHECK ck1 < 100\n" +
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

        execute("ALTER TABLE %s ADD CONSTRAINT cons1 CHECK ck1 < 100");
        execute("ALTER TABLE %s ADD CONSTRAINT cons2 CHECK ck2 > 10");

        String tableCreateStatement = "CREATE TABLE " + KEYSPACE + "." + table + " (\n" +
                                      "    pk int,\n" +
                                      "    ck1 int,\n" +
                                      "    ck2 int,\n" +
                                      "    v int,\n" +
                                      "    PRIMARY KEY (pk, ck1, ck2),\n" +
                                      "    CONSTRAINT cons1 CHECK ck1 < 100,\n" +
                                      "    CONSTRAINT cons2 CHECK ck2 > 10\n" +
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

        execute("ALTER TABLE %s ADD CONSTRAINT cons1 CHECK ck1 < 100");

        String tableCreateStatement = "CREATE TABLE " + KEYSPACE + "." + table + " (\n" +
                                      "    pk int,\n" +
                                      "    ck1 int,\n" +
                                      "    ck2 text,\n" +
                                      "    v int,\n" +
                                      "    PRIMARY KEY (pk, ck1, ck2),\n" +
                                      "    CONSTRAINT cons1 CHECK ck1 < 100\n" +
                                      ") WITH CLUSTERING ORDER BY (ck1 ASC, ck2 ASC)\n" +
                                      "    AND " + tableParametersCql();

        assertRowsNet(executeDescribeNet(KEYSPACE, "DESCRIBE TABLE " + KEYSPACE + "." + table),
                      row(KEYSPACE,
                          "table",
                          table,
                          tableCreateStatement));

        execute("ALTER TABLE %s ADD CONSTRAINT cons2 CHECK LENGTH(ck2) = 4");

        tableCreateStatement = "CREATE TABLE " + KEYSPACE + "." + table + " (\n" +
                                      "    pk int,\n" +
                                      "    ck1 int,\n" +
                                      "    ck2 text,\n" +
                                      "    v int,\n" +
                                      "    PRIMARY KEY (pk, ck1, ck2),\n" +
                                      "    CONSTRAINT cons1 CHECK ck1 < 100,\n" +
                                      "    CONSTRAINT cons2 CHECK LENGTH(ck2) = 4\n" +
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
        String table = createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");

        execute("ALTER TABLE %s ADD CONSTRAINT cons1 CHECK ck1 < 100");
        execute("ALTER TABLE %s DROP CONSTRAINT cons1");

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
    public void testCreateTableWithColumnAddExistingConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");
        execute("ALTER TABLE %s ADD CONSTRAINT cons1 CHECK ck1 < 100");
        assertInvalidThrow(ConstraintViolationException.class, "ALTER TABLE %s ADD CONSTRAINT cons1 CHECK LENGTH(ck2) = 4");
    }

    @Test
    public void testCreateTableWithColumnAndConstraintAddExistingConstraint() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2), CONSTRAINT cons1 CHECK LENGTH(ck1) = 4) WITH CLUSTERING ORDER BY (ck1 ASC);");
        assertInvalidThrow(ConstraintViolationException.class, "ALTER TABLE %s ADD CONSTRAINT cons1 CHECK LENGTH(ck2) = 4");
    }
}
