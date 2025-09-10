/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.schema;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;

import org.junit.Test;

import static org.apache.cassandra.schema.SchemaConstants.FILENAME_LENGTH;
import static org.apache.cassandra.schema.SchemaConstants.NAME_LENGTH;
import static org.apache.cassandra.schema.SchemaConstants.TABLE_NAME_LENGTH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.fail;

public class CreateTableValidationTest extends CQLTester
{
    @Test
    public void testInvalidBloomFilterFPRatio() throws Throwable
    {
        try
        {
            createTableMayThrow("CREATE TABLE %s (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.0000001");
            fail("Expected an fp chance of 0.0000001 to be rejected");
        }
        catch (ConfigurationException exc) { }

        try
        {
            createTableMayThrow("CREATE TABLE %s (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 1.1");
            fail("Expected an fp chance of 1.1 to be rejected");
        }
        catch (ConfigurationException exc) { }

        // sanity check
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.1");
    }

    @Test
    public void testCreateTableOnSelectedClusteringColumn()
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");
    }

    @Test
    public void testCreateTableOnAllClusteringColumns()
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC, ck2 DESC);");
    }

    @Test
    public void testCreateTableErrorOnNonClusteringKey()
    {
        String expectedMessage = "Only clustering key columns can be defined in CLUSTERING ORDER directive";
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC, ck2 DESC, v ASC);",
                        expectedMessage+": [v]");
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (v ASC);",
                        expectedMessage+": [v]");
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (pk ASC);",
                        expectedMessage+": [pk]");
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (pk ASC, ck1 DESC);",
                        expectedMessage+": [pk]");
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC, ck2 DESC, pk DESC);",
                        expectedMessage+": [pk]");
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (pk DESC, v DESC);",
                        expectedMessage+": [pk, v]");
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (pk DESC, v DESC, ck1 DESC);",
                        expectedMessage+": [pk, v]");
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC, v ASC);",
                        expectedMessage+": [v]");
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (v ASC, ck1 DESC);",
                        expectedMessage+": [v]");
    }

    @Test
    public void testCreateTableInWrongOrder()
    {
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck2 ASC, ck1 DESC);",
                        "The order of columns in the CLUSTERING ORDER directive must match that of the clustering columns");
    }

    @Test
    public void testCreateTableWithMissingClusteringColumn()
    {
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck2 ASC);",
                        "Missing CLUSTERING ORDER for column ck1");
    }

    @Test
    public void testCreatingTableWithLongName() throws Throwable
    {
        int tableIdSuffix = "-1b255f4def2540a60000000000000000".length();
        String keyspaceName = "k".repeat(NAME_LENGTH);
        String tableName = "t".repeat(FILENAME_LENGTH - tableIdSuffix);
        String tooLongTableName = "l".repeat(FILENAME_LENGTH - tableIdSuffix + 1);

        execute(String.format("CREATE KEYSPACE %s with replication = " +
                              "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",
                              keyspaceName));

        assertInvalidMessage(String.format("%s.%s: Table name must not be more than %d characters long (got %d characters for \"%2$s\")",
                                           keyspaceName, tooLongTableName, TABLE_NAME_LENGTH, tooLongTableName.length()),
                             String.format("CREATE TABLE %s.%s (" +
                                           "key int PRIMARY KEY," +
                                           "val int)", keyspaceName, tooLongTableName));

        createTable(String.format("CREATE TABLE %s.%s (" +
                                  "key int PRIMARY KEY," +
                                  "val int)", keyspaceName, tableName));
        execute(String.format("INSERT INTO %s.%s (key,val) VALUES (1,1)", keyspaceName, tableName));
        assertThat(execute(String.format("SELECT * from %s.%s", keyspaceName, tableName))).hasSize(1);
        flush(keyspaceName, tableName);
        assertThat(execute(String.format("SELECT * from %s.%s", keyspaceName, tableName))).hasSize(1);
    }

    @Test
    public void testNonAlphanummericTableName() throws Throwable
    {
        assertInvalidMessage(String.format("%s.d-3: Table name must not be empty or not contain non-alphanumeric-underscore characters (got \"d-3\")", KEYSPACE),
                             String.format("CREATE TABLE %s.\"d-3\" (key int PRIMARY KEY, val int)", KEYSPACE));
        assertInvalidMessage(String.format("%s.    : Table name must not be empty or not contain non-alphanumeric-underscore characters (got \"    \")", KEYSPACE),
                             String.format("CREATE TABLE %s.\"    \" (key int PRIMARY KEY, val int)", KEYSPACE));
    }

    private void expectedFailure(String statement, String errorMsg)
    {

        assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> createTableMayThrow(statement)) .withMessageContaining(errorMsg);
    }
}
