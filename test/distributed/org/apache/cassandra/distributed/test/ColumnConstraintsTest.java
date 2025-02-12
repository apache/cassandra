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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.cql3.constraints.ConstraintViolationException;
import org.apache.cassandra.cql3.constraints.InvalidConstraintDefinitionException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ColumnConstraintsTest extends TestBaseImpl
{
    final static Map<String, String> RELATIONS_MAP = Map.of("st", "<",
                                                           "set", "<=",
                                                           "et", "=",
                                                           "net", "!=",
                                                           "bt", ">",
                                                           "bet", ">=");

    @Test
    public void testInvalidConstraintsExceptions() throws IOException
    {
        final String tableName = KEYSPACE + ".tbl1";

        try (Cluster cluster = init(Cluster.build(3).start()))
        {
            assertThrowsInvalidConstraintException(cluster, String.format("CREATE TABLE %s (pk int, ck1 text CHECK ck1 < 100, ck2 int, v int, " +
                                                                          "PRIMARY KEY ((pk), ck1, ck2));", tableName),
                                                   "Column 'ck1' is not a number type.");

            assertThrowsInvalidConstraintException(cluster, String.format("CREATE TABLE %s (pk int, ck1 int CHECK LENGTH(ck1) < 100, ck2 int, v int, " +
                                                                          "PRIMARY KEY ((pk), ck1, ck2));", tableName),
                                                   "Column should be of type class org.apache.cassandra.db.marshal.UTF8Type or " +
                                                   "class org.apache.cassandra.db.marshal.AsciiType but got class org.apache.cassandra.db.marshal.Int32Type");
        }
    }

    @Test
    public void testUpdateConstraint() throws IOException
    {
        final String tableName = KEYSPACE + ".tbl1";

        try (Cluster cluster = init(Cluster.build(3).start()))
        {
            String createTableStatement = "CREATE TABLE %s (pk int, ck1 int CHECK ck1 < 100, ck2 int, v int, PRIMARY KEY ((pk), ck1, ck2));";
            cluster.schemaChange(String.format(createTableStatement, tableName));

            String insertStatement = "INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 200, 3)";

            cluster.coordinator(1).execute(String.format("ALTER TABLE %s ALTER ck2 CHECK ck2 < 100", tableName), ConsistencyLevel.ALL);

            // Can't insert
            assertThrowsConstraintViolationException(cluster,
                                                     String.format(insertStatement, tableName),
                                                     "ck1 value length should be smaller than 100");

            cluster.coordinator(1).execute(String.format("ALTER TABLE %s ALTER ck2 DROP CHECK", tableName), ConsistencyLevel.ALL);

            // Can insert after droping the constraint
            cluster.coordinator(1).execute(String.format(insertStatement, tableName), ConsistencyLevel.ALL);
        }
    }

    @Test
    public void testConstraintWithJsonInsert() throws IOException
    {
        final String tableName = KEYSPACE + ".tbl1";

        try (Cluster cluster = init(Cluster.build(3).start()))
        {
            String createTableStatement = "CREATE TABLE %s (pk int, ck1 int CHECK ck1 < 100, ck2 int, v uuid, PRIMARY KEY ((pk), ck1, ck2));";
            cluster.schemaChange(String.format(createTableStatement, tableName));

            cluster.coordinator(1).execute(String.format("INSERT INTO %s JSON '{\"pk\" : 1, \"ck1\" : 2, \"ck2\" : 2, \"v\" : \"ac064e40-0417-4a4a-bf53-b7cf145afdc2\" }'", tableName), ConsistencyLevel.ALL);

            assertThrowsConstraintViolationException(cluster,
                                                     String.format("INSERT INTO %s JSON '{\"pk\" : 1, \"ck1\" : 200, \"ck2\" : 2, \"v\" : \"ac064e40-0417-4a4a-bf53-b7cf145afdc2\" }'", tableName),
                                                     "ck1 value length should be smaller than 100");

            assertThrowsConstraintViolationException(cluster,
                                                     String.format("INSERT INTO %s JSON '{\"pk\" : 1, \"ck1\": 100, \"ck2\" : 2, \"v\" : \"ac064e40-0417-4a4a-bf53-b7cf145afdc2\" }'", tableName),
                                                     "ck1 value length should be smaller than 100");
        }
    }

    @Test
    public void testScalarTableLevelConstraint() throws IOException
    {
        Set<String> typesSet = Set.of("int", "double", "float", "decimal");

        try (Cluster cluster = init(Cluster.build(3).start()))
        {
            // Create tables
            for (String type : typesSet)
            {
                for (Map.Entry<String, String> relation : RELATIONS_MAP.entrySet())
                {
                    String tableName = String.format(KEYSPACE + ".%s_tbl1_%s", type, relation.getKey());
                    String createTableStatementSmallerThan = "CREATE TABLE " + tableName + " (pk int, ck1 " + type + " CHECK ck1 " + relation.getValue() + " 100, ck2 int, v int, PRIMARY KEY ((pk), ck1, ck2));";
                    cluster.schemaChange(createTableStatementSmallerThan);
                }
            }

            for (String type : typesSet)
            {
                // st
                String tableName = String.format(KEYSPACE + ".%s_tbl1_%s", type, "st");

                cluster.coordinator(1).execute(String.format("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 2, 3)", tableName), ConsistencyLevel.ALL);

                assertThrowsConstraintViolationException(cluster,
                                                         String.format("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 200, 2, 3)", tableName),
                                                         "ck1 value should be smaller than 100.0");

                assertThrowsConstraintViolationException(cluster,
                                                         String.format("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 100, 2, 3)", tableName),
                                                         "ck1 value length be smaller than 100.0");

                // set
                tableName = String.format(KEYSPACE + ".%s_tbl1_%s", type, "set");

                cluster.coordinator(1).execute(String.format("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 2, 3)", tableName), ConsistencyLevel.ALL);
                cluster.coordinator(1).execute(String.format("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 100, 2, 3)", tableName), ConsistencyLevel.ALL);

                assertThrowsConstraintViolationException(cluster,
                                                         String.format("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 200, 2, 3)", tableName),
                                                         "ck1 value should be smaller than 100.0");

                // et
                tableName = String.format(KEYSPACE + ".%s_tbl1_%s", type, "et");

                cluster.coordinator(1).execute(String.format("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 100, 2, 3)", tableName), ConsistencyLevel.ALL);

                assertThrowsConstraintViolationException(cluster,
                                                         String.format("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 200, 2, 3)", tableName),
                                                         "ck1 value should be smaller than 100.0");

                // net
                tableName = String.format(KEYSPACE + ".%s_tbl1_%s", type, "net");

                cluster.coordinator(1).execute(String.format("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 200, 2, 3)", tableName), ConsistencyLevel.ALL);

                assertThrowsConstraintViolationException(cluster,
                                                         String.format("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 100, 2, 3)", tableName),
                                                         "ck1 value should be smaller than 100.0");

                // bt
                tableName = String.format(KEYSPACE + ".%s_tbl1_%s", type, "bt");

                cluster.coordinator(1).execute(String.format("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 200, 2, 3)", tableName), ConsistencyLevel.ALL);

                assertThrowsConstraintViolationException(cluster,
                                                         String.format("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 1, 2, 3)", tableName),
                                                         "ck1 value should be smaller than 100.0");
                assertThrowsConstraintViolationException(cluster,
                                                         String.format("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 100, 2, 3)", tableName),
                                                         "ck1 value should be smaller than 100.0");

                // bet
                tableName = String.format(KEYSPACE + ".%s_tbl1_%s", type, "bet");

                cluster.coordinator(1).execute(String.format("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 200, 2, 3)", tableName), ConsistencyLevel.ALL);
                cluster.coordinator(1).execute(String.format("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 100, 2, 3)", tableName), ConsistencyLevel.ALL);

                assertThrowsConstraintViolationException(cluster,
                                                         String.format("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 1, 2, 3)", tableName),
                                                         "ck1 value should be smaller than 100.0");

            }
        }
    }

    @Test
    public void testLengthTableLevelConstraint() throws IOException
    {
        Set<String> typesSet = Set.of("varchar", "text", "blob", "ascii");

        try (Cluster cluster = init(Cluster.build(3).start()))
        {
            // Create tables
            for (String type : typesSet)
            {
                for (Map.Entry<String, String> relation : RELATIONS_MAP.entrySet())
                {
                    String tableName = String.format(KEYSPACE + ".%s_tbl1_%s", type, relation.getKey());
                    String createTableStatementSmallerThan = "CREATE TABLE " + tableName + " (pk " + type + " CHECK LENGTH(pk) " + relation.getValue() + " 4, ck1 int, ck2 int, v int, PRIMARY KEY ((pk), ck1, ck2));";
                    cluster.schemaChange(createTableStatementSmallerThan);
                }
            }

            for (String type : typesSet)
            {
                String value = "'%s'";
                if (type.equals("blob"))
                    value = "textAsBlob('%s')";
                // st
                String tableName = String.format(KEYSPACE + ".%s_tbl1_%s", type, "st");
                cluster.coordinator(1).execute(String.format("INSERT INTO " + tableName + " (pk, ck1, ck2, v) VALUES (" + value + ", 2, 2, 3)", "foo"), ConsistencyLevel.ALL);

                assertThrowsConstraintViolationException(cluster,
                                                         String.format("INSERT INTO " + tableName + " (pk, ck1, ck2, v) VALUES (" + value + ", 200, 2, 3)", "fooo"),
                                                         "ck1 value length should be smaller than 100");

                assertThrowsConstraintViolationException(cluster,
                                                         String.format("INSERT INTO " + tableName + " (pk, ck1, ck2, v) VALUES (" + value + ", 100, 2, 3)", "foooo"),
                                                         "ck1 value length should be smaller than 100");

                // set
                tableName = String.format(KEYSPACE + ".%s_tbl1_%s", type, "set");
                cluster.coordinator(1).execute(String.format("INSERT INTO " + tableName + " (pk, ck1, ck2, v) VALUES (" + value + ", 2, 2, 3)", "foo"), ConsistencyLevel.ALL);
                cluster.coordinator(1).execute(String.format("INSERT INTO " + tableName + " (pk, ck1, ck2, v) VALUES (" + value + ", 2, 2, 3)", "fooo"), ConsistencyLevel.ALL);

                assertThrowsConstraintViolationException(cluster,
                                                         String.format("INSERT INTO " + tableName + " (pk, ck1, ck2, v) VALUES (" + value + ", 100, 2, 3)", "foooo"),
                                                         "ck1 value length should be smaller than 100");

                // et
                tableName = String.format(KEYSPACE + ".%s_tbl1_%s", type, "et");
                cluster.coordinator(1).execute(String.format("INSERT INTO " + tableName + " (pk, ck1, ck2, v) VALUES (" + value + ", 2, 2, 3)", "fooo"), ConsistencyLevel.ALL);

                assertThrowsConstraintViolationException(cluster,
                                                         String.format("INSERT INTO " + tableName + " (pk, ck1, ck2, v) VALUES (" + value + ", 100, 2, 3)", "foooo"),
                                                         "ck1 value length should be smaller than 100");

                assertThrowsConstraintViolationException(cluster,
                                                         String.format("INSERT INTO " + tableName + " (pk, ck1, ck2, v) VALUES (" + value + ", 100, 2, 3)", "fo"),
                                                         "ck1 value length should be smaller than 100");

                // net
                tableName = String.format(KEYSPACE + ".%s_tbl1_%s", type, "net");
                cluster.coordinator(1).execute(String.format("INSERT INTO " + tableName + " (pk, ck1, ck2, v) VALUES (" + value + ", 2, 2, 3)", "foooo"), ConsistencyLevel.ALL);
                cluster.coordinator(1).execute(String.format("INSERT INTO " + tableName + " (pk, ck1, ck2, v) VALUES (" + value + ", 2, 2, 3)", "foo"), ConsistencyLevel.ALL);

                assertThrowsConstraintViolationException(cluster,
                                                         String.format("INSERT INTO " + tableName + " (pk, ck1, ck2, v) VALUES (" + value + ", 100, 2, 3)", "fooo"),
                                                         "ck1 value length should be smaller than 100");

                // bt
                tableName = String.format(KEYSPACE + ".%s_tbl1_%s", type, "bt");
                cluster.coordinator(1).execute(String.format("INSERT INTO " + tableName + " (pk, ck1, ck2, v) VALUES (" + value + ", 2, 2, 3)", "foooo"), ConsistencyLevel.ALL);

                assertThrowsConstraintViolationException(cluster,
                                                         String.format("INSERT INTO " + tableName + " (pk, ck1, ck2, v) VALUES (" + value + ", 100, 2, 3)", "fooo"),
                                                         "ck1 value length should be smaller than 100");

                assertThrowsConstraintViolationException(cluster,
                                                         String.format("INSERT INTO " + tableName + " (pk, ck1, ck2, v) VALUES (" + value + ", 100, 2, 3)", "foo"),
                                                         "ck1 value length should be smaller than 100");

                // bet
                tableName = String.format(KEYSPACE + ".%s_tbl1_%s", type, "bet");
                cluster.coordinator(1).execute(String.format("INSERT INTO " + tableName + " (pk, ck1, ck2, v) VALUES (" + value + ", 2, 2, 3)", "foooo"), ConsistencyLevel.ALL);
                cluster.coordinator(1).execute(String.format("INSERT INTO " + tableName + " (pk, ck1, ck2, v) VALUES (" + value + ", 2, 2, 3)", "fooo"), ConsistencyLevel.ALL);

                assertThrowsConstraintViolationException(cluster,
                                                         String.format("INSERT INTO " + tableName + " (pk, ck1, ck2, v) VALUES (" + value + ", 100, 2, 3)", "foo"),
                                                         "ck1 value length should be smaller than 100");
            }
        }
    }

    @Test
    public void testNotNullTableLevelConstraint() throws IOException
    {
        Set<String> typesSet = Set.of("varchar", "text", "blob", "ascii", "int", "smallint", "decimal", "float", "double");

        for (String type : typesSet)
        {
            try (Cluster cluster = init(Cluster.build(1).start()))
            {
                String tableName = String.format(KEYSPACE + ".%s_tbl1_%s", type, "st");
                String createTableNotNullValue = "CREATE TABLE " + tableName + " (pk int, value int CHECK NOT_NULL(value), PRIMARY KEY (pk));";
                cluster.schemaChange(createTableNotNullValue);

                Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(String.format("INSERT INTO " + tableName + " (pk, value) VALUES (1, null)"), ConsistencyLevel.ALL))
                          .describedAs("Column value does not satisfy value constraint for column 'value' as it is null.")
                          .has(new Condition<Throwable>(t -> t.getClass().getCanonicalName().equals(ConstraintViolationException.class.getCanonicalName()),
                                                        "Column value does not satisfy value constraint for column 'value' as it is null."));
            }
        }
    }

    private void assertThrowsConstraintViolationException(Cluster cluster, String statement, String description)
    {
        Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(statement, ConsistencyLevel.ALL))
                  .describedAs(description)
                  .has(new Condition<Throwable>(t -> t.getClass().getCanonicalName()
                                                      .equals(InvalidRequestException.class.getCanonicalName()), description));
    }

    private void assertThrowsInvalidConstraintException(Cluster cluster, String statement, String description)
    {
        Assertions.setMaxStackTraceElementsDisplayed(100);
        assertThatThrownBy(() -> cluster.schemaChange(statement))
                  .describedAs(description)
                  .has(new Condition<Throwable>(t -> t.getClass().getCanonicalName()
                                                      .equals(InvalidConstraintDefinitionException.class.getCanonicalName()), description));
    }
}
