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
package org.apache.cassandra.db.virtual;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.exceptions.InvalidRequestException;

import java.util.Arrays;
import java.util.Collection;

/**
 * Parameterized tests for schema metadata virtual tables.
 * <p>
 * This test class runs the same test suite for both {@link SchemaCommentsTable}
 * and {@link SchemaSecurityLabelsTable}, ensuring consistent behavior across both tables.
 * </p>
 */
@RunWith(Parameterized.class)
public class SchemaMetadataTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private static final String KS1 = "ks_metadata_test1";
    private static final String KS2 = "ks_metadata_test2";
    private static final String TABLE1 = "test_table";
    private static final String TABLE2 = "user_data";

    private static final String REPLICATION = "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}";

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[][]{
            {"COMMENT", "schema_comments", "comment", "COMMENT ON", "IS"},
            {"SECURITY_LABEL", "schema_security_labels", "security_label", "SECURITY LABEL ON", "IS"}
        });
    }

    @Parameterized.Parameter(0)
    public String metadataType;

    @Parameterized.Parameter(1)
    public String tableName;

    @Parameterized.Parameter(2)
    public String columnName;

    @Parameterized.Parameter(3)
    public String setStatementPrefix;

    @Parameterized.Parameter(4)
    public String setStatementSuffix;

    @Before
    public void setup()
    {
        if (metadataType.equals("COMMENT"))
        {
            SchemaCommentsTable table = new SchemaCommentsTable(KS_NAME);
            VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
        }
        else
        {
            SchemaSecurityLabelsTable table = new SchemaSecurityLabelsTable(KS_NAME);
            VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
        }
    }

    @After
    public void tearDown()
    {
        execute("DROP KEYSPACE IF EXISTS " + KS1);
        execute("DROP KEYSPACE IF EXISTS " + KS2);
        execute("DROP KEYSPACE IF EXISTS ks_no_metadata");
    }

    @Test
    public void testEmptyResults() throws Throwable
    {
        String ks = "ks_no_metadata";
        createTestKeyspace(ks);
        execute("CREATE TABLE " + ks + ".test (id int PRIMARY KEY, value text)");
        assertEmpty(execute("SELECT * FROM vts." + tableName + " WHERE keyspace_name = ?", ks));
    }

    @Test
    public void testKeyspaceMetadata() throws Throwable
    {
        createTestKeyspace(KS1);

        setMetadata("KEYSPACE", KS1, "Test keyspace metadata");
        assertMetadata("Test keyspace metadata");

        setMetadata("KEYSPACE", KS1, "Updated keyspace metadata");
        assertMetadata("Updated keyspace metadata");

        setMetadata("KEYSPACE", KS1, null);
        assertEmpty(querySchemaMetadata("KEYSPACE"));
    }

    @Test
    public void testTableMetadata()
    {
        createTestKeyspace(KS1);
        execute("CREATE TABLE " + KS1 + '.' + TABLE1 + " (id int PRIMARY KEY, name text)");

        setMetadata("TABLE", KS1 + '.' + TABLE1, "User information table");
        assertTableMetadata(KS1, TABLE1, "User information table");

        setMetadata("TABLE", KS1 + '.' + TABLE1, "Updated table metadata");
        assertTableMetadata(KS1, TABLE1, "Updated table metadata");
    }

    @Test
    public void testColumnMetadata()
    {
        createTestKeyspace(KS1);
        execute("CREATE TABLE " + KS1 + '.' + TABLE1 + " (id int PRIMARY KEY, name text, email text)");

        setMetadata("COLUMN", KS1 + '.' + TABLE1 + ".name", "User full name");
        setMetadata("COLUMN", KS1 + '.' + TABLE1 + ".email", "User email address");

        assertColumnMetadata("name", "User full name");
        assertColumnMetadata("email", "User email address");

        setMetadata("COLUMN", KS1 + '.' + TABLE1 + ".name", "Updated name metadata");
        assertColumnMetadata("name", "Updated name metadata");
    }

    @Test
    public void testUdtMetadata()
    {
        createTestKeyspace(KS1);

        execute("CREATE TYPE " + KS1 + ".address (street text, city text, zip int)");
        setMetadata("TYPE", KS1 + ".address", "User address information");
        assertUdtMetadata("User address information");

        setMetadata("TYPE", KS1 + ".address", "Postal address");
        assertUdtMetadata("Postal address");
    }

    @Test
    public void testPartitionKeyFiltering()
    {
        createTestKeyspace(KS1);
        createTestKeyspace(KS2);

        execute("CREATE TABLE " + KS1 + '.' + TABLE1 + " (id int PRIMARY KEY)");
        execute("CREATE TABLE " + KS2 + '.' + TABLE2 + " (id int PRIMARY KEY)");

        setMetadata("KEYSPACE", KS1, "First keyspace");
        setMetadata("KEYSPACE", KS2, "Second keyspace");
        setMetadata("TABLE", KS1 + '.' + TABLE1, "Table in KS1");
        setMetadata("TABLE", KS2 + '.' + TABLE2, "Table in KS2");

        assertMetadata("First keyspace");
        assertTableMetadata(KS1, TABLE1, "Table in KS1");
        assertTableMetadata(KS2, TABLE2, "Table in KS2");
    }

    @Test
    public void testMetadataRemoval() throws Throwable
    {
        createTestKeyspace(KS1);

        setMetadata("KEYSPACE", KS1, "Test metadata");
        assertRowCount(querySchemaMetadata("KEYSPACE"), 1);

        setMetadata("KEYSPACE", KS1, null);
        assertEmpty(querySchemaMetadata("KEYSPACE"));
    }

    @Test
    public void testMultipleSchemaElements()
    {
        createTestKeyspace(KS1);
        execute("CREATE TABLE " + KS1 + ".users (id int PRIMARY KEY, name text, email text)");
        execute("CREATE TABLE " + KS1 + ".posts (id int PRIMARY KEY, title text)");
        execute("CREATE TYPE " + KS1 + ".address (street text, city text)");

        setMetadata("KEYSPACE", KS1, "Application database");
        setMetadata("TABLE", KS1 + ".users", "User accounts");
        setMetadata("TABLE", KS1 + ".posts", "Blog posts");
        setMetadata("COLUMN", KS1 + ".users.name", "Full name");
        setMetadata("COLUMN", KS1 + ".users.email", "Email address");
        setMetadata("TYPE", KS1 + ".address", "Postal address");

        assertRowCount(execute("SELECT * FROM vts." + tableName + " WHERE keyspace_name = ? ALLOW FILTERING", KS1), 6);

        assertRowCount(querySchemaMetadata("KEYSPACE"), 1);
        assertRowCount(querySchemaMetadata("TABLE"), 2);
        assertRowCount(execute("SELECT * FROM vts." + tableName + " WHERE object_type = 'COLUMN' AND keyspace_name = ? ALLOW FILTERING", KS1), 2);
        assertRowCount(querySchemaMetadata("UDT"), 1);
    }

    @Test
    public void testScanAllMetadata()
    {
        createTestKeyspace(KS1);
        execute("CREATE TABLE " + KS1 + '.' + TABLE1 + " (id int PRIMARY KEY)");

        setMetadata("KEYSPACE", KS1, "Test");
        setMetadata("TABLE", KS1 + '.' + TABLE1, "Test table");

        Object[][] results = getRows(execute("SELECT * FROM vts." + tableName));

        boolean foundKeyspace = false;
        boolean foundTable = false;

        for (Object[] row : results)
        {
            if (row[1].equals(KS1))
            {
                if (row[0].equals("KEYSPACE"))
                    foundKeyspace = true;
                else if (row[0].equals("TABLE"))
                    foundTable = true;
            }
        }

        assert foundKeyspace : "Keyspace metadata not found in full scan";
        assert foundTable : "Table metadata not found in full scan";
    }

    private void createTestKeyspace(String keyspaceName)
    {
        createKeyspace("CREATE KEYSPACE " + keyspaceName + ' ' + REPLICATION);
    }

    private UntypedResultSet querySchemaMetadata(String objectType)
    {
        return execute("SELECT * FROM vts." + tableName + " WHERE object_type = ? AND keyspace_name = ?",
                       objectType, KS1);
    }

    private void setMetadata(String objectType, String objectName, String metadata)
    {
        String value = metadata != null ? '\'' + metadata.replace("'", "''") + '\'' : "NULL";
        execute(setStatementPrefix + ' ' + objectType + ' ' + objectName + ' ' + setStatementSuffix + ' ' + value);
    }

    private void assertMetadata(String expectedMetadata)
    {
        assertRows(execute("SELECT " + columnName + " FROM vts." + tableName + " WHERE object_type = ? AND keyspace_name = ?",
                           "KEYSPACE", KS1),
                   row(expectedMetadata));
    }

    private void assertTableMetadata(String keyspaceName, String table, String expectedMetadata)
    {
        assertRows(execute("SELECT " + columnName + " FROM vts." + tableName + " WHERE object_type = 'TABLE' AND keyspace_name = ? AND table_name = ?",
                          keyspaceName, table),
                   row(expectedMetadata));
    }

    private void assertColumnMetadata(String column, String expectedMetadata)
    {
        assertRows(execute("SELECT " + columnName + " FROM vts." + tableName + " WHERE object_type = 'COLUMN' AND keyspace_name = ? AND table_name = ? AND column_name = ? ALLOW FILTERING",
                           KS1, TABLE1, column),
                   row(expectedMetadata));
    }

    private void assertUdtMetadata(String expectedMetadata)
    {
        assertRows(execute("SELECT " + columnName + " FROM vts." + tableName + " WHERE object_type = 'UDT' AND keyspace_name = ? AND udt_name = ?",
                           KS1, "address"),
                   row(expectedMetadata));
    }

    @Test
    public void testMetadataLengthValidation() throws Throwable
    {
        createTestKeyspace(KS1);

        // Test valid metadata at exactly 1024 characters
        String validMetadata = "a".repeat(1024);
        setMetadata("KEYSPACE", KS1, validMetadata);
        assertMetadata(validMetadata);

        // Test invalid metadata at 1025 characters - should throw InvalidRequestException
        String invalidMetadata = "a".repeat(1025);
        assertInvalidThrowMessage(String.format("%s length (1025) exceeds maximum allowed length (1024)",
                                                metadataType.equals("COMMENT") ? "Comment" : "Security label"),
                                 InvalidRequestException.class,
                                 setStatementPrefix + " KEYSPACE " + KS1 + " " + setStatementSuffix + " '" + invalidMetadata + "'");
    }
}