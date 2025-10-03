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

package org.apache.cassandra.cql3.validation.miscellaneous;

import com.datastax.driver.core.ResultSet;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;

public class CommentAndSecurityLabelTest extends CQLTester
{
    public static final String KEYSPACE_NAME = "ks_comment";
    public static final String SECURITY_KEYSPACE = "ks_security";
    public static final String TABLE_NAME = "tbl_comment";
    public static final String SECURITY_TABLE_NAME = "tbl_security";
    public static final String TYPE_NAME = "address";

    // Test data constants
    private static final String TEST_COMMENT = "Test comment";
    private static final String UPDATED_COMMENT = "Updated comment";
    private static final String TEST_LABEL = "TEST_LABEL";
    private static final String UPDATED_LABEL = "UPDATED_LABEL";

    enum ObjectType
    {KEYSPACE, TABLE, COLUMN, TYPE}

    @Test
    public void testCommentOnKeyspace()
    {
        createKeyspaceWithName(KEYSPACE_NAME);
        testCommentLifecycle(ObjectType.KEYSPACE, KEYSPACE_NAME, KEYSPACE_NAME);
    }

    @Test
    public void testSecurityLabelOnKeyspace()
    {
        createKeyspaceWithName(SECURITY_KEYSPACE);
        testSecurityLabelLifecycle(ObjectType.KEYSPACE, SECURITY_KEYSPACE, SECURITY_KEYSPACE);

        // Test provider warning
        ResultSet result = executeNet(String.format("SECURITY LABEL FOR test_provider ON KEYSPACE %s IS 'SENSITIVE'", SECURITY_KEYSPACE));
        assertWarningsContain(result.getExecutionInfo().getWarnings(), "Provider is not yet implemented");
        assertSecurityLabel(ObjectType.KEYSPACE, SECURITY_KEYSPACE, SECURITY_KEYSPACE, "SENSITIVE");
    }

    @Test
    public void testCommentOnTable()
    {
        createKeyspaceWithName(KEYSPACE_NAME);
        createTableWithName(KEYSPACE_NAME, TABLE_NAME);
        String tableRef = String.format("%s.%s", KEYSPACE_NAME, TABLE_NAME);
        testCommentLifecycle(ObjectType.TABLE, KEYSPACE_NAME, tableRef);
    }

    @Test
    public void testSecurityLabelOnTable()
    {
        createKeyspaceWithName(SECURITY_KEYSPACE);
        createTableWithName(SECURITY_KEYSPACE, SECURITY_TABLE_NAME);
        String tableRef = String.format("%s.%s", SECURITY_KEYSPACE, SECURITY_TABLE_NAME);
        testSecurityLabelLifecycle(ObjectType.TABLE, SECURITY_KEYSPACE, tableRef);

        // Test provider warning
        ResultSet result = executeNet(String.format("SECURITY LABEL FOR my_provider ON TABLE %s IS 'CONFIDENTIAL'", tableRef));
        assertWarningsContain(result.getExecutionInfo().getWarnings(), "Provider is not yet implemented");
        assertSecurityLabel(ObjectType.TABLE, SECURITY_KEYSPACE, tableRef, "CONFIDENTIAL");
    }

    @Test
    public void testCommentOnColumn()
    {
        createKeyspaceWithName(KEYSPACE_NAME);
        createTableWithName(KEYSPACE_NAME, TABLE_NAME);
        String columnRef = String.format("%s.%s.name", KEYSPACE_NAME, TABLE_NAME);
        testCommentLifecycle(ObjectType.COLUMN, KEYSPACE_NAME, columnRef);
    }

    @Test
    public void testSecurityLabelOnColumn()
    {
        createKeyspaceWithName(SECURITY_KEYSPACE);
        String createTableQuery = String.format("CREATE TABLE %s.%s (id int PRIMARY KEY, ssn text, name text)", SECURITY_KEYSPACE, SECURITY_TABLE_NAME);
        createTable(createTableQuery);
        String columnRef = String.format("%s.%s.ssn", SECURITY_KEYSPACE, SECURITY_TABLE_NAME);
        testSecurityLabelLifecycle(ObjectType.COLUMN, SECURITY_KEYSPACE, columnRef);

        // Test provider warning
        ResultSet result = executeNet(String.format("SECURITY LABEL FOR data_classifier ON COLUMN %s IS 'PII'", columnRef));
        assertWarningsContain(result.getExecutionInfo().getWarnings(), "Provider is not yet implemented");
        assertSecurityLabel(ObjectType.COLUMN, SECURITY_KEYSPACE, columnRef, "PII");
    }

    @Test
    public void testCommentOnType()
    {
        createKeyspaceWithName(KEYSPACE_NAME);
        execute(String.format("CREATE TYPE %s.%s (street text, city text, zip int)", KEYSPACE_NAME, TYPE_NAME));
        String typeRef = String.format("%s.%s", KEYSPACE_NAME, TYPE_NAME);
        testCommentLifecycle(ObjectType.TYPE, KEYSPACE_NAME, typeRef);
    }

    @Test
    public void testSecurityLabelOnType()
    {
        createKeyspaceWithName(SECURITY_KEYSPACE);
        String typeName = "personal_info";
        execute(String.format("CREATE TYPE %s.%s (ssn text, dob date)", SECURITY_KEYSPACE, typeName));
        String typeRef = String.format("%s.%s", SECURITY_KEYSPACE, typeName);
        testSecurityLabelLifecycle(ObjectType.TYPE, SECURITY_KEYSPACE, typeRef);

        // Test provider warning
        ResultSet result = executeNet(String.format("SECURITY LABEL FOR security_provider ON TYPE %s IS 'RESTRICTED'", typeRef));
        assertWarningsContain(result.getExecutionInfo().getWarnings(), "Provider is not yet implemented");
        assertSecurityLabel(ObjectType.TYPE, SECURITY_KEYSPACE, typeRef, "RESTRICTED");
    }

    @Test
    public void testErrorCases()
    {
        createKeyspaceWithName(KEYSPACE_NAME);
        createTableWithName(KEYSPACE_NAME, TABLE_NAME);

        // Test non-existent keyspace
        String commentOnKeyspace = "COMMENT ON KEYSPACE nonexistent IS 'comment'";
        assertInvalidMessage("Keyspace 'nonexistent' doesn't exist", commentOnKeyspace);

        // Test non-existent table
        String commentOnTableQuery = String.format("COMMENT ON TABLE %s.nonexistent IS 'comment'", KEYSPACE_NAME);
        assertInvalidMessage(String.format("Table '%s.nonexistent' doesn't exist", KEYSPACE_NAME), commentOnTableQuery);

        // Test non-existent column
        String commentOnColumnQuery = String.format("COMMENT ON COLUMN %s.%s.nonexistent IS 'comment'", KEYSPACE_NAME, TABLE_NAME);
        assertInvalidMessage("Column 'nonexistent' doesn't exist", commentOnColumnQuery);

        // Test non-existent type
        String commentOnType = String.format("COMMENT ON TYPE %s.nonexistent IS 'comment'", KEYSPACE_NAME);
        assertInvalidMessage("Type", commentOnType);
    }

    @Test
    public void testMultipleOperations()
    {
        createKeyspaceWithName(KEYSPACE_NAME);
        createTableWithName(KEYSPACE_NAME, TABLE_NAME);
        execute(String.format("CREATE TYPE %s.contact_info (phone text, address text)", KEYSPACE_NAME));

        // Set comments and labels on multiple objects
        String tableRef = String.format("%s.%s", KEYSPACE_NAME, TABLE_NAME);
        String columnRef = String.format("%s.%s.name", KEYSPACE_NAME, TABLE_NAME);
        String typeRef = String.format("%s.contact_info", KEYSPACE_NAME);

        setComment(ObjectType.TABLE, tableRef, "User table");
        setSecurityLabel(ObjectType.TABLE, tableRef, "USER_DATA");
        setComment(ObjectType.COLUMN, columnRef, "User name");
        setSecurityLabel(ObjectType.COLUMN, columnRef, "PUBLIC");
        setComment(ObjectType.TYPE, typeRef, "Contact information");
        setSecurityLabel(ObjectType.TYPE, typeRef, "PERSONAL");

        // Verify all are set correctly
        assertComment(ObjectType.TABLE, KEYSPACE_NAME, tableRef, "User table");
        assertSecurityLabel(ObjectType.TABLE, KEYSPACE_NAME, tableRef, "USER_DATA");
        assertComment(ObjectType.COLUMN, KEYSPACE_NAME, columnRef, "User name");
        assertSecurityLabel(ObjectType.COLUMN, KEYSPACE_NAME, columnRef, "PUBLIC");
        assertComment(ObjectType.TYPE, KEYSPACE_NAME, typeRef, "Contact information");
        assertSecurityLabel(ObjectType.TYPE, KEYSPACE_NAME, typeRef, "PERSONAL");
    }

    @Test
    public void testEmptyAndSpecialCharacters()
    {
        createKeyspaceWithName(KEYSPACE_NAME);
        createTableWithName(KEYSPACE_NAME, TABLE_NAME);
        String tableRef = String.format("%s.%s", KEYSPACE_NAME, TABLE_NAME);

        // Test empty string
        setComment(ObjectType.TABLE, tableRef, "");
        assertComment(ObjectType.TABLE, KEYSPACE_NAME, tableRef, "");

        // Test special characters
        String specialComment = "Comment with \"quotes\" and 'apostrophes' and \nnewlines";
        setComment(ObjectType.TABLE, tableRef, specialComment);
        assertComment(ObjectType.TABLE, KEYSPACE_NAME, tableRef, "Comment with \"quotes\" and 'apostrophes' and \nnewlines");

        // Test Unicode characters
        String unicodeComment = "Unicode comment: 测试 ñoño 🚀";
        setComment(ObjectType.TABLE, tableRef, unicodeComment);
        assertComment(ObjectType.TABLE, KEYSPACE_NAME, tableRef, unicodeComment);
    }

    @Test
    public void testUseKeyspaceContext()
    {
        createKeyspaceWithName(KEYSPACE_NAME);
        createTableWithName(KEYSPACE_NAME, TABLE_NAME);
        execute(String.format("CREATE TYPE %s.test_type (field1 text, field2 int)", KEYSPACE_NAME));

        // Use the keyspace to set current context
        execute(String.format("USE %s", KEYSPACE_NAME));

        // Test unqualified names with USE KEYSPACE context
        setComment(ObjectType.TABLE, TABLE_NAME, "Table comment via USE");
        setSecurityLabel(ObjectType.TABLE, TABLE_NAME, "TABLE_LABEL");
        setComment(ObjectType.COLUMN, TABLE_NAME + ".name", "Column comment via USE");
        setSecurityLabel(ObjectType.COLUMN, TABLE_NAME + ".name", "COLUMN_LABEL");
        setComment(ObjectType.TYPE, "test_type", "Type comment via USE");
        setSecurityLabel(ObjectType.TYPE, "test_type", "TYPE_LABEL");

        // Verify all are set correctly using the current keyspace context
        assertComment(ObjectType.TABLE, KEYSPACE_NAME, TABLE_NAME, "Table comment via USE");
        assertSecurityLabel(ObjectType.TABLE, KEYSPACE_NAME, TABLE_NAME, "TABLE_LABEL");
        assertComment(ObjectType.COLUMN, KEYSPACE_NAME, TABLE_NAME + ".name", "Column comment via USE");
        assertSecurityLabel(ObjectType.COLUMN, KEYSPACE_NAME, TABLE_NAME + ".name", "COLUMN_LABEL");
        assertComment(ObjectType.TYPE, KEYSPACE_NAME, "test_type", "Type comment via USE");
        assertSecurityLabel(ObjectType.TYPE, KEYSPACE_NAME, "test_type", "TYPE_LABEL");
    }

    // Helper methods for setting comments and security labels
    private void setComment(ObjectType type, String objectName, String comment)
    {
        String statement = buildCommentStatement(type, objectName, comment);
        execute(statement);
    }

    private void setSecurityLabel(ObjectType type, String objectName, String label)
    {
        String statement = buildSecurityLabelStatement(type, objectName, label);
        execute(statement);
    }

    private String buildStatement(String statementType, ObjectType type, String objectName, String value)
    {
        String valueClause = value != null ? String.format("'%s'", value.replace("'", "''")) : "NULL";
        return String.format("%s ON %s %s IS %s", statementType, type.name(), objectName, valueClause);
    }

    private String buildCommentStatement(ObjectType type, String objectName, String comment)
    {
        return buildStatement("COMMENT", type, objectName, comment);
    }

    private String buildSecurityLabelStatement(ObjectType type, String objectName, String label)
    {
        return buildStatement("SECURITY LABEL", type, objectName, label);
    }

    // Helper methods for assertions
    private void assertComment(ObjectType type, String keyspace, String objectName, String expected)
    {
        String actual = getComment(type, keyspace, objectName);
        assertEquals(expected, actual);
    }

    private void assertSecurityLabel(ObjectType type, String keyspace, String objectName, String expected)
    {
        String actual = getSecurityLabel(type, keyspace, objectName);
        assertEquals(expected, actual);
    }

    private String extractObjectName(ObjectType type, String objectName)
    {
        if (type == ObjectType.TABLE || type == ObjectType.TYPE)
        {
            return objectName.contains(".") ? objectName.split("\\.")[1] : objectName;
        }
        return objectName;
    }

    private String[] parseColumnReference(String objectName)
    {
        String[] parts = objectName.split("\\.");
        if (parts.length == 2)
        {
            return new String[]{ parts[0], parts[1] }; // table, column
        }
        else if (parts.length == 3)
        {
            return new String[]{ parts[1], parts[2] }; // table, column (ignore keyspace part)
        }
        else
        {
            throw new IllegalArgumentException("Invalid column reference format: " + objectName);
        }
    }

    private String getMetadataValue(ObjectType type, String keyspace, String objectName, boolean isComment)
    {
        switch (type)
        {
            case KEYSPACE:
                KeyspaceParams keyspaceMetadata = Schema.instance.getKeyspaceMetadata(keyspace).params;
                return isComment ? keyspaceMetadata.comment : keyspaceMetadata.securityLabel;
            case TABLE:
                String tableName = extractObjectName(type, objectName);
                TableParams tableParams = retrieveTableMetadata(keyspace, tableName).params;
                return isComment ? tableParams.comment : tableParams.securityLabel;
            case COLUMN:
                String[] columnParts = parseColumnReference(objectName);
                ColumnMetadata columnMetadata = getColumnMetadata(keyspace, columnParts[0], columnParts[1]);
                return isComment ? columnMetadata.comment : columnMetadata.securityLabel;
            case TYPE:
                String typeName = extractObjectName(type, objectName);
                UserType userType = getUserType(keyspace, typeName);
                return isComment ? userType.comment : userType.securityLabel;
            default:
                throw new IllegalArgumentException("Unsupported object type: " + type);
        }
    }

    private String getComment(ObjectType type, String keyspace, String objectName)
    {
        return getMetadataValue(type, keyspace, objectName, true);
    }

    private String getSecurityLabel(ObjectType type, String keyspace, String objectName)
    {
        return getMetadataValue(type, keyspace, objectName, false);
    }

    // Metadata retrieval helpers
    private TableMetadata retrieveTableMetadata(String keyspace, String table)
    {
        return getColumnFamilyStore(keyspace, table).metadata();
    }

    private ColumnMetadata getColumnMetadata(String keyspace, String table, String column)
    {
        return retrieveTableMetadata(keyspace, table).getColumn(ColumnIdentifier.getInterned(column, false));
    }

    private UserType getUserType(String keyspace, String typeName)
    {
        return Schema.instance.getKeyspaceMetadata(keyspace).types.get(bytes(typeName)).get();
    }

    // Generic lifecycle test method
    private void testMetadataLifecycle(ObjectType type, String keyspace, String objectName, boolean isComment)
    {
        String testValue = isComment ? TEST_COMMENT : TEST_LABEL;
        String updatedValue = isComment ? UPDATED_COMMENT : UPDATED_LABEL;
        if (isComment)
        {
            setComment(type, objectName, testValue);
            assertComment(type, keyspace, objectName, testValue);
            setComment(type, objectName, updatedValue);
            assertComment(type, keyspace, objectName, updatedValue);
            setComment(type, objectName, null);
            assertComment(type, keyspace, objectName, null);
        }
        else
        {
            setSecurityLabel(type, objectName, testValue);
            assertSecurityLabel(type, keyspace, objectName, testValue);
            setSecurityLabel(type, objectName, updatedValue);
            assertSecurityLabel(type, keyspace, objectName, updatedValue);
            setSecurityLabel(type, objectName, null);
            assertSecurityLabel(type, keyspace, objectName, null);
        }
    }

    private void testCommentLifecycle(ObjectType type, String keyspace, String objectName)
    {
        testMetadataLifecycle(type, keyspace, objectName, true);
    }

    private void testSecurityLabelLifecycle(ObjectType type, String keyspace, String objectName)
    {
        testMetadataLifecycle(type, keyspace, objectName, false);
    }

    private void createKeyspaceWithName(String keyspace)
    {
        String query = String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", keyspace);
        createKeyspace(query);
    }

    private void createTableWithName(String keyspace, String taleName)
    {
        String query = String.format("CREATE TABLE %s.%s (id int PRIMARY KEY, name text)", keyspace, taleName);
        createTable(query);
    }
}