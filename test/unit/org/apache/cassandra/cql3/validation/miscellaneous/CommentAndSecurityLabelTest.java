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

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;

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
    {KEYSPACE, TABLE, COLUMN, TYPE, FIELD}

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
        assertWarningsContain(result.getExecutionInfo().getWarnings(), "Provider functionality not implemented.");
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
        assertWarningsContain(result.getExecutionInfo().getWarnings(), "Provider functionality not implemented.");
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
        assertWarningsContain(result.getExecutionInfo().getWarnings(), "Provider functionality not implemented.");
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
        assertWarningsContain(result.getExecutionInfo().getWarnings(), "Provider functionality not implemented.");
        assertSecurityLabel(ObjectType.TYPE, SECURITY_KEYSPACE, typeRef, "RESTRICTED");
    }

    @Test
    public void testCommentOnField()
    {
        createKeyspaceWithName(KEYSPACE_NAME);
        execute(String.format("CREATE TYPE %s.geo_position (latitude double, longitude double, altitude double)", KEYSPACE_NAME));
        String fieldRef = String.format("%s.geo_position.latitude", KEYSPACE_NAME);
        testCommentLifecycle(ObjectType.FIELD, KEYSPACE_NAME, fieldRef);
    }

    @Test
    public void testSecurityLabelOnField()
    {
        createKeyspaceWithName(SECURITY_KEYSPACE);
        execute(String.format("CREATE TYPE %s.patient_record (ssn text, diagnosis text, treatment text)", SECURITY_KEYSPACE));
        String fieldRef = String.format("%s.patient_record.ssn", SECURITY_KEYSPACE);
        testSecurityLabelLifecycle(ObjectType.FIELD, SECURITY_KEYSPACE, fieldRef);

        // Test provider warning
        ResultSet result = executeNet(String.format("SECURITY LABEL FOR healthcare_provider ON FIELD %s IS 'PHI'", fieldRef));
        assertWarningsContain(result.getExecutionInfo().getWarnings(), "Provider functionality not implemented.");
        assertSecurityLabel(ObjectType.FIELD, SECURITY_KEYSPACE, fieldRef, "PHI");
    }

    @Test
    public void testFieldWithUseKeyspace()
    {
        createKeyspaceWithName(KEYSPACE_NAME);
        execute(String.format("CREATE TYPE %s.address (street text, city text, zip int)", KEYSPACE_NAME));
        execute(String.format("USE %s", KEYSPACE_NAME));

        // Test unqualified field reference with USE KEYSPACE context
        setComment(ObjectType.FIELD, "address.street", "Street address");
        setSecurityLabel(ObjectType.FIELD, "address.street", "PUBLIC");
        setComment(ObjectType.FIELD, "address.city", "City name");
        setSecurityLabel(ObjectType.FIELD, "address.city", "PUBLIC");

        // Verify
        assertComment(ObjectType.FIELD, KEYSPACE_NAME, "address.street", "Street address");
        assertSecurityLabel(ObjectType.FIELD, KEYSPACE_NAME, "address.street", "PUBLIC");
        assertComment(ObjectType.FIELD, KEYSPACE_NAME, "address.city", "City name");
        assertSecurityLabel(ObjectType.FIELD, KEYSPACE_NAME, "address.city", "PUBLIC");
    }

    @Test
    public void testErrorCases()
    {
        createKeyspaceWithName(KEYSPACE_NAME);
        createTableWithName(KEYSPACE_NAME, TABLE_NAME);
        execute(String.format("CREATE TYPE %s.test_type (field1 text, field2 int)", KEYSPACE_NAME));

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

        // Test non-existent type for field
        String commentOnNonExistentType = String.format("COMMENT ON FIELD %s.nonexistent.somefield IS 'comment'", KEYSPACE_NAME);
        assertInvalidMessage("doesn't exist", commentOnNonExistentType);

        // Test non-existent field
        String commentOnNonExistentField = String.format("COMMENT ON FIELD %s.test_type.nonexistent IS 'comment'", KEYSPACE_NAME);
        assertInvalidMessage("doesn't exist", commentOnNonExistentField);
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

        // Test empty string - should be rejected
        assertInvalidMessage("Cannot set comment to empty string", buildCommentStatement(ObjectType.TABLE, tableRef, ""));
        assertInvalidMessage("Cannot set security label to empty string", buildSecurityLabelStatement(ObjectType.TABLE, tableRef, ""));

        // Test special characters
        String specialComment = "Comment with \"quotes\" and '' and \nnewlines";
        setComment(ObjectType.TABLE, tableRef, specialComment);
        assertComment(ObjectType.TABLE, KEYSPACE_NAME, tableRef, "Comment with \"quotes\" and '' and \nnewlines");

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


    @Test
    public void testCommentAndSecurityLabelOnVirtualTableFails()
    {
        assertInvalidMessage("is not user-modifiable", "COMMENT ON TABLE system_views.settings IS 'fail'");
        assertInvalidMessage("is not user-modifiable", "SECURITY LABEL ON TABLE system_views.settings IS 'fail'");
    }

    @Test
    public void testCommentAndSecurityLabelOnSystemTableFails()
    {
        assertInvalidMessage("is not user-modifiable", "COMMENT ON TABLE system.local IS 'fail'");
        assertInvalidMessage("is not user-modifiable", "SECURITY LABEL ON TABLE system.local IS 'fail'");
        assertInvalidMessage("is not user-modifiable", "COMMENT ON COLUMN system.local.key IS 'fail'");
        assertInvalidMessage("is not user-modifiable", "SECURITY LABEL ON COLUMN system.local.key IS 'fail'");
    }

    @Test
    public void testMaterializedViewFails() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        String mvFullName = KEYSPACE + ".test_mv";

        // Create view using executeNet with fully qualified names (like GrantAndRevokeTest does)
        executeNet("CREATE MATERIALIZED VIEW " + mvFullName + " AS SELECT * FROM " + KEYSPACE + "." + tableName +
                   " WHERE pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (ck, pk)");

        // Wait for the view to be fully built before testing
        waitForViewBuild("test_mv");

        // Test that comment and security label statements fail
        assertInvalidMessage("Cannot set comment on non-regular table",
                           "COMMENT ON TABLE " + mvFullName + " IS 'fail'");
        assertInvalidMessage("Cannot set security label on non-regular table",
                           "SECURITY LABEL ON TABLE " + mvFullName + " IS 'fail'");
    }

    @Test
    public void testCommentAndSecurityLabelOnSystemKeyspaceFails()
    {
        // Test comment and security label on system keyspaces
        assertInvalidMessage("is not user-modifiable", "COMMENT ON KEYSPACE system IS 'fail'");
        assertInvalidMessage("is not user-modifiable", "SECURITY LABEL ON KEYSPACE system IS 'fail'");
    }

    @Test
    public void testCommentAndSecurityLabelOnVirtualKeyspaceFails()
    {
        // Test comment and security label on virtual keyspaces
        assertInvalidMessage("is not user-modifiable", "COMMENT ON KEYSPACE system_views IS 'fail'");
        assertInvalidMessage("is not user-modifiable", "SECURITY LABEL ON KEYSPACE system_views IS 'fail'");
    }

    @Test
    public void testEmptyStringRejection()
    {
        createKeyspaceWithName(KEYSPACE_NAME);
        createTableWithName(KEYSPACE_NAME, TABLE_NAME);
        execute(String.format("CREATE TYPE %s.test_type (field1 text, field2 int)", KEYSPACE_NAME));

        String tableRef = String.format("%s.%s", KEYSPACE_NAME, TABLE_NAME);
        String columnRef = String.format("%s.%s.name", KEYSPACE_NAME, TABLE_NAME);
        String typeRef = String.format("%s.test_type", KEYSPACE_NAME);
        String fieldRef = String.format("%s.test_type.field1", KEYSPACE_NAME);

        // Test that empty strings are rejected for comments on all schema elements
        assertInvalidMessage("Cannot set comment to empty string", buildCommentStatement(ObjectType.KEYSPACE, KEYSPACE_NAME, ""));
        assertInvalidMessage("Cannot set comment to empty string", buildCommentStatement(ObjectType.TABLE, tableRef, ""));
        assertInvalidMessage("Cannot set comment to empty string", buildCommentStatement(ObjectType.COLUMN, columnRef, ""));
        assertInvalidMessage("Cannot set comment to empty string", buildCommentStatement(ObjectType.TYPE, typeRef, ""));
        assertInvalidMessage("Cannot set comment to empty string", buildCommentStatement(ObjectType.FIELD, fieldRef, ""));

        // Test that empty strings are rejected for security labels on all schema elements
        assertInvalidMessage("Cannot set security label to empty string", buildSecurityLabelStatement(ObjectType.KEYSPACE, KEYSPACE_NAME, ""));
        assertInvalidMessage("Cannot set security label to empty string", buildSecurityLabelStatement(ObjectType.TABLE, tableRef, ""));
        assertInvalidMessage("Cannot set security label to empty string", buildSecurityLabelStatement(ObjectType.COLUMN, columnRef, ""));
        assertInvalidMessage("Cannot set security label to empty string", buildSecurityLabelStatement(ObjectType.TYPE, typeRef, ""));
        assertInvalidMessage("Cannot set security label to empty string", buildSecurityLabelStatement(ObjectType.FIELD, fieldRef, ""));
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
        String typeKeyword = type.name();
        return String.format("%s ON %s %s IS %s", statementType, typeKeyword, objectName, valueClause);
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
            return new String[]{ parts[0], parts[1] }; // table/type, column/field
        }
        else if (parts.length == 3)
        {
            return new String[]{ parts[1], parts[2] }; // table/type, column/field (ignore keyspace part)
        }
        else
        {
            throw new IllegalArgumentException("Invalid reference format: " + objectName);
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
            case FIELD:
                String[] fieldParts = parseColumnReference(objectName);
                UserType type1 = getUserType(keyspace, fieldParts[0]);
                FieldIdentifier fieldId = FieldIdentifier.forUnquoted(fieldParts[1]);
                return isComment ? type1.fieldComment(fieldId) : type1.fieldSecurityLabel(fieldId);
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
            String emptyStringStatement = buildCommentStatement(type, objectName, "");
            assertInvalidMessage("Cannot set comment to empty string", emptyStringStatement);
            setComment(type, objectName, testValue);
            assertComment(type, keyspace, objectName, testValue);
            setComment(type, objectName, updatedValue);
            assertComment(type, keyspace, objectName, updatedValue);
            setComment(type, objectName, null);
            assertComment(type, keyspace, objectName, "");
            setComment(type, objectName, null);
            assertComment(type, keyspace, objectName, "");
        }
        else
        {
            String emptyStringStatement = buildSecurityLabelStatement(type, objectName, "");
            assertInvalidMessage("Cannot set security label to empty string", emptyStringStatement);
            setSecurityLabel(type, objectName, testValue);
            assertSecurityLabel(type, keyspace, objectName, testValue);
            setSecurityLabel(type, objectName, updatedValue);
            assertSecurityLabel(type, keyspace, objectName, updatedValue);
            setSecurityLabel(type, objectName, null);
            assertSecurityLabel(type, keyspace, objectName, "");
            setSecurityLabel(type, objectName, null);
            assertSecurityLabel(type, keyspace, objectName, "");
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
