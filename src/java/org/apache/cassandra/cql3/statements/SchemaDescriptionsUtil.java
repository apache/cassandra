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

package org.apache.cassandra.cql3.statements;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Utility class for appending COMMENT and SECURITY LABEL statements to schema element descriptions.
 * <p>
 * This class provides methods to append CQL statements for comments and security labels on various
 * schema elements (keyspaces, tables, columns, and types) to their CREATE statement output.
 * These are typically used by DESCRIBE statements to show the complete schema definition including
 * any associated metadata.
 * </p>
 */
public class SchemaDescriptionsUtil
{
    private static final String COMMENT_DESCRIPTION_TYPE = "COMMENT";
    private static final String SECURITY_LABEL_DESCRIPTION_TYPE = "SECURITY LABEL";
    private static final String KEYSPACE_SCHEMA_ELEMENT = "KEYSPACE";
    private static final String TABLE_SCHEMA_ELEMENT = "TABLE";
    private static final String COLUMN_SCHEMA_ELEMENT = "COLUMN";
    private static final String TYPE_SCHEMA_ELEMENT = "TYPE";
    private static final String FIELD_SCHEMA_ELEMENT = "FIELD";

    /**
     * Maximum length for comments and security labels.
     * This limit helps prevent excessive memory usage and storage overhead.
     */
    public static final int MAX_METADATA_LENGTH = 1024;

    private SchemaDescriptionsUtil()
    {
    }

    /**
     * Validates that a comment does not exceed the maximum allowed length.
     *
     * @param comment the comment to validate (can be null)
     * @throws InvalidRequestException if the comment exceeds the maximum length
     */
    public static void validateComment(String comment)
    {
        if (comment != null && comment.length() > MAX_METADATA_LENGTH)
        {
            String msg = String.format("Comment length (%d) exceeds maximum allowed length (%d)",
                                       comment.length(),
                                       MAX_METADATA_LENGTH);
            throw new InvalidRequestException(msg);
        }
    }

    /**
     * Validates that a security label does not exceed the maximum allowed length.
     *
     * @param securityLabel the security label to validate (can be null)
     * @throws InvalidRequestException if the security label exceeds the maximum length
     */
    public static void validateSecurityLabel(String securityLabel)
    {
        if (securityLabel != null && securityLabel.length() > MAX_METADATA_LENGTH)
        {
            throw new InvalidRequestException(String.format("Security label length (%d) exceeds maximum allowed length (%d)",
                                                           securityLabel.length(),
                                                           MAX_METADATA_LENGTH));
        }
    }

    /**
     * Appends a COMMENT ON KEYSPACE statement to the builder if the keyspace has a comment.
     *
     * @param builder the StringBuilder to append to
     * @param keyspaceMetadata the keyspace metadata containing the comment
     */
    public static void appendCommentOnKeyspace(StringBuilder builder, KeyspaceMetadata keyspaceMetadata)
    {
        addDescription(builder, COMMENT_DESCRIPTION_TYPE, KEYSPACE_SCHEMA_ELEMENT, keyspaceMetadata.name, keyspaceMetadata.params.comment);
    }

    /**
     * Appends a SECURITY LABEL ON KEYSPACE statement to the builder if the keyspace has a security label.
     *
     * @param builder the StringBuilder to append to
     * @param keyspaceMetadata the keyspace metadata containing the security label
     */
    public static void appendSecurityLabelOnKeyspace(StringBuilder builder, KeyspaceMetadata keyspaceMetadata)
    {
        addDescription(builder, SECURITY_LABEL_DESCRIPTION_TYPE, KEYSPACE_SCHEMA_ELEMENT, keyspaceMetadata.name, keyspaceMetadata.params.securityLabel);
    }

    /**
     * Appends a COMMENT ON TABLE statement to the builder if the table has a comment.
     *
     * @param builder the StringBuilder to append to
     * @param tableMetadata the table metadata containing the comment
     */
    public static void appendCommentOnTable(StringBuilder builder, TableMetadata tableMetadata)
    {
        String schemaElement = String.format("%s.%s", tableMetadata.keyspace, tableMetadata.name);
        addDescription(builder, COMMENT_DESCRIPTION_TYPE, TABLE_SCHEMA_ELEMENT, schemaElement, tableMetadata.params.comment);
    }

    /**
     * Appends SECURITY LABEL ON TABLE statement and COMMENT/SECURITY LABEL ON COLUMN statements
     * to the builder if the table or any of its columns have security labels or comments.
     *
     * @param builder the StringBuilder to append to
     * @param tableMetadata the table metadata containing the security label and columns
     */
    public static void appendSecurityLabelOnTable(StringBuilder builder, TableMetadata tableMetadata)
    {
        String schemaElement = String.format("%s.%s", tableMetadata.keyspace, tableMetadata.name);
        addDescription(builder, SECURITY_LABEL_DESCRIPTION_TYPE, TABLE_SCHEMA_ELEMENT, schemaElement, tableMetadata.params.securityLabel);

        // Add comments and security labels on columns
        for (ColumnMetadata column: tableMetadata.columns())
        {
            appendCommentOnColumn(builder, tableMetadata, column);
            appendSecurityLabelOnColumn(builder, tableMetadata, column);
        }
    }

    /**
     * Appends a COMMENT ON TYPE statement to the builder if the type has a comment.
     *
     * @param builder the StringBuilder to append to
     * @param userType the user-defined type containing the comment
     */
    public static void appendCommentOnType(StringBuilder builder, UserType userType)
    {
        String schemaElement = String.format("%s.%s", userType.keyspace, userType.getNameAsString());
        addDescription(builder, COMMENT_DESCRIPTION_TYPE, TYPE_SCHEMA_ELEMENT, schemaElement, userType.comment);

        // Add comments on fields
        for (int i = 0; i < userType.size(); i++)
            appendCommentOnField(builder, userType, i);
    }

    /**
     * Appends SECURITY LABEL ON TYPE statement and COMMENT/SECURITY LABEL ON FIELD statements
     * to the builder if the type or any of its fields have security labels or comments.
     *
     * @param builder the StringBuilder to append to
     * @param userType the user-defined type containing the security label and fields
     */
    public static void appendSecurityLabelOnType(StringBuilder builder, UserType userType)
    {
        String schemaElement = String.format("%s.%s", userType.keyspace, userType.getNameAsString());
        addDescription(builder, SECURITY_LABEL_DESCRIPTION_TYPE, TYPE_SCHEMA_ELEMENT, schemaElement, userType.securityLabel);

        // Add security labels on fields
        for (int i = 0; i < userType.size(); i++)
            appendSecurityLabelOnField(builder, userType, i);
    }

    /* Helper methods */

    /**
     * Appends a COMMENT ON COLUMN statement to the builder if the column has a comment.
     *
     * @param builder the StringBuilder to append to
     * @param tableMetadata the table containing the column
     * @param columnMetadata the column metadata containing the comment
     */
    private static void appendCommentOnColumn(StringBuilder builder, TableMetadata tableMetadata, ColumnMetadata columnMetadata)
    {
        appendColumnDescription(builder, COMMENT_DESCRIPTION_TYPE, tableMetadata, columnMetadata, columnMetadata.comment);
    }

    /**
     * Appends a SECURITY LABEL ON COLUMN statement to the builder if the column has a security label.
     *
     * @param builder the StringBuilder to append to
     * @param tableMetadata the table containing the column
     * @param columnMetadata the column metadata containing the security label
     */
    private static void appendSecurityLabelOnColumn(StringBuilder builder, TableMetadata tableMetadata, ColumnMetadata columnMetadata)
    {
        appendColumnDescription(builder, SECURITY_LABEL_DESCRIPTION_TYPE, tableMetadata, columnMetadata, columnMetadata.securityLabel);
    }

    /**
     * Helper method to append a column-level comment or security label statement.
     *
     * @param builder the StringBuilder to append to
     * @param descriptionType the type of description (COMMENT or SECURITY LABEL)
     * @param tableMetadata the table containing the column
     * @param columnMetadata the column metadata
     * @param description the comment or security label text
     */
    private static void appendColumnDescription(StringBuilder builder, String descriptionType, TableMetadata tableMetadata, ColumnMetadata columnMetadata, String description)
    {
        String schemaElement = String.format("%s.%s.%s", tableMetadata.keyspace, tableMetadata.name, columnMetadata.name.toCQLString());
        addDescription(builder, descriptionType, COLUMN_SCHEMA_ELEMENT, schemaElement, description);
    }

    /**
     * Appends a COMMENT ON FIELD statement to the builder if the field has a comment.
     *
     * @param builder the StringBuilder to append to
     * @param userType the user-defined type containing the field
     * @param fieldIndex the index of the field
     */
    private static void appendCommentOnField(StringBuilder builder, UserType userType, int fieldIndex)
    {
        String comment = userType.fieldComment(userType.fieldName(fieldIndex));
        appendFieldDescription(builder, COMMENT_DESCRIPTION_TYPE, userType, fieldIndex, comment);
    }

    /**
     * Appends a SECURITY LABEL ON FIELD statement to the builder if the field has a security label.
     *
     * @param builder the StringBuilder to append to
     * @param userType the user-defined type containing the field
     * @param fieldIndex the index of the field
     */
    private static void appendSecurityLabelOnField(StringBuilder builder, UserType userType, int fieldIndex)
    {
        String securityLabel = userType.fieldSecurityLabel(userType.fieldName(fieldIndex));
        appendFieldDescription(builder, SECURITY_LABEL_DESCRIPTION_TYPE, userType, fieldIndex, securityLabel);
    }

    /**
     * Helper method to append a field-level comment or security label statement.
     *
     * @param builder the StringBuilder to append to
     * @param descriptionType the type of description (COMMENT or SECURITY LABEL)
     * @param userType the user-defined type containing the field
     * @param fieldIndex the index of the field
     * @param description the comment or security label text
     */
    private static void appendFieldDescription(StringBuilder builder, String descriptionType, UserType userType, int fieldIndex, String description)
    {
        String schemaElement = String.format("%s.%s.%s", userType.keyspace, userType.getNameAsString(), userType.fieldName(fieldIndex).toString());
        addDescription(builder, descriptionType, FIELD_SCHEMA_ELEMENT, schemaElement, description);
    }

    /**
     * Helper method to append a comment or security label statement to the builder.
     * <p>
     * Generates CQL statements in the format:
     * {@code <descriptionType> ON <schemaElement> <schemaElementName> IS '<comment>';}
     * </p>
     *
     * @param builder the StringBuilder to append to
     * @param descriptionType the type of description (COMMENT or SECURITY LABEL)
     * @param schemaElement the type of schema element (KEYSPACE, TABLE, COLUMN, or TYPE)
     * @param schemaElementName the fully qualified name of the schema element
     * @param comment the comment or security label text (null or empty values are ignored)
     */
    private static void addDescription(StringBuilder builder, String descriptionType, String schemaElement, String schemaElementName, String comment)
    {
        if (StringUtils.isEmpty(comment))
        {
            return;
        }
        builder.append("\n")
               .append(descriptionType)
               .append(" ON ")
               .append(schemaElement)
               .append(" ")
               .append(schemaElementName)
               .append(" IS ")
               .append('\'')
               .append(comment)
               .append('\'')
               .append(';');
    }

}
