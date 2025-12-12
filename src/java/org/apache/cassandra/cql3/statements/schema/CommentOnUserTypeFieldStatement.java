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
package org.apache.cassandra.cql3.statements.schema;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.UTName;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

/**
 * Handles the execution of COMMENT ON FIELD CQL statements.
 * <p>
 * This statement allows users to add descriptive comments to individual fields of user-defined types
 * for documentation purposes. Comments are stored in the type metadata and displayed when using DESCRIBE TYPE.
 * </p>
 * <p>
 * Syntax: {@code COMMENT ON FIELD [keyspace_name.]type_name.field_name IS 'comment text';}
 * </p>
 * <p>
 * Comments can be removed by setting them to an empty string or null.
 * </p>
 *
 * @see CommentOnUserTypeStatement
 * @see CommentOnColumnStatement
 */
public final class CommentOnUserTypeFieldStatement extends SchemaDescriptionStatement
{
    private final String typeName;
    private final FieldIdentifier fieldName;

    public CommentOnUserTypeFieldStatement(String keyspaceName, String typeName, FieldIdentifier fieldName, String comment)
    {
        super(keyspaceName, comment, DescriptionType.COMMENT);
        this.typeName = typeName;
        this.fieldName = fieldName;
    }

    @Override
    public boolean compatibleWith(ClusterMetadata metadata)
    {
        return metadata.directory.commonSerializationVersion.isAtLeast(Version.V8);
    }

    @Override
    public Keyspaces apply(ClusterMetadata metadata)
    {
        Keyspaces schema = metadata.schema.getKeyspaces();
        UserType type = validateAndGetTypeField(schema, typeName, fieldName);

        String effectiveComment = effectiveDescription();
        UserType newType = type.withFieldComment(fieldName, effectiveComment);

        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
        KeyspaceMetadata newKeyspace = keyspace.withSwapped(keyspace.types.withUpdatedUserType(newType));

        return schema.withAddedOrUpdated(newKeyspace);
    }

    @Override
    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.UPDATED, Target.TYPE, keyspaceName, typeName);
    }

    @Override
    public void authorize(ClientState client)
    {
        client.ensureAllTablesPermission(keyspaceName, Permission.ALTER);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.COMMENT_TYPE, keyspaceName, typeName);
    }

    @Override
    public String toString()
    {
        return String.format("%s (%s.%s.%s, %s)", getClass().getSimpleName(), keyspaceName, typeName, fieldName, description);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final UTName typeName;
        private final FieldIdentifier fieldName;
        private final String comment;

        public Raw(UTName typeName, FieldIdentifier fieldName, String comment)
        {
            this.typeName = typeName;
            this.fieldName = fieldName;
            this.comment = comment;
        }

        @Override
        public CommentOnUserTypeFieldStatement prepare(ClientState state)
        {
            String keyspaceName = typeName.hasKeyspace() ? typeName.getKeyspace() : state.getKeyspace();
            return new CommentOnUserTypeFieldStatement(keyspaceName,
                                                       typeName.getStringTypeName(),
                                                       fieldName,
                                                       comment);
        }
    }
}
