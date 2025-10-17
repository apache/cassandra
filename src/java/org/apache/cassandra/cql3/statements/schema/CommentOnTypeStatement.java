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
import org.apache.cassandra.cql3.UTName;
import org.apache.cassandra.cql3.statements.SchemaDescriptionsUtil;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

/**
 * Handles the execution of COMMENT ON TYPE CQL statements.
 * <p>
 * This statement allows users to add descriptive comments to user-defined types for documentation purposes.
 * Comments are stored in the type metadata and displayed when using DESCRIBE TYPE.
 * </p>
 * <p>
 * Syntax: {@code COMMENT ON TYPE [keyspace_name.]type_name IS 'comment text';}
 * </p>
 * <p>
 * Comments can be removed by setting them to an empty string or null.
 * </p>
 *
 * @see CommentOnKeyspaceStatement
 * @see CommentOnTableStatement
 * @see CommentOnColumnStatement
 */
public final class CommentOnTypeStatement extends AlterSchemaStatement
{
    private final String typeName;
    private final String comment;

    public CommentOnTypeStatement(String keyspaceName, String typeName, String comment)
    {
        super(keyspaceName);
        this.typeName = typeName;
        this.comment = comment;
    }

    @Override
    public void validate(ClientState state)
    {
        super.validate(state);
        SchemaDescriptionsUtil.validateComment(comment);
    }

    @Override
    public Keyspaces apply(ClusterMetadata metadata)
    {
        Keyspaces schema = metadata.schema.getKeyspaces();
        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);

        if (null == keyspace)
            throw ire("Keyspace '%s' doesn't exist", keyspaceName);

        UserType type = keyspace.types.getNullable(bytes(typeName));
        if (null == type)
            throw ire("Type '%s.%s' doesn't exist", keyspaceName, typeName);

        UserType newType = type.withComment(comment);
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
        client.ensureKeyspacePermission(keyspaceName, Permission.ALTER);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.COMMENT_TYPE, keyspaceName, typeName);
    }

    @Override
    public String toString()
    {
        return String.format("%s (%s.%s, %s)", getClass().getSimpleName(), keyspaceName, typeName, comment);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final UTName typeName;
        private final String comment;

        public Raw(UTName typeName, String comment)
        {
            this.typeName = typeName;
            this.comment = comment;
        }

        @Override
        public CommentOnTypeStatement prepare(ClientState state)
        {
            String keyspaceName = typeName.hasKeyspace() ? typeName.getKeyspace() : state.getKeyspace();
            return new CommentOnTypeStatement(keyspaceName,
                                            typeName.getStringTypeName(),
                                            comment);
        }
    }
}
