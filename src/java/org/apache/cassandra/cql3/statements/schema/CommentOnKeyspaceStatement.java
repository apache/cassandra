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
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;

/**
 * Handles the execution of COMMENT ON KEYSPACE CQL statements.
 * <p>
 * This statement allows users to add descriptive comments to keyspaces for documentation purposes.
 * Comments are stored in the keyspace metadata and displayed when using DESCRIBE KEYSPACE.
 * </p>
 * <p>
 * Syntax: {@code COMMENT ON KEYSPACE keyspace_name IS 'comment text';}
 * </p>
 * <p>
 * Comments can be removed by setting them to an empty string or null.
 * </p>
 *
 * @see CommentOnTableStatement
 * @see CommentOnColumnStatement
 * @see CommentOnUserTypeStatement
 */
public final class CommentOnKeyspaceStatement extends SchemaDescriptionStatement
{
    public CommentOnKeyspaceStatement(String keyspaceName, String comment)
    {
        super(keyspaceName, comment, DescriptionType.COMMENT);
    }

    @Override
    public Keyspaces apply(ClusterMetadata metadata)
    {
        Keyspaces schema = metadata.schema.getKeyspaces();
        KeyspaceMetadata keyspace = validateAndGetKeyspace(schema);
        KeyspaceParams newParams = keyspace.params.withComment(effectiveDescription());
        KeyspaceMetadata newKeyspace = keyspace.withSwapped(newParams);
        return schema.withAddedOrUpdated(newKeyspace);
    }

    @Override
    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.UPDATED, keyspaceName);
    }

    @Override
    public void authorize(ClientState client)
    {
        client.ensureKeyspacePermission(keyspaceName, Permission.ALTER);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.COMMENT_KEYSPACE, keyspaceName);
    }

    @Override
    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, description);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final String keyspaceName;
        private final String comment;

        public Raw(String keyspaceName, String comment)
        {
            this.keyspaceName = keyspaceName;
            this.comment = comment;
        }

        @Override
        public CommentOnKeyspaceStatement prepare(ClientState state)
        {
            return new CommentOnKeyspaceStatement(keyspaceName, comment);
        }
    }
}
