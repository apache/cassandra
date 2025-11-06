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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;

import static org.apache.cassandra.schema.TableMetadata.Kind.REGULAR;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public abstract class SchemaDescriptionStatement extends AlterSchemaStatement
{
    private static final String NO_DESCRIPTION = "";
    protected final String description;
    private final DescriptionType descriptionType;

    protected SchemaDescriptionStatement(String keyspaceName, String comment, DescriptionType descriptionType)
    {
        super(keyspaceName);
        this.description = comment;
        this.descriptionType = descriptionType;
    }

    @Override
    public void validate(ClientState state)
    {
        super.validate(state);
        if(description == null)
        {
            // Statement is removing comment on schema elements, ignore
            return;
        }

        if(description.isEmpty())
        {
            throw new InvalidRequestException(String.format("Cannot set %s to empty string", descriptionType.value));
        }

        if (description.length() > descriptionType.maxLength)
        {
            String msg = String.format("%s length (%d) exceeds maximum allowed length (%d)",
                                       descriptionType.value,
                                       description.length(),
                                       descriptionType.maxLength);
            throw new InvalidRequestException(msg);
        }
    }

    protected KeyspaceMetadata validateAndGetKeyspace(Keyspaces schema)
    {
        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);

        if (null == keyspace)
            throw ire("Keyspace '%s' doesn't exist", keyspaceName);

        return keyspace;
    }

    protected TableMetadata validateAndGetTable(Keyspaces schema, String tableName)
    {
        KeyspaceMetadata keyspaceMetadata = validateAndGetKeyspace(schema);

        TableMetadata tableMetadata = keyspaceMetadata.getTableOrViewNullable(tableName);
        if (null == tableMetadata)
            throw ire("Table '%s.%s' doesn't exist", keyspaceName, tableName);

        if (tableMetadata.kind != REGULAR)
            throw ire("Cannot set %s on non-regular table '%s.%s'",descriptionType.value, keyspaceName, tableName);

        return tableMetadata;
    }

    protected UserType validateAndGetType(Keyspaces schema, String typeName)
    {
        KeyspaceMetadata keyspaceMetadata = validateAndGetKeyspace(schema);

        UserType type = keyspaceMetadata.types.getNullable(bytes(typeName));
        if (null == type)
            throw ire("Type '%s.%s' doesn't exist", keyspaceName, typeName);

        return type;
    }

    protected UserType validateAndGetTypeField(Keyspaces schema,
                                               String typeName,
                                               FieldIdentifier fieldName)
    {
        UserType type = validateAndGetType(schema, typeName);

        if (type.fieldPosition(fieldName) == -1)
            throw ire("Field '%s' doesn't exist in type '%s.%s'", fieldName, keyspaceName, typeName);

        return type;
    }

    protected String effectiveDescription()
    {
        return description == null ? NO_DESCRIPTION : description;
    }

    protected void providerWarning(String provider)
    {
        if (provider != null)
            ClientWarn.instance.warn("Provider functionality not implemented." +
                                     " Provider '" + provider + "' will not be loaded or invoked.");
    }

    protected enum DescriptionType
    {
        COMMENT("comment", DatabaseDescriptor.getMaxCommentLength()),
        SECURITY_LABEL("security label", DatabaseDescriptor.getMaxSecurityLabelLength());

        private final String value;
        private final int maxLength;

        DescriptionType(String value, int maxLength)
        {
            this.value = value;
            this.maxLength = maxLength;
        }
    }
}
