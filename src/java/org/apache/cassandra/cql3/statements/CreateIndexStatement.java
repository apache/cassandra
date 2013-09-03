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

import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.IndexType;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.messages.ResultMessage;

/** A <code>CREATE INDEX</code> statement parsed from a CQL query. */
public class CreateIndexStatement extends SchemaAlteringStatement
{
    private static final Logger logger = LoggerFactory.getLogger(CreateIndexStatement.class);

    private final String indexName;
    private final ColumnIdentifier columnName;
    private final boolean ifNotExists;
    private final boolean isCustom;
    private final String indexClass;

    public CreateIndexStatement(CFName name, String indexName, ColumnIdentifier columnName, boolean ifNotExists, boolean isCustom, String indexClass)
    {
        super(name);
        this.indexName = indexName;
        this.columnName = columnName;
        this.ifNotExists = ifNotExists;
        this.isCustom = isCustom;
        this.indexClass = indexClass;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.ALTER);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        CFMetaData cfm = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
        ColumnDefinition cd = cfm.getColumnDefinition(columnName.key);

        if (cd == null)
            throw new InvalidRequestException("No column definition found for column " + columnName);

        if (cd.getIndexType() != null)
        {
            if (ifNotExists)
                return;
            else
                throw new InvalidRequestException("Index already exists");
        }

        if (isCustom && indexClass == null)
            throw new InvalidRequestException("CUSTOM index requires specifiying the index class");

        if (!isCustom && indexClass != null)
            throw new InvalidRequestException("Cannot specify index class for a non-CUSTOM index");

        // TODO: we could lift that limitation
        if (cfm.getCfDef().isCompact && cd.type != ColumnDefinition.Type.REGULAR)
            throw new InvalidRequestException(String.format("Secondary index on %s column %s is not yet supported for compact table", cd.type, columnName));

        if (cd.getValidator().isCollection() && !isCustom)
            throw new InvalidRequestException("Indexes on collections are no yet supported");

        if (cd.type == ColumnDefinition.Type.PARTITION_KEY && cd.componentIndex == null)
            throw new InvalidRequestException(String.format("Cannot add secondary index to already primarily indexed column %s", columnName));
    }

    public void announceMigration() throws InvalidRequestException, ConfigurationException
    {
        logger.debug("Updating column {} definition for index {}", columnName, indexName);
        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace(), columnFamily()).clone();
        ColumnDefinition cd = cfm.getColumnDefinition(columnName.key);

        if (cd.getIndexType() != null && ifNotExists)
            return;

        if (isCustom)
            cd.setIndexType(IndexType.CUSTOM, Collections.singletonMap(SecondaryIndex.CUSTOM_INDEX_OPTION_NAME, indexClass));
        else if (cfm.getCfDef().isComposite)
            cd.setIndexType(IndexType.COMPOSITES, Collections.<String, String>emptyMap());
        else
            cd.setIndexType(IndexType.KEYS, Collections.<String, String>emptyMap());

        cd.setIndexName(indexName);
        cfm.addDefaultIndexNames();
        MigrationManager.announceColumnFamilyUpdate(cfm, false);
    }

    public ResultMessage.SchemaChange.Change changeType()
    {
        // Creating an index is akin to updating the CF
        return ResultMessage.SchemaChange.Change.UPDATED;
    }
}
