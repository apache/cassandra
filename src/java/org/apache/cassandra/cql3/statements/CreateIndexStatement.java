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
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.messages.ResultMessage;

/** A <code>CREATE INDEX</code> statement parsed from a CQL query. */
public class CreateIndexStatement extends SchemaAlteringStatement
{
    private static final Logger logger = LoggerFactory.getLogger(CreateIndexStatement.class);

    private final String indexName;
    private final ColumnIdentifier columnName;
    private final IndexPropDefs properties;
    private final boolean ifNotExists;

    public CreateIndexStatement(CFName name,
                                String indexName,
                                ColumnIdentifier columnName,
                                IndexPropDefs properties,
                                boolean ifNotExists)
    {
        super(name);
        this.indexName = indexName;
        this.columnName = columnName;
        this.properties = properties;
        this.ifNotExists = ifNotExists;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.ALTER);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        CFMetaData cfm = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
        if (cfm.getDefaultValidator().isCommutative())
            throw new InvalidRequestException("Secondary indexes are not supported on counter tables");

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

        properties.validate();

        // TODO: we could lift that limitation
        if (cfm.getCfDef().isCompact && cd.type != ColumnDefinition.Type.REGULAR)
            throw new InvalidRequestException(String.format("Secondary index on %s column %s is not yet supported for compact table", cd.type, columnName));

        // It would be possible to support 2ndary index on static columns (but not without modifications of at least ExtendedFilter and
        // CompositesIndex) and maybe we should, but that means a query like:
        //     SELECT * FROM foo WHERE static_column = 'bar'
        // would pull the full partition every time the static column of partition is 'bar', which sounds like offering a
        // fair potential for foot-shooting, so I prefer leaving that to a follow up ticket once we have identified cases where
        // such indexing is actually useful.
        if (cd.type == ColumnDefinition.Type.STATIC)
            throw new InvalidRequestException("Secondary indexes are not allowed on static columns");

        if (cd.getValidator().isCollection() && !properties.isCustom)
            throw new InvalidRequestException("Indexes on collections are no yet supported");

        if (cd.type == ColumnDefinition.Type.PARTITION_KEY && cd.componentIndex == null)
            throw new InvalidRequestException(String.format("Cannot add secondary index to already primarily indexed column %s", columnName));
    }

    public boolean announceMigration() throws RequestValidationException
    {
        logger.debug("Updating column {} definition for index {}", columnName, indexName);
        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace(), columnFamily()).clone();
        ColumnDefinition cd = cfm.getColumnDefinition(columnName.key);

        if (cd.getIndexType() != null && ifNotExists)
            return false;

        if (properties.isCustom)
            cd.setIndexType(IndexType.CUSTOM, properties.getOptions());
        else if (cfm.getCfDef().isComposite)
            cd.setIndexType(IndexType.COMPOSITES, Collections.<String, String>emptyMap());
        else
            cd.setIndexType(IndexType.KEYS, Collections.<String, String>emptyMap());

        cd.setIndexName(indexName);
        cfm.addDefaultIndexNames();
        MigrationManager.announceColumnFamilyUpdate(cfm, false);
        return true;
    }

    public ResultMessage.SchemaChange.Change changeType()
    {
        // Creating an index is akin to updating the CF
        return ResultMessage.SchemaChange.Change.UPDATED;
    }
}
