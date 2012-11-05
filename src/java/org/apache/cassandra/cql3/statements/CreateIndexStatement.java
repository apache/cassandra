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
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.index.composites.CompositesIndex;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.messages.ResultMessage;

/** A <code>CREATE INDEX</code> statement parsed from a CQL query. */
public class CreateIndexStatement extends SchemaAlteringStatement
{
    private static final Logger logger = LoggerFactory.getLogger(CreateIndexStatement.class);

    private final String indexName;
    private final ColumnIdentifier columnName;

    public CreateIndexStatement(CFName name, String indexName, ColumnIdentifier columnName)
    {
        super(name);
        this.indexName = indexName;
        this.columnName = columnName;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.ALTER);
    }

    public void announceMigration() throws InvalidRequestException, ConfigurationException
    {
        CFMetaData oldCfm = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
        boolean columnExists = false;
        // Mutating oldCfm directly would be bad so cloning.
        CFMetaData cfm = oldCfm.clone();
        CFDefinition cfDef = oldCfm.getCfDef();

        for (ColumnDefinition cd : cfm.getColumn_metadata().values())
        {
            if (cd.name.equals(columnName.key))
            {
                if (cd.getIndexType() != null)
                    throw new InvalidRequestException("Index already exists");
                if (logger.isDebugEnabled())
                    logger.debug("Updating column {} definition for index {}", columnName, indexName);

                if (cd.getValidator().isCollection())
                    throw new InvalidRequestException("Indexes on collections are no yet supported");

                if (cfDef.isComposite)
                {
                    CompositeType composite = (CompositeType)cfm.comparator;
                    Map<String, String> opts = new HashMap<String, String>();
                    opts.put(CompositesIndex.PREFIX_SIZE_OPTION, String.valueOf(composite.types.size() - (cfDef.hasCollections ? 2 : 1)));
                    cd.setIndexType(IndexType.COMPOSITES, opts);
                }
                else
                {
                    cd.setIndexType(IndexType.KEYS, Collections.<String, String>emptyMap());
                }
                cd.setIndexName(indexName);
                columnExists = true;
                break;
            }
        }
        if (!columnExists)
        {
            CFDefinition.Name name = cfDef.get(columnName);
            if (name != null)
            {
                switch (name.kind)
                {
                    case KEY_ALIAS:
                    case COLUMN_ALIAS:
                        throw new InvalidRequestException(String.format("Cannot create index on PRIMARY KEY part %s", columnName));
                    case VALUE_ALIAS:
                        throw new InvalidRequestException(String.format("Cannot create index on column %s of compact CF", columnName));
                }
            }
            throw new InvalidRequestException("No column definition found for column " + columnName);
        }

        cfm.addDefaultIndexNames();
        MigrationManager.announceColumnFamilyUpdate(cfm);
    }

    public ResultMessage.SchemaChange.Change changeType()
    {
        // Creating an index is akin to updating the CF
        return ResultMessage.SchemaChange.Change.UPDATED;
    }
}
