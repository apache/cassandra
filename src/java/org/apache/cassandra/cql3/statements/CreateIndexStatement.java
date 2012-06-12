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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.ThriftValidation;

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

    public void announceMigration() throws InvalidRequestException, ConfigurationException
    {
        CFMetaData oldCfm = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
        boolean columnExists = false;
        // Mutating oldCfm directly would be bad so cloning.
        CFMetaData cfm = oldCfm.clone();
        CFDefinition cfDef = oldCfm.getCfDef();

        if (cfDef.isComposite)
            throw new InvalidRequestException("Secondary indexes are not (yet) supported on tables with composite PRIMARY KEY");

        for (ColumnDefinition cd : cfm.getColumn_metadata().values())
        {
            if (cd.name.equals(columnName.key))
            {
                if (cd.getIndexType() != null)
                    throw new InvalidRequestException("Index already exists");
                if (logger.isDebugEnabled())
                    logger.debug("Updating column {} definition for index {}", columnName, indexName);
                cd.setIndexType(IndexType.KEYS, Collections.<String, String>emptyMap());
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
}
