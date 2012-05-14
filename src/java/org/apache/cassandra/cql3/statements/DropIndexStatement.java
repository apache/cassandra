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

import java.io.IOException;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.config.*;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.InvalidRequestException;

public class DropIndexStatement extends SchemaAlteringStatement
{
    public final String indexName;

    public DropIndexStatement(String indexName)
    {
        super(new CFName());
        this.indexName = indexName;
    }

    public void announceMigration() throws InvalidRequestException, ConfigurationException
    {
        CFMetaData updatedCfm = null;

        KSMetaData ksm = Schema.instance.getTableDefinition(keyspace());

        for (CFMetaData cfm : ksm.cfMetaData().values())
        {
            updatedCfm = getUpdatedCFMetadata(cfm);
            if (updatedCfm != null)
                break;
        }

        if (updatedCfm == null)
            throw new InvalidRequestException("Index '" + indexName + "' could not be found in any of the column families of keyspace '" + keyspace() + "'");

        MigrationManager.announceColumnFamilyUpdate(updatedCfm);
    }

    private CFMetaData getUpdatedCFMetadata(CFMetaData cfm) throws InvalidRequestException
    {
        for (ColumnDefinition column : cfm.getColumn_metadata().values())
        {
            if (column.getIndexType() != null && column.getIndexName() != null && column.getIndexName().equals(indexName))
            {
                CFMetaData cloned = cfm.clone();
                ColumnDefinition toChange = cloned.getColumn_metadata().get(column.name);
                assert toChange.getIndexName() != null && toChange.getIndexName().equals(indexName);
                toChange.setIndexName(null);
                toChange.setIndexType(null, null);
                return cloned;
            }
        }

        return null;
    }
}
