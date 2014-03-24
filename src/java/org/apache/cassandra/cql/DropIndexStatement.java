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
package org.apache.cassandra.cql;

import org.apache.cassandra.config.*;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class DropIndexStatement
{
    public final String indexName;
    private String keyspace;

    public DropIndexStatement(String indexName)
    {
        this.indexName = indexName;
    }

    public void setKeyspace(String keyspace)
    {
        this.keyspace = keyspace;
    }

    public String getColumnFamily() throws InvalidRequestException
    {
        return findIndexedCF().cfName;
    }

    public CFMetaData generateCFMetadataUpdate() throws InvalidRequestException
    {
        return updateCFMetadata(findIndexedCF());
    }

    private CFMetaData updateCFMetadata(CFMetaData cfm)
    {
        ColumnDefinition column = findIndexedColumn(cfm);
        assert column != null;
        CFMetaData cloned = cfm.copy();
        ColumnDefinition toChange = cloned.getColumnDefinition(column.name);
        assert toChange.getIndexName() != null && toChange.getIndexName().equals(indexName);
        toChange.setIndexName(null);
        toChange.setIndexType(null, null);
        return cloned;
    }

    private CFMetaData findIndexedCF() throws InvalidRequestException
    {
        KSMetaData ksm = Schema.instance.getKSMetaData(keyspace);
        for (CFMetaData cfm : ksm.cfMetaData().values())
        {
            if (findIndexedColumn(cfm) != null)
                return cfm;
        }
        throw new InvalidRequestException("Index '" + indexName + "' could not be found in any of the column families of keyspace '" + keyspace + "'");
    }

    private ColumnDefinition findIndexedColumn(CFMetaData cfm)
    {
        for (ColumnDefinition column : cfm.regularColumns())
        {
            if (column.getIndexType() != null && column.getIndexName() != null && column.getIndexName().equals(indexName))
                return column;
        }
        return null;
    }
}
