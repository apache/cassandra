/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.cql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.migration.avro.CfDef;
import org.apache.cassandra.db.migration.avro.ColumnDef;
import org.apache.cassandra.db.migration.UpdateColumnFamily;
import org.apache.cassandra.thrift.InvalidRequestException;

public class DropIndexStatement
{
    public final String index;

    public DropIndexStatement(String indexName)
    {
        index = indexName;
    }

    public UpdateColumnFamily generateMutation(String keyspace)
    throws InvalidRequestException, ConfigurationException, IOException
    {
        CfDef cfDef = null;

        KSMetaData ksm = DatabaseDescriptor.getTableDefinition(keyspace);

        for (Map.Entry<String, CFMetaData> cf : ksm.cfMetaData().entrySet())
        {
            CFMetaData cfm = cf.getValue();

            cfDef = getUpdatedCFDef(CFMetaData.convertToAvro(cfm));

            if (cfDef != null)
                break;
        }

        if (cfDef == null)
            throw new InvalidRequestException("Index '" + index + "' could not be found in any of the ColumnFamilies of keyspace '" + keyspace + "'");

        return new UpdateColumnFamily(cfDef);
    }

    private CfDef getUpdatedCFDef(CfDef cfDef) throws InvalidRequestException
    {

        boolean foundColumn = false;

        for (ColumnDef column : cfDef.column_metadata)
        {
            if (column.index_type != null && column.index_name != null && column.index_name.equals(index))
            {
                foundColumn = true;

                column.index_name = null;
                column.index_type = null;
            }
        }

        return (foundColumn) ? cfDef : null;
    }
}
