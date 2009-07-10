/**
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

package org.apache.cassandra.db;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ColumnReadCommand extends ReadCommand
{
    public final String columnFamilyColumn;

    public ColumnReadCommand(String table, String key, String columnFamilyColumn)
    {
        super(table, key, CMD_TYPE_GET_COLUMN);
        this.columnFamilyColumn = columnFamilyColumn;
    }

    @Override
    public String getColumnFamilyName()
    {
        String[] values = RowMutation.getColumnAndColumnFamily(columnFamilyColumn);
        return values[0];
    }

    @Override
    public ReadCommand copy()
    {
        ReadCommand readCommand= new ColumnReadCommand(table, key, columnFamilyColumn);
        readCommand.setDigestQuery(isDigestQuery());
        return readCommand;
    }

    @Override
    public Row getRow(Table table) throws IOException    
    {
        return table.getRow(key, columnFamilyColumn);
    }

    @Override
    public String toString()
    {
        return "GetColumnReadMessage(" +
               "table='" + table + '\'' +
               ", key='" + key + '\'' +
               ", columnFamilyColumn='" + columnFamilyColumn + '\'' +
               ')';
    }
}

class ColumnReadCommandSerializer extends ReadCommandSerializer
{
    @Override
    public void serialize(ReadCommand rm, DataOutputStream dos) throws IOException
    { 
        ColumnReadCommand realRM = (ColumnReadCommand)rm;
        dos.writeBoolean(realRM.isDigestQuery());
        dos.writeUTF(realRM.table);
        dos.writeUTF(realRM.key);
        dos.writeUTF(realRM.columnFamilyColumn);
    }

    @Override
    public ReadCommand deserialize(DataInputStream dis) throws IOException
    {
        boolean isDigest = dis.readBoolean();
        String table = dis.readUTF();
        String key = dis.readUTF();
        String columnFamily_column = dis.readUTF();
        ColumnReadCommand rm = new ColumnReadCommand(table, key, columnFamily_column);
        rm.setDigestQuery(isDigest);
        return rm;
    }
}
