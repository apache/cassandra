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

public class RowReadCommand extends ReadCommand
{
    public RowReadCommand(String table, String key)
    {
        super(table, key, CMD_TYPE_GET_ROW);
    }

    @Override
    public String getColumnFamilyName()
    {
        return null;
    }

    @Override
    public ReadCommand copy()
    {
        ReadCommand readCommand= new RowReadCommand(table, key);
        readCommand.setDigestQuery(isDigestQuery());
        return readCommand;
    }

    @Override
    public Row getRow(Table table) throws IOException    
    {
        return table.get(key);
    }

    @Override
    public String toString()
    {
        return "GetColumnReadMessage(" +
               "table='" + table + '\'' +
               ", key='" + key + '\'' +
               ')';
    }

}

class RowReadCommandSerializer extends ReadCommandSerializer
{
    @Override
    public void serialize(ReadCommand rm, DataOutputStream dos) throws IOException
    { 
        RowReadCommand realRM = (RowReadCommand)rm;
        dos.writeBoolean(realRM.isDigestQuery());
        dos.writeUTF(realRM.table);
        dos.writeUTF(realRM.key);
    }

    @Override
    public ReadCommand deserialize(DataInputStream dis) throws IOException
    {
        boolean isDigest = dis.readBoolean();
        String table = dis.readUTF();
        String key = dis.readUTF();
        RowReadCommand rm = new RowReadCommand(table, key);
        rm.setDigestQuery(isDigest);
        return rm;
    }
}
