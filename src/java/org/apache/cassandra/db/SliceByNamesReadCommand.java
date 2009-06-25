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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class SliceByNamesReadCommand extends ReadCommand
{
    public final String columnFamily;
    public final List<String> columnNames;

    public SliceByNamesReadCommand(String table, String key, String columnFamily, List<String> columnNames)
    {
        super(table, key, CMD_TYPE_GET_SLICE_BY_NAMES);
        this.columnFamily = columnFamily;
        this.columnNames = Collections.unmodifiableList(columnNames);
    }

    @Override
    public String getColumnFamilyName()
    {
        return columnFamily;
    }

    @Override
    public ReadCommand copy()
    {
        ReadCommand readCommand= new SliceByNamesReadCommand(table, key, columnFamily, columnNames);
        readCommand.setDigestQuery(isDigestQuery());
        return readCommand;
    }
    
    @Override
    public Row getRow(Table table) throws IOException
    {        
        return table.getRow(key, columnFamily, columnNames);
    }

    @Override
    public String toString()
    {
        return "SliceByNamesReadCommand(" +
               "table='" + table + '\'' +
               ", key='" + key + '\'' +
               ", columnFamily='" + columnFamily + '\'' +
               ", columns=[" + StringUtils.join(columnNames, ", ") + "]" +
               ')';
    }

}

class SliceByNamesReadCommandSerializer extends ReadCommandSerializer
{
    @Override
    public void serialize(ReadCommand rm, DataOutputStream dos) throws IOException
    {
        SliceByNamesReadCommand realRM = (SliceByNamesReadCommand)rm;
        dos.writeBoolean(realRM.isDigestQuery());
        dos.writeUTF(realRM.table);
        dos.writeUTF(realRM.key);
        dos.writeUTF(realRM.columnFamily);
        dos.writeInt(realRM.columnNames.size());
        if (realRM.columnNames.size() > 0)
        {
            for (String cName : realRM.columnNames)
            {
                dos.writeInt(cName.getBytes().length);
                dos.write(cName.getBytes());
            }
        }
    }

    @Override
    public ReadCommand deserialize(DataInputStream dis) throws IOException
    {
        boolean isDigest = dis.readBoolean();
        String table = dis.readUTF();
        String key = dis.readUTF();
        String columnFamily = dis.readUTF();

        int size = dis.readInt();
        List<String> columns = new ArrayList<String>();
        for (int i = 0; i < size; ++i)
        {
            byte[] bytes = new byte[dis.readInt()];
            dis.readFully(bytes);
            columns.add(new String(bytes));
        }
        SliceByNamesReadCommand rm = new SliceByNamesReadCommand(table, key, columnFamily, columns);
        rm.setDigestQuery(isDigest);
        return rm;
    }
}
