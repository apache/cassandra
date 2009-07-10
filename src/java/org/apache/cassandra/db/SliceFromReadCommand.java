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

public class SliceFromReadCommand extends ReadCommand
{
    public final String columnFamilyColumn;
    public final String start, finish;
    public final boolean isAscending;
    public final int offset;
    public final int count;

    public SliceFromReadCommand(String table, String key, String columnFamilyColumn, String start, String finish, boolean isAscending, int offset, int count)
    {
        super(table, key, CMD_TYPE_GET_SLICE);
        this.columnFamilyColumn = columnFamilyColumn;
        this.start = start;
        this.finish = finish;
        this.isAscending = isAscending;
        this.offset = offset;
        this.count = count;
    }

    @Override
    public String getColumnFamilyName()
    {
        return RowMutation.getColumnAndColumnFamily(columnFamilyColumn)[0];
    }

    @Override
    public ReadCommand copy()
    {
        ReadCommand readCommand = new SliceFromReadCommand(table, key, columnFamilyColumn, start, finish, isAscending, offset, count);
        readCommand.setDigestQuery(isDigestQuery());
        return readCommand;
    }

    @Override
    public Row getRow(Table table) throws IOException
    {
        return table.getRow(key, columnFamilyColumn, start, finish, isAscending, offset, count);
    }

    @Override
    public String toString()
    {
        return "SliceFromReadCommand(" +
               "table='" + table + '\'' +
               ", key='" + key + '\'' +
               ", columnFamily='" + columnFamilyColumn + '\'' +
               ", start='" + start + '\'' +
               ", finish='" + finish + '\'' +
               ", isAscending=" + isAscending +
               ", offset=" + offset +
               ", count=" + count +
               ')';
    }
}

class SliceFromReadCommandSerializer extends ReadCommandSerializer
{
    @Override
    public void serialize(ReadCommand rm, DataOutputStream dos) throws IOException
    {
        SliceFromReadCommand realRM = (SliceFromReadCommand)rm;
        dos.writeBoolean(realRM.isDigestQuery());
        dos.writeUTF(realRM.table);
        dos.writeUTF(realRM.key);
        dos.writeUTF(realRM.columnFamilyColumn);
        dos.writeUTF(realRM.start);
        dos.writeUTF(realRM.finish);
        dos.writeBoolean(realRM.isAscending);
        dos.writeInt(realRM.offset);
        dos.writeInt(realRM.count);
    }

    @Override
    public ReadCommand deserialize(DataInputStream dis) throws IOException
    {
        boolean isDigest = dis.readBoolean();
        SliceFromReadCommand rm = new SliceFromReadCommand(dis.readUTF(), dis.readUTF(), dis.readUTF(), dis.readUTF(), dis.readUTF(), dis.readBoolean(), dis.readInt(), dis.readInt());
        rm.setDigestQuery(isDigest);
        return rm;
    }
}
