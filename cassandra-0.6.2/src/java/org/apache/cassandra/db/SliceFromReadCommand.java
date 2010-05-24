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

import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.thrift.ColumnParent;

public class SliceFromReadCommand extends ReadCommand
{
    public final byte[] start, finish;
    public final boolean reversed;
    public final int count;

    public SliceFromReadCommand(String table, String key, ColumnParent column_parent, byte[] start, byte[] finish, boolean reversed, int count)
    {
        this(table, key, new QueryPath(column_parent), start, finish, reversed, count);
    }

    public SliceFromReadCommand(String table, String key, QueryPath path, byte[] start, byte[] finish, boolean reversed, int count)
    {
        super(table, key, path, CMD_TYPE_GET_SLICE);
        this.start = start;
        this.finish = finish;
        this.reversed = reversed;
        this.count = count;
    }

    @Override
    public ReadCommand copy()
    {
        ReadCommand readCommand = new SliceFromReadCommand(table, key, queryPath, start, finish, reversed, count);
        readCommand.setDigestQuery(isDigestQuery());
        return readCommand;
    }

    @Override
    public Row getRow(Table table) throws IOException
    {
        return table.getRow(new SliceQueryFilter(key, queryPath, start, finish, reversed, count));
    }

    @Override
    public String toString()
    {
        return "SliceFromReadCommand(" +
               "table='" + table + '\'' +
               ", key='" + key + '\'' +
               ", column_parent='" + queryPath + '\'' +
               ", start='" + getComparator().getString(start) + '\'' +
               ", finish='" + getComparator().getString(finish) + '\'' +
               ", reversed=" + reversed +
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
        realRM.queryPath.serialize(dos);
        ColumnSerializer.writeName(realRM.start, dos);
        ColumnSerializer.writeName(realRM.finish, dos);
        dos.writeBoolean(realRM.reversed);
        dos.writeInt(realRM.count);
    }

    @Override
    public ReadCommand deserialize(DataInputStream dis) throws IOException
    {
        boolean isDigest = dis.readBoolean();
        SliceFromReadCommand rm = new SliceFromReadCommand(dis.readUTF(),
                                                           dis.readUTF(),
                                                           QueryPath.deserialize(dis),
                                                           ColumnSerializer.readName(dis),
                                                           ColumnSerializer.readName(dis),
                                                           dis.readBoolean(), 
                                                           dis.readInt());
        rm.setDigestQuery(isDigest);
        return rm;
    }
}
