package org.apache.cassandra.db;
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


import java.io.*;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.utils.FBUtilities;

public class ColumnSerializer implements ICompactSerializer2<IColumn>
{
    public static void writeName(byte[] name, DataOutput out) throws IOException
    {
        int length = name.length;
        assert length <= 65535;
        out.writeByte((length >> 8) & 0xFF);
        out.writeByte(length & 0xFF);
        out.write(name);
    }

    public static byte[] readName(DataInput in) throws IOException
    {
        int length = 0;
        length |= (in.readByte() << 8);
        length |= in.readByte();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    public void serialize(IColumn column, DataOutput dos) throws IOException
    {
        ColumnSerializer.writeName(column.name(), dos);
        dos.writeBoolean(column.isMarkedForDelete());
        dos.writeLong(column.timestamp());
        FBUtilities.writeByteArray(column.value(), dos);
    }

    public Column deserialize(DataInput dis) throws IOException
    {
        byte[] name = ColumnSerializer.readName(dis);
        boolean delete = dis.readBoolean();
        long ts = dis.readLong();
        byte[] value = FBUtilities.readByteArray(dis);
        return new Column(name, value, ts, delete);
    }
}
