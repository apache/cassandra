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
import java.util.List;

import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.utils.ByteArrayListSerializer;
import org.apache.cassandra.utils.FBUtilities;

public class SliceFromReadCommand extends ReadCommand
{
    public final byte[] start, finish;
    public final boolean reversed;
    public final int count;
    public final List<byte[]> bitmasks;

    public SliceFromReadCommand(String table, byte[] key, ColumnParent column_parent, byte[] start, byte[] finish, boolean reversed, int count)
    {
        this(table, key, new QueryPath(column_parent), start, finish, null, reversed, count);
    }

    public SliceFromReadCommand(String table, byte[] key, QueryPath path, byte[] start, byte[] finish, boolean reversed, int count)
    {
        this(table, key, path, start, finish, null, reversed, count);
    }

    public SliceFromReadCommand(String table, byte[] key, QueryPath path, byte[] start, byte[] finish, List<byte[]> bitmasks, boolean reversed, int count)
    {
        super(table, key, path, CMD_TYPE_GET_SLICE);
        this.start = start;
        this.finish = finish;
        this.reversed = reversed;
        this.count = count;
        this.bitmasks = bitmasks;
    }

    @Override
    public ReadCommand copy()
    {
        ReadCommand readCommand = new SliceFromReadCommand(table, key, queryPath, start, finish, bitmasks, reversed, count);
        readCommand.setDigestQuery(isDigestQuery());
        return readCommand;
    }

    @Override
    public Row getRow(Table table) throws IOException
    {
        DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);
        return table.getRow(QueryFilter.getSliceFilter(dk, queryPath, start, finish, bitmasks, reversed, count));
    }

    @Override
    public String toString()
    {
        String bitmaskString = getBitmaskDescription(bitmasks);

        return "SliceFromReadCommand(" +
               "table='" + table + '\'' +
               ", key='" + FBUtilities.bytesToHex(key) + '\'' +
               ", column_parent='" + queryPath + '\'' +
               ", start='" + getComparator().getString(start) + '\'' +
               ", finish='" + getComparator().getString(finish) + '\'' +
               ", bitmasks=" + bitmaskString +
               ", reversed=" + reversed +
               ", count=" + count +
               ')';
    }

    public static String getBitmaskDescription(List<byte[]> masks)
    {
        StringBuffer bitmaskBuf = new StringBuffer("[");

        if (masks != null)
        {
            bitmaskBuf.append(masks.size()).append(" bitmasks: ");
            for (byte[] bitmask: masks)
            {
                for (byte b: bitmask)
                {
                    bitmaskBuf.append(String.format("0x%02x ", b));
                }
                bitmaskBuf.append("; ");
            }
        }
        bitmaskBuf.append("]");
        return bitmaskBuf.toString();
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
        FBUtilities.writeShortByteArray(realRM.key, dos);
        realRM.queryPath.serialize(dos);
        FBUtilities.writeShortByteArray(realRM.start, dos);
        FBUtilities.writeShortByteArray(realRM.finish, dos);
        ByteArrayListSerializer.serialize(realRM.bitmasks, dos);
        dos.writeBoolean(realRM.reversed);
        dos.writeInt(realRM.count);
    }

    @Override
    public ReadCommand deserialize(DataInputStream dis) throws IOException
    {
        boolean isDigest = dis.readBoolean();
        SliceFromReadCommand rm = new SliceFromReadCommand(dis.readUTF(),
                                                           FBUtilities.readShortByteArray(dis),
                                                           QueryPath.deserialize(dis),
                                                           FBUtilities.readShortByteArray(dis),
                                                           FBUtilities.readShortByteArray(dis),
                                                           ByteArrayListSerializer.deserialize(dis),
                                                           dis.readBoolean(), 
                                                           dis.readInt());
        rm.setDigestQuery(isDigest);
        return rm;
    }
}
