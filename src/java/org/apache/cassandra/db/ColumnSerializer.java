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

import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.utils.FBUtilities;

import java.nio.ByteBuffer;

public class ColumnSerializer implements ICompactSerializer2<IColumn>
{
    public final static int DELETION_MASK = 0x01;
    public final static int EXPIRATION_MASK = 0x02;

    public void serialize(IColumn column, DataOutput dos)
    {
        FBUtilities.writeShortByteArray(column.name(), dos);
        try
        {
            if (column instanceof ExpiringColumn) {
              dos.writeByte(EXPIRATION_MASK);
              dos.writeInt(((ExpiringColumn) column).getTimeToLive());
              dos.writeInt(column.getLocalDeletionTime());
            } else {
              dos.writeByte((column.isMarkedForDelete()) ? DELETION_MASK : 0);
            }
            dos.writeLong(column.timestamp());
            FBUtilities.writeByteArray(column.value(), dos);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public Column deserialize(DataInput dis) throws IOException
    {
        byte[] name = FBUtilities.readShortByteArray(dis);
        int b = dis.readUnsignedByte();
        if (FBUtilities.testBitUsingBitMask(b, EXPIRATION_MASK))
        {
            int ttl = dis.readInt();
            int expiration = dis.readInt();
            long ts = dis.readLong();
            byte[] value = FBUtilities.readByteArray(dis);
            if ((int) (System.currentTimeMillis() / 1000 ) > expiration)
            {
                // the column is now expired, we can safely return a simple
                // tombstone
                ByteBuffer bytes = ByteBuffer.allocate(4);
                bytes.putInt(expiration);
                return new DeletedColumn(name, bytes.array(), ts);
            }
            else
            {
                return new ExpiringColumn(name, value, ts, ttl, expiration);
            }
        }
        else
        {
            boolean delete = FBUtilities.testBitUsingBitMask(b, DELETION_MASK);
            long ts = dis.readLong();
            byte[] value = FBUtilities.readByteArray(dis);
            if (FBUtilities.testBitUsingBitMask(b, DELETION_MASK)) {
                return new DeletedColumn(name, value, ts);
            } else {
                return new Column(name, value, ts);
            }
        }
    }
}
