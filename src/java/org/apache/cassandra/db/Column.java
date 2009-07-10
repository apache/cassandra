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
import java.util.Collection;
import java.nio.ByteBuffer;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.io.ICompactSerializer;


/**
 * Column is immutable, which prevents all kinds of confusion in a multithreaded environment.
 * (TODO: look at making SuperColumn immutable too.  This is trickier but is probably doable
 *  with something like PCollections -- http://code.google.com
 *
 * Author : Avinash Lakshman ( alakshman@facebook.com ) & Prashant Malik ( pmalik@facebook.com )
 */

public final class Column implements IColumn
{
    private static ColumnSerializer serializer_ = new ColumnSerializer();

    static ColumnSerializer serializer()
    {
        return serializer_;
    }

    private final String name;
    private final byte[] value;
    private final long timestamp;
    private final boolean isMarkedForDelete;

    Column(String name)
    {
        this(name, ArrayUtils.EMPTY_BYTE_ARRAY);
    }

    Column(String name, byte[] value)
    {
        this(name, value, 0);
    }

    Column(String name, byte[] value, long timestamp)
    {
        this(name, value, timestamp, false);
    }

    Column(String name, byte[] value, long timestamp, boolean isDeleted)
    {
        assert name != null;
        assert value != null;
        this.name = name;
        this.value = value;
        this.timestamp = timestamp;
        isMarkedForDelete = isDeleted;
    }

    public String name()
    {
        return name;
    }

    public IColumn getSubColumn(String columnName)
    {
        throw new UnsupportedOperationException("This operation is unsupported on simple columns.");
    }

    public byte[] value()
    {
        return value;
    }

    public byte[] value(String key)
    {
        throw new UnsupportedOperationException("This operation is unsupported on simple columns.");
    }

    public Collection<IColumn> getSubColumns()
    {
        throw new UnsupportedOperationException("This operation is unsupported on simple columns.");
    }

    public int getObjectCount()
    {
        return 1;
    }

    public long timestamp()
    {
        return timestamp;
    }

    public long timestamp(String key)
    {
        throw new UnsupportedOperationException("This operation is unsupported on simple columns.");
    }

    public boolean isMarkedForDelete()
    {
        return isMarkedForDelete;
    }

    public long getMarkedForDeleteAt()
    {
        if (!isMarkedForDelete())
        {
            throw new IllegalStateException("column is not marked for delete");
        }
        return timestamp;
    }

    public int size()
    {
        /*
         * Size of a column is =
         *   size of a name (UtfPrefix + length of the string)
         * + 1 byte to indicate if the column has been deleted
         * + 8 bytes for timestamp
         * + 4 bytes which basically indicates the size of the byte array
         * + entire byte array.
        */

        /*
           * We store the string as UTF-8 encoded, so when we calculate the length, it
           * should be converted to UTF-8.
           */
        return IColumn.UtfPrefix_ + FBUtilities.getUTF8Length(name) + DBConstants.boolSize_ + DBConstants.tsSize_ + DBConstants.intSize_ + value.length;
    }

    /*
     * This returns the size of the column when serialized.
     * @see com.facebook.infrastructure.db.IColumn#serializedSize()
    */
    public int serializedSize()
    {
        return size();
    }

    public void addColumn(IColumn column)
    {
        throw new UnsupportedOperationException("This operation is not supported for simple columns.");
    }

    public IColumn diff(IColumn column)
    {
        if (timestamp() < column.timestamp())
        {
            return column;
        }
        return null;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append(":");
        sb.append(isMarkedForDelete());
        sb.append(":");
        sb.append(value().length);
        sb.append("@");
        sb.append(timestamp());
        return sb.toString();
    }

    public byte[] digest()
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(name);
        stringBuilder.append(":");
        stringBuilder.append(timestamp);
        return stringBuilder.toString().getBytes();
    }

    public int getLocalDeletionTime()
    {
        assert isMarkedForDelete;
        return ByteBuffer.wrap(value).getInt();
    }

    // note that we do not call this simply compareTo since it also makes sense to compare Columns by name
    public long comparePriority(Column o)
    {
        if (isMarkedForDelete)
        {
            // tombstone always wins ties.
            return timestamp < o.timestamp ? -1 : 1;
        }
        return timestamp - o.timestamp;
    }
}

class ColumnSerializer implements ICompactSerializer<IColumn>
{
    public void serialize(IColumn column, DataOutputStream dos) throws IOException
    {
        dos.writeUTF(column.name());
        dos.writeBoolean(column.isMarkedForDelete());
        dos.writeLong(column.timestamp());
        dos.writeInt(column.value().length);
        dos.write(column.value());
    }

    public IColumn deserialize(DataInputStream dis) throws IOException
    {
        String name = dis.readUTF();
        boolean delete = dis.readBoolean();
        long ts = dis.readLong();
        int size = dis.readInt();
        byte[] value = new byte[size];
        dis.readFully(value);
        return new Column(name, value, ts, delete);
    }
}
