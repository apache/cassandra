package org.apache.cassandra.db.filter;
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
import java.nio.ByteBuffer;

import org.apache.cassandra.db.DBConstants;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class QueryPath
{
    public final String columnFamilyName;
    public final ByteBuffer superColumnName;
    public final ByteBuffer columnName;

    public QueryPath(String columnFamilyName, ByteBuffer superColumnName, ByteBuffer columnName)
    {
        this.columnFamilyName = columnFamilyName;
        this.superColumnName = superColumnName;
        this.columnName = columnName;
    }

    public QueryPath(ColumnParent columnParent)
    {
        this(columnParent.column_family, columnParent.super_column, null);
    }

    public QueryPath(String columnFamilyName, ByteBuffer superColumnName)
    {
        this(columnFamilyName, superColumnName, null);
    }

    public QueryPath(String columnFamilyName)
    {
        this(columnFamilyName, null);
    }

    public QueryPath(ColumnPath column_path)
    {
        this(column_path.column_family, column_path.super_column, column_path.column);
    }

    public static QueryPath column(ByteBuffer columnName)
    {
        return new QueryPath(null, null, columnName);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" +
               "columnFamilyName='" + columnFamilyName + '\'' +
               ", superColumnName='" + superColumnName + '\'' +
               ", columnName='" + columnName + '\'' +
               ')';
    }

    public void serialize(DataOutput dos) throws IOException
    {
        assert !"".equals(columnFamilyName);
        assert superColumnName == null || superColumnName.remaining() > 0;
        assert columnName == null || columnName.remaining() > 0;
        dos.writeUTF(columnFamilyName == null ? "" : columnFamilyName);
        ByteBufferUtil.writeWithShortLength(superColumnName == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : superColumnName, dos);
        ByteBufferUtil.writeWithShortLength(columnName == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : columnName, dos);
    }

    public static QueryPath deserialize(DataInput din) throws IOException
    {
        String cfName = din.readUTF();
        ByteBuffer scName = ByteBufferUtil.readWithShortLength(din);
        ByteBuffer cName = ByteBufferUtil.readWithShortLength(din);
        return new QueryPath(cfName.isEmpty() ? null : cfName, 
                             scName.remaining() == 0 ? null : scName, 
                             cName.remaining() == 0 ? null : cName);
    }

    public int serializedSize()
    {
        int size = DBConstants.shortSize + (columnFamilyName == null ? 0 : FBUtilities.encodedUTF8Length(columnFamilyName));
        size += DBConstants.shortSize + (superColumnName == null ? 0 : superColumnName.remaining());
        size += DBConstants.shortSize + (columnName == null ? 0 : columnName.remaining());
        return size;
    }
}
