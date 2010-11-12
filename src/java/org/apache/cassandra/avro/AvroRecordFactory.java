package org.apache.cassandra.avro;
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


import java.nio.ByteBuffer;
import java.util.List;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.util.Utf8;

public class AvroRecordFactory
{
    public static Column newColumn(ByteBuffer name, ByteBuffer value, long timestamp)
    {
        Column column = new Column();
        column.name = name;
        column.value = value;
        column.timestamp = timestamp;
        return column;
    }

    public static Column newColumn(byte[] name, byte[] value, long timestamp)
    {
        return newColumn(ByteBuffer.wrap(name), ByteBuffer.wrap(value), timestamp);
    }
    
    public static SuperColumn newSuperColumn(ByteBuffer name, List<Column> columns)
    {
        SuperColumn column = new SuperColumn();
        column.name = name;
        column.columns = columns;
        return column;
    }
    
    public static SuperColumn newSuperColumn(byte[] name, List<Column> columns)
    {
        return newSuperColumn(ByteBuffer.wrap(name), columns);
    }
    
    public static ColumnOrSuperColumn newColumnOrSuperColumn(Column column)
    {
        ColumnOrSuperColumn col = new ColumnOrSuperColumn();
        col.column = column;
        return col;
    }
    
    public static ColumnOrSuperColumn newColumnOrSuperColumn(SuperColumn superColumn)
    {
        ColumnOrSuperColumn column = new ColumnOrSuperColumn();
        column.super_column = superColumn;
        return column;
    }

    public static ColumnPath newColumnPath(String cfName, ByteBuffer superColumn, ByteBuffer column)
    {
        ColumnPath cPath = new ColumnPath();
        cPath.column_family = new Utf8(cfName);
        cPath.super_column = superColumn;
        cPath.column = column;
        return cPath;
    }

    public static ColumnPath newColumnPath(String cfName, byte[] superColumn, byte[] column)
    {
        ByteBuffer wrappedSuperColumn = (superColumn != null) ? ByteBuffer.wrap(superColumn) : null;
        ByteBuffer wrappedColumn = (column != null) ? ByteBuffer.wrap(column) : null;
        return newColumnPath(cfName, wrappedSuperColumn, wrappedColumn);
    }

    public static ColumnParent newColumnParent(String cfName, byte[] superColumn)
    {
        ColumnParent cp = new ColumnParent();
        cp.column_family = new Utf8(cfName);
        if (superColumn != null)
            cp.super_column = ByteBuffer.wrap(superColumn);
        return cp;
    }
    
    public static CoscsMapEntry newCoscsMapEntry(ByteBuffer key, GenericArray<ColumnOrSuperColumn> columns)
    {
        CoscsMapEntry entry = new CoscsMapEntry();
        entry.key = key;
        entry.columns = columns;
        return entry;
    }

    public static KeySlice newKeySlice(ByteBuffer key, List<ColumnOrSuperColumn> columns) {
        KeySlice slice = new KeySlice();
        slice.key = key;
        slice.columns = columns;
        return slice;
    }

}