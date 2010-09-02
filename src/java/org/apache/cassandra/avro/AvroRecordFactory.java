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
import java.util.Map;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.util.Utf8;

public class AvroRecordFactory
{
    public static Column newColumn(ByteBuffer name, ByteBuffer value, Clock clock)
    {
        Column column = new Column();
        column.name = name;
        column.value = value;
        column.clock = clock;
        return column;
    }

    public static Clock newClock(long timestamp)
    {
        Clock clock = new Clock();
        clock.timestamp = timestamp;
        return clock;
    }
    
    public static Column newColumn(byte[] name, byte[] value, Clock clock)
    {
        return newColumn(ByteBuffer.wrap(name), ByteBuffer.wrap(value), clock);
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

    public static KeySlice newKeySlice(byte[] key, List<ColumnOrSuperColumn> columns) {
        KeySlice slice = new KeySlice();
        ByteBuffer wrappedKey = (key != null) ? ByteBuffer.wrap(key) : null;
        slice.key = wrappedKey;
        slice.columns = columns;
        return slice;
    }

}

class ErrorFactory
{
    static InvalidRequestException newInvalidRequestException(Utf8 why)
    {
        InvalidRequestException exception = new InvalidRequestException();
        exception.why = why;
        return exception;
    }
    
    static InvalidRequestException newInvalidRequestException(String why)
    {
        return newInvalidRequestException(new Utf8(why));
    }

    static InvalidRequestException newInvalidRequestException(Throwable e)
    {
        InvalidRequestException exception = newInvalidRequestException(e.getMessage());
        exception.initCause(e);
        return exception;
    }
    
    static NotFoundException newNotFoundException(Utf8 why)
    {
        NotFoundException exception = new NotFoundException();
        exception.why = why;
        return exception;
    }
    
    static NotFoundException newNotFoundException(String why)
    {
        return newNotFoundException(new Utf8(why));
    }
    
    static NotFoundException newNotFoundException()
    {
        return newNotFoundException(new Utf8());
    }
    
    static TimedOutException newTimedOutException(Utf8 why)
    {
        TimedOutException exception = new TimedOutException();
        exception.why = why;
        return exception;
    }
    
    static TimedOutException newTimedOutException(String why)
    {
        return newTimedOutException(new Utf8(why));
    }

    static TimedOutException newTimedOutException()
    {
        return newTimedOutException(new Utf8());
    }
    
    static UnavailableException newUnavailableException(Utf8 why)
    {
        UnavailableException exception = new UnavailableException();
        exception.why = why;
        return exception;
    }
    
    static UnavailableException newUnavailableException(String why)
    {
        return newUnavailableException(new Utf8(why));
    }

    static UnavailableException newUnavailableException(Throwable t) 
    {
        UnavailableException exception = newUnavailableException(t.getMessage());
        exception.initCause(t);
        return exception;
    }
    
    static UnavailableException newUnavailableException()
    {
        return newUnavailableException(new Utf8());
    }
    
    public static TokenRange newTokenRange(String startRange, String endRange, List<? extends CharSequence> endpoints)
    {
        TokenRange tRange = new TokenRange();
        tRange.start_token = startRange;
        tRange.end_token = endRange;
        tRange.endpoints = (List<CharSequence>) endpoints;
        return tRange;
    }
}
