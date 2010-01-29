package org.apache.cassandra.avro;

import java.nio.ByteBuffer;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.util.Utf8;

class RecordFactory
{
    static Column newColumn(ByteBuffer name, ByteBuffer value, long timestamp)
    {
        Column column = new Column();
        column.name = name;
        column.value = value;
        column.timestamp = timestamp;
        return column;
    }
    
    static Column newColumn(byte[] name, byte[] value, long timestamp)
    {
        return newColumn(ByteBuffer.wrap(name), ByteBuffer.wrap(value), timestamp);
    }
    
    static SuperColumn newSuperColumn(ByteBuffer name, GenericArray<Column> columns)
    {
        SuperColumn column = new SuperColumn();
        column.name = name;
        column.columns = columns;
        return column;
    }
    
    static SuperColumn newSuperColumn(byte[] name, GenericArray<Column> columns)
    {
        return newSuperColumn(ByteBuffer.wrap(name), columns);
    }
    
    static ColumnOrSuperColumn newColumnOrSuperColumn(Column column)
    {
        ColumnOrSuperColumn col = new ColumnOrSuperColumn();
        col.column = column;
        return col;
    }
    
    static ColumnOrSuperColumn newColumnOrSuperColumn(SuperColumn superColumn)
    {
        ColumnOrSuperColumn column = new ColumnOrSuperColumn();
        column.super_column = superColumn;
        return column;
    }

    static ColumnPath newColumnPath(String cfName, ByteBuffer superColumn, ByteBuffer column)
    {
        ColumnPath cPath = new ColumnPath();
        cPath.super_column = superColumn;
        cPath.column = column;
        return cPath;
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
}