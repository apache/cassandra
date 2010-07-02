package org.apache.cassandra.service;

import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;

public class ExampleColumnValidator implements ColumnValidator
{
    @Override
    public void validate(String keyspace, ColumnParent column_parent, Column column)
    {
        if (column.value.length % 2 == 0)
            throw new MarshalException("column.value.length is even");
    }
}
