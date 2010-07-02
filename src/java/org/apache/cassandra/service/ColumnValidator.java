package org.apache.cassandra.service;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;

public interface ColumnValidator
{
    public void validate(String keyspace, ColumnParent column_parent, Column column);
}
