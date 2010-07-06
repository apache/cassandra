package org.apache.cassandra.config;

import org.apache.cassandra.thrift.IndexType;

public class RawColumnDefinition
{
    public String name;
    public String validator_class;
    public IndexType index_type;
    public String index_name;
}
