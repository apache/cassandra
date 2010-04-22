package org.apache.cassandra.config;

public class Keyspace {
    public String name;
    public String replica_placement_strategy;
    public Integer replication_factor;
    public ColumnFamily[] column_families;
}
