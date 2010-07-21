package org.apache.cassandra.config;

/**
 * @deprecated Yaml configuration for Keyspaces and ColumnFamilies is deprecated in 0.7
 */
public class RawKeyspace
{
    public String name;
    public String replica_placement_strategy;
    public Integer replication_factor;
    public RawColumnFamily[] column_families;
}
