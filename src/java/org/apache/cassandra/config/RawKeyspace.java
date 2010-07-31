package org.apache.cassandra.config;

import java.util.Map;

/**
 * @deprecated Yaml configuration for Keyspaces and ColumnFamilies is deprecated in 0.7
 */
public class RawKeyspace
{
    public String name;
    public String replica_placement_strategy;
    public Map<String,String> strategy_options;
    public Integer replication_factor;
    public RawColumnFamily[] column_families;
}
