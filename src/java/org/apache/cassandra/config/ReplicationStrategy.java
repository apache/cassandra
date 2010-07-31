package org.apache.cassandra.config;

import java.util.Map;

public class ReplicationStrategy
{
    public String strategy_class;
    public Map<String, String> strategy_options;
}
