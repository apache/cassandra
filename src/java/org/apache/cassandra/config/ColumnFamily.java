package org.apache.cassandra.config;

import org.apache.cassandra.db.ClockType;
import org.apache.cassandra.db.ColumnFamilyType;

public class ColumnFamily {
    public String name;            
    public ColumnFamilyType column_type;
    public ClockType clock_type;
    public String reconciler;
    public String compare_with;
    public String compare_subcolumns_with;
    public String comment;
    public double rows_cached = CFMetaData.DEFAULT_ROW_CACHE_SIZE; 
    public double keys_cached = CFMetaData.DEFAULT_KEY_CACHE_SIZE; 
    public double read_repair_chance = CFMetaData.DEFAULT_READ_REPAIR_CHANCE;
    public boolean preload_row_cache = CFMetaData.DEFAULT_PRELOAD_ROW_CACHE;
}
