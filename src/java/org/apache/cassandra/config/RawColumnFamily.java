package org.apache.cassandra.config;
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


import java.util.Collections;
import java.util.Map;

import org.apache.cassandra.db.ClockType;
import org.apache.cassandra.db.ColumnFamilyType;

/**
 * @deprecated Yaml configuration for Keyspaces and ColumnFamilies is deprecated in 0.7
 */
public class RawColumnFamily
{
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
    public int gc_grace_seconds = CFMetaData.DEFAULT_GC_GRACE_SECONDS;
    public String default_validation_class;
    public int min_compaction_threshold = CFMetaData.DEFAULT_MIN_COMPACTION_THRESHOLD;
    public int max_compaction_threshold = CFMetaData.DEFAULT_MAX_COMPACTION_THRESHOLD;
    public RawColumnDefinition[] column_metadata = new RawColumnDefinition[0];
}
